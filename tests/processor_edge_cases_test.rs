#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use serial_test::serial;
    use sidekiq::{
        Counter, Error, Processor, ProcessorConfig, RedisConnectionManager, RedisPool, Result,
        Scheduled, StatsPublisher, UnitOfWork, WorkFetcher, Worker,
    };

    #[async_trait]
    trait FlushAll {
        async fn flushall(&self);
    }

    #[async_trait]
    impl FlushAll for RedisPool {
        async fn flushall(&self) {
            let mut conn = self.get().await.unwrap();
            let _: String = redis::cmd("FLUSHALL")
                .arg("SYNC")
                .query_async(conn.unnamespaced_borrow_mut())
                .await
                .unwrap();
        }
    }

    async fn new_base_processor(queue: String) -> (Processor, RedisPool) {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        let p = Processor::new(redis.clone(), vec![queue]);

        (p, redis)
    }

    // -----------------------------------------------------------------------
    // Worker not found: job flows through retry middleware (matching Ruby behavior)
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn unregistered_worker_retries_through_middleware() {
        let queue = "unknown_worker_queue".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        // Intentionally do NOT register a worker for "GhostWorker"

        sidekiq::opts()
            .queue(queue)
            .retry(true)
            .perform_async(&redis, "GhostWorker".to_string(), ())
            .await
            .unwrap();

        // Process the job — worker won't be found, error flows through
        // retry middleware which schedules it for retry.
        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        let sched = Scheduled::new(redis.clone());
        let sets = vec!["retry".to_string()];
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        let n = sched.enqueue_jobs(future_date, &sets).await.unwrap();
        assert_eq!(n, 1, "unknown worker job should be retried via middleware");

        // Verify the retried job has the error message
        let mut p2 = Processor::new(redis.clone(), vec!["unknown_worker_queue".to_string()]);
        let work = p2.fetch().await.unwrap().unwrap();
        assert_eq!(work.job.retry_count, Some(1));
        assert!(
            work.job
                .error_message
                .as_ref()
                .unwrap()
                .contains("Worker not found"),
            "error message should indicate worker not found"
        );
    }

    // -----------------------------------------------------------------------
    // Dead job: once max retries exceeded, job is NOT re-enqueued
    // -----------------------------------------------------------------------

    #[derive(Clone)]
    struct AlwaysFailWorker;

    #[async_trait]
    impl Worker<()> for AlwaysFailWorker {
        fn max_retries(&self) -> usize {
            2
        }

        async fn perform(&self, _args: ()) -> Result<()> {
            Err(Error::Message("always fails".to_string()))
        }
    }

    #[tokio::test]
    #[serial]
    async fn job_exceeding_max_retries_is_dead() {
        let queue = "dead_job_queue".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(AlwaysFailWorker);

        let mut job = AlwaysFailWorker::opts()
            .queue(queue)
            .retry(true)
            .into_opts()
            .create_job(AlwaysFailWorker::class_name(), ())
            .expect("creates job");

        // Set retry_count past the worker's max_retries of 2
        job.retry_count = Some(3);

        UnitOfWork::from_job(job)
            .enqueue(&redis)
            .await
            .expect("enqueues");

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        // The job should NOT be in the retry queue
        let sched = Scheduled::new(redis.clone());
        let sets = vec!["retry".to_string()];
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        let n = sched.enqueue_jobs(future_date, &sets).await.unwrap();
        assert_eq!(n, 0, "dead job should not be re-enqueued");

        // The job SHOULD be in the dead set
        let mut conn = redis.get().await.unwrap();
        let dead_jobs: Vec<String> = conn
            .zrange("dead".to_string(), isize::MIN, isize::MAX)
            .await
            .unwrap();
        assert_eq!(dead_jobs.len(), 1, "dead job should be in the dead set");

        let dead_job: serde_json::Value = serde_json::from_str(&dead_jobs[0]).unwrap();
        assert_eq!(dead_job["class"], "AlwaysFailWorker");
        assert!(
            dead_job["error_message"].as_str().is_some(),
            "dead job should have an error message"
        );
    }

    // -----------------------------------------------------------------------
    // retry(false) on first attempt: job is never retried even on first failure
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn retry_never_drops_job_on_first_failure() {
        let queue = "never_retry_queue".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(AlwaysFailWorker);

        AlwaysFailWorker::opts()
            .queue(queue)
            .retry(false)
            .perform_async(&redis, ())
            .await
            .expect("enqueues");

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        let sched = Scheduled::new(redis.clone());
        let sets = vec!["retry".to_string()];
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        let n = sched.enqueue_jobs(future_date, &sets).await.unwrap();
        assert_eq!(n, 0, "retry=false should never enqueue a retry");

        // retry=false jobs should still go to the dead set
        let mut conn = redis.get().await.unwrap();
        let dead_jobs: Vec<String> = conn
            .zrange("dead".to_string(), isize::MIN, isize::MAX)
            .await
            .unwrap();
        assert_eq!(dead_jobs.len(), 1, "retry=false job should be in the dead set");
    }

    // -----------------------------------------------------------------------
    // Stats publisher writes expected keys to Redis
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn stats_publisher_writes_process_info() {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        let queues = vec!["default".to_string(), "critical".to_string()];
        let busy = Counter::new(0);
        let publisher =
            StatsPublisher::new("test-host".to_string(), queues.clone(), busy.clone(), 4);

        publisher.publish_stats(redis.clone()).await.unwrap();

        // Verify the process was added to the "processes" set
        let mut conn = redis.get().await.unwrap();
        let processes: Vec<String> = redis::cmd("SMEMBERS")
            .arg("processes")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();

        assert_eq!(processes.len(), 1, "one process should be registered");
        assert!(
            processes[0].starts_with("test-host:"),
            "identity should start with hostname"
        );

        // Verify the hash has the expected fields
        let identity = &processes[0];
        let info: String = redis::cmd("HGET")
            .arg(identity)
            .arg("info")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();

        let info: serde_json::Value = serde_json::from_str(&info).unwrap();
        assert_eq!(info["hostname"], "test-host");
        assert_eq!(info["concurrency"], 4);
        assert_eq!(info["queues"], serde_json::json!(["default", "critical"]));

        let busy_val: usize = redis::cmd("HGET")
            .arg(identity)
            .arg("busy")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
        assert_eq!(busy_val, 0);

        // Verify TTL was set
        let ttl: i64 = redis::cmd("TTL")
            .arg(identity)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
        assert!(ttl > 0 && ttl <= 30, "TTL should be set to ~30s, got {ttl}");
    }

    // -----------------------------------------------------------------------
    // Stats publisher tracks busy count
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn stats_publisher_reflects_busy_count() {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        let busy = Counter::new(0);
        busy.incrby(7);

        let publisher = StatsPublisher::new(
            "busy-host".to_string(),
            vec!["default".to_string()],
            busy.clone(),
            10,
        );

        publisher.publish_stats(redis.clone()).await.unwrap();

        let mut conn = redis.get().await.unwrap();
        let processes: Vec<String> = redis::cmd("SMEMBERS")
            .arg("processes")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();

        let identity = &processes[0];
        let busy_val: usize = redis::cmd("HGET")
            .arg(identity)
            .arg("busy")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();

        assert_eq!(busy_val, 7, "busy count should reflect counter value");
    }

    // -----------------------------------------------------------------------
    // Balance strategy: RoundRobin rotates queue priority
    // -----------------------------------------------------------------------

    #[derive(Clone)]
    struct TrackingWorker;

    #[async_trait]
    impl Worker<()> for TrackingWorker {
        async fn perform(&self, _args: ()) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    #[serial]
    async fn round_robin_rotates_queue_fetch_order() {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        let queues = vec!["alpha".to_string(), "beta".to_string()];
        let mut p = Processor::new(redis.clone(), queues)
            .with_config(ProcessorConfig::default().num_workers(1));

        p.register(TrackingWorker);

        // Enqueue a job in each queue
        TrackingWorker::opts()
            .queue("alpha")
            .perform_async(&redis, ())
            .await
            .unwrap();
        TrackingWorker::opts()
            .queue("beta")
            .perform_async(&redis, ())
            .await
            .unwrap();

        // First fetch — due to rotation, one queue is checked first
        let work1 = p.fetch().await.unwrap();
        assert!(work1.is_some(), "should find work in first fetch");
        let q1 = work1.unwrap().job.queue.clone();

        // Second fetch — rotation should let us find the other queue's job
        let work2 = p.fetch().await.unwrap();
        assert!(work2.is_some(), "should find work in second fetch");
        let q2 = work2.unwrap().job.queue.clone();

        // Both queues should have been served
        assert_ne!(
            q1, q2,
            "round robin should serve both queues, got {q1} and {q2}"
        );
    }

    // -----------------------------------------------------------------------
    // Graceful shutdown: cancellation token stops process_one
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn cancellation_token_stops_processing() {
        let queue = "cancel_queue".to_string();
        let (p, _redis) = new_base_processor(queue).await;

        let token = p.get_cancellation_token();
        let mut p_clone = p.clone();

        // Spawn process_one in a task — it will block waiting for work
        let handle = tokio::spawn(async move { p_clone.process_one().await });

        // Give it a moment to start blocking, then cancel
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        token.cancel();

        // process_one should return promptly after cancellation
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        assert!(
            result.is_ok(),
            "process_one should exit within 5s of cancellation"
        );
        assert!(
            result.unwrap().unwrap().is_ok(),
            "process_one should return Ok on cancellation"
        );
    }

    // -----------------------------------------------------------------------
    // NoWorkFound when queue is empty
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn empty_queue_returns_no_work_found() {
        let queue = "empty_queue".to_string();
        let (mut p, _redis) = new_base_processor(queue).await;

        let result = p.process_one_tick_once().await.unwrap();
        assert_eq!(result, WorkFetcher::NoWorkFound);
    }

    // -----------------------------------------------------------------------
    // Namespace isolation: namespaced keys don't collide with default
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[serial]
    async fn namespaced_jobs_are_isolated() {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let default_redis = Pool::builder().build(manager).await.unwrap();
        default_redis.flushall().await;

        // Create a namespaced pool
        let ns_manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let ns_redis = Pool::builder()
            .connection_customizer(sidekiq::with_custom_namespace("myapp".to_string()))
            .build(ns_manager)
            .await
            .unwrap();

        #[derive(Clone)]
        struct NsWorker;

        #[async_trait]
        impl Worker<()> for NsWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                Ok(())
            }
        }

        // Enqueue into namespaced redis
        NsWorker::opts()
            .queue("nsqueue")
            .perform_async(&ns_redis, ())
            .await
            .unwrap();

        // Default (non-namespaced) processor should NOT see the job
        let mut default_p = Processor::new(default_redis.clone(), vec!["nsqueue".to_string()]);
        default_p.register(NsWorker);

        let result = default_p.process_one_tick_once().await.unwrap();
        assert_eq!(
            result,
            WorkFetcher::NoWorkFound,
            "default processor should not see namespaced jobs"
        );

        // Namespaced processor SHOULD see the job
        let mut ns_p = Processor::new(ns_redis.clone(), vec!["nsqueue".to_string()]);
        ns_p.register(NsWorker);

        let result = ns_p.process_one_tick_once().await.unwrap();
        assert_eq!(
            result,
            WorkFetcher::Done,
            "namespaced processor should find the job"
        );
    }
}
