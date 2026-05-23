#[cfg(test)]
mod test {
    use bb8::Pool;
    use sidekiq::{Processor, ProcessorConfig, RedisConnectionManager, RedisPool};
    use std::time::Duration;

    async fn new_pool() -> RedisPool {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        Pool::builder().build(manager).await.unwrap()
    }

    async fn flushall(redis: &RedisPool) {
        let mut conn = redis.get().await.unwrap();
        let _: String = redis::cmd("FLUSHALL")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    async fn scard(redis: &RedisPool, key: &str) -> i64 {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("SCARD")
            .arg(key)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap_or(0)
    }

    /// Graceful cancellation must remove the process from the `processes` set.
    ///
    /// Waits for the 5-second stats heartbeat to fire naturally, then cancels and
    /// verifies the set is empty. This is an integration test and intentionally slow.
    #[tokio::test]
    async fn graceful_shutdown_removes_process_from_processes_set() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let p = Processor::new(redis.clone(), vec!["default".to_string()])
            .with_config(ProcessorConfig::default().num_workers(1));
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        // The stats loop publishes every 5 s; wait for the first heartbeat.
        tokio::time::sleep(Duration::from_secs(6)).await;

        assert_eq!(
            scard(&redis, "processes").await,
            1,
            "process should be registered in set after first heartbeat"
        );

        token.cancel();
        handle.await.unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "processes set must be empty after graceful shutdown"
        );
    }

    /// Cancellation before the first heartbeat fires must leave the set empty.
    /// deregister() is a no-op when nothing was published (SREM on a missing member is safe).
    #[tokio::test]
    async fn early_shutdown_leaves_processes_set_empty() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let p = Processor::new(redis.clone(), vec!["default".to_string()]);
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        token.cancel();
        handle.await.unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "processes set must be empty when cancelled before first heartbeat"
        );
    }
}
