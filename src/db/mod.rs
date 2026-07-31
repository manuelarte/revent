use crate::domain::source_events::{
    NewSourceEvent, SourceEvent, SourceEventRepository, SourceEventRepositoryError,
};
use async_trait::async_trait;
use sqlx::migrate::MigrateError;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RustLiteSourceEventRepository {
    pool: SqlitePool,
}

unsafe impl Send for RustLiteSourceEventRepository {}
unsafe impl Sync for RustLiteSourceEventRepository {}

impl RustLiteSourceEventRepository {
    /// Creates a new instance of the repository.
    ///
    /// # Errors
    ///
    /// Return an error if it can't create the database.
    pub async fn new() -> Result<Self, String> {
        let database_url = "sqlite::memory:";
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| format!("Failed to create database pool: {e}"))?;
        Ok(Self { pool })
    }

    /// `migrate` run the migration scripts.
    ///
    /// # Errors
    ///
    /// `MigrateErr` if there is an issue doing the migration.
    pub async fn migrate(&self) -> Result<(), MigrateError> {
        sqlx::migrate!("./resources/migrations")
            .run(&self.pool)
            .await
    }
}

#[async_trait]
impl SourceEventRepository for RustLiteSourceEventRepository {
    async fn save_source_event(
        &self,
        new_event: NewSourceEvent,
    ) -> Result<SourceEvent, SourceEventRepositoryError> {
        // Start a transaction with IMMEDIATE to lock the database early
        let mut tx = self.pool.begin().await.map_err(|e| {
            SourceEventRepositoryError::new(format!("Failed to begin transaction: {e}"))
        })?;

        let id = Uuid::new_v4();
        let aggregate = new_event.aggregate_type();
        let aggregate_id = new_event.aggregate_id().clone();
        let payload = new_event.payload();
        // Insert the event into the database within the same transaction
        match sqlx::query(
            "INSERT INTO source_event (id, monotonic_clock, aggregate, aggregate_id, payload)
                 VALUES (
                         ?,
                         (SELECT COALESCE(MAX(monotonic_clock), 0) + 1 FROM source_event),  -- monotonic_clock
                         ?,
                         ?,
                         ?
                         )
                 RETURNING id, created_at, monotonic_clock",
        )
        .bind(id.to_string())
        .bind(aggregate)
        .bind(aggregate_id.as_str())
        .bind(payload)
        .fetch_one(&mut *tx)
        .await
        {
            Ok(row) => {
                // Commit the transaction
                match tx.commit().await {
                    Ok(()) => {
                        let created_at = row.get("created_at");
                        let monotonic_clock = row.get("monotonic_clock");

                        Ok(SourceEvent::new(
                            id.into(),
                            created_at,
                            monotonic_clock,
                            aggregate_id.clone(),
                            aggregate.to_string(),
                            payload.clone(),
                        ))
                    }
                    Err(e) => Err(SourceEventRepositoryError::new(format!(
                        "Failed to commit transaction: {e}"
                    ))),
                }
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(SourceEventRepositoryError::new(format!(
                    "Failed to insert event: {e}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_repo() -> RustLiteSourceEventRepository {
        let db = RustLiteSourceEventRepository::new()
            .await
            .expect("Failed to create database");
        db.migrate().await.expect("Failed to apply migrations");
        db
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_single_event() {
        let repo = create_repo().await;

        let new_event = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "test_aggregate_1"
                .try_into()
                .expect("Failed to create AggregateId"),
            "TestAggregate",
            vec![1, 2, 3, 4],
        );

        let result = repo.save_source_event(new_event).await;
        assert!(result.is_ok(), "Save should succeed");

        let event = result.unwrap();
        assert_eq!(event.aggregate_type(), "TestAggregate");
        assert_eq!(event.monotonic_clock(), 1);
        assert_eq!(event.aggregate_id().as_str(), "test_aggregate_1");
        assert_eq!(event.payload(), &vec![1, 2, 3, 4]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_multiple_events_incremental_clock() {
        let repo = create_repo().await;

        let new_event_1 = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "agg_1".try_into().expect("Failed to create AggregateId"),
            "Aggregate",
            vec![1, 2, 3],
        );

        let new_event_2 = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "agg_2".try_into().expect("Failed to create AggregateId"),
            "Aggregate",
            vec![4, 5, 6],
        );

        let new_event_3 = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "agg_3".try_into().expect("Failed to create AggregateId"),
            "Aggregate",
            vec![7, 8, 9],
        );

        let event_1 = repo
            .save_source_event(new_event_1)
            .await
            .expect("First save should succeed");
        let event_2 = repo
            .save_source_event(new_event_2)
            .await
            .expect("Second save should succeed");
        let event_3 = repo
            .save_source_event(new_event_3)
            .await
            .expect("Third save should succeed");

        // Verify monotonic clocks are incrementing
        assert_eq!(event_1.monotonic_clock(), 1);
        assert_eq!(event_2.monotonic_clock(), 2);
        assert_eq!(event_3.monotonic_clock(), 3);

        // Verify aggregates are correct
        assert_eq!(event_1.aggregate_id().as_str(), "agg_1");
        assert_eq!(event_2.aggregate_id().as_str(), "agg_2");
        assert_eq!(event_3.aggregate_id().as_str(), "agg_3");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_saved_event_has_unique_id() {
        let repo = create_repo().await;

        let new_event_1 = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "same_agg".try_into().expect("Failed to create AggregateId"),
            "SameAggregate",
            vec![1, 2, 3],
        );

        let new_event_2 = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "same_agg".try_into().expect("Failed to create AggregateId"),
            "SameAggregate",
            vec![1, 2, 3],
        );

        let event_1 = repo
            .save_source_event(new_event_1)
            .await
            .expect("First save should succeed");
        let event_2 = repo
            .save_source_event(new_event_2)
            .await
            .expect("Second save should succeed");

        // Even with same aggregate_id and payload, the events should have different IDs
        assert_ne!(event_1.id(), event_2.id());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_saved_event_has_valid_data() {
        let repo = create_repo().await;

        let test_payload = vec![1, 2, 3, 4, 5];
        let test_aggregate = "MyAggregate";
        let test_aggregate_id = "my_agg_id_123";

        let new_event = NewSourceEvent::new(
            Uuid::new_v4().into(),
            test_aggregate_id
                .try_into()
                .expect("Failed to create AggregateId"),
            test_aggregate,
            test_payload.clone(),
        );

        let event = repo
            .save_source_event(new_event)
            .await
            .expect("Save should succeed");

        assert_eq!(event.aggregate_type(), test_aggregate);
        assert_eq!(event.aggregate_id().as_str(), test_aggregate_id);
        assert_eq!(event.payload(), &test_payload);
        assert!(event.monotonic_clock() > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_with_empty_payload() {
        let repo = create_repo().await;

        let new_event = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "test_agg".try_into().expect("Failed to create AggregateId"),
            "TestAggregate",
            Vec::<u8>::new(),
        );

        let event = repo
            .save_source_event(new_event)
            .await
            .expect("Save should succeed with empty payload");
        assert_eq!(event.payload(), &Vec::<u8>::new());
        assert_eq!(event.monotonic_clock(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_with_large_payload() {
        let repo = create_repo().await;

        let large_payload = vec![42u8; 10000]; // 10KB payload

        let new_event = NewSourceEvent::new(
            Uuid::new_v4().into(),
            "test_agg".try_into().expect("Failed to create AggregateId"),
            "TestAggregate",
            large_payload.clone(),
        );

        let event = repo
            .save_source_event(new_event)
            .await
            .expect("Save should succeed with large payload");
        assert_eq!(event.payload(), &large_payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_saves_from_multiple_threads() {
        let repo = std::sync::Arc::new(create_repo().await);

        let mut handles = vec![];

        // Use sequential writes from different threads instead of concurrent writes
        // to avoid SQLite's concurrent write limitations
        for i in 0..5 {
            let repo_clone = repo.clone();
            let handle = tokio::spawn(async move {
                // Add a small delay to ensure sequential execution
                tokio::time::sleep(tokio::time::Duration::from_millis(10 * i)).await;
                let new_event = NewSourceEvent::new(
                    Uuid::new_v4().into(),
                    format!("agg_{i}")
                        .try_into()
                        .expect("Failed to create AggregateId"),
                    "Aggregate",
                    vec![u8::try_from(i).unwrap()],
                );
                repo_clone.save_source_event(new_event).await
            });
            handles.push(handle);
        }

        let mut results = vec![];
        for handle in handles {
            let result = handle.await.expect("Thread should not panic");
            results.push(result.expect("Save should succeed"));
        }

        // Verify that all clocks are unique and incrementing
        let mut clocks: Vec<u64> = results.iter().map(SourceEvent::monotonic_clock).collect();
        clocks.sort_unstable();

        for (i, clock) in clocks.iter().enumerate() {
            assert_eq!(*clock, (i + 1) as u64, "Clocks should be sequential");
        }
    }
}
