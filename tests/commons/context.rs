use diesel::{
    r2d2::{ConnectionManager, Pool},
    Connection, PgConnection, RunQueryDsl,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const BASE_DB_URL: &str = "postgres://user:password@localhost:5432/postgres";
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

pub struct TestContext {
    pub db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    pub db_name: String,
}

impl TestContext {
    pub fn new(db_name: &str) -> Self {
        let db_connection_manager = ConnectionManager::<PgConnection>::new(BASE_DB_URL);
        let db_connection_pool = Pool::builder()
            .build(db_connection_manager)
            .expect("could not build connection pool for database");

        let mut db_connection = db_connection_pool
            .get()
            .expect("cannot get connection to database");

        diesel::sql_query(format!("CREATE DATABASE {}", db_name).as_str())
            .execute(&mut db_connection)
            .expect(format!("could not create database {}", db_name).as_str());

        db_connection
            .run_pending_migrations(MIGRATIONS)
            .expect("could not apply pending migrations");

        Self {
            db_connection_pool,
            db_name: db_name.to_string(),
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let mut db_connection = self
            .db_connection_pool
            .get()
            .expect("cannot get connection to database");

        diesel::sql_query(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}';",
                self.db_name
            )
            .as_str(),
        )
        .execute(&mut db_connection)
        .expect("could not terminate backend for database");

        let query = diesel::sql_query(format!("DROP DATABASE {}", self.db_name).as_str());
        query
            .execute(&mut db_connection)
            .expect("couldn't drop database");
    }
}
