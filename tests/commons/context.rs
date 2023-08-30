use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection, RunQueryDsl,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const BASE_DB_URL: &str = "postgres://user:password@localhost:5432";
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

pub struct TestContext {
    pub maintenance_db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    pub db_connection_pool: Pool<ConnectionManager<PgConnection>>,
    pub db_name: String,
}

impl TestContext {
    pub fn new(db_name: &str) -> Self {
        // connect to maintenance db
        let maintenance_db_connection_manager =
            ConnectionManager::<PgConnection>::new(format!("{BASE_DB_URL}/postgres"));
        let maintenance_db_connection_pool = Pool::builder()
            .build(maintenance_db_connection_manager)
            .expect("could not build connection pool for maintenance database");

        let mut maintenance_db_connection = maintenance_db_connection_pool
            .get()
            .expect("cannot get connection to maintenance database");

        // create test db
        diesel::sql_query(format!("CREATE DATABASE {}", db_name).as_str())
            .execute(&mut maintenance_db_connection)
            .expect(format!("could not create test database {}", db_name).as_str());

        // connect to test db
        let db_connection_manager =
            ConnectionManager::<PgConnection>::new(format!("{BASE_DB_URL}/{db_name}"));
        let db_connection_pool = Pool::builder()
            .build(db_connection_manager)
            .expect("could not build connection pool for test database");

        let mut db_connection = db_connection_pool
            .get()
            .expect("cannot get connection to test database");

        db_connection
            .run_pending_migrations(MIGRATIONS)
            .expect("could not apply pending migrations to test database");

        Self {
            maintenance_db_connection_pool,
            db_connection_pool,
            db_name: db_name.to_string(),
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let mut maintenance_db_connection = self
            .maintenance_db_connection_pool
            .get()
            .expect("cannot get connection to database");

        diesel::sql_query(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}';",
                self.db_name
            )
            .as_str(),
        )
        .execute(&mut maintenance_db_connection)
        .expect("could not terminate backend for database");

        let query = diesel::sql_query(format!("DROP DATABASE {}", self.db_name).as_str());
        query
            .execute(&mut maintenance_db_connection)
            .expect("couldn't drop database");
    }
}
