use diesel::{Connection, PgConnection, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const BASE_DB_URL: &str = "postgres://user:password@localhost:5432";
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

pub struct TestContext {
    pub maintenance_db_connection: PgConnection,
    pub db_connection: PgConnection,
    pub db_name: String,
}

impl TestContext {
    pub fn new(db_name: &str) -> Self {
        // connect to maintenance db
        let mut maintenance_db_connection =
            PgConnection::establish(format!("{BASE_DB_URL}/postgres").as_str())
                .expect("could not establish connection to maintenance database");

        // create test db
        diesel::sql_query(format!("CREATE DATABASE {}", db_name).as_str())
            .execute(&mut maintenance_db_connection)
            .expect(format!("could not create test database {}", db_name).as_str());

        // connect to test db
        let mut db_connection =
            PgConnection::establish(format!("{BASE_DB_URL}/{db_name}").as_str())
                .expect("could not establish connection to database");

        db_connection
            .run_pending_migrations(MIGRATIONS)
            .expect("could not apply pending migrations to test database");

        Self {
            maintenance_db_connection,
            db_connection,
            db_name: db_name.to_string(),
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        diesel::sql_query(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}';",
                self.db_name
            )
            .as_str(),
        )
        .execute(&mut self.maintenance_db_connection)
        .expect("could not terminate backend for database");

        let query = diesel::sql_query(format!("DROP DATABASE {}", self.db_name).as_str());
        query
            .execute(&mut self.maintenance_db_connection)
            .expect("couldn't drop database");
    }
}
