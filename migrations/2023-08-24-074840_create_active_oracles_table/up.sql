CREATE TABLE active_oracles (
    address CHAR(42) PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    specification JSONB NOT NULL
);