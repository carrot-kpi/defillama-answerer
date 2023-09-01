CREATE TABLE active_oracles (
    address BYTEA PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    specification JSONB NOT NULL,
    answer_tx_hash BYTEA,

    CONSTRAINT address_length CHECK (LENGTH(address) = 20),
    CONSTRAINT answer_tx_hash_length CHECK (LENGTH(answer_tx_hash) = 32)
);