CREATE TABLE active_oracles (
    address BYTEA NOT NULL,
    chain_id INTEGER NOT NULL,
    measurement_timestamp TIMESTAMP(0) NOT NULL,
    specification JSONB NOT NULL,
    answer_tx_hash BYTEA,

    PRIMARY KEY(address, chain_id),
    CONSTRAINT address_length CHECK (LENGTH(address) = 20),
    CONSTRAINT answer_tx_hash_length CHECK (LENGTH(answer_tx_hash) = 32)
);