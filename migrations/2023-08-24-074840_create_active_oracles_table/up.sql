CREATE TABLE active_oracles (
    address BYTEA NOT NULL,
    chain_id INTEGER NOT NULL,
    measurement_timestamp TIMESTAMP(0) NOT NULL,
    specification JSONB NOT NULL,
    answer_tx_hash BYTEA,
    -- using raw bytes as even bigint is too small to store 
    -- 18-decimal formatted integers
    answer BYTEA,

    PRIMARY KEY(address, chain_id),
    CONSTRAINT address_length CHECK (LENGTH(address) = 20),
    CONSTRAINT answer_tx_hash_length CHECK (LENGTH(answer_tx_hash) = 32),
    CONSTRAINT answer_length CHECK (LENGTH(answer_tx_hash) = 32)
);