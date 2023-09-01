// @generated automatically by Diesel CLI.

diesel::table! {
    active_oracles (address) {
        address -> Bytea,
        chain_id -> Int4,
        specification -> Jsonb,
        answer_tx_hash -> Nullable<Bytea>,
    }
}

diesel::table! {
    checkpoints (chain_id) {
        chain_id -> Int4,
        block_number -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    active_oracles,
    checkpoints,
);
