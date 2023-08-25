// @generated automatically by Diesel CLI.

diesel::table! {
    active_oracles (address) {
        #[max_length = 42]
        address -> Bpchar,
        chain_id -> Int4,
        specification -> Jsonb,
    }
}

diesel::table! {
    snapshots (chain_id) {
        chain_id -> Int4,
        block_number -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    active_oracles,
    snapshots,
);
