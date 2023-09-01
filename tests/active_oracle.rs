mod commons;

use crate::commons::context::TestContext;
use anyhow::Context;
use defillama_answerer::{
    db::{
        models::{self, ActiveOracle},
        DbAddress, DbTxHash,
    },
    specification::{handlers::tvl::TvlPayload, Specification},
};
use ethers::{abi::Address, types::H256};

#[test]
fn test_to_from_sql() {
    let context = TestContext::new("active_oracle_to_from_sql");

    let active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
            timestamp: 10,
        }),
        answer_tx_hash: None,
    };

    let mut db_connection = context
        .db_connection_pool
        .get()
        .expect("could not get connection from pool");
    models::ActiveOracle::create(
        &mut db_connection,
        active_oracle.address.0,
        active_oracle.chain_id as u64,
        active_oracle.specification.clone(),
    )
    .expect("could not save active oracle to database");

    let oracles = models::ActiveOracle::get_all_for_chain_id(
        &mut db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);
}

#[test]
fn test_answer_tx_hash_update() {
    let context = TestContext::new("active_oracle_answer_tx_hash_update");

    let mut active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
            timestamp: 10,
        }),
        answer_tx_hash: None,
    };

    let mut db_connection = context
        .db_connection_pool
        .get()
        .expect("could not get connection from pool");
    models::ActiveOracle::create(
        &mut db_connection,
        active_oracle.address.0,
        active_oracle.chain_id as u64,
        active_oracle.specification.clone(),
    )
    .expect("could not save active oracle to database");

    let oracles = models::ActiveOracle::get_all_for_chain_id(
        &mut db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);

    let hash = H256::random();
    active_oracle
        .update_answer_tx_hash(&mut db_connection, hash)
        .context("could not update answer tx hash")
        .unwrap();

    let oracles = models::ActiveOracle::get_all_for_chain_id(
        &mut db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);

    let active_oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(active_oracle_from_db, active_oracle);
    assert_eq!(active_oracle_from_db.answer_tx_hash, Some(DbTxHash(hash)));
}
