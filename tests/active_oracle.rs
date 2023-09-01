mod commons;

use crate::commons::context::TestContext;
use anyhow::Context;
use defillama_answerer::{
    db::{
        models::{self, ActiveOracle},
        schema::active_oracles,
        DbAddress, DbTxHash,
    },
    specification::{handlers::tvl::TvlPayload, Specification},
};
use diesel::prelude::*;
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

#[test]
fn test_answer_tx_hash_deletion() {
    let context = TestContext::new("active_oracle_answer_tx_hash_deletion");

    // save initial active oracle to db
    let active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
            timestamp: 10,
        }),
        answer_tx_hash: Some(DbTxHash(H256::random())),
    };
    let mut db_connection = context
        .db_connection_pool
        .get()
        .expect("could not get connection from pool");
    diesel::insert_into(active_oracles::table)
        .values(&active_oracle)
        .execute(&mut db_connection)
        .context("could not insert oracle into database")
        .expect("could not save active oracle to database");

    // get it back and check that it's the same as the one we actually wanted to save
    let oracles = models::ActiveOracle::get_all_for_chain_id(
        &mut db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    let mut oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(oracle_from_db, active_oracle);

    // remove its tx hash
    oracle_from_db
        .delete_answer_tx_hash(&mut db_connection)
        .expect("could not delete answer tx hash");
    assert!(oracle_from_db.answer_tx_hash.is_none());

    // get it once again from the database and verify that the tx hash is not there anymore
    let oracles = models::ActiveOracle::get_all_for_chain_id(
        &mut db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    let updated_oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(updated_oracle_from_db, oracle_from_db);
    assert!(updated_oracle_from_db.answer_tx_hash.is_none());
}
