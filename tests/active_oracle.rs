mod commons;

use std::time::{Duration, UNIX_EPOCH};

use crate::commons::context::TestContext;
use anyhow::Context;
use defillama_answerer::{
    db::{
        models::{self, ActiveOracle},
        schema::active_oracles,
        DbAddress, DbTxHash, DbU256,
    },
    specification::{handlers::tvl::TvlPayload, Specification},
};
use diesel::prelude::*;
use ethers::{
    abi::Address,
    types::{H256, U256},
};

#[test]
fn test_to_from_sql_specification() {
    let mut context = TestContext::new("active_oracle_to_from_sql_specification");

    let active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        measurement_timestamp: UNIX_EPOCH,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        expiration: Some(UNIX_EPOCH + Duration::from_secs(10)),
        answer_tx_hash: None,
        answer: None,
    };

    models::ActiveOracle::create(
        &mut context.db_connection,
        active_oracle.address.0,
        active_oracle.chain_id as u64,
        active_oracle.measurement_timestamp,
        active_oracle.specification.clone(),
        active_oracle.expiration.unwrap(),
    )
    .expect("could not save active oracle to database");

    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);
}

// this also tests the answer update
#[test]
fn test_to_from_sql_answer() {
    let mut context = TestContext::new("active_oracle_to_from_sql_answer");

    let answer = U256::from(1);
    let active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        measurement_timestamp: UNIX_EPOCH,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        expiration: Some(UNIX_EPOCH + Duration::from_secs(10)),
        answer_tx_hash: None,
        answer: Some(DbU256(answer)),
    };

    let mut active_oracle = models::ActiveOracle::create(
        &mut context.db_connection,
        active_oracle.address.0,
        active_oracle.chain_id as u64,
        active_oracle.measurement_timestamp,
        active_oracle.specification.clone(),
        active_oracle.expiration.unwrap(),
    )
    .expect("could not save active oracle to database");

    // refetch the saved oracle and check that the answer is none
    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);

    // update the answer in the database
    active_oracle
        .update_answer(&mut context.db_connection, answer)
        .expect("could not update answer");
    assert_eq!(active_oracle.answer, Some(DbU256(answer)));

    // refetch the saved oracle and check that the answer is now correctly set
    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);
}

#[test]
fn test_answer_tx_hash_update() {
    let mut context = TestContext::new("active_oracle_answer_tx_hash_update");

    let mut active_oracle = models::ActiveOracle::create(
        &mut context.db_connection,
        Address::random(),
        100,
        UNIX_EPOCH,
        Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        UNIX_EPOCH + Duration::from_secs(10),
    )
    .expect("could not save active oracle to database");

    let oracles = active_oracles::table
        .filter(active_oracles::dsl::answer_tx_hash.is_null())
        .select(ActiveOracle::as_select())
        .load(&mut context.db_connection)
        .expect("could not get active oracle by null answer tx hash from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);

    let hash = H256::random();
    active_oracle
        .update_answer_tx_hash(&mut context.db_connection, hash)
        .context("could not update answer tx hash")
        .unwrap();

    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);

    let active_oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(active_oracle_from_db, active_oracle);

    // check that there are no more oracles with a null answer tx hash
    assert_eq!(
        active_oracles::table
            .filter(active_oracles::dsl::answer_tx_hash.is_null())
            .select(ActiveOracle::as_select())
            .load(&mut context.db_connection)
            .expect("could not get active oracle by null answer tx hash from database")
            .len(),
        0
    );
}

#[test]
fn test_answer_tx_hash_update_multiple_oracles() {
    let mut context = TestContext::new("active_oracle_answer_tx_hash_update_multiple");

    let mut active_oracle_1 = models::ActiveOracle::create(
        &mut context.db_connection,
        Address::random(),
        100,
        UNIX_EPOCH,
        Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        UNIX_EPOCH + Duration::from_secs(10),
    )
    .expect("could not save active oracle 1 to database");
    let active_oracle_2 = models::ActiveOracle::create(
        &mut context.db_connection,
        Address::random(),
        100,
        UNIX_EPOCH,
        Specification::Tvl(TvlPayload {
            protocol: "bar".to_owned(),
        }),
        UNIX_EPOCH + Duration::from_secs(10),
    )
    .expect("could not save active oracle 2 to database");

    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle_1.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 2);
    assert_eq!(&oracles[1], &active_oracle_2);
    assert_eq!(&oracles[0], &active_oracle_1);

    // check that there are 2 oracles with a null answer tx hash nopw
    assert_eq!(
        active_oracles::table
            .filter(active_oracles::dsl::answer_tx_hash.is_null())
            .select(ActiveOracle::as_select())
            .load(&mut context.db_connection)
            .expect("could not get active oracle by null answer tx hash from database")
            .len(),
        2
    );

    let hash = H256::random();
    active_oracle_1
        .update_answer_tx_hash(&mut context.db_connection, hash)
        .context("could not update oracle 1 answer tx hash")
        .unwrap();

    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle_1.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 2);

    let active_oracle_1_from_db = &oracles[1];
    assert_eq!(active_oracle_1_from_db, &active_oracle_1);

    let active_oracle_2_from_db = &oracles[0];
    assert_eq!(active_oracle_2_from_db, &active_oracle_2);

    // check that there is only one oracle with a null answer tx hash now
    assert_eq!(
        active_oracles::table
            .filter(active_oracles::dsl::answer_tx_hash.is_null())
            .select(ActiveOracle::as_select())
            .load(&mut context.db_connection)
            .expect("could not get active oracle by null answer tx hash from database")
            .len(),
        1
    );
}

#[test]
fn test_answer_tx_hash_deletion() {
    let mut context = TestContext::new("active_oracle_answer_tx_hash_deletion");

    // save initial active oracle to db
    let active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        measurement_timestamp: UNIX_EPOCH,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        expiration: Some(UNIX_EPOCH + Duration::from_secs(10)),
        answer_tx_hash: Some(DbTxHash(H256::random())),
        answer: None,
    };
    diesel::insert_into(active_oracles::table)
        .values(&active_oracle)
        .execute(&mut context.db_connection)
        .context("could not insert oracle into database")
        .expect("could not save active oracle to database");

    // get it back and check that it's the same as the one we actually wanted to save
    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    let mut oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(oracle_from_db, active_oracle);

    // remove its tx hash
    oracle_from_db
        .delete_answer_tx_hash(&mut context.db_connection)
        .expect("could not delete answer tx hash");
    assert!(oracle_from_db.answer_tx_hash.is_none());

    // get it once again from the database and verify that the tx hash is not there anymore
    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    let updated_oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(updated_oracle_from_db, oracle_from_db);
    assert!(updated_oracle_from_db.answer_tx_hash.is_none());
}

#[test]
fn test_answer_deletion() {
    let mut context = TestContext::new("active_oracle_answer_deletion");

    // save initial active oracle to db
    let active_oracle = ActiveOracle {
        address: DbAddress(Address::random()),
        chain_id: 100,
        measurement_timestamp: UNIX_EPOCH,
        specification: Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        expiration: Some(UNIX_EPOCH + Duration::from_secs(10)),
        answer_tx_hash: Some(DbTxHash(H256::random())),
        answer: None,
    };
    diesel::insert_into(active_oracles::table)
        .values(&active_oracle)
        .execute(&mut context.db_connection)
        .expect("could not save active oracle to database");

    // get it back and check that it's the same as the one we actually wanted to save
    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    let mut oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(oracle_from_db, active_oracle);

    // remove its answer
    oracle_from_db
        .delete_answer(&mut context.db_connection)
        .expect("could not delete answer");
    assert!(oracle_from_db.answer.is_none());

    // get it once again from the database and verify that the tx hash is not there anymore
    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    let updated_oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(updated_oracle_from_db, oracle_from_db);
    assert!(updated_oracle_from_db.answer.is_none());
}

#[test]
fn test_expiration_update() {
    let mut context = TestContext::new("active_oracle_expiration_update");

    let old_expiration = UNIX_EPOCH + Duration::from_secs(10);
    let mut active_oracle = models::ActiveOracle::create(
        &mut context.db_connection,
        Address::random(),
        100,
        UNIX_EPOCH,
        Specification::Tvl(TvlPayload {
            protocol: "foo".to_owned(),
        }),
        old_expiration,
    )
    .expect("could not save active oracle to database");

    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(oracles.into_iter().nth(0).unwrap(), active_oracle);

    assert_eq!(
        active_oracles::table
            .filter(active_oracles::dsl::expiration.eq(old_expiration))
            .select(ActiveOracle::as_select())
            .load(&mut context.db_connection)
            .expect("could not get active oracle by old expiration from database")
            .len(),
        1
    );

    let new_expiration = UNIX_EPOCH + Duration::from_secs(1_000);
    active_oracle
        .update_expiration(&mut context.db_connection, new_expiration)
        .context("could not update expiration")
        .unwrap();

    let oracles = models::ActiveOracle::get_all_answerable_for_chain_id(
        &mut context.db_connection,
        active_oracle.chain_id as u64,
    )
    .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);

    let active_oracle_from_db = oracles.into_iter().nth(0).unwrap();
    assert_eq!(active_oracle_from_db, active_oracle);

    assert_eq!(
        active_oracles::table
            .filter(active_oracles::dsl::expiration.eq(old_expiration))
            .select(ActiveOracle::as_select())
            .load(&mut context.db_connection)
            .expect("could not get active oracle by old expiration from database")
            .len(),
        0
    );
}
