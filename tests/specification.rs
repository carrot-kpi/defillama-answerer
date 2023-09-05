mod commons;

use std::time::UNIX_EPOCH;

use crate::commons::context::TestContext;
use defillama_answerer::{
    db::models,
    specification::{handlers::tvl::TvlPayload, Specification},
};
use ethers::types::Address;

#[test]
fn test_to_from_sql() {
    let context = TestContext::new("specification_to_from_sql");

    let chain_id = 100;
    let specification = Specification::Tvl(TvlPayload {
        protocol: "foo".to_owned(),
    });

    let mut db_connection = context
        .db_connection_pool
        .get()
        .expect("could not get connection from pool");
    models::ActiveOracle::create(
        &mut db_connection,
        Address::random(),
        chain_id,
        UNIX_EPOCH,
        specification.clone(),
    )
    .expect("could not save active oracle to database");

    let oracles =
        models::ActiveOracle::get_all_answerable_for_chain_id(&mut db_connection, chain_id)
            .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(
        oracles.into_iter().nth(0).unwrap().specification,
        specification
    );
}
