mod commons;

use std::time::{Duration, UNIX_EPOCH};

use crate::commons::context::TestContext;
use defillama_answerer::{
    db::models,
    specification::{handlers::tvl::TvlPayload, Specification},
};
use ethers::types::Address;

#[test]
fn test_to_from_sql() {
    let mut context = TestContext::new("specification_to_from_sql");

    let chain_id = 100;
    let specification = Specification::Tvl(TvlPayload {
        protocol: "foo".to_owned(),
    });

    models::ActiveOracle::create(
        &mut context.db_connection,
        Address::random(),
        chain_id,
        UNIX_EPOCH,
        specification.clone(),
        UNIX_EPOCH + Duration::from_secs(10),
    )
    .expect("could not save active oracle to database");

    let oracles =
        models::ActiveOracle::get_all_answerable_for_chain_id(&mut context.db_connection, chain_id)
            .expect("could not get active oracles from database");
    assert_eq!(oracles.len(), 1);
    assert_eq!(
        oracles.into_iter().nth(0).unwrap().specification,
        specification
    );
}
