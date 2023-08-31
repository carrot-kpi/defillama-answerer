mod commons;

use crate::commons::context::TestContext;
use defillama_answerer::db::models::{self, Checkpoint};

#[test]
fn test_find() {
    let context = TestContext::new("find_checkpoint");

    let chain_id = 100i32;
    let block_number = 10;

    let mut db_connection = context
        .db_connection_pool
        .get()
        .expect("could not get connection from pool");
    models::Checkpoint::update(&mut db_connection, chain_id as u64, block_number)
        .expect("could not save checkpoint to database");

    // find the checkpoint that was just now inserted
    let checkpoint = models::Checkpoint::get_for_chain_id(&mut db_connection, chain_id as u64)
        .expect("could not get checkpoint from database");
    assert_eq!(
        checkpoint,
        Some(Checkpoint {
            chain_id: chain_id as i32,
            block_number
        })
    );

    // find a checkpoint for a non existent chain id
    let checkpoint = models::Checkpoint::get_for_chain_id(&mut db_connection, 1234)
        .expect("could not get checkpoint from database");
    assert!(checkpoint.is_none());

    // manually drop the test db
    // let db_connection_pool = context.db_connection_pool.clone();
    drop(context);

    // since the db was dropped, this should result in an error
    assert!(models::Checkpoint::get_for_chain_id(&mut db_connection, chain_id as u64).is_err());
}
