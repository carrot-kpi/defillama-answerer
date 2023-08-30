use anyhow::Context;
use diesel::prelude::*;
use ethers::types::Address;

use crate::specification::Specification;

use super::{
    schema::{active_oracles, checkpoints},
    DbAddress,
};

#[derive(Queryable, Selectable, Insertable, Debug, PartialEq)]
#[diesel(table_name = active_oracles)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ActiveOracle {
    pub address: DbAddress,
    pub chain_id: i32,
    pub specification: Specification,
}

impl ActiveOracle {
    pub fn create(
        connection: &mut PgConnection,
        address: Address,
        chain_id: u64,
        specification: Specification,
    ) -> anyhow::Result<()> {
        let oracle = ActiveOracle {
            address: DbAddress(address),
            chain_id: i32::try_from(chain_id).unwrap(), // this should never panic
            specification,
        };

        diesel::insert_into(active_oracles::table)
            .values(&oracle)
            .execute(connection)
            .context("could not insert oracle into database")?;

        Ok(())
    }

    pub fn delete(&self, connection: &mut PgConnection) -> anyhow::Result<()> {
        diesel::delete(active_oracles::dsl::active_oracles.find(&self.address))
            .execute(connection)
            .context(format!(
                "could not delete oracle {} from database",
                self.address.0
            ))?;

        Ok(())
    }

    pub fn get_all_for_chain_id(
        connection: &mut PgConnection,
        chain_id: u64,
    ) -> anyhow::Result<Vec<ActiveOracle>> {
        let chain_id = i32::try_from(chain_id).unwrap(); // this should never panic
        Ok(active_oracles::table
            .filter(active_oracles::dsl::chain_id.eq(chain_id))
            .select(ActiveOracle::as_select())
            .load(connection)?)
    }
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = checkpoints)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Checkpoint {
    pub chain_id: i32,
    pub block_number: i64,
}

impl Checkpoint {
    pub fn update(
        connection: &mut PgConnection,
        chain_id: u64,
        block_number: i64,
    ) -> anyhow::Result<()> {
        let chain_id: i32 = i32::try_from(chain_id).unwrap(); // this should never panic

        let snapshot = Checkpoint {
            chain_id,
            block_number,
        };

        diesel::insert_into(checkpoints::dsl::checkpoints)
            .values(&snapshot)
            .on_conflict(checkpoints::dsl::chain_id)
            .do_update()
            .set(checkpoints::dsl::block_number.eq(block_number))
            .execute(connection)?;

        Ok(())
    }

    pub fn get_for_chain_id(
        connection: &mut PgConnection,
        chain_id: u64,
    ) -> anyhow::Result<Checkpoint> {
        let chain_id = i32::try_from(chain_id).unwrap(); // this should never panic
        Ok(checkpoints::dsl::checkpoints
            .find(chain_id)
            .first(connection)?)
    }
}
