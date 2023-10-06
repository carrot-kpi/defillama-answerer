use std::time::SystemTime;

use anyhow::Context;
use diesel::prelude::*;
use ethers::types::{Address, H256, U256};

use crate::specification::Specification;

use super::{
    schema::{
        active_oracles::{self},
        checkpoints,
    },
    DbAddress, DbTxHash, DbU256,
};

#[derive(Queryable, Selectable, Insertable, Debug, PartialEq)]
#[diesel(treat_none_as_null = true)]
#[diesel(table_name = active_oracles)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ActiveOracle {
    pub address: DbAddress,
    pub chain_id: i32,
    pub measurement_timestamp: SystemTime,
    pub specification: Specification,
    pub expiration: Option<SystemTime>,
    pub answer_tx_hash: Option<DbTxHash>,
    pub answer: Option<DbU256>,
}

impl ActiveOracle {
    pub fn create(
        connection: &mut PgConnection,
        address: Address,
        chain_id: u64,
        measurement_timestamp: SystemTime,
        specification: Specification,
        expiration: SystemTime,
    ) -> anyhow::Result<ActiveOracle> {
        let oracle = ActiveOracle {
            address: DbAddress(address),
            chain_id: i32::try_from(chain_id).unwrap(), // this should never panic
            measurement_timestamp,
            specification,
            expiration: Some(expiration),
            answer_tx_hash: None,
            answer: None,
        };

        diesel::insert_into(active_oracles::table)
            .values(&oracle)
            .execute(connection)
            .context("could not insert oracle into database")?;

        Ok(oracle)
    }

    pub fn update_answer_tx_hash(
        &mut self,
        connection: &mut PgConnection,
        answer_tx_hash: H256,
    ) -> anyhow::Result<()> {
        let sql =
            diesel::update(active_oracles::dsl::active_oracles.find((self.address, self.chain_id)))
                .set(active_oracles::dsl::answer_tx_hash.eq(DbTxHash(answer_tx_hash)))
                .execute(connection)
                .context(format!(
                    "could not update active oracle 0x{:x} answer tx hash",
                    self.address.0
                ))?;
        println!("{}", sql);
        self.answer_tx_hash = Some(DbTxHash(answer_tx_hash));
        Ok(())
    }

    pub fn delete_answer_tx_hash(&mut self, connection: &mut PgConnection) -> anyhow::Result<()> {
        diesel::update(active_oracles::dsl::active_oracles.find((self.address, self.chain_id)))
            .set(active_oracles::dsl::answer_tx_hash.eq(None::<DbTxHash>))
            .execute(connection)
            .context(format!(
                "could not delete active oracle 0x{:x} answer tx hash",
                self.address.0
            ))?;
        self.answer_tx_hash = None;
        Ok(())
    }

    pub fn update_answer(
        &mut self,
        connection: &mut PgConnection,
        answer: U256,
    ) -> anyhow::Result<()> {
        diesel::update(active_oracles::dsl::active_oracles.find((self.address, self.chain_id)))
            .set(active_oracles::dsl::answer.eq(DbU256(answer)))
            .execute(connection)
            .context(format!(
                "could not update active oracle 0x{:x} answer",
                self.address.0
            ))?;
        self.answer = Some(DbU256(answer));
        Ok(())
    }

    pub fn delete_answer(&mut self, connection: &mut PgConnection) -> anyhow::Result<()> {
        diesel::update(active_oracles::dsl::active_oracles.find((self.address, self.chain_id)))
            .set(active_oracles::dsl::answer.eq(None::<DbU256>))
            .execute(connection)
            .context(format!(
                "could not delete active oracle 0x{:x} answer",
                self.address.0
            ))?;
        self.answer = None;
        Ok(())
    }

    pub fn update_expiration(
        &mut self,
        connection: &mut PgConnection,
        expiration: SystemTime,
    ) -> anyhow::Result<()> {
        diesel::update(active_oracles::dsl::active_oracles.find((self.address, self.chain_id)))
            .set(active_oracles::dsl::expiration.eq(Some(expiration)))
            .execute(connection)
            .context(format!(
                "could not update active oracle 0x{:x} expiration",
                self.address.0
            ))?;
        self.expiration = Some(expiration);
        Ok(())
    }

    // by getting ownership of self instead of a reference to it, we know that the active
    // oracle model instance will be dropped at the end of the function after having been
    // deleted from the db
    pub fn delete(self, connection: &mut PgConnection) -> anyhow::Result<()> {
        diesel::delete(active_oracles::dsl::active_oracles.find((&self.address, &self.chain_id)))
            .execute(connection)
            .context(format!(
                "could not delete oracle {} from database",
                self.address.0
            ))?;
        Ok(())
    }

    pub fn get_all_answerable_for_chain_id(
        connection: &mut PgConnection,
        chain_id: u64,
    ) -> anyhow::Result<Vec<ActiveOracle>> {
        let chain_id = i32::try_from(chain_id).unwrap(); // this should never panic
        Ok(active_oracles::table
            .filter(
                active_oracles::dsl::chain_id
                    .eq(chain_id)
                    .and(active_oracles::dsl::measurement_timestamp.lt(SystemTime::now())),
            )
            .select(ActiveOracle::as_select())
            .load(connection)?)
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, PartialEq)]
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
    ) -> anyhow::Result<Option<Checkpoint>> {
        let chain_id = i32::try_from(chain_id).unwrap(); // this should never panic
        match checkpoints::dsl::checkpoints
            .find(chain_id)
            .first(connection)
        {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(error) => {
                if error == diesel::NotFound {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!(error))
                }
            }
        }
    }
}
