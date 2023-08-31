pub mod models;
pub mod schema;

use std::ops::Deref;

use anyhow::Context;
use diesel::{
    deserialize::{self, FromSql},
    pg::{Pg, PgValue},
    serialize::{self, ToSql},
    sql_types::{Jsonb, Text},
    AsExpression, FromSqlRow,
};
use ethers::{types::Address, utils};

use crate::specification::Specification;

#[derive(FromSqlRow, AsExpression, Debug, PartialEq)]
#[diesel(sql_type = Text)]
pub struct DbAddress(Address);

impl Deref for DbAddress {
    type Target = Address;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromSql<Text, Pg> for DbAddress {
    fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
        let str = String::from_utf8_lossy(bytes.as_bytes());
        Ok(DbAddress(
            utils::parse_checksummed(str.as_ref(), None).context(format!(
                "could not parse checksummed address {} from database",
                str.as_ref()
            ))?,
        ))
    }
}

impl ToSql<Text, Pg> for DbAddress {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let value = utils::to_checksum(&self.0, None);
        <String as ToSql<Text, Pg>>::to_sql(&value, &mut out.reborrow())
    }
}

impl FromSql<Jsonb, Pg> for Specification {
    fn from_sql(value: PgValue) -> deserialize::Result<Self> {
        let value = <serde_json::Value as FromSql<Jsonb, Pg>>::from_sql(value)?;
        Ok(serde_json::from_value(value)?)
    }
}

impl ToSql<Jsonb, Pg> for Specification {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let value = serde_json::to_value(self)?;
        <serde_json::Value as ToSql<Jsonb, Pg>>::to_sql(&value, &mut out.reborrow())
    }
}
