pub mod models;
pub mod schema;

use std::ops::Deref;

use diesel::{
    deserialize::{self, FromSql},
    pg::{Pg, PgValue},
    serialize::{self, ToSql},
    sql_types::{Jsonb, Text},
    AsExpression, FromSqlRow,
};
use ethers::types::Address;
use serde_json::Value;

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
        Ok(DbAddress(
            String::from_utf8_lossy(bytes.as_bytes()).parse::<Address>()?,
        ))
    }
}

impl ToSql<Text, Pg> for DbAddress {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let value = self.to_string();
        <String as ToSql<Text, Pg>>::to_sql(&value, &mut out.reborrow())
    }
}

impl FromSql<Jsonb, Pg> for Specification {
    fn from_sql(value: PgValue) -> deserialize::Result<Self> {
        Ok(serde_json::from_slice(value.as_bytes())?)
    }
}

impl ToSql<Jsonb, Pg> for Specification {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let value = serde_json::to_value(self)?;
        <Value as ToSql<Jsonb, Pg>>::to_sql(&value, &mut out.reborrow())
    }
}
