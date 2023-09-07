pub mod models;
pub mod schema;

use std::ops::Deref;

use diesel::{
    deserialize::{self, FromSql},
    pg::{Pg, PgValue},
    serialize::{self, ToSql},
    sql_types::{Bytea, Jsonb},
    AsExpression, FromSqlRow,
};
use ethers::types::{Address, H256, U256};

use crate::specification::Specification;

#[derive(FromSqlRow, AsExpression, Debug, PartialEq)]
#[diesel(sql_type = Bytea)]
pub struct DbAddress(pub Address);

impl Deref for DbAddress {
    type Target = Address;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromSql<Bytea, Pg> for DbAddress {
    fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
        let value = <Vec<u8> as FromSql<Bytea, Pg>>::from_sql(bytes)?;
        Ok(DbAddress(Address::from_slice(value.as_slice())))
    }
}

impl ToSql<Bytea, Pg> for DbAddress {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let value = self.0.as_bytes();
        <&[u8] as ToSql<Bytea, Pg>>::to_sql(&value, &mut out.reborrow())
    }
}

#[derive(FromSqlRow, AsExpression, Debug, PartialEq)]
#[diesel(sql_type = Bytea)]
pub struct DbTxHash(pub H256);

impl Deref for DbTxHash {
    type Target = H256;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromSql<Bytea, Pg> for DbTxHash {
    fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
        let value = <Vec<u8> as FromSql<Bytea, Pg>>::from_sql(bytes)?;
        Ok(DbTxHash(H256::from_slice(value.as_slice())))
    }
}

impl ToSql<Bytea, Pg> for DbTxHash {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let value = self.0.as_bytes();
        <&[u8] as ToSql<Bytea, Pg>>::to_sql(&value, &mut out.reborrow())
    }
}

#[derive(FromSqlRow, AsExpression, Debug, PartialEq)]
#[diesel(sql_type = Bytea)]
pub struct DbU256(pub U256);

impl Deref for DbU256 {
    type Target = U256;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromSql<Bytea, Pg> for DbU256 {
    fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
        let value = <Vec<u8> as FromSql<Bytea, Pg>>::from_sql(bytes)?;
        Ok(DbU256(U256::from_big_endian(value.as_slice())))
    }
}

impl ToSql<Bytea, Pg> for DbU256 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        let mut value = [0u8; 32];
        self.0.to_big_endian(&mut value);
        <&[u8] as ToSql<Bytea, Pg>>::to_sql(&value.as_slice(), &mut out.reborrow())
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
