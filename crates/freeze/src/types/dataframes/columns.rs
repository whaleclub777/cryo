use alloy::{
    dyn_abi::DynSolValue,
    primitives::{I256, U256},
};
use polars::{
    prelude::{Column, NamedFrom},
    series::Series,
};

use crate::{
    err, schemas::TableConfig, CollectError, ColumnType, RawBytes, ToU256Series, ToVecHex,
};

/// A vector that can hold either values or optional values.
pub enum OptionVec<T> {
    /// A vector of values
    Some(Vec<T>),
    /// A vector of optional values
    Option(Vec<Option<T>>),
}

impl<T> OptionVec<T> {
    /// Returns whether the vector is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            OptionVec::Some(v) => v.is_empty(),
            OptionVec::Option(v) => v.is_empty(),
        }
    }

    /// Get the length of the vector
    pub fn len(&self) -> usize {
        match self {
            OptionVec::Some(v) => v.len(),
            OptionVec::Option(v) => v.len(),
        }
    }

    /// Map the elements of the vector
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> OptionVec<U> {
        match self {
            OptionVec::Some(v) => OptionVec::Some(v.into_iter().map(f).collect()),
            OptionVec::Option(v) => {
                OptionVec::Option(v.into_iter().map(|opt| opt.map(&mut f)).collect())
            }
        }
    }

    /// Convert into a polars Column
    pub fn into_column<P1: ?Sized, P2: ?Sized>(self, name: String) -> Column
    where
        Series: NamedFrom<Vec<T>, P1>,
        Series: NamedFrom<Vec<Option<T>>, P2>,
    {
        match self {
            OptionVec::Some(v) => Column::new(name.into(), v),
            OptionVec::Option(v) => Column::new(name.into(), v),
        }
    }

    /// Convert into a variety of u256 Column representations
    pub fn into_u256_columns(
        self,
        name: String,
        config: &TableConfig,
    ) -> Result<Vec<Column>, CollectError>
    where
        Vec<T>: ToU256Series,
        Vec<Option<T>>: ToU256Series,
    {
        match self {
            OptionVec::Some(v) => {
                let mut series_vec = Vec::new();
                for u256_type in config.u256_types.iter() {
                    series_vec.push(v.to_u256_series(
                        name.to_string(),
                        u256_type.clone(),
                        config,
                    )?)
                }
                Ok(series_vec)
            }
            OptionVec::Option(v) => {
                let mut series_vec = Vec::new();
                for u256_type in config.u256_types.iter() {
                    series_vec.push(v.to_u256_series(
                        name.to_string(),
                        u256_type.clone(),
                        config,
                    )?)
                }
                Ok(series_vec)
            }
        }
    }
}

/// A collection of dynamic values that can be used in a DataFrame column.
pub enum DynValues {
    /// int
    Ints(OptionVec<i64>),
    /// uint
    UInts(OptionVec<u64>),
    /// u256
    U256s(OptionVec<U256>),
    /// i256
    I256s(OptionVec<I256>),
    /// bytes
    Bytes(OptionVec<RawBytes>),
    /// bool
    Bools(OptionVec<bool>),
    /// string
    Strings(OptionVec<String>),
}

macro_rules! impl_from_vec {
    ($($ty:ty => $variant:ident,)+) => {
        $(impl From<Vec<$ty>> for DynValues {
            fn from(value: Vec<$ty>) -> Self {
                DynValues::$variant(OptionVec::Some(value))
            }
        }
        impl From<Vec<Option<$ty>>> for DynValues {
            fn from(value: Vec<Option<$ty>>) -> Self {
                DynValues::$variant(OptionVec::Option(value))
            }
        })+
    };
}

impl_from_vec! {
    i64 => Ints,
    u64 => UInts,
    U256 => U256s,
    I256 => I256s,
    RawBytes => Bytes,
    String => Strings,
    bool => Bools,
}

impl DynValues {
    /// Returns the data type as a string.
    pub fn dtype_str(&self) -> &'static str {
        match self {
            DynValues::Ints(_) => "Int64",
            DynValues::UInts(_) => "UInt64",
            DynValues::U256s(_) => "UInt256",
            DynValues::I256s(_) => "Int256",
            DynValues::Bytes(_) => "Binary",
            DynValues::Bools(_) => "Boolean",
            DynValues::Strings(_) => "String",
        }
    }

    /// Create a new `DynValues` instance from a vector of `DynSolValue`s.
    pub fn from_sol_values(
        data: Vec<DynSolValue>,
        _config: &TableConfig,
    ) -> Result<Self, CollectError> {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut u256s: Vec<U256> = vec![];
        let mut i256s: Vec<I256> = vec![];
        let mut bytes: Vec<RawBytes> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        // TODO: support array & tuple types

        for token in data {
            match token {
                DynSolValue::Address(a) => {
                    bytes.push(a.to_vec());
                }
                DynSolValue::FixedBytes(b, _) => {
                    bytes.push(b.to_vec());
                }
                DynSolValue::Bytes(b) => {
                    bytes.push(b);
                }
                DynSolValue::Uint(i, size) => {
                    if size <= 64 {
                        uints.push(i.wrapping_to::<u64>())
                    } else {
                        u256s.push(i)
                    }
                }
                DynSolValue::Int(i, size) => {
                    if size <= 64 {
                        ints.push(i.unchecked_into());
                    } else {
                        i256s.push(i);
                    }
                }
                DynSolValue::Bool(b) => bools.push(b),
                DynSolValue::String(s) => strings.push(s),
                DynSolValue::Array(_) | DynSolValue::FixedArray(_) => {}
                DynSolValue::Tuple(_) => {}
                DynSolValue::Function(_) => {}
            }
        }

        let result = if !ints.is_empty() {
            DynValues::Ints(OptionVec::Some(ints))
        } else if !i256s.is_empty() {
            DynValues::I256s(OptionVec::Some(i256s))
        } else if !u256s.is_empty() {
            DynValues::U256s(OptionVec::Some(u256s))
        } else if !uints.is_empty() {
            DynValues::UInts(OptionVec::Some(uints))
        } else if !bytes.is_empty() {
            DynValues::Bytes(OptionVec::Some(bytes))
        } else if !bools.is_empty() {
            DynValues::Bools(OptionVec::Some(bools))
        } else if !strings.is_empty() {
            DynValues::Strings(OptionVec::Some(strings))
        } else {
            return Err(err("could not parse column, mixed type"))
        };
        Ok(result)
    }

    /// Returns whether the underlying data is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length of the underlying data.
    pub fn len(&self) -> usize {
        match self {
            DynValues::Ints(ints) => ints.len(),
            DynValues::I256s(i256s) => i256s.len(),
            DynValues::U256s(u256s) => u256s.len(),
            DynValues::UInts(uints) => uints.len(),
            DynValues::Bytes(bytes) => bytes.len(),
            DynValues::Bools(bools) => bools.len(),
            DynValues::Strings(strings) => strings.len(),
        }
    }

    /// Converts the `DynValues` into `Vec<Column>`.
    pub fn into_columns(
        self,
        name: String,
        col_type: ColumnType,
        config: &TableConfig,
    ) -> Result<Vec<Column>, CollectError> {
        let mixed_type_err = format!("could not parse column {name}, mixed type expect {col_type:?}, actually {}", self.dtype_str());
        match self {
            Self::Ints(ints) => match col_type {
                ColumnType::Int256 => {
                    ints.map(|v| I256::try_from(v).unwrap()).into_u256_columns(name, config)
                }
                ColumnType::Int32 => Ok(vec![ints.map(|v| v as i32).into_column(name)]),
                ColumnType::Int64 => Ok(vec![ints.into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::UInts(uints) => match col_type {
                ColumnType::UInt256 => uints.map(|v| U256::from(v)).into_u256_columns(name, config),
                ColumnType::UInt32 => Ok(vec![uints.map(|v| v as u32).into_column(name)]),
                ColumnType::UInt64 => Ok(vec![uints.into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::I256s(i256s) if col_type == ColumnType::Int256 => {
                i256s.into_u256_columns(name, config)
            }
            Self::U256s(u256s) if col_type == ColumnType::UInt256 => {
                u256s.into_u256_columns(name, config)
            }
            Self::Bytes(bytes) => match col_type {
                ColumnType::Binary => Ok(vec![bytes.into_column(name)]),
                ColumnType::Hex => Ok(vec![bytes.to_vec_hex(config.hex_prefix).into_column(name)]),
                _ => Err(err(&mixed_type_err)),
            },
            Self::Bools(bools) => Ok(vec![bools.into_column(name)]),
            Self::Strings(strings) => Ok(vec![strings.into_column(name)]),
            _ => Err(err(&mixed_type_err)),
        }
    }
}

impl ColumnType {
    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_column_from_values(
        self,
        name: String,
        data: Vec<DynSolValue>,
        chunk_len: usize,
        config: &TableConfig,
    ) -> Result<Vec<Column>, CollectError> {
        let values = DynValues::from_sol_values(data, config)?;
        let mixed_length_err = format!("could not parse column {name}, mixed type");

        if values.len() != chunk_len {
            return Err(err(&mixed_length_err))
        }
        values.into_columns(name, self, config)
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_empty_columns(self, name: &str, config: &TableConfig) -> Vec<Column> {
        if self.is_256() {
            return self.create_empty_u256_columns(name, config);
        }
        vec![self.create_single_empty_column(name)]
    }

    /// Create empty columns for U256 types
    pub fn create_empty_u256_columns(self, name: &str, config: &TableConfig) -> Vec<Column> {
        config
            .u256_types
            .iter()
            .map(|u256_type| {
                let new_type = u256_type.to_columntype(config.binary_type);
                let full_name = name.to_string() + u256_type.suffix(self).as_str();
                new_type.create_single_empty_column(&full_name)
            })
            .collect()
    }

    /// Create an empty column of the specified type
    pub fn create_single_empty_column(self, name: &str) -> Column {
        match self {
            ColumnType::Boolean => Column::new(name.into(), Vec::<bool>::new()),
            ColumnType::UInt32 => Column::new(name.into(), Vec::<u32>::new()),
            ColumnType::UInt64 => Column::new(name.into(), Vec::<u64>::new()),
            ColumnType::UInt256 => {
                Column::new(format!("{name}_u256binary").into(), Vec::<RawBytes>::new())
            }
            ColumnType::Int32 => Column::new(name.into(), Vec::<i32>::new()),
            ColumnType::Int64 => Column::new(name.into(), Vec::<i64>::new()),
            ColumnType::Int256 => {
                Column::new(format!("{name}_i256binary").into(), Vec::<RawBytes>::new())
            }
            ColumnType::Float32 => Column::new(name.into(), Vec::<f32>::new()),
            ColumnType::Float64 => Column::new(name.into(), Vec::<f64>::new()),
            ColumnType::Decimal128 => Column::new(name.into(), Vec::<RawBytes>::new()),
            ColumnType::String => Column::new(name.into(), Vec::<String>::new()),
            ColumnType::Binary => Column::new(name.into(), Vec::<RawBytes>::new()),
            ColumnType::Hex => Column::new(name.into(), Vec::<String>::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::DataType;

    use crate::{ColumnEncoding, U256Type};

    use super::*;

    fn get_config() -> TableConfig {
        TableConfig {
            hex_prefix: true,
            binary_type: ColumnEncoding::Binary,
            u256_types: vec![U256Type::NamedBinary, U256Type::String, U256Type::F64],
        }
    }

    macro_rules! test_empty_column {
        ($($ty:ident)+) => {
            $({
                let cols = ColumnType::$ty
                    .create_empty_columns(stringify!($ty), &get_config());
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].dtype(), &DataType::$ty);
                assert_eq!(cols[0].len(), 0);
            })+
        };
    }

    macro_rules! test_column {
        ($($ty:ident => $exp:expr,)+) => {
            $({
                let len = $exp.len();
                let cols = ColumnType::$ty
                    .create_column_from_values(stringify!($ty).to_string(), $exp.clone().into_iter().map(|i| i.into()).collect(), len, &get_config())
                    .unwrap();
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].dtype(), &DataType::$ty);
                assert_eq!(cols[0].len(), len);
            })+
        };
    }

    #[test]
    fn test_empty_column_creation() {
        test_empty_column!(Boolean UInt32 UInt64 Int32 Int64 Float32 Float64 String Binary);
        let cols = ColumnType::Hex.create_empty_columns("Hex", &get_config());
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].dtype(), &DataType::String);
        assert_eq!(cols[0].len(), 0);

        let cols = ColumnType::Decimal128.create_empty_columns("Decimal128", &get_config());
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].dtype(), &DataType::Binary);
        assert_eq!(cols[0].len(), 0);

        let cols = ColumnType::UInt256.create_empty_columns("UInt256", &get_config());
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols.iter().map(|c| c.dtype().clone()).collect::<Vec<_>>(),
            vec![DataType::Binary, DataType::String, DataType::Float64]
        );
        assert_eq!(
            cols.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
            vec![
                "UInt256_u256binary".to_string(),
                "UInt256_string".to_string(),
                "UInt256_f64".to_string()
            ]
        );
        assert!(cols.iter().all(|c| c.len() == 0));

        let cols = ColumnType::Int256.create_empty_columns("Int256", &get_config());
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols.iter().map(|c| c.dtype().clone()).collect::<Vec<_>>(),
            vec![DataType::Binary, DataType::String, DataType::Float64]
        );
        assert_eq!(
            cols.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
            vec![
                "Int256_i256binary".to_string(),
                "Int256_string".to_string(),
                "Int256_f64".to_string()
            ]
        );
        assert!(cols.iter().all(|c| c.len() == 0));
    }

    #[test]
    fn test_column_creation() {
        test_column!(
            Boolean => vec![true, false, true],
            UInt32 => vec![1u32, 2, 3],
            UInt64 => vec![1u64, 2, 3],
            Int32 => vec![-1i32, 0, 1],
            Int64 => vec![-1i64, 0, 1],
            // Float32 => vec![1.0, 2.0, 3.0],
            // Float64 => vec![1.0, 2.0, 3.0],
            String => vec!["a".to_string(), "b".to_string(), "c".to_string()],
            Binary => vec![vec![1, 2], vec![3, 4], vec![5, 6]],
        );
    }
}
