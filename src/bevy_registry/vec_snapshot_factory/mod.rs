use std::sync::Arc;

use bevy_ecs::prelude::*;
use bevy_ecs::ptr::OwningPtr;

use arrow::datatypes::{DataType, Field, FieldRef};

use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_arrow::schema::TracingOptions;
use serde_arrow::utils::Item;
use serde_json::Value;

mod factory;
use crate::binary_archive::arrow_column::ArrowColumn;
use crate::binary_archive::arrow_column::JsonConversion;
use crate::prelude::SnapshotMode;
pub use factory::ArrowSnapshotFactory;
pub use factory::SnapshotError;

pub type ArrowToJsonFn = fn(&ArrowColumn) -> Result<Vec<serde_json::Value>, String>;
pub type JsonToArrowFn = fn(&[FieldRef], &Vec<serde_json::Value>) -> Result<ArrowColumn, String>;

pub trait DefaultSchema {
    fn default_schema<'de, T: Deserialize<'de>>() -> Vec<FieldRef> {
        let ret: Result<Vec<FieldRef>, _> = Vec::from_type::<T>(TracingOptions::default());
        match ret {
            Ok(fields) => fields,
            Err(_e) => Vec::from_type::<Item<T>>(TracingOptions::default().allow_null_fields(true))
                .unwrap_or(Vec::new()),
        }
    }
    fn forced_null_schema<'de, T: Deserialize<'de>>(mode: SnapshotMode) -> Vec<FieldRef> {
        let field = Field::new("item", DataType::Boolean, true);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("mode".to_string(), serde_json::to_string(&mode).unwrap());
        let field = field.with_metadata(metadata);
        Vec::from(vec![Arc::new(field)])
    }
    fn with_null_schema<'de, T: Deserialize<'de>>() -> Vec<FieldRef> {
        let a = TracingOptions::default();
        Vec::from_type::<T>(a.allow_null_fields(true)).unwrap()
    }
}
impl JsonConversion for ArrowColumn {
    fn from_json<T>(
        json: &Vec<Value>,
        fields: Option<&[FieldRef]>,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
        Self: Sized,
    {
        let binding = Vec::<FieldRef>::default_schema::<T>();
        let fields = fields.unwrap_or(&binding);
        let data: Vec<T> =
            serde_json::from_value::<Vec<T>>(serde_json::Value::Array(json.to_vec()))
                .map_err(|x| x.to_string())?;
        let a = ArrowColumn::from_slice_option(&data, fields);
        a
    }

    fn to_json<T>(&self) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let items: Vec<T> = self.to_vec()?;
        let v: Vec<Value> = items
            .iter()
            .map(|x| serde_json::to_value(x))
            .collect::<Result<_, _>>()?;
        Ok(v)
    }
}
