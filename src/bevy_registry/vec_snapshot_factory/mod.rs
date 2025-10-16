use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};

use bevy_ecs::ptr::OwningPtr;
use bevy_ecs::{component::ComponentId, prelude::*};

use arrow::datatypes::{DataType, Field, FieldRef};
use parquet::arrow::ArrowWriter;

use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_arrow::schema::TracingOptions;
use serde_arrow::utils::Item;
use serde_json::Value;

mod factory;
use crate::prelude::{SnapshotMode, ArenaBox};
pub use factory::ArrowSnapshotFactory;
pub use factory::SnapshotError;

pub type ArrowToJsonFn = fn(&ArrowColumn) -> Result<Vec<serde_json::Value>, String>;
pub type JsonToArrowFn = fn(&[FieldRef], &Vec<serde_json::Value>) -> Result<ArrowColumn, String>;

#[derive(Default, Clone, Debug)]
pub struct ArrowColumn {
    pub fields: Vec<FieldRef>,
    pub data: Vec<ArrayRef>,
}

pub struct RawTData<'a> {
    pub comp_id: ComponentId,
    pub data: Vec<ArenaBox<'a>>,
}

pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

impl ArrowColumn {
    pub fn to_arrow(&self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Build the record batch
        let arrow_fields = self.fields.clone();

        let arrow_arrays = self.data.clone();

        let record_batch = arrow::array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(arrow_fields)),
            arrow_arrays,
        );
        Ok(record_batch?)
    }
    pub fn to_parquet(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = self.to_arrow()?;
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }
    // pub fn parse_parquet<T>(v: &[u8]) -> Result<Vec<T>, Box<dyn std::error::Error>>
    // where
    //     T: for<'de> Deserialize<'de>,
    // {
    //     let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(v)?
    //         .with_batch_size(8192)
    //         .build()?;
    //     let mut batches = Vec::new();

    //     for batch in parquet_reader {
    //         batches.push(batch?);
    //     }
    //     let d = batches[0];
    //     let d: Vec<T> = serde_arrow::from_record_batch(&d)?;
    //     // let fields = schema
    //     //     .fields()
    //     //     .iter()
    //     //     .map(serde_arrow::marrow::datatypes::Field::try_from)
    //     //     .collect::<Result<Vec<_>, _>>()?;

    //     Ok(d)
    // }

    pub fn to_vec<T>(&self) -> Result<Vec<T>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let data: Vec<T> = serde_arrow::from_arrow(&self.fields, &self.data)?;
        Ok(data)
    }
    pub fn from_slice_option<T>(
        v: &[T],
        fields: &[FieldRef],
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let data = serde_arrow::to_arrow(&fields, v)?;
        Ok(Self {
            fields: fields.to_vec(),
            data,
        })
    }
    pub fn from_slice<T>(v: &[T]) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let fields = Vec::from_type::<T>(TracingOptions::default())?;
        let data = serde_arrow::to_arrow(&fields, v)?;
        Ok(Self { fields, data })
    }
}

pub trait JsonConversion {
    fn from_json<T>(
        json: &Vec<Value>,
        fields: Option<&[FieldRef]>,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de> + Serialize,
        Self: Sized;
    fn to_json<T>(&self) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de> + Serialize;
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
