use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};

use bevy_ecs::{component::ComponentId, prelude::*};

use arrow::datatypes::FieldRef;
use parquet::arrow::ArrowWriter;

use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_arrow::schema::TracingOptions;
use serde_json::Value;

use crate::prelude::ArenaBox;

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
