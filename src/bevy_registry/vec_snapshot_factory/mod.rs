use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::error::ArrowError;
use bevy_ecs::ptr::OwningPtr;
use bevy_ecs::{component::ComponentId, prelude::*};

use arrow::datatypes::{Field, FieldRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_arrow::schema::TracingOptions;
use serde_json::Value;
pub type ArrExportFn = fn(&[FieldRef], &World, &[Entity]) -> Result<ArrowColumn, String>;
pub type ArrImportFn = fn(&[FieldRef], &ArrowColumn, &mut World, &[Entity]) -> Result<(), String>;

pub type ArrDynFn =
    fn(&[FieldRef], &ArrowColumn, &mut World, &[Entity]) -> Result<RawTData, String>;

pub type ArrowToJsonFn = fn(&ArrowColumn) -> Result<Vec<serde_json::Value>, String>;
pub type JsonToArrowFn = fn(&[FieldRef], &Vec<serde_json::Value>) -> Result<ArrowColumn, String>;

#[derive(Default, Clone, Debug)]
pub struct ArrowColumn {
    pub fields: Vec<FieldRef>,
    pub data: Vec<ArrayRef>,
}

#[derive(Clone)]
pub struct RawTData {
    pub comp_id: ComponentId,
    pub data: Vec<u8>,
}
pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

#[derive(Default, Debug, Clone, Copy)]
pub enum SnapshotMode {
    #[default]
    Full,
    Placeholder,
    PlaceholderEmplaceIfNotExists,
}

#[derive(Clone)]
pub struct ArrowSnapshotExtension {
    pub arr_export: ArrExportFn,
    pub arr_import: ArrImportFn,
    pub schema: Vec<FieldRef>,
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

    pub fn to_vec<T>(&self) -> Result<Vec<T>, String>
    where
        T: for<'de> Deserialize<'de>,
    {
    
        serde_arrow::from_arrow(&self.fields, &self.data).map_err(|e| e.to_string())
    }
    pub fn from_slice_option<T>(v: &[T], fields: &[FieldRef]) -> Result<Self, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let data = serde_arrow::to_arrow(&fields, v).unwrap();
        Ok(Self {
            fields: fields.to_vec(),
            data,
        })
    }
    pub fn from_slice<T>(v: &[T]) -> Result<Self, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let fields = Vec::from_type::<T>(TracingOptions::default()).unwrap();
        let data = serde_arrow::to_arrow(&fields, v).unwrap();
        Ok(Self { fields, data })
    }
}

pub trait JsonConversion {
    fn from_json<T>(json: &Vec<Value>, fields: Option<&[FieldRef]>) -> Result<Self, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
        Self: Sized;
    fn to_json<T>(&self) -> Result<Vec<serde_json::Value>, String>
    where
        T: for<'de> Deserialize<'de> + Serialize;
}
impl JsonConversion for ArrowColumn {
    fn from_json<T>(json: &Vec<Value>, fields: Option<&[FieldRef]>) -> Result<Self, String>
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

    fn to_json<T>(&self) -> Result<Vec<serde_json::Value>, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let items: Vec<T> = self.to_vec()?;
        let v = items
            .iter()
            .map(|x| serde_json::to_value(x).unwrap())
            .collect();
        Ok(v)
    }
}
trait DefaultSchema {
    fn default_schema<'de, T: Deserialize<'de>>() -> Vec<FieldRef> {
        Vec::from_type::<T>(TracingOptions::default()).unwrap()
    }
    fn default_null_schema<'de, T: Deserialize<'de>>() -> Vec<FieldRef> {
        let a = TracingOptions::default();
        Vec::from_type::<T>(a.allow_null_fields(true)).unwrap()
    }
}

macro_rules! gen_import {
    (full,$t:ty) => {
        |fields, arr, world, entities| {
            let d = arr.data.as_slice();
            let batch: Vec<$t> = serde_arrow::from_arrow(&fields, &d).unwrap();
            let batch = entities.iter().zip(batch.into_iter()).map(|(a, b)| (*a, b));
            world.insert_batch(batch);

            Ok(())
        }
    };
    (placeholder,$t:ty) => {
        |fields, arr, world, entities| {
            let d = arr.data.as_slice();
            let batch: Vec<$t> = serde_arrow::from_arrow(&fields, &d).unwrap();
            let batch = entities
                .iter()
                .zip(batch.iter().map(|_| T::default()))
                .map(|(a, b)| (*a, b));
            world.insert_batch(batch);

            Ok(())
        }
    };
    (emplace, $t:ty) => {
        |fields, arr, world, entities| {
            let d = arr.data.as_slice();
            let batch: Vec<$t> = serde_arrow::from_arrow(&fields, &d).unwrap();
            let batch = entities.iter().zip(batch.into_iter()).map(|(a, b)| (*a, b));
            world.insert_batch_if_new(batch);

            Ok(())
        }
    };
}

impl DefaultSchema for Vec<FieldRef> {}

impl ArrowSnapshotExtension {
    pub fn new_with_wrapper<T, T1>() -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let schema = Vec::<FieldRef>::default_schema::<T1>();
        let arr_export = build_export_wrapper::<T, T1>();
        let arr_import: ArrImportFn = |fields, arr, world, entities| {
            let d = &arr.data;
            let batch: Vec<T1> = serde_arrow::from_arrow(&fields, &d).unwrap();
            let batch = entities
                .iter()
                .zip(batch.into_iter())
                .map(|(a, b)| (*a, Into::<T>::into(b)));
            world.insert_batch(batch);

            Ok(())
        };

        ArrowSnapshotExtension {
            arr_export,
            arr_import,
            schema,
        }
    }
    pub fn new_full<T>() -> Self
    where
        T: Serialize + DeserializeOwned + Component,
    {
        let schema = Vec::<FieldRef>::default_schema::<T>();
        let arr_export = build_export::<T>(SnapshotMode::Full);
        let arr_import: ArrImportFn = gen_import!(full, T);
        ArrowSnapshotExtension {
            arr_export,
            arr_import,
            schema,
        }
    }
    pub fn new<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default,
    {
        let schema = match mode {
            SnapshotMode::Full => Vec::<FieldRef>::default_schema::<T>(),
            SnapshotMode::Placeholder => Vec::<FieldRef>::default_null_schema::<()>(),
            SnapshotMode::PlaceholderEmplaceIfNotExists => {
                Vec::<FieldRef>::default_null_schema::<()>()
            }
        };
        let arr_export = build_export::<T>(mode);
        let arr_import: ArrImportFn = match mode {
            SnapshotMode::Full => gen_import!(full, T),
            SnapshotMode::Placeholder => gen_import!(placeholder, T),
            SnapshotMode::PlaceholderEmplaceIfNotExists => gen_import!(emplace, T),
        };

        ArrowSnapshotExtension {
            arr_export,
            arr_import,
            schema,
        }
    }
}
fn build_export_wrapper<T, T1>() -> ArrExportFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
{
    let arr_export: ArrExportFn = |fields, world, entities| {
        let v: Vec<T1> = entities
            .iter()
            .map(|x| T1::from(world.get::<T>(*x).unwrap()))
            .collect();
        let data = serde_arrow::to_arrow(&fields, v).unwrap();
        Ok(ArrowColumn {
            fields: fields.to_vec(),
            data: data,
        })
    };

    arr_export
}

fn build_export<T>(mode: SnapshotMode) -> ArrExportFn
where
    T: Serialize + DeserializeOwned + Component,
{
    let arr_export: ArrExportFn = match mode {
        SnapshotMode::Full => |fields, world, entities| {
            let v: Vec<_> = entities
                .iter()
                .map(|x| world.get::<T>(*x).unwrap())
                .collect();
            let data = serde_arrow::to_arrow(&fields, v).unwrap();
            Ok(ArrowColumn {
                fields: fields.to_vec(),
                data: data,
            })
        },
        _ => |fields, _world, entities| {
            let v: Vec<()> = entities.iter().map(|_x| ()).collect();
            let data = serde_arrow::to_arrow(&fields, v).unwrap();
            Ok(ArrowColumn {
                fields: fields.to_vec(),
                data: data,
            })
        },
    };
    arr_export
}
