use bevy_ecs::ptr::OwningPtr;
use bevy_ecs::{component::ComponentId, prelude::*};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_arrow::marrow::array::Array;
use serde_arrow::marrow::datatypes::Field;
use serde_arrow::schema::SchemaLike;
use serde_arrow::schema::TracingOptions;
use serde_json::Value;
use std::ptr::NonNull;
pub type ArrExportFn = fn(&Vec<Field>, &World, &[Entity]) -> Result<ArrowColumn, String>;
pub type ArrImportFn = fn(&Vec<Field>, &ArrowColumn, &mut World, &[Entity]) -> Result<(), String>;

pub type ArrDynFn =
    fn(&Vec<Field>, &ArrowColumn, &mut World, &[Entity]) -> Result<RawTData, String>;

pub type ArrowToJsonFn = fn(&ArrowColumn) -> Result<Vec<serde_json::Value>, String>;
pub type JsonToArrowFn = fn(&Vec<Field>, &Vec<serde_json::Value>) -> Result<ArrowColumn, String>;

#[derive(Default, Clone, Debug)]
pub struct ArrowColumn {
    pub fields: Vec<Field>,
    pub data: Vec<Array>,
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
    pub schema: Vec<Field>,
}
impl ArrowColumn {
    pub fn to_vec<T>(&self) -> Result<Vec<T>, String>
    where
        T: for<'de> Deserialize<'de>,
    {
        let d: Vec<_> = self.data.iter().map(|x| x.as_view()).collect();
        serde_arrow::from_marrow(&self.fields, &d).map_err(|e| e.to_string())
    }
    pub fn from_slice_option<T>(v: &[T], fields: &[Field]) -> Result<Self, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let data = serde_arrow::to_marrow(&fields, v).unwrap();
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
        let data = serde_arrow::to_marrow(&fields, v).unwrap();
        Ok(Self { fields, data })
    }
}

pub trait JsonConversion {
    fn from_json<T>(json: &Vec<Value>, fields: Option<&[Field]>) -> Result<Self, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
        Self: Sized;
    fn to_json<T>(&self) -> Result<Vec<serde_json::Value>, String>
    where
        T: for<'de> Deserialize<'de> + Serialize;
}
impl JsonConversion for ArrowColumn {
    fn from_json<T>(json: &Vec<Value>, fields: Option<&[Field]>) -> Result<Self, String>
    where
        T: for<'de> Deserialize<'de> + Serialize,
        Self: Sized,
    {
        let binding = Vec::<Field>::default_schema::<T>();
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
    fn default_schema<'de, T: Deserialize<'de>>() -> Vec<Field> {
        Vec::from_type::<T>(TracingOptions::default()).unwrap()
    }
    fn default_null_schema<'de, T: Deserialize<'de>>() -> Vec<Field> {
        let a = TracingOptions::default();
        Vec::from_type::<T>(a.allow_null_fields(true)).unwrap()
    }
}

macro_rules! gen_import {
    (full,$t:ty) => {
        |fields, arr, world, entities| {
            let d = arr.data.iter().map(|x| x.as_view()).collect::<Vec<_>>();
            let batch: Vec<$t> = serde_arrow::from_marrow(&fields, &d).unwrap();
            let batch = entities.iter().zip(batch.into_iter()).map(|(a, b)| (*a, b));
            world.insert_batch(batch);

            Ok(())
        }
    };
    (placeholder,$t:ty) => {
        |fields, arr, world, entities| {
            let d = arr.data.iter().map(|x| x.as_view()).collect::<Vec<_>>();
            let batch: Vec<T> = serde_arrow::from_marrow(&fields, &d).unwrap();
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
            let d = arr.data.iter().map(|x| x.as_view()).collect::<Vec<_>>();
            let batch: Vec<$t> = serde_arrow::from_marrow(&fields, &d).unwrap();
            let batch = entities.iter().zip(batch.into_iter()).map(|(a, b)| (*a, b));
            world.insert_batch_if_new(batch);

            Ok(())
        }
    };
}

impl DefaultSchema for Vec<Field> {}

impl ArrowSnapshotExtension {
    pub fn new_full<T>() -> Self
    where
        T: Serialize + DeserializeOwned + Component,
    {
        let schema = Vec::<Field>::default_schema::<T>();
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
            SnapshotMode::Full => Vec::<Field>::default_schema::<T>(),
            SnapshotMode::Placeholder => Vec::<Field>::default_null_schema::<()>(),
            SnapshotMode::PlaceholderEmplaceIfNotExists => {
                Vec::<Field>::default_null_schema::<()>()
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
            let data = serde_arrow::to_marrow(&fields, v).unwrap();
            Ok(ArrowColumn {
                fields: fields.clone(),
                data: data,
            })
        },
        _ => |fields, _world, entities| {
            let v: Vec<()> = entities.iter().map(|_x| ()).collect();
            let data = serde_arrow::to_marrow(&fields, v).unwrap();
            Ok(ArrowColumn {
                fields: fields.clone(),
                data: data,
            })
        },
    };
    arr_export
}
