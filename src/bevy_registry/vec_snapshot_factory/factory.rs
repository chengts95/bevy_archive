use crate::prelude::{CompIdFn, CompRegFn, vec_snapshot_factory::*};
use arrow::datatypes::FieldRef;
use serde::de::DeserializeOwned;

pub type ArrExportFn = fn(&[FieldRef], &World, &[Entity]) -> Result<ArrowColumn, String>;
pub type ArrImportFn = fn(&[FieldRef], &ArrowColumn, &mut World, &[Entity]) -> Result<(), String>;

pub type ArrDynFn =
    fn(&[FieldRef], &ArrowColumn, &mut World, &[Entity]) -> Result<RawTData, String>;

impl DefaultSchema for Vec<FieldRef> {}
#[derive(Clone)]
pub struct ArrowSnapshotFactory {
    pub arr_export: ArrExportFn,
    pub arr_import: ArrImportFn,
    pub schema: Vec<FieldRef>,
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

impl ArrowSnapshotFactory {
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

        ArrowSnapshotFactory {
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
        ArrowSnapshotFactory {
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

        ArrowSnapshotFactory {
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
