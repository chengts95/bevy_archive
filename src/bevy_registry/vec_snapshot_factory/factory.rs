use crate::prelude::{CompIdFn, CompRegFn, vec_snapshot_factory::*};
use arrow::datatypes::FieldRef;
use serde::de::DeserializeOwned;

pub type ArrExportFn = fn(&[FieldRef], &World, &[Entity]) -> Result<ArrowColumn, String>;
pub type ArrImportFn = fn(&[FieldRef], &ArrowColumn, &mut World, &[Entity]) -> Result<(), String>;

pub type ArrDynFn = for<'a> fn(
    &ArrowColumn,
    &'a bumpalo::Bump,
    &mut World,
    &[Entity],
) -> Result<RawTData<'a>, String>;

impl DefaultSchema for Vec<FieldRef> {}
#[derive(Clone)]
pub struct ArrowSnapshotFactory {
    pub arr_export: ArrExportFn,
    pub arr_import: ArrImportFn,
    pub schema: Vec<FieldRef>,
}

macro_rules! gen_export {
    (full, $t:ty) => {
        build_export_default::<$t>()
    };
    ($mode:ident, $t:ty) => {
        build_export_defaul::<$t>($mode)
    };
    (full, $t:ty, $t1:ty) => {
        build_export_default::<$t>()
    };
    ($mode:ident, $t:ty, $t1:ty) => {
        build_export_wrapper::<$t>($mode)
    };
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

impl ArrowSnapshotFactory {}

fn build_export_full<T>() -> ArrExportFn
where
    T: Serialize + DeserializeOwned + Component,
{
    let arr_export: ArrExportFn = |fields, world, entities| {
        let v: Vec<_> = entities
            .iter()
            .map(|x| world.get::<T>(*x).unwrap())
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
    T: Serialize + DeserializeOwned + Component + Default,
{
    let arr_export: ArrExportFn = match mode {
        SnapshotMode::Full => build_export_full::<T>(),
        _ => |fields, _world, entities| {
            let v: Vec<_> = entities.iter().map(|_x| ()).collect();
            let data = serde_arrow::to_arrow(&fields, v).unwrap();
            Ok(ArrowColumn {
                fields: fields.to_vec(),
                data: data,
            })
        },
    };
    arr_export
}
fn build_export_wrapper_full<T, T1>() -> ArrExportFn
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
fn build_export_wrapper<T, T1>(mode: SnapshotMode) -> ArrExportFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>  ,
{
    let arr_export: ArrExportFn = match mode {
        SnapshotMode::Full => build_export_wrapper_full::<T, T1>(),
        _ => |fields, _world, entities| {
            let v: Vec<_> = entities.iter().map(|_x|  ()).collect();
            let data = serde_arrow::to_arrow(&fields, v).unwrap();
            Ok(ArrowColumn {
                fields: fields.to_vec(),
                data: data,
            })
        },
    };

    arr_export
}
