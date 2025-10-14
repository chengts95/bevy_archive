use std::{marker::PhantomData, sync::Arc};

use crate::{
    binary_archive::arrow_column::ArrowColumn,
    flecs_registry::snapshot_factory::codec::ComponentAccess,
    prelude::vec_snapshot_factory::DefaultSchema,
};
use arrow::{
    array::Array,
    datatypes::{Field, FieldRef},
};
use flecs_ecs::{prelude::*, sys::ecs_set_id};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_arrow::{marrow, utils::Item};

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("missing factory for component/resource: {0}")]
    MissingFactory(String),
    #[error("serde error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("arrow error: {0}")]
    ArrowError(#[from] marrow::error::MarrowError),
    #[error("parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),
    #[error("failed to resolve entity id: {0}")]
    InvalidEntityID(u32),
    #[error("unexpected null component {0}")]
    MissingComponent(String),
    #[error("generic error: {0}")]
    Generic(String),
    #[error("generic error: {0}")]
    GenericBox(#[from] Box<dyn std::error::Error>),
}

pub type ArrExportFn = fn(&[FieldRef], &World, &[Entity]) -> Result<ArrowColumn, SnapshotError>;
pub type ArrImportFn = fn(&ArrowColumn, &World, &[Entity]) -> Result<(), SnapshotError>;
fn export_full<T>() -> ArrExportFn
where
    T: Serialize + DeserializeOwned + ComponentId,
{
    let arr_export: ArrExportFn = |fields, world, entities| {
        let v: Vec<_> = entities
            .iter()
            .map(|x| {
                world.get_data_ref::<T>(*x).ok_or_else(|| {
                    SnapshotError::MissingComponent(std::any::type_name::<T>().to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let data = serialize_data(fields, v)?;
        Ok(ArrowColumn {
            fields: fields.to_vec(),
            data,
        })
    };
    arr_export
}

fn import_full<T>() -> ArrImportFn
where
    T: Serialize + DeserializeOwned + ComponentId,
{
    let arr_import: ArrImportFn = |arrow, world, entities| {
        let id = T::id(world.get_world());
        let data: Vec<T> = deserialize_data(arrow)?;
        // this should be called in defer section
        entities
            .iter()
            .map(|x| *x)
            .zip(data.into_iter())
            .into_iter()
            .for_each(|(entity, data)| {
                let size = std::mem::size_of::<T>();
                unsafe {
                    ecs_set_id(
                        world.world_ptr() as *mut _,
                        *entity,
                        id,
                        size,
                        (&data as *const T).cast(),
                    )
                };
            });

        Ok(())
    };
    arr_import
}

fn export_wrapper<T, T1>() -> ArrExportFn
where
    T: ComponentId,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T>,
{
    let arr_export: ArrExportFn = |fields, world, entities| {
        let v: Vec<_> = entities
            .iter()
            .map(|x| {
                world.get_data_ref::<T>(*x).ok_or_else(|| {
                    SnapshotError::MissingComponent(std::any::type_name::<T>().to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let v: Vec<T1> = v.into_iter().map(|x| T1::from(x)).collect();
        let data = serailize_data_owned(fields, v)?;
        Ok(ArrowColumn {
            fields: fields.to_vec(),
            data,
        })
    };
    arr_export
}

fn import_wrapper<T, T1>() -> ArrImportFn
where
    T: ComponentId + From<T1>,
    T1: Serialize + DeserializeOwned,
{
    let arr_import: ArrImportFn = |arrow, world, entities| {
        let id = T::id(world.get_world());
        let data: Vec<T1> = deserialize_data(arrow)?;
        // this should be called in defer section
        entities
            .iter()
            .map(|x| *x)
            .zip(data.into_iter())
            .into_iter()
            .for_each(|(entity, data)| {
                let data = T::from(data);
                let size = std::mem::size_of::<T>();
                unsafe {
                    ecs_set_id(
                        world.world_ptr() as *mut _,
                        *entity,
                        id,
                        size,
                        (&data as *const T).cast(),
                    )
                };
            });

        Ok(())
    };
    arr_import
}
#[derive(Clone, Debug)]
pub struct ArrowSnapshotFactory {
    pub arr_export: ArrExportFn,
    pub arr_import: ArrImportFn,
    pub schema: Vec<FieldRef>,
}
impl ArrowSnapshotFactory {
    pub fn new<T>() -> Self
    where
        T: Serialize + for<'a> Deserialize<'a> + ComponentId,
    {
        let schema: Vec<FieldRef> =
            <Vec<FieldRef> as DefaultSchema>::default_schema::<T>().to_vec();
        Self {
            arr_export: export_full::<T>(),
            arr_import: import_full::<T>(),
            schema,
        }
    }

    pub fn new_with<T, T1>() -> Self
    where
        T: ComponentId + From<T1>,
        T1: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a T>,
    {
        let schema: Vec<FieldRef> =
            <Vec<FieldRef> as DefaultSchema>::default_schema::<T1>().to_vec();
        Self {
            arr_export: export_wrapper::<T, T1>(),
            arr_import: import_wrapper::<T, T1>(),
            schema,
        }
    }
}
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct TagHolder<T> {
    pub item: bool,
    _p: PhantomData<T>,
}
impl<T> From<&T> for TagHolder<T> {
    fn from(_value: &T) -> Self {
        TagHolder::default()
    }
}

impl<T> Default for TagHolder<T> {
    fn default() -> Self {
        Self {
            item: true,
            _p: PhantomData::default(),
        }
    }
}

fn serailize_data_owned<T>(
    fields: &[Arc<Field>],
    v: Vec<T>,
) -> Result<Vec<Arc<dyn Array>>, Box<dyn std::error::Error>>
where
    T: Serialize + DeserializeOwned,
{
    let data = serde_arrow::to_arrow(&fields, &v);
    let data = match data {
        Ok(data) => data,
        Err(_error) => serde_arrow::to_arrow(
            &fields,
            v.iter()
                .map(|x| serde_arrow::utils::Item(x))
                .collect::<Vec<_>>(),
        )?,
    };
    Ok(data)
}
fn serialize_data<T>(
    fields: &[Arc<Field>],
    v: Vec<&T>,
) -> Result<Vec<Arc<dyn Array>>, SnapshotError>
where
    T: Serialize + DeserializeOwned,
{
    serde_arrow::to_arrow(fields, &v)
        .or_else(|_| {
            let items: Vec<_> = v.iter().map(|x| serde_arrow::utils::Item(x)).collect();
            serde_arrow::to_arrow(fields, &items)
        })
        .map_err(|x| SnapshotError::GenericBox(x.into()))
}

fn deserialize_data<T>(arrow: &ArrowColumn) -> Result<Vec<T>, SnapshotError>
where
    T: DeserializeOwned,
{
    let data = arrow.to_vec::<T>();
    let data: Vec<T> = match data {
        Ok(data) => data,
        Err(_e) => arrow
            .to_vec::<Item<T>>()
            .map_err(|x| SnapshotError::Generic(x.to_string()))?
            .into_iter()
            .map(|x| x.0)
            .collect(),
    };
    Ok(data)
}
