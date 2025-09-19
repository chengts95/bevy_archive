use std::ptr::NonNull;

use crate::prelude::{SnapshotMode, vec_snapshot_factory::*};
use arrow::{array::Array, datatypes::FieldRef};
use serde::de::DeserializeOwned;
use serde_arrow::marrow;
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
pub type ArrImportFn = fn(&ArrowColumn, &mut World, &[Entity]) -> Result<(), SnapshotError>;
pub type ArrDynFn = for<'a> fn(
    &ArrowColumn,
    &'a bumpalo::Bump,
    &World,
) -> Result<Vec<OwningPtr<'a>>, SnapshotError>;

impl DefaultSchema for Vec<FieldRef> {}
#[derive(Clone, Debug)]
pub struct ArrowSnapshotFactory {
    pub arr_export: ArrExportFn,
    pub arr_import: ArrImportFn,
    pub arr_dyn: ArrDynFn,
    pub schema: Vec<FieldRef>,
}
impl ArrowSnapshotFactory {
    pub fn new_with<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        Self::with_mode::<T>(mode)
    }
    pub fn new<T>() -> Self
    where
        T: Serialize + DeserializeOwned + Component + 'static,
    {
        let arr_export = build_export_full::<T>();
        let arr_import = build_import_full::<T>();
        let arr_dyn = build_dyn_ctor_full::<T>();
        let schema: Vec<FieldRef> =
            <Vec<FieldRef> as DefaultSchema>::default_schema::<T>().to_vec();

        Self {
            arr_export,
            arr_import,
            arr_dyn,
            schema,
        }
    }
    pub fn new_with_wrapper_full<T, T1>() -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let arr_export = build_export_wrapper_full::<T, T1>();
        let arr_import = build_import_wrapper_full::<T, T1>();
        let arr_dyn = build_dyn_ctor_wrapper_full::<T, T1>();
        let schema: Vec<FieldRef> =
            <Vec<FieldRef> as DefaultSchema>::default_schema::<T1>().to_vec();

        Self {
            arr_export,
            arr_import,
            arr_dyn,
            schema,
        }
    }
    pub fn new_with_wrapper<T, T1>(mode: SnapshotMode) -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        let arr_export = build_export_wrapper::<T, T1>(mode);
        let arr_import = build_import_wrapper::<T, T1>(mode);
        let arr_dyn = build_dyn_ctor_wrapper::<T, T1>(mode);
        let schema: Vec<FieldRef> =
            <Vec<FieldRef> as DefaultSchema>::default_schema::<T1>().to_vec();

        Self {
            arr_export,
            arr_import,
            arr_dyn,
            schema,
        }
    }

    pub fn with_mode<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        let arr_export = build_export::<T>(mode);
        let arr_import = build_import::<T>(mode);
        let arr_dyn = build_dyn_ctor::<T>(mode);
        let schema: Vec<FieldRef> =
            <Vec<FieldRef> as DefaultSchema>::default_schema::<T>().to_vec();

        Self {
            arr_export,
            arr_import,
            arr_dyn,
            schema,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct TagHolder {
    item: bool,
}
impl Default for TagHolder {
    fn default() -> Self {
        Self { item: true }
    }
}
fn build_import_full<T>() -> ArrImportFn
where
    T: Serialize + DeserializeOwned + Component,
{
    let arr_import: ArrImportFn = |arrow, world, entities| {
        let data: Vec<T> = deserialize_data(arrow)?;
        let temp_data: Vec<(Entity, T)> =
            entities.iter().map(|x| *x).zip(data.into_iter()).collect();
        world.insert_batch(temp_data);
        Ok(())
    };
    arr_import
}
fn build_import<T>(mode: SnapshotMode) -> ArrImportFn
where
    T: Serialize + DeserializeOwned + Component + Default,
{
    let arr_import: ArrImportFn = match mode {
        SnapshotMode::Full => build_import_full::<T>(),
        SnapshotMode::Placeholder => |_arrow, world, entities| {
            let temp_data: Vec<(Entity, T)> = entities.iter().map(|x| (*x, T::default())).collect();
            world.insert_batch(temp_data);
            Ok(())
        },
        SnapshotMode::PlaceholderEmplaceIfNotExists => |_arrow, world, entities| {
            let temp_data: Vec<(Entity, T)> = entities
                .iter()
                .filter_map(|x| {
                    let not_appeared = world.get::<T>(*x).is_none();
                    if not_appeared {
                        Some((*x, T::default()))
                    } else {
                        None
                    }
                })
                .collect();
            world.insert_batch(temp_data);
            Ok(())
        },
    };
    arr_import
}

fn build_import_wrapper_full<T, T1>() -> ArrImportFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
{
    let arr_import: ArrImportFn = |arrow, world, entities| {
        let data: Vec<T1> = deserialize_data(arrow)?;
        let data = data.into_iter().map(|x| x.into());
        let temp_data: Vec<(Entity, T)> =
            entities.iter().map(|x| *x).zip(data.into_iter()).collect();
        world.insert_batch(temp_data);
        Ok(())
    };

    arr_import
}
fn build_import_wrapper<T, T1>(mode: SnapshotMode) -> ArrImportFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T> + Default,
{
    let arr_import: ArrImportFn = match mode {
        SnapshotMode::Full => build_import_wrapper_full::<T, T1>(),
        SnapshotMode::Placeholder => |_arrow, world, entities| {
            let temp_data = entities
                .into_iter()
                .map(|x| (*x, Into::<T>::into(T1::default())));

            world.insert_batch(temp_data);
            Ok(())
        },
        SnapshotMode::PlaceholderEmplaceIfNotExists => |_arrow, world, entities| {
            let temp_data: Vec<_> = entities
                .into_iter()
                .filter_map(|x| {
                    let not_appeared = world.get::<T>(*x).is_none();
                    if not_appeared {
                        Some((*x, Into::<T>::into(T1::default())))
                    } else {
                        None
                    }
                })
                .collect();

            world.insert_batch(temp_data);
            Ok(())
        },
    };

    arr_import
}

fn build_export_full<T>() -> ArrExportFn
where
    T: Serialize + DeserializeOwned + Component,
{
    let arr_export: ArrExportFn = |fields, world, entities| {
        let v: Vec<_> = entities
            .iter()
            .map(|x| {
                world.get::<T>(*x).ok_or_else(|| {
                    SnapshotError::MissingComponent(std::any::type_name::<T>().to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let data = serailize_data(fields, v)?;
        Ok(ArrowColumn {
            fields: fields.to_vec(),
            data,
        })
    };
    arr_export
}

fn serailize_data<T>(
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
fn build_export<T>(mode: SnapshotMode) -> ArrExportFn
where
    T: Serialize + DeserializeOwned + Component + Default,
{
    let arr_export: ArrExportFn = match mode {
        SnapshotMode::Full => build_export_full::<T>(),
        _ => |_fields, _world, entities| {
            let fields = &<Vec<FieldRef> as DefaultSchema>::default_schema::<TagHolder>();
            let v: Vec<_> = entities.iter().map(|_x| TagHolder::default()).collect();
            let data = serailize_data_owned(fields, v)?;
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
        let data = serailize_data_owned(fields, v)?;
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
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
{
    let arr_export: ArrExportFn = match mode {
        SnapshotMode::Full => build_export_wrapper_full::<T, T1>(),
        _ => |_fields, _world, entities| {
            let fields = &<Vec<FieldRef> as DefaultSchema>::default_schema::<TagHolder>();
            let v: Vec<_> = entities.iter().map(|_x| TagHolder::default()).collect();
            let data = serailize_data_owned(fields, v)?;
            Ok(ArrowColumn {
                fields: fields.to_vec(),
                data: data,
            })
        },
    };

    arr_export
}

fn build_dyn_ctor_full<T>() -> ArrDynFn
where
    T: Serialize + DeserializeOwned + Component,
{
    let arr_dyn_ctor: ArrDynFn = |arrow, bump, _world| {
        let data = deserialize_data(arrow)?;
        let data = data
            .into_iter()
            .map(|component| {
                let ptr = bump.alloc(component) as *mut T;
                unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
            })
            .collect();
        Ok(data)
    };
    arr_dyn_ctor
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

fn build_dyn_ctor<T>(mode: SnapshotMode) -> ArrDynFn
where
    T: Serialize + DeserializeOwned + Component + Default,
{
    let arr_dyn_ctor: ArrDynFn = match mode {
        SnapshotMode::Full => build_dyn_ctor_full::<T>(),
        _ => |arrow, bump, _world| {
            let length = arrow.data.len();
            let data = [0..length]
                .into_iter()
                .map(|_| {
                    let ptr = bump.alloc(T::default()) as *mut T;
                    unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
                })
                .collect();
            Ok(data)
        },
    };
    arr_dyn_ctor
}
fn build_dyn_ctor_wrapper_full<T, T1>() -> ArrDynFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
{
    let arr_dyn_ctor: ArrDynFn = |arrow, bump, _world| {
        let data: Vec<T1> = deserialize_data(arrow)?;
        let data = data
            .into_iter()
            .map(|component| {
                let ptr = bump.alloc(Into::<T>::into(component)) as *mut T;
                unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
            })
            .collect();
        Ok(data)
    };
    arr_dyn_ctor
}

fn build_dyn_ctor_wrapper<T, T1>(mode: SnapshotMode) -> ArrDynFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T> + Default,
{
    let arr_dyn_ctor: ArrDynFn = match mode {
        SnapshotMode::Full => build_dyn_ctor_wrapper_full::<T, T1>(),
        _ => |arrow, bump, _world| {
            let length = arrow.data.len();
            let data = [0..length]
                .into_iter()
                .map(|_| {
                    let ptr = bump.alloc(Into::<T>::into(T1::default())) as *mut T;
                    unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
                })
                .collect();
            Ok(data)
        },
    };
    arr_dyn_ctor
}
