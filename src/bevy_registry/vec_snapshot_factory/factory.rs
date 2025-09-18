use std::ptr::NonNull;

use crate::prelude::{SnapshotMode, vec_snapshot_factory::*};
use arrow::datatypes::FieldRef;
use serde::de::DeserializeOwned;

pub type ArrExportFn = fn(&[FieldRef], &World, &[Entity]) -> Result<ArrowColumn, String>;
pub type ArrImportFn = fn(&ArrowColumn, &mut World, &[Entity]) -> Result<(), String>;

pub type ArrDynFn =
    for<'a> fn(&ArrowColumn, &'a bumpalo::Bump, &World) -> Result<RawTData<'a>, String>;

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
        let data: Vec<T> = arrow.to_vec().map_err(|x| x.to_string())?;
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
        let data: Vec<T1> = arrow.to_vec().map_err(|x| x.to_string())?;
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
            let v: Vec<_> = entities.iter().map(|_x| TagHolder::default()).collect();
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
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
{
    let arr_export: ArrExportFn = match mode {
        SnapshotMode::Full => build_export_wrapper_full::<T, T1>(),
        _ => |fields, _world, entities| {
            let v: Vec<_> = entities.iter().map(|_x| TagHolder::default()).collect();
            let data = serde_arrow::to_arrow(&fields, v).unwrap();
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
    let arr_dyn_ctor: ArrDynFn = |arrow, bump, world| {
        let comp_id = world.component_id::<T>().unwrap();

        let data: Vec<T> = arrow.to_vec().unwrap();
        let data = data
            .into_iter()
            .map(|component| {
                let ptr = bump.alloc(component) as *mut T;
                unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
            })
            .collect();
        Ok(RawTData { comp_id, data })
    };
    arr_dyn_ctor
}

fn build_dyn_ctor<T>(mode: SnapshotMode) -> ArrDynFn
where
    T: Serialize + DeserializeOwned + Component + Default,
{
    let arr_dyn_ctor: ArrDynFn = match mode {
        SnapshotMode::Full => build_dyn_ctor_full::<T>(),
        _ => |arrow, bump, world| {
            let comp_id = world.component_id::<T>().unwrap();

            let length = arrow.data.len();
            let data = [0..length]
                .into_iter()
                .map(|_| {
                    let ptr = bump.alloc(T::default()) as *mut T;
                    unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
                })
                .collect();
            Ok(RawTData { comp_id, data })
        },
    };
    arr_dyn_ctor
}
fn build_dyn_ctor_wrapper_full<T, T1>() -> ArrDynFn
where
    T: Component,
    T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
{
    let arr_dyn_ctor: ArrDynFn = |arrow, bump, world| {
        let comp_id = world.component_id::<T>().unwrap();

        let data: Vec<T1> = arrow.to_vec().unwrap();
        let data = data
            .into_iter()
            .map(|component| {
                let ptr = bump.alloc(Into::<T>::into(component)) as *mut T;
                unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
            })
            .collect();
        Ok(RawTData { comp_id, data })
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
        _ => |arrow, bump, world| {
            let comp_id = world.component_id::<T>().unwrap();

            let length = arrow.data.len();
            let data = [0..length]
                .into_iter()
                .map(|_| {
                    let ptr = bump.alloc(Into::<T>::into(T1::default())) as *mut T;
                    unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) }
                })
                .collect();
            Ok(RawTData { comp_id, data })
        },
    };
    arr_dyn_ctor
}
