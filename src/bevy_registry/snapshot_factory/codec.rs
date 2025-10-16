use std::ptr::NonNull;

use bevy_ecs::prelude::*;
use bevy_ecs::ptr::OwningPtr;

use serde::{Deserialize, Serialize};

use crate::prelude::ArenaBox;
pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &mut World, Entity) -> Result<(), String>;
pub type DynBuilderFn =
    for<'a> fn(&serde_json::Value, &'a bumpalo::Bump) -> Result<ArenaBox<'a>, String>;

fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

#[derive(Clone, Debug)]
pub struct JsonValueCodec {
    pub export: ExportFn,
    pub import: ImportFn,
    pub dyn_ctor: DynBuilderFn,
}

fn export<T>(world: &World, entity: Entity) -> Option<serde_json::Value>
where
    T: Serialize + Component,
{
    world
        .entity(entity)
        .get::<T>()
        .and_then(|t| serde_json::to_value(t).ok())
}

fn import<T>(val: &serde_json::Value, world: &mut World, entity: Entity) -> Result<(), String>
where
    T: for<'a> Deserialize<'a> + Component,
{
    let name = short_type_name::<T>();
    serde_json::from_value::<T>(val.clone())
        .map_err(|e| format!("Deserialization error for {}:{}", name, e))
        .map(|v| {
            world.entity_mut(entity).insert(v);
        })
        .map(|_| ())
}
fn dyn_ctor<'a, T>(val: &serde_json::Value, bump: &'a bumpalo::Bump) -> Result<ArenaBox<'a>, String>
where
    T: Serialize + for<'de> Deserialize<'de> + Component,
{
    let name = short_type_name::<T>();
    let component: T = serde_json::from_value(val.clone())
        .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
    let ptr = bump.alloc(component) as *mut T;
    Ok(unsafe { ArenaBox::new::<T>(OwningPtr::new(NonNull::new_unchecked(ptr.cast()))) })
}
fn export_wrapper<T, T1>(world: &World, entity: Entity) -> Option<serde_json::Value>
where
    T: Component,
    T1: Serialize + for<'a> From<&'a T>,
{
    world
        .entity(entity)
        .get::<T>()
        .and_then(|t| serde_json::to_value(T1::from(t)).ok())
}

fn import_wrapper<T, T1>(
    val: &serde_json::Value,
    world: &mut World,
    entity: Entity,
) -> Result<(), String>
where
    T: Component + From<T1>,
    T1: for<'a> Deserialize<'a> + for<'a> From<&'a T>,
{
    let name = short_type_name::<T>();
    serde_json::from_value::<T1>(val.clone())
        .map_err(|e| format!("Deserialization error for {}:{}", name, e))
        .map(|v| {
            world.entity_mut(entity).insert(T::from(v));
        })
        .map(|_| ())
}
fn dyn_ctor_wrapper<'a, T, T1>(
    val: &serde_json::Value,
    bump: &'a bumpalo::Bump,
) -> Result<ArenaBox<'a>, String>
where
    T: Component + From<T1>,
    T1: Serialize + for<'de> Deserialize<'de> + for<'b> From<&'b T>,
{
    let name = short_type_name::<T>();
    let component: T1 = serde_json::from_value(val.clone())
        .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
    let ptr = bump.alloc(T::from(component)) as *mut T;
    Ok(unsafe { ArenaBox::new::<T>(OwningPtr::new(NonNull::new_unchecked(ptr.cast()))) })
}

impl JsonValueCodec {
    pub fn new<T>() -> Self
    where
        T: Serialize + for<'a> Deserialize<'a> + Component,
    {
        Self {
            export: export::<T>,
            import: import::<T>,
            dyn_ctor: dyn_ctor::<T>,
        }
    }

    pub fn new_with<T, T1>() -> Self
    where
        T: Component + From<T1>,
        T1: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a T>,
    {
        Self {
            export: export_wrapper::<T, T1>,
            import: import_wrapper::<T, T1>,
            dyn_ctor: dyn_ctor_wrapper::<T, T1>,
        }
    }
}

pub struct BincodeCodec;
