use bevy_ecs::prelude::*;
use bevy_ecs::ptr::OwningPtr;

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ptr::NonNull;
pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &mut World, Entity) -> Result<(), String>;
pub type DynBuilderFn =
    for<'a> fn(&serde_json::Value, &'a bumpalo::Bump) -> Result<OwningPtr<'a>, String>;
macro_rules! gen_export {
    (full, $t:ty) => {
        |world, entity| {
            world
                .entity(entity)
                .get::<$t>()
                .and_then(|t| serde_json::to_value(t).ok())
        }
    };
    (placeholder, $t:ty) => {
        |_, _| Some(serde_json::Value::Null)
    };
    (full, $t:ty, $t1:ty) => {
        |world, entity| {
            world
                .entity(entity)
                .get::<$t>()
                .and_then(|t| serde_json::to_value(<$t1>::from(t)).ok())
        }
    };
    (placeholder, $t:ty, $t1:ty) => {
        |_, _| Some(serde_json::Value::Null)
    };
}

macro_rules! gen_import {
    (full,$t:ty) => {
        |val, world, entity| {
            let name = short_type_name::<$t>();
            serde_json::from_value::<$t>(val.clone())
                .map_err(|e| format!("Deserialization error for {}:{}", name, e))
                .map(|v| {
                    world.entity_mut(entity).insert(v);
                })
                .map(|_| ())
        }
    };
    (placeholder,$t:ty) => {
        |_, world, entity| {
            world.entity_mut(entity).insert(<$t>::default());
            Ok(())
        }
    };
    (emplace, $t:ty) => {
        |_, world, entity| {
            if !world.entity(entity).contains::<$t>() {
                world.entity_mut(entity).insert(<$t>::default());
            }
            Ok(())
        }
    };
    (full, $t:ty, $t1:ty) => {
        |val, world, entity| {
            let name = short_type_name::<$t>();
            serde_json::from_value::<$t1>(val.clone())
                .map_err(|e| format!("Deserialization error for {}:{}", name, e))
                .map(|v| {
                    world.entity_mut(entity).insert::<$t>(v.into());
                })
                .map(|_| ())
        }
    };
    (placeholder, $t:ty, $t1:ty) => {
        |_, world, entity| {
            world
                .entity_mut(entity)
                .insert(Into::<$t>::into(<$t1>::default()));
            Ok(())
        }
    };
    (emplace, $t:ty, $t1:ty) => {
        |_, world, entity| {
            if !world.entity(entity).contains::<$t>() {
                world
                    .entity_mut(entity)
                    .insert(Into::<$t>::into(<$t1>::default()));
            }
            Ok(())
        }
    };
}
fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

macro_rules! gen_ctor {
    (full, $t:ty) => {
        |val, bump| {
            let name = short_type_name::<$t>();
            let component: $t = serde_json::from_value(val.clone())
                .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
            let ptr = bump.alloc(component) as *mut $t;
            Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
        }
    };
    (placeholder,$t:ty) => {
        |_val, bump| {
            let ptr = bump.alloc(<$t>::default()) as *mut $t;
            Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
        }
    };
    (full, $t:ty, $t1:ty) => {
        |val, bump| {
            let name = short_type_name::<$t>();
            let component: $t1 = serde_json::from_value(val.clone())
                .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
            let ptr = bump.alloc(Into::<$t>::into(component)) as *mut $t;
            Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
        }
    };
    (placeholder, $t:ty, $t1:ty) => {
        |_, bump| {
            let ptr = bump.alloc(Into::<$t>::into(<$t1>::default())) as *mut $t;
            Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
        }
    };
}
macro_rules! gen_all {
    // case: no wrapper
    (emplace, $t:ty) => {
        (
            gen_export!(full, $t),
            gen_import!(emplace, $t),
            gen_ctor!(placeholder, $t),
        )
    };
    // case: no wrapper
    ($mode:ident, $t:ty) => {
        (
            gen_export!($mode, $t),
            gen_import!($mode, $t),
            gen_ctor!($mode, $t),
        )
    };

    // case: with wrapper
    (emplace, $t:ty, $t1:ty) => {
        (
            gen_export!(full, $t, $t1),
            gen_import!(emplace, $t, $t1),
            gen_ctor!(placeholder, $t, $t1),
        )
    };
    // case: with wrapper
    ($mode:ident, $t:ty, $t1:ty) => {
        (
            gen_export!($mode, $t, $t1),
            gen_import!($mode, $t, $t1),
            gen_ctor!($mode, $t, $t1),
        )
    };
}
type SnapshotTuple = (ExportFn, ImportFn, DynBuilderFn);
impl JsonValueCodec {
    #[inline]
    fn from_tuple(parts: SnapshotTuple) -> Self {
        let (export, import, dyn_ctor) = parts;
        JsonValueCodec {
            export,
            import,
            dyn_ctor,
        }
    }
}

macro_rules! build_snapshot {
    ($t:ty, $mode:expr,  $parts:expr) => {
        JsonValueCodec::from_tuple($parts)
    };
}

macro_rules! gen_all_full         { ($($t:tt)+) => { gen_all!(full, $($t)+) }; }
macro_rules! gen_all_placeholder { ($($t:tt)+) => { gen_all!(placeholder, $($t)+) }; }
macro_rules! gen_all_emplace     { ($($t:tt)+) => { gen_all!(emplace, $($t)+) }; }

macro_rules! gen_and_build {
    ($t:ty, $mode:expr, $gen_macro:ident) => {{
        let parts: SnapshotTuple = $gen_macro!($t);
        build_snapshot!($t, $mode, parts)
    }};
    ($t:ty, $t1:ty, $mode:expr, $gen_macro:ident) => {{
        let parts: SnapshotTuple = $gen_macro!($t, $t1);
        build_snapshot!($t, $mode, parts)
    }};
}
use crate::prelude::SnapshotMode;
macro_rules! make_snapshot_factory {
    (T = $t:ty) => {{ gen_and_build!($t, SnapshotMode::Full, gen_all_full) }};
    (T = $t:ty, mode = $mode:expr) => {{
        match $mode {
            SnapshotMode::Full => gen_and_build!($t, $mode, gen_all_full),
            SnapshotMode::Placeholder => gen_and_build!($t, $mode, gen_all_placeholder),
            SnapshotMode::PlaceholderEmplaceIfNotExists => {
                gen_and_build!($t, $mode, gen_all_emplace)
            }
        }
    }};
    (T = $t:ty, T1 = $t1:ty) => {{ gen_and_build!($t, $t1, SnapshotMode::Full, gen_all_full) }};
    (T = $t:ty, T1 = $t1:ty, mode = $mode:expr) => {{
        match $mode {
            SnapshotMode::Full => gen_and_build!($t, $t1, $mode, gen_all_full),
            SnapshotMode::Placeholder => gen_and_build!($t, $t1, $mode, gen_all_placeholder),
            SnapshotMode::PlaceholderEmplaceIfNotExists => {
                gen_and_build!($t, $t1, $mode, gen_all_emplace)
            }
        }
    }};
}

#[derive(Clone, Debug)]
pub struct JsonValueCodec {
    pub export: ExportFn,
    pub import: ImportFn,
    pub dyn_ctor: DynBuilderFn,
}
impl JsonValueCodec {
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
        make_snapshot_factory!(T = T)
    }
    pub fn new_with_wrapper_full<T, T1>() -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        make_snapshot_factory!(T = T, T1 = T1)
    }
    pub fn new_with_wrapper<T, T1>(mode: SnapshotMode) -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        make_snapshot_factory!(T = T, T1 = T1, mode = mode)
    }

    pub fn with_mode<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        make_snapshot_factory!(T = T, mode = mode)
    }
}

pub struct BincodeCodec;
