use bevy_ecs::ptr::OwningPtr;
use bevy_ecs::{component::ComponentId, prelude::*};

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ptr::NonNull;

use crate::prelude::codec::JsonValueCodec;
pub mod codec;
pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &mut World, Entity) -> Result<(), String>;
pub type CompIdFn = fn(&World) -> Option<ComponentId>;
pub type CompRegFn = fn(&mut World) -> ComponentId;
pub type DynBuilderFn =
    for<'a> fn(&serde_json::Value, &'a bumpalo::Bump) -> Result<OwningPtr<'a>, String>;

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

#[derive(Clone, Debug)]
pub struct SnapshotFactory {
    pub js_value: JsonValueCodec,
    pub comp_id: CompIdFn,
    pub register: CompRegFn,
    pub mode: SnapshotMode,
}

type SnapshotTuple = JsonValueCodec;
impl SnapshotFactory {
    #[inline]
    fn from_mode_tuple(
        mode: SnapshotMode,
        comp_id: CompIdFn,
        register: CompRegFn,
        parts: SnapshotTuple,
    ) -> Self {
        let js_value = parts;
        SnapshotFactory {
            js_value,
            mode,
            comp_id,
            register,
        }
    }
}

macro_rules! build_common {
    ($t:ty ) => {
        (SnapshotFactory::component_id::<$t>, |world| {
            world.register_component::<$t>()
        })
    };
}

impl SnapshotFactory {
    #[inline]
    fn component_id<T: Component>(world: &World) -> Option<ComponentId> {
        world.component_id::<T>()
    }

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
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::new::<T>();
        SnapshotFactory::from_mode_tuple(SnapshotMode::Full, comp_id, register, js)
    }
    pub fn new_with_wrapper_full<T, T1>() -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::new_with_wrapper_full::<T, T1>();
        SnapshotFactory::from_mode_tuple(SnapshotMode::Full, comp_id, register, js)
    }
    pub fn new_with_wrapper<T, T1>(mode: SnapshotMode) -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::new_with_wrapper::<T, T1>(mode);
        SnapshotFactory::from_mode_tuple(mode, comp_id, register, js)
    }

    pub fn with_mode<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::with_mode::<T>(mode);
        SnapshotFactory::from_mode_tuple(mode, comp_id, register, js)
    }
}
