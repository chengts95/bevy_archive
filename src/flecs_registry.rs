use crate::flecs_registry::snapshot_factory::SnapshotFactory;
use crate::flecs_registry::snapshot_factory::SnapshotMode;
use flecs_ecs::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::TypeId;
use std::collections::HashMap;
pub mod snapshot_factory;

pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}
pub trait SnapshotMerge {
    fn merge_only_new(&mut self, other: &Self);
    fn merge(&mut self, other: &Self);
}
#[derive(Clone, Default, Debug)]
pub struct SnapshotRegistry {
    pub type_registry: HashMap<&'static str, TypeId>,
    pub entries: HashMap<&'static str, SnapshotFactory>,
    pub resource_entries: HashMap<&'static str, SnapshotFactory>,
}
impl SnapshotMerge for SnapshotRegistry {
    fn merge_only_new(&mut self, other: &Self) {
        for (name, type_id) in &other.type_registry {
            self.type_registry.entry(*name).or_insert(*type_id);
        }
        for (name, factory) in &other.entries {
            self.entries.entry(*name).or_insert_with(|| factory.clone());
        }
        for (name, factory) in &other.resource_entries {
            self.resource_entries
                .entry(*name)
                .or_insert_with(|| factory.clone());
        }
    }

    fn merge(&mut self, other: &Self) {
        for (name, type_id) in &other.type_registry {
            self.type_registry.insert(*name, *type_id);
        }
        for (name, factory) in &other.entries {
            self.entries.insert(*name, factory.clone());
        }
        for (name, factory) in &other.resource_entries {
            self.resource_entries.insert(*name, factory.clone());
        }
    }
}

impl SnapshotRegistry {
    pub fn register<T>(&mut self)
    where
        T: Serialize + DeserializeOwned + ComponentId + DataComponent + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new::<T>(SnapshotMode::Full));
    }
    pub fn register_with_name<T, T1>(&mut self, name: &'static str)
    where
        T: ComponentId + From<T1> + DataComponent,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T>,
    {
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries.insert(
            name,
            SnapshotFactory::new_with_wrapper::<T, T1>(SnapshotMode::Full),
        );
    }
    pub fn register_with_name_mode<T, T1>(&mut self, name: &'static str, mode: SnapshotMode)
    where
        T: ComponentId + DataComponent + From<T1>,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new_with_wrapper::<T, T1>(mode));
    }
    pub fn register_named<T>(&mut self, name: &'static str)
    where
        T: ComponentId + DataComponent + Serialize + DeserializeOwned,
    {
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new::<T>(SnapshotMode::Full));
    }
    pub fn register_with<T, T1>(&mut self)
    where
        T: ComponentId + DataComponent + From<T1>,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries.insert(
            name,
            SnapshotFactory::new_with_wrapper::<T, T1>(SnapshotMode::Full),
        );
    }
    pub fn register_with_mode<T>(&mut self, mode: SnapshotMode)
    where
        T: Serialize + DeserializeOwned + ComponentId + DataComponent + Default + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries.insert(name, SnapshotFactory::new::<T>(mode));
    }

    pub fn get_factory(&self, name: &str) -> Option<&SnapshotFactory> {
        self.entries.get(name)
    }
    pub fn get_factory_mut(&mut self, name: &str) -> Option<&mut SnapshotFactory> {
        self.entries.get_mut(name)
    }
    pub fn comp_id_by_name(&self, name: &str, world: &World) -> Option<u64> {
        self.entries
            .get(name)
            .and_then(|entry| (entry.comp_id)(world))
    }

    pub fn reg_by_name(&self, name: &str, world: &mut World) -> u64 {
        (self.entries.get(name).unwrap().register)(world)
    }

    pub fn comp_id<T>(&self, world: &World) -> Option<u64> {
        let name = short_type_name::<T>();
        self.entries
            .get(name)
            .and_then(|entry| (entry.comp_id)(world))
    }
}

impl SnapshotRegistry {
    // pub fn get_res_factory(&self, name: &str) -> Option<&SnapshotFactory> {
    //     self.resource_entries.get(name)
    // }

    // pub fn resource_register<T:   Serialize + DeserializeOwned>(&mut self) {
    //     let mode = SnapshotMode::Full;
    //     let factory = SnapshotFactory {
    //         js_value: JsonValueCodec {
    //             export: |world, _| {
    //                 world
    //                     .get_resource::<T>()
    //                     .map(|r| serde_json::to_value(r).unwrap())
    //             },
    //             import: |value, world, _| match serde_json::from_value::<T>(value.clone()) {
    //                 Ok(resource) => {
    //                     world.insert_resource(resource);
    //                     Ok(())
    //                 }
    //                 Err(e) => Err(format!("Deserialization error: {}", e)),
    //             },
    //             dyn_ctor: |val, bump| {
    //                 let name = short_type_name::<T>();
    //                 let component: T = serde_json::from_value(val.clone())
    //                     .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
    //                 let ptr = bump.alloc(component) as *mut T;
    //                 Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
    //             },
    //         },

    //         comp_id: |world| world.resource_id::<T>(),
    //         register: |world| world.register_resource::<T>(),
    //         mode,
    //         #[cfg(feature = "arrow_rs")]
    //         arrow: None,
    //     };
    //     self.resource_entries
    //         .insert(short_type_name::<T>(), factory);
    // }
}
