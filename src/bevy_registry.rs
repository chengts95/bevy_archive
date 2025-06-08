use bevy_ecs::ptr::{Aligned, OwningPtr};
use bevy_ecs::{component::ComponentId, prelude::*};
use bumpalo::Bump;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::TypeId;
use std::collections::HashMap;
use std::ptr::NonNull;
mod snapshot_factory;
pub use snapshot_factory::*;

pub struct DeferredEntityBuilder<'a> {
    world: &'a mut World,
    entity: Entity,
    ids: Vec<ComponentId>,
    ptrs: Vec<OwningPtr<'a, Aligned>>,
    bump: &'a Bump,
}

impl<'a> DeferredEntityBuilder<'a> {
    pub fn new(world: &'a mut World, bump: &'a Bump, entity: Entity) -> Self {
        Self {
            world,
            entity,
            ids: vec![],
            ptrs: vec![],
            bump,
        }
    }
    pub fn insert<T: Component>(&mut self, value: T) {
        let id = self
            .world
            .component_id::<T>()
            .unwrap_or_else(|| self.world.register_component::<T>());
        let ptr = self.bump.alloc(value) as *mut T;
        let ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) };
        self.insert_by_id(id, ptr);
    }
    pub fn insert_if_new_by_id(&mut self, id: ComponentId, ptr: OwningPtr<'a>) {
        if self.world.entity(self.entity).contains_id(id) {
            return;
        }
        self.insert_by_id(id, ptr);
    }
    pub fn insert_by_id(&mut self, id: ComponentId, ptr: OwningPtr<'a>) {
        if let Some(i) = self.ids.iter().position(|&existing| existing == id) {
            self.ptrs[i] = ptr; // replace old value
        } else {
            self.ids.push(id);
            self.ptrs.push(ptr);
        }
    }

    pub fn commit(mut self) {
        let mut entity = self.world.entity_mut(self.entity);
        unsafe { entity.insert_by_ids(&self.ids, self.ptrs.drain(..)) };
    }
}
pub trait SnapshotMerge {
    fn merge_only_new(&mut self, other: &Self);
    fn merge(&mut self, other: &Self);
}
#[derive(Resource, Clone, Default, Debug)]
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
    pub fn resource_register<T: Resource + Serialize + DeserializeOwned>(&mut self) {
        let mode = SnapshotMode::Full;
        let factory = SnapshotFactory {
            export: |world, _| {
                world
                    .get_resource::<T>()
                    .map(|r| serde_json::to_value(r).unwrap())
            },
            import: |value, world, _| match serde_json::from_value::<T>(value.clone()) {
                Ok(resource) => {
                    world.insert_resource(resource);
                    Ok(())
                }
                Err(e) => Err(format!("Deserialization error: {}", e)),
            },
            dyn_ctor: |val, bump| {
                let name = short_type_name::<T>();
                let component: T = serde_json::from_value(val.clone())
                    .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
                let ptr = bump.alloc(component) as *mut T;
                Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
            },
            comp_id: |world| world.resource_id::<T>(),
            register: |world| world.register_resource::<T>(),
            mode,
        };
        self.resource_entries
            .insert(short_type_name::<T>(), factory);
    }
    pub fn register<T>(&mut self)
    where
        T: Serialize + DeserializeOwned + Component + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries.insert(name, SnapshotFactory::new::<T>());
    }
    pub fn register_with_name<T, T1>(&mut self, name: &'static str)
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new_with_wrapper_full::<T, T1>());
    }
    pub fn register_with_name_mode<T, T1>(&mut self, name: &'static str, mode: SnapshotMode)
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new_with_wrapper::<T, T1>(mode));
    }
    pub fn register_named<T>(&mut self, name: &'static str)
    where
        T: Component + Serialize + DeserializeOwned,
    {
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries.insert(name, SnapshotFactory::new::<T>());
    }
    pub fn register_with<T, T1>(&mut self)
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new_with_wrapper_full::<T, T1>());
    }
    pub fn register_with_mode<T>(&mut self, mode: SnapshotMode)
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::with_mode::<T>(mode));
    }

    pub fn get_factory(&self, name: &str) -> Option<&SnapshotFactory> {
        self.entries.get(name)
    }
    pub fn get_res_factory(&self, name: &str) -> Option<&SnapshotFactory> {
        self.resource_entries.get(name)
    }

    pub fn comp_id_by_name(&self, name: &str, world: &World) -> Option<ComponentId> {
        self.entries
            .get(name)
            .and_then(|entry| (entry.comp_id)(world))
    }

    pub fn reg_by_name(&self, name: &str, world: &mut World) -> ComponentId {
        (self.entries.get(name).unwrap().register)(world)
    }

    pub fn comp_id<T>(&self, world: &World) -> Option<ComponentId> {
        let name = short_type_name::<T>();
        self.entries
            .get(name)
            .and_then(|entry| (entry.comp_id)(world))
    }
}
