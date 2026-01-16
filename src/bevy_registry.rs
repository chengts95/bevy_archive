use bevy_ecs::ptr::{Aligned, OwningPtr, PtrMut};
use bevy_ecs::{component::ComponentId, prelude::*};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::TypeId;
use std::collections::HashMap;
use std::ptr::NonNull;
mod snapshot_factory;
#[cfg(feature = "arrow_rs")]
pub mod vec_snapshot_factory;
pub use snapshot_factory::*;

use crate::prelude::codec::JsonValueCodec;

//this is a workaround
//it allows to have a type erased box that can drop the inner type correctly
//it must be dropped manually or it will leak memory.
pub struct ArenaBox<'a> {
    pub ptr: OwningPtr<'a, Aligned>,
    pub drop_fn: unsafe fn(OwningPtr<'a, Aligned>),
}
impl<'a> ArenaBox<'a> {
    pub fn new<T>(ptr: OwningPtr<'a, Aligned>) -> Self {
        Self {
            ptr,
            drop_fn: |ptr| unsafe {
                ptr.drop_as::<T>();
            },
        }
    }
    pub fn manual_drop(self) {
        unsafe { (self.drop_fn)(self.ptr) }
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
    pub fn get_ptr_mut(&mut self) -> PtrMut<'a> {
        // SAFETY: We have &mut self, so we have exclusive access to the owning ptr and its data.
        unsafe { PtrMut::new(NonNull::new_unchecked(self.ptr.as_ptr() as *mut u8)) }
    }
}

pub struct IDRemapRegistry {
    pub hooks: HashMap<TypeId, Box<dyn Fn(PtrMut, &dyn EntityRemapper) + Send + Sync>>,
}

impl Default for IDRemapRegistry {
    fn default() -> Self {
        Self {
            hooks: HashMap::new(),
        }
    }
}

impl IDRemapRegistry {
    pub fn register_remap_hook<T: Component>(
        &mut self,
        hook: impl Fn(&mut T, &dyn EntityRemapper) + 'static + Send + Sync,
    ) {
        self.hooks.insert(
            TypeId::of::<T>(),
            Box::new(move |ptr, mapper| {
                // ptr is PtrMut
                let val = unsafe { ptr.deref_mut::<T>() };
                hook(val, mapper);
            }),
        );
    }

    pub fn get_hook(
        &self,
        type_id: TypeId,
    ) -> Option<&(dyn Fn(PtrMut, &dyn EntityRemapper) + Send + Sync)> {
        self.hooks.get(&type_id).map(|b| b.as_ref())
    }
}

pub trait EntityRemapper {
    fn map(&self, old_id: u32) -> Entity;
}

impl EntityRemapper for HashMap<u32, Entity> {
    fn map(&self, old_id: u32) -> Entity {
        *self.get(&old_id).unwrap_or(&Entity::PLACEHOLDER)
    }
}

use crate::bevy_cmdbuffer::HarvardCommandBuffer;

pub struct DeferredEntityBuilder<'w> {
    buffer: &'w mut HarvardCommandBuffer,
    entity: Entity,
}

impl<'w> DeferredEntityBuilder<'w> {
    pub fn new(buffer: &'w mut HarvardCommandBuffer, entity: Entity) -> Self {
        Self { buffer, entity }
    }
    
    pub fn insert<T: Component>(&mut self, _world: &mut World, _value: T) {
         // This method signature is problematic because we need ComponentId.
         // And HarvardCommandBuffer expects ArenaBox.
         // The original insert took `value: T` and `world` (implicitly via self.world).
         // It used self.bump to alloc.
         unimplemented!("Use insert_by_id with ArenaBox");
    }
    
    pub fn insert_by_id(&mut self, id: ComponentId, ptr: ArenaBox<'_>) {
        self.buffer.insert_box(self.entity, id, ptr);
    }
    
    pub fn insert_if_new_by_id(&mut self, world: &World, id: ComponentId, ptr: ArenaBox<'_>) {
         if world.entity(self.entity).contains_id(id) {
            ptr.manual_drop();
            return;
        }
        self.insert_by_id(id, ptr);
    }
    
    pub fn commit(self) {
        // No-op, buffer handles it on flush/apply
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
    pub fn register<T>(&mut self)
    where
        T: Serialize + DeserializeOwned + Component + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.entries
            .insert(name, SnapshotFactory::new::<T>(SnapshotMode::Full));
    }
    pub fn register_with_name<T, T1>(&mut self, name: &'static str)
    where
        T: Component + From<T1>,
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
        T: Component + From<T1>,
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
        self.entries
            .insert(name, SnapshotFactory::new::<T>(SnapshotMode::Full));
    }
    pub fn register_with<T, T1>(&mut self)
    where
        T: Component + From<T1>,
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
        T: Serialize + DeserializeOwned + Component + Default + 'static,
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

impl SnapshotRegistry {
    pub fn get_res_factory(&self, name: &str) -> Option<&SnapshotFactory> {
        self.resource_entries.get(name)
    }

    pub fn resource_register<T: Resource + Serialize + DeserializeOwned>(&mut self) {
        let mode = SnapshotMode::Full;
        let factory = SnapshotFactory {
            js_value: JsonValueCodec {
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
                    Ok(ArenaBox::new::<T>(unsafe {
                        OwningPtr::new(NonNull::new_unchecked(ptr.cast()))
                    }))
                },
            },

            comp_id: |world| world.resource_id::<T>(),
            register: |world| world.register_resource::<T>(),
            mode,
            #[cfg(feature = "arrow_rs")]
            arrow: None,
        };
        self.resource_entries
            .insert(short_type_name::<T>(), factory);
    }
}
