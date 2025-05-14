use bevy_ecs::ptr::{Aligned, OwningPtr};
use bevy_ecs::{component::ComponentId, prelude::*};
use bumpalo::Bump;
use std::any::TypeId;
use std::collections::HashMap;
use std::ptr::NonNull;

pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &mut World, Entity) -> Result<(), String>;
pub type CompIdFn = fn(&World) -> Option<ComponentId>;
pub type CompRegFn = fn(&mut World) -> ComponentId;
pub type DynBuilderFn =
    for<'a> fn(&serde_json::Value, &'a bumpalo::Bump) -> Result<OwningPtr<'a>, String>;

#[derive(Default, Debug, Clone, Copy)]
pub enum SnapshotMode {
    #[default]
    Full,
    Placeholder,
    PlaceholderEmplaceIfNotExists,
}

#[derive(Resource, Default, Debug)]
pub struct SnapshotRegistry {
    pub exporters: HashMap<&'static str, ExportFn>,
    pub importers: HashMap<&'static str, ImportFn>,
    pub type_registry: HashMap<&'static str, TypeId>,
    pub component_id: HashMap<&'static str, CompIdFn>,
    pub component_register: HashMap<&'static str, CompRegFn>,
    pub mode: HashMap<&'static str, SnapshotMode>,
    pub dyn_ctors: HashMap<&'static str, DynBuilderFn>,
}
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

impl SnapshotRegistry {
    #[inline]
    fn register_component<T: Component>(world: &World) -> Option<ComponentId> {
        world.component_id::<T>()
    }
    fn common_init<T: Component>(&mut self) {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.component_id
            .insert(name, Self::register_component::<T>);
        self.component_id
            .insert(name, Self::register_component::<T>);
        self.component_register
            .insert(name, |world| world.register_component::<T>());
    }
    pub fn register_with<T, T1>(&mut self)
    where
        T: Component + 'static,
        T1: serde::Serialize + serde::de::DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let name = short_type_name::<T>();
        self.common_init::<T>();
        self.exporters.insert(name, |world, entity| {
            world
                .entity(entity)
                .get::<T>()
                .and_then(|t| serde_json::to_value(T1::from(t)).ok())
        });

        self.importers.insert(name, |val, world, entity| {
            let val = serde_json::from_value::<T1>(val.clone()).map_err(|e| {
                format!(
                    "Deserialization error for {}:{} ",
                    short_type_name::<T>(),
                    e
                )
            })?;

            world.entity_mut(entity).insert(val.into());
            Ok(())
        });
        self.dyn_ctors.insert(name, |val, bump| {
            //   let components = world.components();

            // SAFETY: This component exists because it is present on the archetype.
            let component: T1 = serde_json::from_value(val.clone()).map_err(|e| {
                format!(
                    "Deserialization error for {}:{} ",
                    short_type_name::<T>(),
                    e
                )
            })?;
            let component: T = component.into();
            // SAFETY: bump alloc is properly aligned and has the right layout
            let ptr = bump.alloc(component) as *mut T;
            let ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) };
            Ok(ptr)
        });
    }
    pub fn register<T>(&mut self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Component + 'static,
    {
        let name = short_type_name::<T>();
        self.common_init::<T>();
        self.exporters.insert(name, |world, entity| {
            // any.downcast_ref::<T>()
            //     .and_then(|t| serde_json::to_value(t).ok())
            world
                .entity(entity)
                .get::<T>()
                .and_then(|t| serde_json::to_value(t).ok())
        });

        self.importers.insert(name, |val, world, entity| {
            let val = serde_json::from_value::<T>(val.clone()).map_err(|e| {
                format!(
                    "Deserialization error for {}:{} ",
                    short_type_name::<T>(),
                    e
                )
            })?;

            world.entity_mut(entity).insert(val);
            Ok(())
        });
        self.dyn_ctors.insert(name, |val, bump| {
            //   let components = world.components();

            // SAFETY: This component exists because it is present on the archetype.
            let component: T = serde_json::from_value(val.clone()).map_err(|e| {
                format!(
                    "Deserialization error for {}:{} ",
                    short_type_name::<T>(),
                    e
                )
            })?;
            // SAFETY: bump alloc is properly aligned and has the right layout
            let ptr = bump.alloc(component) as *mut T;
            let ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) };
            Ok(ptr)
        });
    }

    pub fn register_with_flag<T>(&mut self, mode: SnapshotMode)
    where
        T: Default + serde::Serialize + serde::de::DeserializeOwned + Component + 'static,
    {
        let name = short_type_name::<T>();
        self.common_init::<T>();
        self.mode.insert(name, mode);
        match mode {
            SnapshotMode::Full => {
                self.register::<T>();
            }
            SnapshotMode::Placeholder => {
                self.exporters
                    .insert(name, |_, _| Some(serde_json::Value::Null));
                self.importers.insert(name, |_, w, e| {
                    w.entity_mut(e).insert(T::default());
                    Ok(())
                });
            }
            SnapshotMode::PlaceholderEmplaceIfNotExists => {
                self.exporters
                    .insert(name, |_, _| Some(serde_json::Value::Null));
                self.importers.insert(name, |_, w, e| {
                    if !w.entity(e).contains::<T>() {
                        w.entity_mut(e).insert(T::default());
                    }
                    Ok(())
                });
            }
        }
        self.dyn_ctors.insert(name, |_val, bump| {
            let ptr = bump.alloc(T::default()) as *mut T;
            let ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) };
            Ok(ptr)
        });
    }
    pub fn comp_id_by_name(&self, name: &str, world: &World) -> Option<ComponentId> {
        self.component_id.get(name).and_then(|f| f(world))
    }
    pub fn reg_by_name(&self, name: &str, world: &mut World) -> ComponentId {
        self.component_register.get(name).unwrap()(world)
    }
    pub fn comp_id<T>(&self, world: &World) -> Option<ComponentId> {
        let name = short_type_name::<T>();
        self.component_id.get(name).and_then(|f| f(world))
    }
}
pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}
