use bevy_ecs::ptr::{Aligned, OwningPtr};
use bevy_ecs::{component::ComponentId, prelude::*};
use bumpalo::Bump;
use serde::Serialize;
use serde::de::DeserializeOwned;
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

fn export_default<T: Serialize + Component>() -> ExportFn {
    |world, entity| {
        world
            .entity(entity)
            .get::<T>()
            .and_then(|t| serde_json::to_value(t).ok())
    }
}

fn import_default<T: DeserializeOwned + Component>() -> ImportFn {
    |val, world, entity| {
        let name = short_type_name::<T>();
        serde_json::from_value::<T>(val.clone())
            .map_err(|e| format!("Deserialization error for {}:{}", name, e))
            .map(|v| {
                world.entity_mut(entity).insert(v);
            })
            .map(|_| ())
    }
}

fn dyn_ctor_default<T: DeserializeOwned + Component>() -> DynBuilderFn {
    |val, bump| {
        let name = short_type_name::<T>();
        let component: T = serde_json::from_value(val.clone())
            .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
        let ptr = bump.alloc(component) as *mut T;
        Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
    }
}
mod with_wrapper {
    use super::*;
    pub fn export_default<T: Component, T1: Serialize>() -> ExportFn
    where
        T1: for<'a> From<&'a T> + Into<T>,
    {
        |world, entity| {
            world
                .entity(entity)
                .get::<T>()
                .and_then(|t| serde_json::to_value(T1::from(t)).ok())
        }
    }

    pub fn import_default<T: Component, T1: DeserializeOwned>() -> ImportFn
    where
        T1: for<'a> From<&'a T> + Into<T>,
    {
        |val, world, entity| {
            let name = short_type_name::<T>();
            serde_json::from_value::<T1>(val.clone())
                .map_err(|e| format!("Deserialization error for {}:{}", name, e))
                .map(|v| {
                    let v: T = v.into();
                    world.entity_mut(entity).insert(v);
                })
                .map(|_| ())
        }
    }

    pub fn dyn_ctor_default<T, T1: DeserializeOwned>() -> DynBuilderFn
    where
        T1: for<'a> From<&'a T> + Into<T>,
    {
        |val, bump| {
            let name = short_type_name::<T>();
            let component: T1 = serde_json::from_value(val.clone())
                .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
            let component: T = component.into();
            let ptr = bump.alloc(component) as *mut T;
            Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
        }
    }
}
#[derive(Clone, Debug)]
pub struct SnapshotFactory {
    pub import: ImportFn,
    pub export: ExportFn,
    pub dyn_ctor: DynBuilderFn,
    pub comp_id: CompIdFn,
    pub register: CompRegFn,
    pub mode: SnapshotMode,
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
        Self {
            export: export_default::<T>(),
            import: import_default::<T>(),
            dyn_ctor: dyn_ctor_default::<T>(),
            comp_id: Self::component_id::<T>,
            register: |world| world.register_component::<T>(),
            mode: SnapshotMode::Full,
        }
    }
    pub fn new_with_wrapper_full<T, T1>() -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        Self {
            export: with_wrapper::export_default::<T, T1>(),
            import: with_wrapper::import_default::<T, T1>(),
            dyn_ctor: with_wrapper::dyn_ctor_default::<T, T1>(),
            comp_id: Self::component_id::<T>,
            register: |world| world.register_component::<T>(),
            mode: SnapshotMode::Full,
        }
    }
    pub fn new_with_wrapper<T, T1>(mode: SnapshotMode) -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        let export: ExportFn = match mode {
            SnapshotMode::Full => |world, entity| {
                world
                    .entity(entity)
                    .get::<T>()
                    .and_then(|t| serde_json::to_value(T1::from(t)).ok())
            },
            _ => |_, _| Some(serde_json::Value::Null),
        };

        let import: ImportFn = match mode {
            SnapshotMode::Full => |val, world, entity| {
                let name = short_type_name::<T>();
                serde_json::from_value::<T1>(val.clone())
                    .map_err(|e| format!("Deserialization error for {}:{}", name, e))
                    .map(|v| {
                        world.entity_mut(entity).insert::<T>(v.into());
                    })
                    .map(|_| ())
            },
            SnapshotMode::Placeholder => |_, world, entity| {
                world
                    .entity_mut(entity)
                    .insert(Into::<T>::into(T1::default()));
                Ok(())
            },
            SnapshotMode::PlaceholderEmplaceIfNotExists => |_, world, entity| {
                if !world.entity(entity).contains::<T>() {
                    world
                        .entity_mut(entity)
                        .insert(Into::<T>::into(T1::default()));
                }
                Ok(())
            },
        };

        let dyn_ctor: DynBuilderFn = match mode {
            SnapshotMode::Full => |val, bump| {
                let name = short_type_name::<T>();
                let component: T1 = serde_json::from_value(val.clone())
                    .map_err(|e| format!("Deserialization error for {}:{}", name, e))?;
                let ptr = bump.alloc(Into::<T>::into(component)) as *mut T;
                Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
            },
            _ => |_val, bump| {
                let ptr = bump.alloc(Into::<T>::into(T1::default())) as *mut T;
                Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
            },
        };

        Self {
            export,
            import,
            dyn_ctor,
            comp_id: Self::component_id::<T>,
            register: |world| world.register_component::<T>(),
            mode,
        }
    }
    pub fn with_mode<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        let export: ExportFn = match mode {
            SnapshotMode::Full => export_default::<T>(),
            _ => |_, _| Some(serde_json::Value::Null),
        };

        let import: ImportFn = match mode {
            SnapshotMode::Full => import_default::<T>(),
            SnapshotMode::Placeholder => |_, world, entity| {
                world.entity_mut(entity).insert(T::default());
                Ok(())
            },
            SnapshotMode::PlaceholderEmplaceIfNotExists => |_, world, entity| {
                if !world.entity(entity).contains::<T>() {
                    world.entity_mut(entity).insert(T::default());
                }
                Ok(())
            },
        };

        let dyn_ctor: DynBuilderFn = match mode {
            SnapshotMode::Full => dyn_ctor_default::<T>(),
            _ => |_val, bump| {
                let ptr = bump.alloc(T::default()) as *mut T;
                Ok(unsafe { OwningPtr::new(NonNull::new_unchecked(ptr.cast())) })
            },
        };

        Self {
            export,
            import,
            dyn_ctor,
            comp_id: Self::component_id::<T>,
            register: |world| world.register_component::<T>(),
            mode,
        }
    }
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

#[derive(Resource, Clone, Default, Debug)]
pub struct SnapshotRegistry {
    pub type_registry: HashMap<&'static str, TypeId>,
    pub entries: HashMap<&'static str, SnapshotFactory>,
    pub resource_entries: HashMap<&'static str, SnapshotFactory>,
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

pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}
