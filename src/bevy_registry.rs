use bevy_ecs::component::{self, Components};
use bevy_ecs::ptr::{Aligned, OwningPtr, PtrMut};
use bevy_ecs::{component::ComponentId, prelude::*};
use std::any::TypeId;
use std::collections::HashMap;
use std::ptr::NonNull;

pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &mut World, Entity) -> Result<(), String>;
pub type CompIdFn = fn(&World) -> Option<ComponentId>;
pub type DynBuilderFn =
    fn(&serde_json::Value, &mut World) -> Result<(ComponentId, OwningPtr<'static>), String>;
pub enum SnapshotMode {
    /// 完整序列化、反序列化（默认）
    Full,
    /// 不输出内容，仅导出结构标记，Load 时调用 Default 构造
    Placeholder,
    PlaceholderEmplaceIfNotExists,
}

#[derive(Resource, Default, Debug)]
pub struct SnapshotRegistry {
    pub exporters: HashMap<&'static str, ExportFn>,
    pub importers: HashMap<&'static str, ImportFn>,
    pub dyn_ctors: HashMap<&'static str, DynBuilderFn>,
    pub type_registry: HashMap<&'static str, TypeId>,
    pub component_id: HashMap<&'static str, CompIdFn>,
}

impl SnapshotRegistry {
    pub fn register_with<T, T1>(&mut self)
    where
        T: Component + 'static,
        T1: serde::Serialize + serde::de::DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.component_id
            .insert(name, |world| world.component_id::<T>());
        self.exporters.insert(name, |world, entity| {
            // any.downcast_ref::<T>()
            //     .and_then(|t| serde_json::to_value(t).ok())
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
    }
    pub fn register<T>(&mut self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Component + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.component_id
            .insert(name, |world| world.component_id::<T>());
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
        self.dyn_ctors.insert(name, |val, world| {
            let id = world.register_component::<T>();
            //   let components = world.components();
            // SAFETY: This component exists because it is present on the archetype.
            let component: T = serde_json::from_value(val.clone()).map_err(|e| {
                format!(
                    "Deserialization error for {}:{} ",
                    short_type_name::<T>(),
                    e
                )
            })?;
            let boxed = Box::new(component);
            // SAFETY: boxed is properly aligned and has the right layout
            let ptr =
                unsafe { OwningPtr::new(NonNull::new_unchecked(Box::into_raw(boxed).cast())) };

            Ok((id, ptr))
        });
    }

    pub fn register_with_flag<T>(&mut self, mode: SnapshotMode)
    where
        T: Default + serde::Serialize + serde::de::DeserializeOwned + Component + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.component_id
            .insert(name, |world| world.component_id::<T>());
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
    }
    pub fn comp_id_by_name(&self, name: &str, world: &World) -> Option<ComponentId> {
        self.component_id.get(name).and_then(|f| f(world))
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
