use std::{any::TypeId, collections::HashMap};

use flecs_ecs::prelude::*;
pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &World, Entity) -> Result<(), String>;

pub enum SnapshotMode {
    /// 完整序列化、反序列化（默认）
    Full,
    /// 不输出内容，仅导出结构标记，Load 时调用 Default 构造
    Placeholder,
    PlaceholderEmplaceIfNotExists,
}
#[derive(Default, Debug)]
pub struct SnapshotRegistry {
    pub exporters: HashMap<&'static str, ExportFn>,
    pub importers: HashMap<&'static str, ImportFn>,
    pub type_registry: HashMap<&'static str, TypeId>,
}
pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

impl SnapshotRegistry {
    #[inline(always)]
    fn get_json<T>(v: &EntityView) -> Option<serde_json::Value>
    where
        T: serde::Serialize + DataComponent + ComponentId,
    {
        let mut ret = None;
        v.try_get::<Option<&T>>(|t| {
            ret = t.map(|x| serde_json::to_value(x).unwrap());
        });
        ret
    }
    pub fn register<T>(&mut self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + DataComponent + ComponentId,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());
        self.exporters.insert(name, |world, entity| {
            let v = EntityView::new_from(world, entity);
            Self::get_json::<T>(&v)
        });

        self.importers.insert(name, |value, world, entity| {
            let name = short_type_name::<T>();
            let a: T = serde_json::from_value(value.clone())
                .map_err(|e| format!("cannot convert Type:{} Entity:{}: {}", name, entity, e))?;
            //if world.has_id(entity) {
            let v = EntityView::new_from(world, entity);
            v.set(a);
            Ok(())
            // } else {
            // world.entity_from_id(entity).set(a);
            // Ok(())
            //}
        });
    }
}

impl SnapshotRegistry {
    pub fn register_with_flag<T>(&mut self, mode: SnapshotMode)
    where
        T: Default
            + serde::Serialize
            + serde::de::DeserializeOwned
            + DataComponent
            + ComponentId
            + 'static,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());

        match mode {
            SnapshotMode::Full => {
                self.register::<T>();
            }
            SnapshotMode::Placeholder => {
                self.exporters
                    .insert(name, |_, _| Some(serde_json::Value::Null));
                self.importers.insert(name, |_, world, entity| {
                    let view = EntityView::new_from(world, entity);
                    view.set(T::default());
                    Ok(())
                });
            }
            SnapshotMode::PlaceholderEmplaceIfNotExists => {
                self.exporters
                    .insert(name, |_, _| Some(serde_json::Value::Null));
                self.importers.insert(name, |_, world, entity| {
                    let view = EntityView::new_from(world, entity);
                    if !view.has::<T>() {
                        view.set(T::default());
                    }
                    Ok(())
                });
            }
        }
    }
}

impl SnapshotRegistry {
    pub fn register_with<T, T1>(&mut self)
    where
        T: DataComponent + ComponentId + 'static,
        T1: serde::Serialize + serde::de::DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let name = short_type_name::<T>();
        self.type_registry.insert(name, TypeId::of::<T>());

        self.exporters.insert(name, |world, entity| {
            let view = EntityView::new_from(world, entity);
            let mut ret = None;
            view.try_get::<Option<&T>>(|t| {
                ret = t.map(|x| serde_json::to_value(T1::from(x)).unwrap());
            });
            ret
        });

        self.importers.insert(name, |value, world, entity| {
            let name = short_type_name::<T>();
            let parsed: T1 = serde_json::from_value(value.clone())
                .map_err(|e| format!("Deserialize error for {}: {}", name, e))?;
            let view = EntityView::new_from(world, entity);
            view.set::<T>(parsed.into());
            Ok(())
        });
    }
}
