use flecs_ecs::prelude::*;
 
use serde::{Deserialize, Serialize};
pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &World, Entity) -> Result<(), String>;

fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

type SnapshotTuple = (ExportFn, ImportFn);
impl JsonValueCodec {
    #[inline]
    fn from_tuple(parts: SnapshotTuple) -> Self {
        let (export, import) = parts;
        JsonValueCodec { export, import }
    }
}

fn export_full<T>(world: &World, entity: Entity) -> Option<serde_json::Value>
where
    T: DataComponent + ComponentId + Serialize + for<'a> Deserialize<'a>,
{
    let v = EntityView::new_from(world, entity);
    let mut ret = None;
    v.try_get::<Option<&T>>(|t| {
        ret = t.and_then(|x| serde_json::to_value(x).ok());
    });
    ret
}

fn import_full<T>(value: &serde_json::Value, world: &World, entity: Entity) -> Result<(), String>
where
    T: DataComponent + ComponentId + Serialize + for<'a> Deserialize<'a>,
{
    let v = EntityView::new_from(world, entity);

    let res = serde_json::from_value::<T>(value.clone()).map_err(|e| {
        format!(
            "cannot convert Type:{} Entity:{}: {}",
            short_type_name::<T>(),
            entity,
            e
        )
    })?;
    v.set::<T>(res);
    Ok(())
}

fn export_wrapper<T, T1>(world: &World, entity: Entity) -> Option<serde_json::Value>
where
    T: ComponentId + DataComponent,
    T1: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a T> + Into<T>,
{
    let v = EntityView::new_from(world, entity);
    let mut ret = None;
  
    v.try_get::<Option<&T>>(|t| {
        ret = t.and_then(|x| serde_json::to_value(T1::from(x)).ok());
    });
    ret
}

fn import_wrapper<T, T1>(
    value: &serde_json::Value,
    world: &World,
    entity: Entity,
) -> Result<(), String>
where
    T: ComponentId + DataComponent,
    T1: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a T> + Into<T>,
{
    let v = EntityView::new_from(world, entity);

    let res = serde_json::from_value::<T1>(value.clone()).map_err(|e| {
        format!(
            "cannot convert Type:{} Entity:{}: {}",
            short_type_name::<T>(),
            entity,
            e
        )
    })?;
    v.set::<T>(res.into());
    Ok(())
}

#[derive(Clone, Debug)]
pub struct JsonValueCodec {
    pub export: ExportFn,
    pub import: ImportFn,
}
impl JsonValueCodec {
    pub fn new<T>() -> Self
    where
        T: DataComponent + ComponentId + Serialize + for<'a> Deserialize<'a>,
    {
        let export = export_full::<T>;
        let import = import_full::<T>;
        Self { export, import }
    }
    pub fn new_with<T, T1>() -> Self
    where
        T: ComponentId + DataComponent,
        T1: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a T> + Into<T>,
    {
        let export = export_wrapper::<T, T1>;
        let import = import_wrapper::<T, T1>;
        Self { export, import }
    }
}
