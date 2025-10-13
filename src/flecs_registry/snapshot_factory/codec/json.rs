use flecs_ecs::prelude::*;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
pub type ExportFn = fn(&World, Entity) -> Option<serde_json::Value>;
pub type ImportFn = fn(&serde_json::Value, &mut World, Entity) -> Result<(), String>;

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
        ret = t.map(|x| serde_json::to_value(x).unwrap());
    });
    ret
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
        Self {
            export,
            import: todo!(),
        }
    }
    pub fn new_with<T, T1>() -> Self
    where
        T1: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a T> + Into<T>,
    {
       
        Self {
            export: todo!(),
            import: todo!(),
        }
    }
}
