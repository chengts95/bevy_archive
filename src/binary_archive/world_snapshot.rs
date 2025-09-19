use bevy_ecs::{
    component::ComponentId,
    entity::{Entity, hash_set::Iter},
    world::{Mut, World},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
pub enum BinFormat {
    #[default]
    Parquet,
    MsgPack,
}
use crate::{
    arrow_snapshot::{ComponentTable, EntityID},
    prelude::{
        DeferredEntityBuilder, SnapshotFactory, SnapshotMode, SnapshotRegistry,
        vec_snapshot_factory::RawTData,
    },
};
#[derive(Debug, Clone, Default)]
pub struct WorldArrowSnapshot {
    pub entities: Vec<u32>,
    pub archetypes: Vec<ComponentTable>,
    pub resources: HashMap<String, BinBlob>,
    pub meta: HashMap<String, String>,
}

impl WorldArrowSnapshot {
    pub fn load_world_resource(
        data: &HashMap<String, BinBlob>,
        world: &mut World,
        reg: &SnapshotRegistry,
    ) {
        let loadable_resource = data.keys();
        for res in loadable_resource {
            let factory = reg.get_res_factory(res);
            match factory {
                Some(factory) => {
                    let value: serde_json::Value = rmp_serde::from_slice(&data[res].0).unwrap();
                    (factory.js_value.import)(&value, world, Entity::from_raw(0)).unwrap();
                }
                None => {
                    //may need to emit warnings here
                }
            }
        }
    }
    pub fn save_archetypes<'a, I>(
        world: &World,
        registry: &SnapshotRegistry,
        archetypes: I,
        reg_comp_ids: HashMap<ComponentId, &str>,
    ) -> impl Iterator<Item = ComponentTable>
    where
        I: Iterator<Item = &'a Archetype>,
    {
        let snap = archetypes.map(move |archetype| {
            let can_be_stored = archetype
                .components()
                .any(|x| reg_comp_ids.contains_key(&x));
            if !can_be_stored {
                return ComponentTable::default();
            }
            let mut archetype_snapshot = ComponentTable::default();
            let entities: Vec<_> = archetype.entities().iter().map(|x| x.id()).collect();
            let entities_ids: Vec<_> = archetype
                .entities()
                .iter()
                .map(|x| (EntityID { id: x.id().index() }))
                .collect();
            archetype_snapshot.entities.extend(entities_ids.as_slice());

            archetype.components().for_each(|x| {
                if reg_comp_ids.contains_key(&x) {
                    let type_name = reg_comp_ids[&x];
                    // let t = archetype.get_storage_type(x).map(|x| match x {
                    //     StorageType::Table => StorageTypeFlag::Table,
                    //     StorageType::SparseSet => StorageTypeFlag::SparseSet,
                    // });
                    let arrow = &registry.get_factory(type_name).unwrap().arrow;
                    let arrow = arrow.as_ref().unwrap();
                    let column = (arrow.arr_export)(&arrow.schema, &world, &entities);
                    archetype_snapshot.insert_column(type_name, column.unwrap());
                }
            });

            archetype_snapshot
        });
        snap
    }
    pub fn save_world_resource(world: &World, reg: &SnapshotRegistry) -> HashMap<String, BinBlob> {
        let mut map = HashMap::new();
        let saveable_resource = reg.resource_entries.keys();
        for res in saveable_resource {
            let value =
                (reg.get_res_factory(res).unwrap().js_value.export)(world, Entity::from_raw(0));
            if let Some(value) = value {
                let bin = BinBlob(rmp_serde::to_vec(&value).unwrap());
                map.insert(res.to_string(), bin);
            }
        }
        map
    }
}
fn count_entities(snapshot: &[u32]) -> u32 {
    unsafe { *snapshot.iter().max().unwrap_unchecked() + 1 }
}
impl WorldArrowSnapshot {
    pub fn from_world(world: &World) -> Self {
        let reg = world.resource::<SnapshotRegistry>();
        Self::from_world_reg(world, reg)
    }
    pub fn from_world_reg(world: &World, registry: &SnapshotRegistry) -> Self {
        let archetypes = world.archetypes().iter().filter(|x| !x.is_empty());

        let reg_comp_ids: HashMap<ComponentId, &str> = registry
            .type_registry
            .keys()
            .filter_map(|&name| {
                registry
                    .comp_id_by_name(name, &world)
                    .map(|cid| (cid, name))
            })
            .collect();
        let mut world_snapshot = WorldArrowSnapshot::default();
        world_snapshot.entities = world.iter_entities().map(|x| x.id().index()).collect();
        let snap = Self::save_archetypes(world, registry, archetypes, reg_comp_ids);
        world_snapshot.archetypes = snap.collect();
        world_snapshot.resources = Self::save_world_resource(world, registry);

        world_snapshot
    }
    pub fn to_world(&self, world: &mut World) {
        world.resource_scope(|world, reg: Mut<SnapshotRegistry>| self.to_world_reg(world, &reg))
    }
    pub fn to_world_reg(&self, world: &mut World, reg: &SnapshotRegistry) {
        world
            .entities()
            .reserve_entities(count_entities(&self.entities));
        world.flush();
        Self::load_world_resource(&self.resources, world, reg);
        let mut bump = bumpalo::Bump::new();
        for archetype in &self.archetypes {
            let mut columns = Vec::new();
            let types = archetype.columns();

            for (type_name, data) in types {
                if let Some(arrow) = reg.get_factory(type_name).and_then(|x| x.arrow.as_ref()) {
                    let comp_id = reg
                        .comp_id_by_name(type_name.as_str(), world)
                        .or_else(|| Some(reg.reg_by_name(type_name, world)))
                        .unwrap();
                    let mode = unsafe { reg.get_factory(type_name).unwrap_unchecked().mode };
                    let data = (arrow.arr_dyn)(data, &bump, world).unwrap();
                    let raw_vec = RawTData { comp_id, data };
                    columns.push((mode, raw_vec));
                } else {
                    println!("warning type {} cannot be converted", type_name);
                }
            }
            for id in archetype.entities.iter().rev() {
                let entity = world.entities().resolve_from_id(id.id).unwrap();
                let mut builder = DeferredEntityBuilder::new(world, &bump, entity);
                for (mode, raw) in &mut columns {
                    let ptr = raw.data.pop().unwrap();
                    match mode {
                        SnapshotMode::Full | SnapshotMode::Placeholder => {
                            builder.insert_by_id(raw.comp_id, ptr);
                        }
                        crate::prelude::SnapshotMode::PlaceholderEmplaceIfNotExists => {
                            builder.insert_if_new_by_id(raw.comp_id, ptr);
                        }
                    }
                }
                builder.commit();
            }

            bump.reset();
        }
    }
}

use bevy_ecs::archetype::Archetype;

#[derive(Serialize, Clone, Debug, Default, Deserialize)]
pub struct BinBlob(Vec<u8>);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorldBinArchSnapshot {
    pub entities: Vec<u32>,
    pub archetypes: Vec<BinBlob>,
    pub resources: HashMap<String, BinBlob>,
    pub format: BinFormat,
    pub meta: HashMap<String, String>,
}
impl From<WorldArrowSnapshot> for WorldBinArchSnapshot {
    fn from(value: WorldArrowSnapshot) -> Self {
        let archetypes = value
            .archetypes
            .iter()
            .map(|x| BinBlob(x.to_parquet().unwrap()))
            .collect();
        Self {
            entities: value.entities,
            archetypes,
            resources: value.resources,
            format: BinFormat::Parquet,
            meta: value.meta,
        }
    }
}
impl From<WorldBinArchSnapshot> for WorldArrowSnapshot {
    fn from(value: WorldBinArchSnapshot) -> Self {
        if value.format != BinFormat::Parquet {
            panic!(
                "mismatched format: desired {:?} got {:?}",
                BinFormat::Parquet,
                value.format
            );
        }
        let archetypes = value
            .archetypes
            .iter()
            .map(|x| ComponentTable::from_parquet_u8(&x.0).unwrap())
            .collect();
        Self {
            entities: value.entities,
            archetypes,
            resources: value.resources,
            meta: value.meta,
        }
    }
}
