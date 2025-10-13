use crate::binary_archive::arrow_column::RawTData;
use bevy_ecs::{component::ComponentId, entity::EntityRow, prelude::*};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
mod sparse_entitiy_list;
mod zip_snapshot;

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
pub enum BinFormat {
    #[default]
    Parquet,
    MsgPack,
}
use crate::{
    archetype_archive::WorldExt,
    arrow_snapshot::{ComponentTable, EntityID},
    prelude::{
        DeferredEntityBuilder, SnapshotMode, SnapshotRegistry, vec_snapshot_factory::SnapshotError,
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
    ) -> Result<(), SnapshotError> {
        for res in data.keys() {
            match reg.get_res_factory(res) {
                Some(factory) => {
                    let blob = data.get(res).ok_or_else(|| {
                        SnapshotError::Generic(format!("Missing binary blob for resource {res}"))
                    })?;

                    let value: serde_json::Value = rmp_serde::from_slice(&blob.0).map_err(|e| {
                        SnapshotError::Generic(format!("Deserialization failed: {e}"))
                    })?;

                    (factory.js_value.import)(&value, world, Entity::from_raw_u32(0).unwrap())
                        .map_err(|e| {
                            SnapshotError::Generic(format!(
                                "Import for resource {res} failed: {e:?}"
                            ))
                        })?;
                }
                None => {
                    println!("No factory found for resource `{res}`, skipping.");
                }
            }
        }
        Ok(())
    }

    pub fn save_archetypes<'a, I>(
        world: &'a World,
        registry: &'a SnapshotRegistry,
        archetypes: I,
        reg_comp_ids: HashMap<ComponentId, &'a str>,
    ) -> impl Iterator<Item = Result<ComponentTable, SnapshotError>> + 'a
    where
        I: Iterator<Item = &'a Archetype> + 'a,
    {
        archetypes.map(move |archetype| {
            let can_be_stored = archetype
                .components()
                .iter()
                .any(|x| reg_comp_ids.contains_key(&x));

            if !can_be_stored {
                return Ok(ComponentTable::default());
            }

            let mut archetype_snapshot = ComponentTable::default();
            let entities: Vec<_> = archetype.entities().iter().map(|x| x.id()).collect();

            let entities_ids: Vec<_> = entities
                .iter()
                .map(|&id| EntityID { id: id.index() })
                .collect();
            archetype_snapshot.entities.extend(entities_ids);

            for cid in archetype.components() {
                if let Some(&type_name) = reg_comp_ids.get(&cid) {
                    let arrow = registry
                        .get_factory(type_name)
                        .and_then(|f| f.arrow.as_ref())
                        .ok_or_else(|| SnapshotError::MissingFactory(type_name.to_string()))?;

                    let column = (arrow.arr_export)(&arrow.schema, world, &entities)?;
                    archetype_snapshot.insert_column(type_name, column);
                }
            }

            Ok(archetype_snapshot)
        })
    }

    pub fn save_world_resource(
        world: &World,
        reg: &SnapshotRegistry,
    ) -> Result<HashMap<String, BinBlob>, SnapshotError> {
        let mut map = HashMap::new();

        for res in reg.resource_entries.keys() {
            let factory = reg
                .get_res_factory(res)
                .ok_or_else(|| SnapshotError::MissingFactory(res.to_string()))?;

            let value = (factory.js_value.export)(world, Entity::from_raw_u32(0).unwrap())
                .ok_or_else(|| SnapshotError::Generic(format!("resource {res} export failed")))?;

            let bin = BinBlob(
                rmp_serde::to_vec(&value)
                    .map_err(|e| SnapshotError::Generic(format!("rmp encode error: {e}")))?,
            );
            map.insert(res.to_string(), bin);
        }

        Ok(map)
    }
}

fn count_entities(snapshot: &[u32]) -> u32 {
    unsafe { *snapshot.iter().max().unwrap_unchecked() + 1 }
}
impl WorldArrowSnapshot {
    pub fn from_world(world: &World) -> Self {
        let reg = world.resource::<SnapshotRegistry>();
        Self::from_world_reg(world, reg).unwrap()
    }
    pub fn from_world_reg(
        world: &World,
        registry: &SnapshotRegistry,
    ) -> Result<Self, SnapshotError> {
        let archetypes = world.archetypes().iter().filter(|x| !x.is_empty());

        let reg_comp_ids: HashMap<ComponentId, &str> = registry
            .type_registry
            .keys()
            .filter_map(|&name| registry.comp_id_by_name(name, world).map(|cid| (cid, name)))
            .collect();

        let mut world_snapshot = WorldArrowSnapshot::default();
        world_snapshot.entities = WorldExt::iter_entities(world).map(|x| x.index()).collect();

        let snap = Self::save_archetypes(world, registry, archetypes, reg_comp_ids);
        world_snapshot.archetypes = snap.collect::<Result<_, _>>()?;

        world_snapshot.resources = Self::save_world_resource(world, registry)?;

        Ok(world_snapshot)
    }

    pub fn to_world(&self, world: &mut World) -> Result<(), SnapshotError> {
        world.resource_scope(|world, reg: Mut<SnapshotRegistry>| self.to_world_reg(world, &reg))
    }
    pub fn to_world_reg(
        &self,
        world: &mut World,
        reg: &SnapshotRegistry,
    ) -> Result<(), SnapshotError> {
        world
            .entities()
            .reserve_entities(count_entities(&self.entities));
        world.flush();
        Self::load_world_resource(&self.resources, world, reg)?;
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
                    let data = (arrow.arr_dyn)(data, &bump, world)?;
                    let raw_vec = RawTData { comp_id, data };
                    columns.push((mode, raw_vec));
                } else {
                    println!("warning type {} cannot be converted", type_name);
                }
            }
            for id in archetype.entities.iter().rev() {
                let entity = world
                    .entities()
                    .resolve_from_id(EntityRow::from_raw_u32(id.id as u32).unwrap())
                    .ok_or_else(|| SnapshotError::Generic(format!("missing entity {}", id.id)))?;
                let mut builder = DeferredEntityBuilder::new(world, &bump, entity);
                for (mode, raw) in &mut columns {
                    let ptr = raw.data.pop().unwrap();
                    match mode {
                        SnapshotMode::Full => {
                            builder.insert_by_id(raw.comp_id, ptr);
                        }
                        crate::prelude::SnapshotMode::EmplaceIfNotExists => {
                            builder.insert_if_new_by_id(raw.comp_id, ptr);
                        }
                    }
                }
                builder.commit();
            }

            bump.reset();
        }
        Ok(())
    }
}

use bevy_ecs::archetype::Archetype;

#[derive(Serialize, Clone, Debug, Default, Deserialize)]
pub struct BinBlob(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorldBinArchSnapshot {
    pub entities: sparse_entitiy_list::SparseU32List,
    pub archetypes: Vec<BinBlob>,
    pub resources: HashMap<String, BinBlob>,
    pub format: BinFormat,
    pub meta: HashMap<String, String>,
}

impl WorldBinArchSnapshot {
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }
}
impl From<WorldArrowSnapshot> for WorldBinArchSnapshot {
    fn from(value: WorldArrowSnapshot) -> Self {
        let archetypes = value
            .archetypes
            .iter()
            .map(|x| BinBlob(x.to_parquet().unwrap()))
            .collect();
        let entities = sparse_entitiy_list::SparseU32List::from_unsorted(value.entities);
        Self {
            entities,
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
            entities: value.entities.to_vec(),
            archetypes,
            resources: value.resources,
            meta: value.meta,
        }
    }
}
