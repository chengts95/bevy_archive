use crate::archetype_archive::{
    load_world_arch_snapshot_defragment, save_single_archetype_snapshot, ArchetypeSnapshot,
    WorldArchSnapshot, WorldExt,
};
use crate::bevy_registry::SnapshotRegistry;
use crate::binary_archive::common::{BinBlob, BinFormat, SparseU32List, WorldBinArchSnapshot};
use bevy_ecs::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self};
use std::path::Path;

pub struct MsgPackArchive(pub WorldBinArchSnapshot);

impl MsgPackArchive {
    /// Save the world to an in-memory MsgPackArchive
    pub fn from_world(world: &World, reg: &SnapshotRegistry) -> Result<Self, io::Error> {
        let mut snapshot = WorldBinArchSnapshot::default();
        snapshot.format = BinFormat::MsgPack;

        // 1. Entities
        let entities: Vec<u32> = WorldExt::iter_entities(world).map(|e| e.index()).collect();
        snapshot.entities = SparseU32List::from_unsorted(entities);

        // 2. Archetypes
        let reg_comp_ids: HashMap<bevy_ecs::component::ComponentId, &str> = reg
            .type_registry
            .keys()
            .filter_map(|&name| reg.comp_id_by_name(name, world).map(|cid| (cid, name)))
            .collect();

        let archetypes = world.archetypes().iter().filter(|x| !x.is_empty());

        for arch in archetypes {
            let arch_snap = save_single_archetype_snapshot(world, arch, reg, &reg_comp_ids);
            if !arch_snap.entities.is_empty() {
                // Serialize ArchetypeSnapshot to MsgPack bytes
                let bytes = rmp_serde::to_vec(&arch_snap)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                snapshot.archetypes.push(BinBlob(bytes));
            }
        }

        // 3. Resources
        for (name, factory) in &reg.resource_entries {
            if let Some(value) = (factory.js_value.export)(world, Entity::from_raw_u32(0).unwrap())
            {
                let bytes = rmp_serde::to_vec(&value)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                snapshot.resources.insert(name.to_string(), BinBlob(bytes));
            }
        }

        Ok(Self(snapshot))
    }

    /// Load the archive into the world
    pub fn to_world(&self, world: &mut World, reg: &SnapshotRegistry) -> Result<(), io::Error> {
        if self.0.format != BinFormat::MsgPack {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected MsgPack format, got {:?}", self.0.format),
            ));
        }

        // 1. Entities & Archetypes
        // Reconstruct WorldArchSnapshot (the structure used by archetype_archive loader)
        let mut world_arch_snap = WorldArchSnapshot::default();
        world_arch_snap.entities = self.0.entities.to_vec();

        for blob in &self.0.archetypes {
            let arch_snap: ArchetypeSnapshot = rmp_serde::from_slice(&blob.0)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            world_arch_snap.archetypes.push(arch_snap);
        }

        // Use the existing defragmenting loader
        load_world_arch_snapshot_defragment(world, &world_arch_snap, reg);

        // 2. Resources
        for (name, blob) in &self.0.resources {
            if let Some(factory) = reg.get_res_factory(name) {
                let value: serde_json::Value = rmp_serde::from_slice(&blob.0)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                
                (factory.js_value.import)(&value, world, Entity::from_raw_u32(0).unwrap())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }
        }

        Ok(())
    }

    pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), io::Error> {
        let mut file = File::create(path)?;
        rmp_serde::encode::write(&mut file, &self.0)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let file = File::open(path)?;
        let snapshot: WorldBinArchSnapshot = rmp_serde::decode::from_read(file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Self(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_ecs::prelude::*;
    use serde::{Deserialize, Serialize};

    #[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Position {
        x: f32,
        y: f32,
    }

    #[derive(Resource, Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct GameConfig {
        difficulty: u32,
        mode: String,
    }

    fn setup_registry() -> SnapshotRegistry {
        let mut registry = SnapshotRegistry::default();
        registry.register::<Position>();
        registry.resource_register::<GameConfig>();
        registry
    }

    #[test]
    fn test_msgpack_archive_roundtrip() {
        let mut world = World::new();
        let registry = setup_registry();

        // Spawn entities
        world.spawn(Position { x: 10.0, y: 20.0 });
        world.spawn(Position { x: 5.0, y: 5.0 });

        // Insert resource
        world.insert_resource(GameConfig {
            difficulty: 3,
            mode: "Hardcore".to_string(),
        });

        // Save
        let archive = MsgPackArchive::from_world(&world, &registry).unwrap();
        
        // Verify internal structure
        assert_eq!(archive.0.format, BinFormat::MsgPack);
        assert!(!archive.0.archetypes.is_empty());
        assert!(archive.0.resources.contains_key("GameConfig"));

        // Load into new world
        let mut new_world = World::new();
        archive.to_world(&mut new_world, &registry).unwrap();

        // Verify entities
        let mut query = new_world.query::<&Position>();
        let positions: Vec<&Position> = query.iter(&new_world).collect();
        assert_eq!(positions.len(), 2);

        // Verify resource
        let config = new_world.resource::<GameConfig>();
        assert_eq!(config.difficulty, 3);
        assert_eq!(config.mode, "Hardcore");
    }

    #[test]
    fn test_file_io() {
         let mut world = World::new();
        let registry = setup_registry();
        world.spawn(Position { x: 1.0, y: 2.0 });
        
        let path = "test_msgpack_archive.bin";
        
        let archive = MsgPackArchive::from_world(&world, &registry).unwrap();
        archive.to_file(path).unwrap();
        
        let loaded_archive = MsgPackArchive::from_file(path).unwrap();
        
        let mut new_world = World::new();
        loaded_archive.to_world(&mut new_world, &registry).unwrap();
        
        let mut query = new_world.query::<&Position>();
        let pos = query.single(&new_world).unwrap();
        assert_eq!(pos.x, 1.0);
        
        std::fs::remove_file(path).unwrap();
    }
}
