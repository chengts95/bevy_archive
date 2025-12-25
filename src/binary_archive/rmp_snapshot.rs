use crate::archetype_archive::{
    load_world_arch_snapshot_defragment, save_world_arch_snapshot, WorldArchSnapshot,
};
use crate::bevy_registry::SnapshotRegistry;
use bevy_ecs::prelude::World;
use std::fs::File;
use std::io::{self};
use std::path::Path;

/// Saves the world to a MessagePack file using the archetype snapshot format.
/// This format is binary and does not depend on Arrow/Parquet.
pub fn save_rmp_snapshot(
    world: &World,
    reg: &SnapshotRegistry,
    path: impl AsRef<Path>,
) -> Result<(), io::Error> {
    let snapshot = save_world_arch_snapshot(world, reg);
    let mut file = File::create(path)?;
    rmp_serde::encode::write(&mut file, &snapshot)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

/// Loads the world from a MessagePack file using the archetype snapshot format.
pub fn load_rmp_snapshot(
    world: &mut World,
    reg: &SnapshotRegistry,
    path: impl AsRef<Path>,
) -> Result<(), io::Error> {
    let file = File::open(path)?;
    let snapshot: WorldArchSnapshot = rmp_serde::decode::from_read(file)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    load_world_arch_snapshot_defragment(world, &snapshot, reg);
    Ok(())
}

/// Helper to serialize to bytes
pub fn to_rmp_bytes(snapshot: &WorldArchSnapshot) -> Result<Vec<u8>, io::Error> {
    rmp_serde::to_vec(snapshot).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

/// Helper to deserialize from bytes
pub fn from_rmp_bytes(bytes: &[u8]) -> Result<WorldArchSnapshot, io::Error> {
    rmp_serde::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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

    #[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Velocity {
        dx: f32,
        dy: f32,
    }

    #[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
    struct Tag;

    fn setup_registry() -> SnapshotRegistry {
        let mut registry = SnapshotRegistry::default();
        registry.register::<Position>();
        registry.register::<Velocity>();
        registry.register::<Tag>();
        registry
    }

    #[test]
    fn test_rmp_roundtrip() {
        let mut world = World::new();
        let registry = setup_registry();

        // Spawn some entities
        world.spawn((Position { x: 1.0, y: 2.0 }, Velocity { dx: 0.1, dy: -0.2 }));
        world.spawn((Tag, Position { x: 9.0, y: 3.5 }));
        world.spawn((Tag, Velocity { dx: 0.0, dy: 0.0 }));

        // Save to bytes
        let snapshot = save_world_arch_snapshot(&world, &registry);
        let bytes = to_rmp_bytes(&snapshot).expect("Failed to serialize to RMP");

        println!("RMP Snapshot size: {} bytes", bytes.len());

        // Load from bytes
        let loaded_snapshot = from_rmp_bytes(&bytes).expect("Failed to deserialize from RMP");
        
        let mut new_world = World::new();
        load_world_arch_snapshot_defragment(&mut new_world, &loaded_snapshot, &registry);

        // Verify
        let mut query = new_world.query::<(Option<&Position>, Option<&Velocity>, Option<&Tag>)>();
        let mut count = 0;
        for (pos, vel, tag) in query.iter(&new_world) {
            count += 1;
            println!("Entity {}: {:?} {:?} {:?}", count, pos, vel, tag);
        }
        assert_eq!(count, 3);
    }
    
    #[test]
    fn test_rmp_file_io() {
        let mut world = World::new();
        let registry = setup_registry();
        world.spawn((Position { x: 10.0, y: 20.0 },));
        
        let path = "test_rmp_snapshot.bin";
        save_rmp_snapshot(&world, &registry, path).expect("Failed to save file");
        
        let mut new_world = World::new();
        load_rmp_snapshot(&mut new_world, &registry, path).expect("Failed to load file");
        
        let mut query = new_world.query::<&Position>();
        let pos = query.single(&new_world).unwrap();
        assert_eq!(pos.x, 10.0);
        assert_eq!(pos.y, 20.0);
        
        std::fs::remove_file(path).unwrap();
    }
}
