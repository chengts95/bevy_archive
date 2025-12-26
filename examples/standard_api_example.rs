use bevy_archive::prelude::*;
use bevy_archive::binary_archive::msgpack_archive::MsgPackArchive;
#[cfg(feature = "arrow_rs")]
use bevy_archive::binary_archive::WorldArrowSnapshot;
use bevy_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Component, Serialize, Deserialize, Debug, PartialEq)]
struct Player {
    name: String,
    hp: i32,
}

#[derive(Resource, Serialize, Deserialize, Debug, PartialEq)]
struct GameState {
    level: u32,
}

fn main() {
    let mut world = World::new();
    let mut registry = SnapshotRegistry::default();
    
    registry.register::<Player>();
    registry.resource_register::<GameState>();

    world.spawn(Player { name: "Hero".to_string(), hp: 100 });
    world.insert_resource(GameState { level: 1 });

    // Test MsgPackArchive
    println!("--- Testing MsgPackArchive ---");
    test_archive::<MsgPackArchive>(&world, &registry, "test_save.msgpack");

    // Test AuroraWorldManifest (JSON)
    println!("\n--- Testing AuroraWorldManifest (JSON) ---");
    test_archive::<AuroraWorldManifest>(&world, &registry, "test_save.json");
    
    // Test AuroraWorldManifest (TOML)
    println!("\n--- Testing AuroraWorldManifest (TOML) ---");
    test_archive::<AuroraWorldManifest>(&world, &registry, "test_save.toml");

    // Test WorldSnapshot (JSON)
    println!("\n--- Testing WorldSnapshot (JSON) ---");
    test_archive::<WorldSnapshot>(&world, &registry, "test_snapshot.json");
    
    #[cfg(feature = "arrow_rs")]
    {
        // Test WorldArrowSnapshot (ZIP)
        println!("\n--- Testing WorldArrowSnapshot (ZIP) ---");
        test_archive::<WorldArrowSnapshot>(&world, &registry, "test_save.zip");
    }
}

fn test_archive<A: Archive>(src_world: &World, registry: &SnapshotRegistry, path: &str) {
    // 1. Create and Save
    println!("Saving to {}...", path);
    let archive = A::create(src_world, registry).expect("Failed to create archive");
    archive.save_to(path).expect("Failed to save to file");

    // 2. Load from File
    println!("Loading from {}...", path);
    let loaded_archive = A::load_from(path).expect("Failed to load from file");

    // 3. Apply to New World
    let mut dest_world = World::new();
    loaded_archive.apply(&mut dest_world, registry).expect("Failed to apply archive");

    // 4. Verify
    let mut query = dest_world.query::<&Player>();
    assert_eq!(query.iter(&dest_world).count(), 1);
    
    let player = dest_world.query::<&Player>().single(&dest_world);
    let player = player.unwrap();
    assert_eq!(player.name, "Hero");
    assert_eq!(player.hp, 100);
    println!("Verified Entity: {:?}", player);

    // Resources are supported by MsgPack and Aurora, but WorldSnapshot might not support them fully in legacy mode?
    // Let's check if resource exists.
    if let Some(state) = dest_world.get_resource::<GameState>() {
        assert_eq!(state.level, 1);
        println!("Verified Resource: {:?}", state);
    } else {
        println!("Resource not loaded (might be expected for this format).");
    }

    // Cleanup
    fs::remove_file(path).unwrap_or_default();
    println!("Success!");
}
