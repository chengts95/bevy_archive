use bevy_archive::prelude::*;
use bevy_archive::binary_archive::msgpack_archive::MsgPackArchive;
#[cfg(feature = "arrow_rs")]
use bevy_archive::binary_archive::WorldArrowSnapshot;
use bevy_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Component, Serialize, Deserialize, Debug, PartialEq)]
struct WiringTable {
    #[serde(with = "entity_serializer")]
    target: Entity, // Points to another entity
}

impl Default for WiringTable {
    fn default() -> Self {
        Self {
            target: Entity::PLACEHOLDER,
        }
    }
}

#[derive(Component, Serialize, Deserialize, Default, Debug, PartialEq)]
struct BlockName(String);

fn main() {
    // 1. Setup Source World
    let mut source_world = World::new();
    let mut registry = SnapshotRegistry::default();
    
    registry.register::<WiringTable>();
    registry.register::<BlockName>();

    let e1 = source_world.spawn(BlockName("Node1".into())).id();
    let e2 = source_world.spawn((BlockName("Node2".into()), WiringTable { target: e1 })).id();

    println!("Source: e1={:?}, e2={:?}, e2.target={:?}", e1, e2, e1);

    // 2. Setup Remapping Logic (Common)
    let mut id_registry = IDRemapRegistry::default();
    id_registry.register_remap_hook::<WiringTable>(|comp, mapper| {
        let old_idx = comp.target.index();
        let new_entity = mapper.map(old_idx);
        comp.target = new_entity;
    });

    // 3. Test MsgPackArchive
    println!("\n--- Testing MsgPackArchive Remap ---");
    test_remap::<MsgPackArchive>(&source_world, &registry, &id_registry, "remap_test.msgpack");

    // 4. Test AuroraWorldManifest (JSON)
    println!("\n--- Testing AuroraWorldManifest (JSON) Remap ---");
    test_remap::<AuroraWorldManifest>(&source_world, &registry, &id_registry, "remap_test.json");

    // 5. Test WorldSnapshot (JSON)
    println!("\n--- Testing WorldSnapshot (JSON) Remap ---");
    test_remap::<WorldSnapshot>(&source_world, &registry, &id_registry, "remap_snapshot.json");
    
    #[cfg(feature = "arrow_rs")]
    {
        println!("\n--- Testing WorldArrowSnapshot Remap ---");
        test_remap::<WorldArrowSnapshot>(&source_world, &registry, &id_registry, "remap_test.arrow");
    }
}

fn test_remap<A: Archive>(
    src_world: &World,
    registry: &SnapshotRegistry,
    id_registry: &IDRemapRegistry,
    path: &str,
) {
    // Save
    let archive = A::create(src_world, registry).expect("Failed to create archive");
    archive.save_to(path).expect("Failed to save");

    // Load & Remap
    let loaded = A::load_from(path).expect("Failed to load");
    let mut dest_world = World::new();
    
    // Shift IDs
    for _ in 0..100 {
        dest_world.spawn_empty();
    }

    // Build mapper using get_entities()
    let old_entities = loaded.get_entities();
    let mut mapper = HashMap::new();
    for &old_id in &old_entities {
        let new_entity = dest_world.spawn_empty().id();
        mapper.insert(old_id, new_entity);
    }
    
    println!("Built Map for {} entities", mapper.len());
    
    loaded.apply_with_remap(&mut dest_world, registry, id_registry, &mapper).expect("Failed to apply with remap");
    
    // Verify
    let mut found = false;
    let mut query = dest_world.query::<(Entity, &BlockName, Option<&WiringTable>)>();
    for (e, name, wiring) in query.iter(&dest_world) {
        if let Some(w) = wiring {
            if name.0 == "Node2" {
                println!("Node2 loaded at {:?}, target {:?}", e, w.target);
                let node1_old_id = 0;
                let node1_new_id = *mapper.get(&node1_old_id).expect("Node1 mapping missing");
                
                assert_eq!(w.target, node1_new_id);
                found = true;
            }
        }
    }
    assert!(found, "Node2 not found or remapped correctly");
    
    fs::remove_file(path).unwrap_or_default();
    println!("Success!");
}
