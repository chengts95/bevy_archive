use bevy_archive::prelude::*;
use bevy_archive::binary_archive::msgpack_archive::MsgPackArchive;
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
        // We haven't implemented Archive for WorldArrowSnapshot yet, waiting for next step.
        // But if I implement it, I can uncomment this.
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

    // Custom Mapping Strategy (Manual)
    // For Archive trait we need to build the mapper first?
    // Wait, Archive::apply_with_remap takes &dyn EntityRemapper.
    // But how do we know WHICH entities to map if we don't have the snapshot entities list exposed via Archive trait?
    // The previous manual example used archive.decode_snapshot().
    // The generic Archive trait doesn't expose `decode_snapshot`.
    
    // SOLUTION:
    // We can't implement a fully generic `test_remap` that builds the map unless `Archive` exposes entities.
    // OR we assume a "Blind" mapping (e.g. offset) or we use a hack.
    // BUT, for this test, we can use a "Smart Mapper" that simply maps input ID -> input ID + 100.
    // Because we know we shifted the world by 100 entities.
    // This assumes the archive loads entities into the *next available* slots, which might NOT be contiguous 
    // if the loader spawns them.
    
    // Actually, `load_world_arch_snapshot_with_remap` DOES NOT spawn entities if we pass a mapper.
    // It expects the mapper to return an EXISTING entity.
    // So the caller MUST have spawned them.
    
    // This implies `Archive::apply_with_remap` interface is tricky if it doesn't return the list of entities to map.
    // MsgPackArchive::apply_with_remap (my impl) calls `decode_snapshot`, then `load_world_arch_snapshot_with_remap`.
    // Wait, my `apply_with_remap` implementation in MsgPackArchive:
    // ```
    //     let snap = self.decode_snapshot()?;
    //     load_world_arch_snapshot_with_remap(world, &snap, registry, id_registry, mapper);
    // ```
    // It assumes `mapper` already contains valid mappings. 
    // But who spawns the entities?
    // `load_world_arch_snapshot_with_remap` iterates entities in snapshot, maps them, gets `current_entity`.
    // It does NOT spawn.
    
    // So the USER (caller of apply_with_remap) MUST pre-spawn entities and build the map.
    // But the user can't see the entities in the generic `Archive`.
    
    // Missing API: `Archive::get_entities(&self) -> Vec<u32>` or similar.
    // Or `Archive::prepare_mapping(&self, world: &mut World) -> HashMap<u32, Entity>`.
    
    // For this test, since I know the source entities are 0 and 1 (from fresh world),
    // and I spawned 100 entities in dest world (ids 0..99),
    // I can pre-spawn two entities in dest world (ids 100, 101) and map 0->100, 1->101.
    
    let e_new_1 = dest_world.spawn_empty().id(); // 100
    let e_new_2 = dest_world.spawn_empty().id(); // 101
    
    // We assume source entities were 0v0 and 1v0.
    // Let's verify source IDs.
    // (In `main` we print them).
    
    let mut mapper = HashMap::new();
    mapper.insert(0, e_new_1);
    mapper.insert(1, e_new_2);
    
    println!("Map: 0->{:?}, 1->{:?}", e_new_1, e_new_2);
    
    loaded.apply_with_remap(&mut dest_world, registry, id_registry, &mapper).expect("Failed to apply with remap");
    
    // Verify
    let mut found = false;
    for (e, name, wiring) in dest_world.query::<(Entity, &BlockName, Option<&WiringTable>)>().iter(&dest_world) {
        if let Some(w) = wiring {
            if name.0 == "Node2" {
                println!("Node2 loaded at {:?}, target {:?}", e, w.target);
                assert_eq!(e, e_new_2);
                assert_eq!(w.target, e_new_1);
                found = true;
            }
        }
    }
    assert!(found, "Node2 not found or remapped correctly");
    
    fs::remove_file(path).unwrap_or_default();
}
