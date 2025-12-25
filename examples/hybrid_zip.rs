#![cfg(feature = "arrow_rs")]

use bevy_archive::prelude::*;
use bevy_archive::zip; // re-exported
use bevy_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::io::Write;

#[derive(Component, Serialize, Deserialize)]
struct Position {
    x: f32,
    y: f32,
}

#[derive(Component, Serialize, Deserialize)]
struct Velocity(f32);

fn main() {
    let mut world = World::new();
    let mut registry = SnapshotRegistry::default();
    registry.register::<Position>();
    registry.register::<Velocity>();

    // Spawn entities
    // Archetype 0: Position + Velocity
    world.spawn((Position { x: 1.0, y: 2.0 }, Velocity(10.0)));
    // Archetype 1: Position
    world.spawn((Position { x: 3.0, y: 4.0 },));

    println!("World created.");

    // Guidance: Save Arch 0 as Parquet (Binary), Arch 1 as CSV (Text).
    let mut guidance = ExportGuidance::embed_all(ExportFormat::Csv); // Default fallback

    // Find archetype indices dynamically
    for (i, arch) in world.archetypes().iter().enumerate() {
        if arch.is_empty() {
            continue;
        }

        let has_pos = arch.contains(world.component_id::<Position>().unwrap());
        let has_vel = arch.contains(world.component_id::<Velocity>().unwrap());

        if has_pos && has_vel {
            println!("Archetype {} (Pos+Vel) -> Parquet", i);
            guidance.set_strategy_for(
                i,
                OutputStrategy::Return(ExportFormat::Parquet, "data/pos_vel.parquet".to_string()),
            );
        } else if has_pos {
            println!("Archetype {} (Pos) -> CSV", i);
            guidance.set_strategy_for(
                i,
                OutputStrategy::Return(ExportFormat::Csv, "data/pos.csv".to_string()),
            );
        }
    }

    // Save to ZIP
    let zip_bytes = save_to_zip_memory(&world, &registry, &guidance);
    std::fs::write("hybrid.zip", &zip_bytes).unwrap();
    println!("Saved hybrid.zip ({} bytes)", zip_bytes.len());

    // Load from ZIP
    let mut new_world = World::new();
    let file = std::fs::File::open("hybrid.zip").unwrap();
    let  archive = zip::ZipArchive::new(file).unwrap();

    // We need a loader that implements BlobLoader
    let mut loader = ZipBlobLoader { archive };

    // Read manifest from ZIP manually to parse it
    let mut manifest_file = loader
        .archive
        .by_name("manifest.toml")
        .expect("manifest.toml missing in zip");
    let mut manifest_content = String::new();
    std::io::Read::read_to_string(&mut manifest_file, &mut manifest_content).unwrap();
    drop(manifest_file); // release borrow to allow loader usage

    let manifest: AuroraWorldManifest = toml::from_str(&manifest_content).unwrap();
    println!("Loaded manifest.");

    // Load with loader
    load_world_manifest_with_loader(&mut new_world, &manifest, &registry, &mut loader).unwrap();

    // Verify
    let mut loaded_count = 0;
    for e in new_world.query::<EntityRef>().iter(& new_world) {
        let id = e.id();
        if let Some(pos) = new_world.get::<Position>(id) {
            println!(" - Entity {:?} has Position: {}, {}", id, pos.x, pos.y);
            loaded_count += 1;
        } else {
            println!(" - Entity {:?} is empty (ignored)", id);
        }
    }
    println!("Loaded relevant entities: {}", loaded_count);
    assert_eq!(loaded_count, 2);

    // Check components
    let mut query = new_world.query::<&Position>();
    for pos in query.iter(&new_world) {
        println!("Loaded Position: {}, {}", pos.x, pos.y);
    }

    println!("Successfully loaded hybrid zip!");

    std::fs::remove_file("hybrid.zip").unwrap();
}

fn save_to_zip_memory(world: &World, reg: &SnapshotRegistry, guidance: &ExportGuidance) -> Vec<u8> {
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let mut zip = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();

    // Generate Aurora Manifest (Hybrid)
    let manifest = WorldWithAurora::from_guided(world, reg, guidance);

    // Write external blobs
    for (path, bytes) in &manifest.external_payloads {
        zip.start_file(path, options).unwrap();
        zip.write_all(bytes).unwrap();
        println!("Wrote {} to zip ({} bytes)", path, bytes.len());
    }

    // Write manifest
    // Create wrapper
    let wrapper = AuroraWorldManifest {
        metadata: None,
        world: manifest,
    };
    let toml = toml::to_string_pretty(&wrapper).unwrap();
    zip.start_file("manifest.toml", options).unwrap();
    zip.write_all(toml.as_bytes()).unwrap();

    zip.finish().unwrap();
    buffer
}
