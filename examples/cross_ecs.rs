//! Basic example for the entity_snapshot archive system
//! Demonstrates full-cycle snapshot: save → serialize → load → verify

use bevy_archive::archetype_archive::WorldArchSnapshot;
use bevy_archive::flecs_archsnaphot::*;
use bevy_archive::flecs_registry::*;
use bevy_archive::prelude::AuroraWorldManifest;
use bevy_archive::prelude::WorldWithAurora;
use bevy_ecs::prelude::Component as BevyComponent;
use flecs_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
// === Test Components ===
#[derive(Component, BevyComponent, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Position {
    x: f32,
    y: f32,
}

#[derive(Component, BevyComponent, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Velocity {
    dx: f32,
    dy: f32,
}

// #[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
// struct Tag;

#[derive(Component, BevyComponent, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Inventory(Vec<String>);

#[derive(Component, BevyComponent, Serialize, Deserialize, Debug, Clone)]
struct NestedComponent {
    inner: Vector2,
    name: String,
}

#[derive(Clone, BevyComponent, Serialize, Deserialize, Debug, Component)]
pub struct Vector2([f32; 2]);

#[derive(Clone, BevyComponent, Serialize, Deserialize, Debug)]
pub struct Vector2Wrapper {
    pub x: f32,
    pub y: f32,
}
impl From<&Vector2> for Vector2Wrapper {
    fn from(p: &Vector2) -> Self {
        Self {
            x: p.0[0],
            y: p.0[1],
        }
    }
}
impl Into<Vector2> for Vector2Wrapper {
    fn into(self) -> Vector2 {
        Vector2([self.x, self.y])
    }
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ChildOfWrapper(pub u32);
// impl From<&ChildOf> for ChildOfWrapper {
//     fn from(c: &ChildOf) -> Self {
//         ChildOfWrapper(c.0.index())
//     }
// }
// impl Into<ChildOf> for ChildOfWrapper {
//     fn into(self) -> ChildOf {
//         ChildOf(Entity::from_raw(self.0))
//     }
// }
fn setup_registry() -> SnapshotRegistry {
    let mut registry = SnapshotRegistry::default();
    registry.register::<Position>();
    registry.register::<Velocity>();
    // registry.register::<Tag>();
    registry.register::<Inventory>();
    registry.register::<NestedComponent>();
    registry.register::<NameID>();
    registry.register_with::<Vector2, Vector2Wrapper>();
    //registry.register_with::<ChildOf, ChildOfWrapper>();
    registry
}

fn setup_registry_bevy() -> bevy_archive::prelude::SnapshotRegistry {
    let mut registry = bevy_archive::prelude::SnapshotRegistry::default();
    registry.register::<Position>();
    registry.register::<Velocity>();
    //registry.register::<Tag>();
    registry.register::<Inventory>();
    registry.register::<NestedComponent>();
    registry.register_with::<Vector2, Vector2Wrapper>();
    //registry.register_with::<ChildOf, ChildOfWrapper>();
    registry
}

fn build_sample_world(world: &mut World) -> Entity {
    println!("Snapshot:\n");
    // 2. 构建 500 个实体，随机分配组件（直接贴入上面 Python 生成的数据）
    for e in 0..50 {
        let ent = world.entity();
        match e % 7 {
            0 => {
                ent.set(Position {
                    x: 1.0 * e as f32,
                    y: -1.0 * e as f32,
                });
                ent.set_name(format!("entity_{}", e).as_str());
            }
            1 => {
                ent.set(Velocity {
                    dx: e as f32,
                    dy: -e as f32,
                });
                ent.set(Vector2([e as f32, 2.0 * e as f32]));
            }
            2 => {
                ent.set(Inventory(vec!["sword".into(), "apple".into()]));
                ent.set_name(format!("hero_{}", e).as_str());
            }
            3 => {
                ent.set(NestedComponent {
                    inner: Vector2([0.5 * e as f32, 0.25 * e as f32]),
                    name: "omega".into(),
                });
                ent.set_name(format!("boss_{}", e).as_str());
            }
            4 => {
                ent.set(Position {
                    x: 1.1 * e as f32,
                    y: -1.1 * e as f32,
                });
                ent.set(Velocity {
                    dx: -0.5 * e as f32,
                    dy: 0.5 * e as f32,
                });
                ent.set_name(format!("combo_{}", e).as_str());
            }
            5 => {
                ent.set_name(format!("flagged_{}", e).as_str());
            }
            6 => {
                ent.set(Vector2([42.0, -42.0]));
                ent.set_name(format!("vec_{}", e).as_str());
            }
            _ => {}
        }
    }
    world.lookup("boss_3").id()
}
/// Save a snapshot of the ECS `World` into an `AuroraWorldManifest`, which includes
/// archetypes and optionally embedded data.
///
/// This serves as a serializable container that can be persisted or diffed later.
///
/// # Parameters
/// - `world`: The Bevy ECS world to capture.
/// - `registry`: Snapshot registry for (de)serialization logic.
///
/// # Returns
/// A fully structured `AuroraWorldManifest`.
pub fn save_world_manifest(
    world: &World,
    registry: &SnapshotRegistry,
) -> Result<AuroraWorldManifest, String> {
    let snapshot = save_world_arch_snapshot(world, registry);
    let world_with_aurora = WorldWithAurora::from(&snapshot);

    Ok(AuroraWorldManifest {
        metadata: None,
        world: world_with_aurora,
    })
}

/// Load an ECS world from a manifest structure.
///
/// Converts the manifest into internal snapshot data and inserts the data into a world.
///
/// # Parameters
/// - `world`: A mutable ECS world to populate.
/// - `manifest`: The manifest to load from.
/// - `registry`: Component (de)serialization registry.
///
/// # Returns
/// Ok on success, or a string describing the failure.
pub fn load_world_manifest(
    world: &mut World,
    manifest: &AuroraWorldManifest,
    registry: &SnapshotRegistry,
) -> Result<(), String> {
    let snapshot: WorldArchSnapshot = (&manifest.world).into();
    load_world_arch_snapshot(world, &snapshot, registry);
    Ok(())
}
fn test_roundtrip_with_children() {
    let mut world = World::new();

    let registry = setup_registry();

    let _boss_id = build_sample_world(&mut world);

    let snapshot = save_world_manifest(&world, &registry).unwrap();
    println!(
        "\n\u{1F4C8} Snapshot: {}",
        toml::to_string_pretty(&snapshot).unwrap()
    );

    let path = "example_output.toml";
    snapshot.to_file(path, None).unwrap();
    println!("\u{1F4BE} Snapshot saved to `{}`", path);

    let mut new_world = bevy_ecs::prelude::World::new();
    let registry = setup_registry_bevy();
    let loaded = AuroraWorldManifest::from_file(path, None).unwrap();
    bevy_archive::prelude::load_world_manifest(&mut new_world, &loaded, &registry).unwrap();
    let snapshot = bevy_archive::prelude::save_world_manifest(&new_world, &registry).unwrap();
    println!(
        "\n\u{1F4C8} Reloaded Snapshot: {}",
        toml::to_string_pretty(&snapshot).unwrap()
    );

    let _ = fs::remove_file(path);
}

fn main() {
    test_roundtrip_with_children();
}
