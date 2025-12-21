//! Basic example for the entity_snapshot archive system
//! Demonstrates full-cycle snapshot: save → serialize → load → verify

use bevy_archive::prelude::*;
use bevy_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;

// === Test Components ===
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

#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Tag;

#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Inventory(Vec<String>);

#[derive(Component, Serialize, Deserialize, Debug, Clone)]
struct NestedComponent {
    inner: Vector2,
    name: String,
}
#[derive(Resource, Serialize, Deserialize, Debug, Clone)]
struct ResComponent {
    inner: Vector2,
    name: String,
    sim_duration: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Component)]
pub struct Vector2([f32; 2]);

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Vector2Wrapper {
    pub x: f32,
    pub y: f32,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct ChildOfWrapper(pub u32);

impl From<&Vector2> for Vector2Wrapper {
    fn from(p: &Vector2) -> Self {
        Self {
            x: p.0[0],
            y: p.0[1],
        }
    }
}
impl From<Vector2Wrapper> for Vector2 {
    fn from(value: Vector2Wrapper) -> Self {
        Vector2([value.x, value.y])
    }
}
impl From<&ChildOf> for ChildOfWrapper {
    fn from(c: &ChildOf) -> Self {
        ChildOfWrapper(c.0.index())
    }
}
impl From<ChildOfWrapper> for ChildOf {
    fn from(value: ChildOfWrapper) -> Self {
        ChildOf(Entity::from_raw_u32(value.0).unwrap())
    }
}
fn setup_registry() -> SnapshotRegistry {
    let mut registry = SnapshotRegistry::default();
    registry.register::<Position>();
    registry.register::<Velocity>();
    registry.register::<Tag>();
    registry.register::<Inventory>();
    registry.register::<NestedComponent>();
    registry.register_with::<Vector2, Vector2Wrapper>();
    registry.register_with::<ChildOf, ChildOfWrapper>();
    registry.resource_register::<ResComponent>();
    registry
}

fn build_sample_world(world: &mut World) -> Entity {
    world.spawn((Position { x: 1.0, y: 2.0 }, Velocity { dx: 0.1, dy: -0.2 }));
    world.spawn((Tag, Position { x: 9.0, y: 3.5 }));
    world.spawn((Inventory(vec!["potion".into(), "sword".into()]),));
    world.insert_resource(ResComponent {
        inner: Vector2([0.0, 3.0]),
        name: "sim_cfg".to_string(),
        sim_duration: 10.0,
    });
    let mut boss = world.spawn((
        Position { x: 0.0, y: 0.0 },
        Tag,
        NestedComponent {
            inner: Vector2([3.0, 2.0]),
            name: "Boss".into(),
        },
        Vector2([3.0, 2.0]),
    ));
    boss.with_children(|children| {
        let minion1 = children
            .spawn((
                Position { x: -1.0, y: 0.0 },
                Inventory(vec!["dagger".into()]),
            ))
            .id();
        let minion2 = children
            .spawn((
                Position { x: 1.0, y: 0.0 },
                Inventory(vec!["shield".into()]),
            ))
            .id();
        println!(
            "Spawned parent {:?} with children {:?} and {:?}",
            children.target_entity(),
            minion1,
            minion2
        );
    });
    boss.id()
}

fn test_roundtrip_with_children() {
    let mut world = World::new();
    let registry = setup_registry();
    let boss_id = build_sample_world(&mut world);

    let snapshot = save_world_manifest(&world, &registry).unwrap();
    println!(
        "\n\u{1F4C8} Snapshot: {}",
        toml::to_string_pretty(&snapshot).unwrap()
    );

    let path = "example_output.toml";
    snapshot.to_file(path, None).unwrap();
    println!("\u{1F4BE} Snapshot saved to `{}`", path);

    let mut new_world = World::new();
    let registry = setup_registry();
    let loaded = AuroraWorldManifest::from_file(path, None).unwrap();
    load_world_manifest(&mut new_world, &loaded, &registry).unwrap();
    let snapshot = save_world_manifest(&new_world, &registry).unwrap();
    println!(
        "\n\u{1F4C8} Reloaded Snapshot: {}",
        toml::to_string_pretty(&snapshot).unwrap()
    );

    if let Some(children) = new_world.entity(boss_id).get::<Children>() {
        println!("Children of boss {:?}: {:?}", boss_id, children);
    } else {
        println!("⚠️ Boss {:?} has no children after reload", boss_id);
    }

    let _ = fs::remove_file(path);
    println!("new archtypes len:{}", new_world.archetypes().len());
    println!("old archtypes len:{}", world.archetypes().len());
}

fn main() {
    test_roundtrip_with_children();
}
