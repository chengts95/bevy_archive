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

#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct NestedComponent {
    inner: Position,
    name: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, Component)]
pub struct Vector2([f32; 2]);

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
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
#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct ChildOfWrapper(pub u32);
impl From<&ChildOf> for ChildOfWrapper {
    fn from(c: &ChildOf) -> Self {
        ChildOfWrapper(c.0.index())
    }
}
impl Into<ChildOf> for ChildOfWrapper {
    fn into(self) -> ChildOf {
        ChildOf(Entity::from_raw_u32(self.0).unwrap())
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
    registry
}

fn build_sample_world(world: &mut World) -> Entity {
    world.spawn((Position { x: 1.0, y: 2.0 }, Velocity { dx: 0.1, dy: -0.2 }));
    world.spawn((Tag, Position { x: 9.0, y: 3.5 }));
    world.spawn((Inventory(vec!["potion".into(), "sword".into()]),));

    let mut boss = world.spawn((Position { x: 0.0, y: 0.0 }, Tag));
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

    let snapshot = save_world_snapshot(&world, &registry);
    println!(
        "\n\u{1F4C8} Snapshot: {}",
        serde_json::to_string_pretty(&snapshot).unwrap()
    );

    let path = "example_output.toml";
    save_snapshot_to_file(&snapshot, path).expect("Failed to write TOML");
    println!("\u{1F4BE} Snapshot saved to `{}`", path);

    let loaded = load_snapshot_from_file(path).expect("Failed to load snapshot");
    let mut new_world = World::new();
    let registry = setup_registry();
    load_world_snapshot(&mut new_world, &loaded, &registry);

    let snapshot = save_world_snapshot(&new_world, &registry);
    println!(
        "\n\u{1F4C8} Reloaded Snapshot: {}",
        serde_json::to_string_pretty(&snapshot).unwrap()
    );

    if let Some(children) = new_world.entity(boss_id).get::<Children>() {
        println!("Children of boss {:?}: {:?}", boss_id, children);
    } else {
        println!("⚠️ Boss {:?} has no children after reload", boss_id);
    }

    let _ = fs::remove_file(path);
}

fn main() {
    test_roundtrip_with_children();
}
