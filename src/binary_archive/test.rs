//! Basic example for the arrow_world_snapshot archive system
//! Demonstrates full-cycle snapshot: save → serialize → load → verify
use crate::{
    binary_archive::{WorldArrowSnapshot, WorldBinArchSnapshot},
    prelude::*,
};
use bevy_ecs::prelude::*;
use serde::{Deserialize, Serialize};

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

#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
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

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct TagWrapper {
    value: bool,
}
impl From<&Tag> for TagWrapper {
    fn from(_p: &Tag) -> Self {
        Self { value: true }
    }
}
impl From<TagWrapper> for Tag {
    fn from(_p: TagWrapper) -> Self {
        Self
    }
}
#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct ChildOfWrapper(pub u32);
impl From<&ChildOf> for ChildOfWrapper {
    fn from(c: &ChildOf) -> Self {
        ChildOfWrapper(c.0.index())
    }
}
impl Into<ChildOf> for ChildOfWrapper {
    fn into(self) -> ChildOf {
        ChildOf(Entity::from_raw(self.0))
    }
}

 
fn setup_registry() -> SnapshotRegistry {
    let mut registry = SnapshotRegistry::default();
    registry.register::<Position>();
    registry.register::<Velocity>();

    registry.register_with_mode::<Tag>(SnapshotMode::Placeholder);
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

#[test]
fn test_full_roundtrip_with_arrow_and_manifest() {
    use std::fs;
    use bevy_ecs::world::World;   
    // === 构造原始世界 ===
    let mut world = World::new();
    let registry = setup_registry();
    let boss_id = build_sample_world(&mut world);

    // === ⏺️ Roundtrip 1: Manifest TOML 文件序列化测试 ===
    let snapshot = save_world_manifest(&world, &registry).unwrap();
    let path = "example_output.toml";
    snapshot.to_file(path, None).unwrap();

    let mut new_world = World::new();
    let loaded = AuroraWorldManifest::from_file(path, None).unwrap();
    load_world_manifest(&mut new_world, &loaded, &registry).unwrap();

    if let Some(children) = new_world.entity(boss_id).get::<Children>() {
        println!("🧒 Boss {:?} has children: {:?}", boss_id, children);
        assert!(children.len() >= 2);
    } else {
        panic!("❌ Boss {:?} has no children after reload", boss_id);
    }

    fs::remove_file(path).unwrap();

    // === ⏺️ Roundtrip 2: Arrow → Binary Snapshot → Arrow Snapshot → World ===
    let arrow = WorldArrowSnapshot::from_world_reg(&world, &registry).unwrap();
    let data = WorldBinArchSnapshot::from(arrow.clone());
    let encoded = rmp_serde::to_vec(&data).unwrap();
    let decoded: WorldBinArchSnapshot = rmp_serde::from_slice(&encoded).unwrap();
    let re_arrow = WorldArrowSnapshot::from(decoded);
    let mut binary_world = World::new();
    re_arrow.to_world_reg(&mut binary_world, &registry).unwrap();

    let mut q = binary_world.query::<(Entity, &Position)>();
    let mut found = 0;
    for (entity, pos) in q.iter(&binary_world) {
        println!("📦 entity {:?} → pos {:?}", entity, pos);
        found += 1;
    }

    assert!(found >= 3, "Expected at least 3 Position entities");
    println!("✅ Binary roundtrip complete with {} Position entities", found);
}