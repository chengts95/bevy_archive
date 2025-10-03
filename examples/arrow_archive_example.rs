//! Basic example for the arrow_world_snapshot archive system
//! Demonstrates full-cycle snapshot: save → serialize → load → verify
use std::io::Write;

use bevy_archive::{
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
        ChildOf(Entity::from_raw_u32(self.0).unwrap())
    }
}

#[derive(Debug, Component, Serialize, Deserialize, Clone)]
struct Test {
    pub v: Vec<i32>,
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

// fn test_roundtrip_with_children() {
//     let mut world = World::new();
//     let registry = setup_registry();
//     let boss_id = build_sample_world(&mut world);

//     let snapshot = save_world_manifest(&world, &registry).unwrap();
//     println!(
//         "\n\u{1F4C8} Snapshot: {}",
//         toml::to_string_pretty(&snapshot).unwrap()
//     );

//     let path = "example_output.toml";
//     snapshot.to_file(path, None).unwrap();
//     println!("\u{1F4BE} Snapshot saved to `{}`", path);

//     let mut new_world = World::new();
//     let registry = setup_registry();
//     let loaded = AuroraWorldManifest::from_file(path, None).unwrap();

//     load_world_manifest(&mut new_world, &loaded, &registry).unwrap();
//     let snapshot = save_world_manifest(&new_world, &registry).unwrap();
//     println!(
//         "\n\u{1F4C8} Reloaded Snapshot: {}",
//         toml::to_string_pretty(&snapshot).unwrap()
//     );

//     if let Some(children) = new_world.entity(boss_id).get::<Children>() {
//         println!("Children of boss {:?}: {:?}", boss_id, children);
//     } else {
//         println!("⚠️ Boss {:?} has no children after reload", boss_id);
//     }

//     let _ = fs::remove_file(path);
//     println!("new archtypes len:{}", new_world.archetypes().len());
//     println!("old archtypes len:{}", world.archetypes().len());
// }

fn main() {
    // 初始化世界和组件数据
    let mut world = World::new();
    build_sample_world(&mut world);
    // 注册组件类型
    let registry = setup_registry();

    let arrow = WorldArrowSnapshot::from_world_reg(&world, &registry).unwrap();
    let data = WorldBinArchSnapshot::from(arrow);
    let final_data = rmp_serde::to_vec(&data).unwrap();
    let data: WorldBinArchSnapshot = rmp_serde::from_slice(&final_data).unwrap();
    let arrow = WorldArrowSnapshot::from(data);
    let mut new_world = World::new();
    arrow.to_world_reg(&mut new_world, &registry).unwrap();
    let mut q = new_world.query::<(Entity, &Position)>();
    let zip = arrow.to_zip(Some(6)).unwrap();

    let mut f = std::fs::File::create("ecs_world.zip").unwrap();
    f.write_all(&zip).unwrap();
    
    for (entity, data) in q.iter(&new_world) {
        println!("entity: {} data: {:?}", entity, data);
    }
}
