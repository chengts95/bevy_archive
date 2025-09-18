//! Basic example for the entity_snapshot archive system
//! Demonstrates full-cycle snapshot: save → serialize → load → verify

use bevy_archive::{
    arrow_archive::{ComponentTable, EntityID},
    prelude::*,
};
use bevy_ecs::{component::ComponentId, prelude::*};
use serde::{Deserialize, Serialize};
use serde_arrow::{
    marrow::{self, datatypes::Field},
    schema::SchemaLike,
};
use std::collections::HashMap;
#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct PositionInner {
    x: f32,
    y: f32,
}

// === Test Components ===
#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Position(pub f32, pub PositionInner);
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
// fn setup_registry() -> SnapshotRegistry {
//     let mut registry = SnapshotRegistry::default();
//     registry.register::<Position>();
//     registry.register::<Velocity>();
//     registry.register::<Tag>();
//     registry.register::<Inventory>();
//     registry.register::<NestedComponent>();
//     registry.register_with::<Vector2, Vector2Wrapper>();
//     registry.register_with::<ChildOf, ChildOfWrapper>();
//     registry.resource_register::<ResComponent>();
//     registry
// }

// fn build_sample_world(world: &mut World) -> Entity {
//     world.spawn((Position { x: 1.0, y: 2.0 }, Velocity { dx: 0.1, dy: -0.2 }));
//     world.spawn((Tag, Position { x: 9.0, y: 3.5 }));
//     world.spawn((Inventory(vec!["potion".into(), "sword".into()]),));
//     world.insert_resource(ResComponent {
//         inner: Vector2([0.0, 3.0]),
//         name: "sim_cfg".to_string(),
//         sim_duration: 10.0,
//     });
//     let mut boss = world.spawn((
//         Position { x: 0.0, y: 0.0 },
//         Tag,
//         NestedComponent {
//             inner: Vector2([3.0, 2.0]),
//             name: "Boss".into(),
//         },
//         Vector2([3.0, 2.0]),
//     ));
//     boss.with_children(|children| {
//         let minion1 = children
//             .spawn((
//                 Position { x: -1.0, y: 0.0 },
//                 Inventory(vec!["dagger".into()]),
//             ))
//             .id();
//         let minion2 = children
//             .spawn((
//                 Position { x: 1.0, y: 0.0 },
//                 Inventory(vec!["shield".into()]),
//             ))
//             .id();
//         println!(
//             "Spawned parent {:?} with children {:?} and {:?}",
//             children.target_entity(),
//             minion1,
//             minion2
//         );
//     });
//     boss.id()
// }

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
use bevy_archive::bevy_registry::vec_snapshot_factory::*;

#[derive(Debug, Component, Serialize, Deserialize, Clone)]
struct Test {
    pub v: Vec<i32>,
}
#[derive(Debug, Clone, Default)]
pub struct WorldArrowSnapshot {
    pub entities: Vec<u32>,
    pub archetypes: Vec<ComponentTable>,
}
fn main() {
    use arrow::datatypes;

    use serde_arrow::schema::TracingOptions;
    use serde_arrow::to_arrow;

    // 初始化世界和组件数据
    let mut world = World::new();
    let positions: Vec<Position> = (0..1000)
        .map(|i| {
            Position(
                i as f32,
                PositionInner {
                    x: i as f32,
                    y: (i * 2) as f32,
                },
            )
        })
        .collect();
    let velocities: Vec<Velocity> = (0..1000)
        .map(|i| Velocity {
            dx: (i + 1) as f32,
            dy: (i * 4) as f32,
        })
        .collect();

    // 批量生成实体
    world.spawn_batch(positions.into_iter().zip(velocities.into_iter()));

    // 获取字段定义
    let pos_fields =
        Vec::<datatypes::FieldRef>::from_type::<Position>(TracingOptions::default()).unwrap();
    let vel_fields =
        Vec::<datatypes::FieldRef>::from_type::<Velocity>(TracingOptions::default()).unwrap();

    // 注册组件类型
    let mut registry = SnapshotRegistry::default();
    registry.register::<Position>();
    registry.register::<Velocity>();
    registry.register::<Test>();

    // 查询组件数据
    let pos_data: Vec<_> = world.query::<&Position>().iter(&world).collect();
    // 序列化为 Arrow 格式
    let arrays = to_arrow(&pos_fields, pos_data).unwrap();
    let vel_data: Vec<_> = world.query::<&Velocity>().iter(&world).collect();
    let entities: Vec<_> = world
        .iter_entities()
        .map(|x| EntityID { id: x.id().index() })
        .collect();
    // 序列化为 Arrow 格式
    let v_arrays = to_arrow(&vel_fields, vel_data).unwrap();
    let mut e = Field::default();
    e.data_type = marrow::datatypes::DataType::Int32;
    e.name = String::from("id");

    let arrow_column = ArrowColumn {
        fields: pos_fields.clone(),
        data: arrays.clone(),
    };
    let v_arrow_column = ArrowColumn {
        fields: vel_fields.clone(),
        data: v_arrays.clone(),
    };

    let mut table = ComponentTable::default();

    table.insert_column("Position", arrow_column);
    table.insert_column("Velocity", v_arrow_column);
    table.entities.extend_from_slice(&entities);
    let _record_batch = table.to_record_batch().unwrap();

    // let csv = table.to_csv().unwrap();
    // println!("{ }", csv);
    let parquet = table.to_parquet().unwrap();
    let new_table = ComponentTable::from_parquet_u8(&parquet).unwrap();

    let d = new_table.get_column("Position").unwrap();

    let archetypes = world.archetypes().iter().filter(|x| !x.is_empty());

    let reg_comp_ids: HashMap<ComponentId, &str> = registry
        .type_registry
        .keys()
        .filter_map(|&name| {
            registry
                .comp_id_by_name(name, &world)
                .map(|cid| (cid, name))
        })
        .collect();
    let mut world_snapshot = WorldArrowSnapshot::default();
    let snap = archetypes.map(|archetype| {
        let can_be_stored = archetype
            .components()
            .any(|x| reg_comp_ids.contains_key(&x));
        if !can_be_stored {
            return ComponentTable::default();
        }
        let mut archetype_snapshot = ComponentTable::default();
        let entities: Vec<_> = archetype.entities().iter().map(|x| x.id()).collect();
        let entities_ids: Vec<_> = archetype
            .entities()
            .iter()
            .map(|x| (EntityID { id: x.id().index() }))
            .collect();
        archetype_snapshot.entities.extend(entities_ids.as_slice());

        archetype.components().for_each(|x| {
            if reg_comp_ids.contains_key(&x) {
                let type_name = reg_comp_ids[&x];
                // let t = archetype.get_storage_type(x).map(|x| match x {
                //     StorageType::Table => StorageTypeFlag::Table,
                //     StorageType::SparseSet => StorageTypeFlag::SparseSet,
                // });
                let arrow = &registry.get_factory(type_name).unwrap().arrow;
                let arrow = arrow.as_ref().unwrap();
                let column = (arrow.arr_export)(&arrow.schema, &world, &entities);
                archetype_snapshot.insert_column(type_name, column.unwrap());
            }
        });

        archetype_snapshot
    });
    world_snapshot.archetypes = snap.collect();
    world_snapshot.entities = world.iter_entities().map(|x| x.id().index()).collect();
}
