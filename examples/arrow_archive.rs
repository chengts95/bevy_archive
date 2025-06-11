//! Basic example for the entity_snapshot archive system
//! Demonstrates full-cycle snapshot: save → serialize → load → verify

use arrow::{
    array::RecordBatch,
    compute::concat_batches,
    datatypes::{FieldRef, Schema},
};
use bevy_archive::prelude::*;
use bevy_ecs::prelude::*;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde::{Deserialize, Serialize};
use serde_arrow::{
    marrow::{self, datatypes::Field},
    schema::{SchemaLike, TracingOptions},
};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Cursor,
    sync::Arc,
};
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
#[derive(Debug, Default, Clone)]
struct ComponentTable {
    name: String,
    columns: BTreeMap<String, ArrowColumn>,
    entities: Vec<EntityID>,
}
impl ComponentTable {
    pub fn to_record_batch(&self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        let mut type_map = HashMap::new();
        let ent = ArrowColumn::from_slice(&self.entities).unwrap();
        type_map.insert("id".to_string(), vec!["id".to_string()]);
        for f in &ent.fields {
            fields.push(f.clone());
            arrays.extend(ent.data.clone());
        }
        for (type_name, col) in &self.columns {
            let mut str_fields = Vec::with_capacity(col.fields.len());
            for f in &col.fields {
                let mut meta = f.metadata().clone();
                let mut f = (**f).clone();

                if f.name() == "" {
                    f = f.with_name(format!("{}", type_name));
                } else {
                    let name = format!("{}.{}", type_name, f.name());
                    f = f.with_name(name);
                    meta.insert("prefix".to_string(), type_name.clone());
                }

                str_fields.push(f.name().to_owned());
                fields.push(Arc::new(f.with_metadata(meta)));
            }
            type_map.insert(type_name.to_owned(), str_fields);
            let arrow_arrays = col.data.clone();
            arrays.extend(arrow_arrays);
        }
        let mut schema = arrow::datatypes::Schema::new(fields);

        schema.metadata.insert(
            "type_mapping".to_string(),
            serde_json::to_string(&type_map)?,
        );
        let record_batch = arrow::array::RecordBatch::try_new(Arc::new(schema), arrays);
        Ok(record_batch?)
    }
}
impl ComponentTable {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    pub fn insert_column(&mut self, name: &str, column: ArrowColumn) {
        self.columns.insert(name.to_string(), column);
    }
    pub fn remove_column(&mut self, name: &str) {
        self.columns.remove(name);
    }
    pub fn get_column_mut(&mut self, name: &str) -> Option<&mut ArrowColumn> {
        self.columns.get_mut(name)
    }
    pub fn get_column(&self, name: &str) -> Option<&ArrowColumn> {
        self.columns.get(name)
    }
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct EntityID {
    id: u32,
}
fn main() {
    use arrow::datatypes;
    use base64::prelude::BASE64_STANDARD;
    use serde_arrow::schema::TracingOptions;
    use serde_arrow::{from_arrow, to_arrow};

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
    let record_batch = table.to_record_batch().unwrap();

    let buffer = Cursor::new(Vec::new());

    let data = arrow::csv::WriterBuilder::new();
    let data = data.with_header(true);
    let mut w = data.build(buffer);
    let _ = w.write(&record_batch);
    let buffer = w.into_inner();
    println!("{}", String::from_utf8(buffer.into_inner()).unwrap());
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None).unwrap();
    writer.write(&record_batch).unwrap();
    writer.close().unwrap();

    let buffer_bytes = bytes::Bytes::from_owner(buffer);
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(buffer_bytes)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();
    let batches: Vec<_> = parquet_reader.map(|x| x.unwrap()).collect();
    println!("{:?}", batches[0].schema().metadata());
    let batches = concat_batches(&batches[0].schema(), &batches).unwrap();
    let mut new_table = ComponentTable::default();
    println!("{:?}", batches.schema().metadata());
    //let d: Vec<Position> = serde_arrow::from_record_batch(&batches).unwrap();
    let fields = batches.schema().fields().clone();

    let schema = batches.schema();
    let mut table_builder = HashMap::new();
    for i in fields.iter() {
        let data_type = i.metadata().get("prefix").map_or(i.name(), |v| v);

        let data = table_builder.entry(data_type).or_insert(Vec::new());
        let a = batches.column_by_name(i.name()).unwrap();
        let final_name = i
            .name()
            .strip_prefix(format!("{}.", data_type).as_str())
            .unwrap_or(i.name());
        let field = (**i).clone().with_name(final_name);
        data.push((Arc::new(field), a.clone()));
    }
    for (name, data) in table_builder {
        let arr = ArrowColumn {
            fields: data.iter().map(|(a, _)| a.clone()).collect(),
            data: data.iter().map(|(_, b)| b.clone()).collect(),
        };
        if name == "id" {
            new_table.entities = arr.to_vec().unwrap();
        } else {
            new_table.insert_column(name, arr);
        }
    }
    println!("{ }", new_table.entities.len());
    let d = new_table.get_column("Position").unwrap();

    println!("{:?}", d.to_vec::<Position>().unwrap());
}
