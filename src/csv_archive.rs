use csv::Reader;
use csv::Writer;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{Read, Result as IoResult, Write};

use super::archetype_archive::ArchetypeSnapshot;
use super::archetype_archive::StorageTypeFlag;

#[derive(Debug, Clone)]
pub struct ComponentColumnGroup {
    pub component: String,   // "TestComponentA"
    pub fields: Vec<String>, // ["TestComponentA.value"]
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnarCsv {
    pub headers: Vec<String>,
    pub columns: Vec<Vec<Value>>,
    pub row_index: Vec<u32>,
    #[serde(skip)]
    pub header_index_map: HashMap<String, usize>,
}

impl ColumnarCsv {
    pub fn new(row_count: usize) -> Self {
        Self {
            headers: Vec::new(),
            columns: Vec::new(),
            row_index: (0..row_count as u32).collect(),
            header_index_map: HashMap::new(),
        }
    }

    fn add_column(&mut self, name: String) -> Result<(), String> {
        if self.header_index_map.contains_key(&name) {
            return Err(format!("Column '{}' already exists", name));
        }
        let idx = self.headers.len();
        self.headers.push(name.clone());
        self.header_index_map.insert(name, idx);
        self.columns.push(vec![Value::Null; self.row_index.len()]);
        Ok(())
    }

    fn add_columns<I: IntoIterator<Item = String>>(&mut self, names: I) -> Result<(), String> {
        for n in names {
            self.add_column(n)?;
        }
        Ok(())
    }

    pub fn get_column_mut(&mut self, name: &str) -> Option<&mut Vec<Value>> {
        self.header_index_map
            .get(name)
            .map(|&i| &mut self.columns[i])
    }

    pub fn to_csv<W: Write>(&self, mut w: W) -> IoResult<()> {
        let mut writer = csv::Writer::from_writer(&mut w);
        writer
            .write_record(std::iter::once("id").chain(self.headers.iter().map(|s| s.as_str())))?;

        for (row, &id) in self.row_index.iter().enumerate() {
            let mut record = Vec::with_capacity(self.headers.len() + 1);
            record.push(id.to_string());
            for col in &self.columns {
                let v = &col[row];
                record.push(if v.is_null() {
                    "".into()
                } else {
                    v.to_string()
                });
            }
            writer.write_record(&record)?;
        }
        writer.flush()
    }

    pub fn from_csv<R: Read>(r: R) -> Result<Self, Box<dyn std::error::Error>> {
        let mut reader = csv::Reader::from_reader(r);
        let mut headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
        assert_eq!(headers.first().map(String::as_str), Some("id"));
        headers.remove(0);

        let mut row_index = Vec::new();
        let mut columns = vec![Vec::new(); headers.len()];

        for rec in reader.records() {
            let rec = rec?;
            row_index.push(rec[0].parse::<u32>()?);
            for (j, f) in rec.iter().skip(1).enumerate() {
                let v = if f.trim().is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(f).unwrap_or(Value::String(f.to_string()))
                };
                columns[j].push(v);
            }
        }

        let header_index_map = headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h.clone(), i))
            .collect();

        Ok(Self {
            headers,
            columns,
            row_index,
            header_index_map,
        })
    }
}
impl From<&ArchetypeSnapshot> for ColumnarCsv {
    fn from(snap: &ArchetypeSnapshot) -> Self {
        columnar_from_snapshot(snap)
    }
}

fn build_schema(snapshot: &ArchetypeSnapshot, strict: bool) -> Vec<ComponentColumnGroup> {
    snapshot
        .columns
        .iter()
        .zip(snapshot.component_types.iter())
        .map(|(col, comp)| {
            let fields: Vec<String> = if strict {
                // 扫描所有行，收集完整字段集合
                let mut set = HashSet::new();
                for v in col {
                    set.extend(infer_schema(comp, v).fields);
                }
                set.into_iter().collect()
            } else {
                // 只看第一行，假定 schema 固定
                infer_schema(comp, col.first().unwrap()).fields
            };
            ComponentColumnGroup {
                component: comp.clone(),
                fields,
            }
        })
        .collect()
}

pub fn columnar_from_snapshot(snapshot: &ArchetypeSnapshot) -> ColumnarCsv {
    columnar_core(snapshot, true) // strict
}

pub unsafe fn columnar_from_snapshot_unchecked(snapshot: &ArchetypeSnapshot) -> ColumnarCsv {
    columnar_core(snapshot, false) // fast but unsafe
}

fn columnar_core(snapshot: &ArchetypeSnapshot, strict: bool) -> ColumnarCsv {
    let schemas = build_schema(snapshot, strict);

    let mut csv = ColumnarCsv::new(snapshot.entities.len());
    csv.add_columns(schemas.iter().flat_map(|s| s.fields.clone()))
        .unwrap();
    csv.row_index.clone_from(&snapshot.entities());

    // 填充数据
    for (values, schema) in snapshot.columns.iter().zip(schemas) {
        for field in schema.fields {
            let suffix = field
                .strip_prefix(&format!("{}.", schema.component))
                .unwrap_or("");
            let col = csv.get_column_mut(&field).unwrap();
            for (i, item) in values.iter().enumerate() {
                col[i] = if let Value::Object(map) = item {
                    map.get(suffix).cloned().unwrap_or(Value::Null)
                } else {
                    item.clone()
                };
            }
        }
    }
    csv
}

pub fn infer_schema(component: &str, value: &Value) -> ComponentColumnGroup {
    match value {
        Value::Object(map) => {
            let mut fields = Vec::new();
            let mut values = Vec::new();

            for (k, v) in map {
                fields.push(format!("{}.{}", component, k));
                values.push(v.clone());
            }

            ComponentColumnGroup {
                component: component.to_string(),
                fields,
            }
        }
        _other => ComponentColumnGroup {
            component: component.to_string(),
            fields: vec![component.to_string()], // 整体值
        },
    }
}

impl ColumnarCsv {
    pub fn to_csv_writer<W: Write>(&self, w: W) -> IoResult<()> {
        let mut writer = Writer::from_writer(w);

        // 写入 header 行
        writer
            .write_record(std::iter::once("id").chain(self.headers.iter().map(|s| s.as_str())))?;

        let row_count = self.row_index.len();
        for row in 0..row_count {
            let mut record = Vec::with_capacity(self.headers.len() + 1);
            record.push(self.row_index[row].to_string());
            for col in &self.columns {
                let value = &col[row];
                record.push(match value {
                    Value::Null => "".into(),
                    _ => value.to_string(),
                });
            }
            writer.write_record(&record)?;
        }

        writer.flush()
    }
}

impl ColumnarCsv {
    pub fn from_csv_reader<R: Read>(r: R) -> Result<Self, Box<dyn std::error::Error>> {
        let mut reader = Reader::from_reader(r);
        let mut headers = reader
            .headers()?
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        assert!(headers.first() == Some(&"id".to_string()));

        headers.remove(0); // remove id from header list
        let mut row_index = Vec::new();
        let mut columns = vec![Vec::new(); headers.len()];

        for result in reader.records() {
            let record = result?;
            row_index.push(record.get(0).unwrap().parse::<u32>()?); // 👈 ID 列

            for (j, field) in record.iter().skip(1).enumerate() {
                let value = if field.trim().is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(field).unwrap_or(Value::String(field.to_string()))
                };
                columns[j].push(value);
            }
        }

        let header_index_map = headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h.clone(), i))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            headers,
            columns,
            row_index,
            header_index_map,
        })
    }
}
fn to_archetype_snapshot(csv: &ColumnarCsv) -> ArchetypeSnapshot {
    let mut component_fields: HashMap<String, Vec<(Option<String>, usize)>> = HashMap::new();

    for (i, header) in csv.headers.iter().enumerate() {
        if let Some((comp, field)) = header.split_once('.') {
            component_fields
                .entry(comp.to_string())
                .or_default()
                .push((Some(field.to_string()), i));
        } else {
            // 整体组件（非结构）
            component_fields
                .entry(header.clone())
                .or_default()
                .push((None, i));
        }
    }

    let mut component_types = Vec::new();
    let mut storage_types = Vec::new();
    let mut columns = Vec::new();
    let entities = csv.row_index.clone();

    for (comp, fields) in component_fields {
        let mut component_column = Vec::new();

        for row in 0..csv.row_index.len() {
            if fields.len() == 1 && fields[0].0.is_none() {
                // 直接是 value
                let col_idx = fields[0].1;
                component_column.push(csv.columns[col_idx][row].clone());
            } else {
                let mut map = serde_json::Map::new();
                for (field_name, col_idx) in &fields {
                    let name = field_name.as_ref().unwrap();
                    map.insert(name.clone(), csv.columns[*col_idx][row].clone());
                }
                component_column.push(Value::Object(map));
            }
        }

        component_types.push(comp);
        storage_types.push(StorageTypeFlag::Table); // default
        columns.push(component_column);
    }

    ArchetypeSnapshot {
        component_types,
        storage_types,
        columns,
        entities,
    }
}

impl From<&ColumnarCsv> for ArchetypeSnapshot {
    fn from(csv: &ColumnarCsv) -> Self {
        to_archetype_snapshot(csv)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::archetype_archive::load_world_arch_snapshot;
    use crate::archetype_archive::save_world_arch_snapshot;
    use crate::bevy_registry::SnapshotRegistry;
    use bevy_ecs::prelude::*;
    use serde::Deserialize;
    use serde::Serialize;
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentA {
        pub value: i32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentB {
        pub value: f32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentC {
        pub value: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentD {
        pub value: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentE(Vec<f64>);
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentF(TestComponentC);
    fn init_world() -> (World, SnapshotRegistry) {
        let mut world = World::new();
        let mut registry = SnapshotRegistry::default();

        // 注册组件类型
        registry.register::<TestComponentA>();
        registry.register::<TestComponentB>();
        registry.register::<TestComponentC>();
        registry.register::<TestComponentD>();
        registry.register::<TestComponentE>();
        registry.register::<TestComponentF>();
        // 构建不同组合的 archetype
        for i in 0..10 {
            world.spawn((
                TestComponentA { value: i },
                TestComponentB {
                    value: i as f32 * 0.1,
                },
            ));
            world.spawn((
                TestComponentB {
                    value: i as f32 * 0.2,
                },
                TestComponentC {
                    value: format!("EntityC{}", i),
                },
            ));
            world.spawn((
                TestComponentA { value: i * 2 },
                TestComponentC {
                    value: format!("EntityAC{}", i),
                },
                TestComponentD { value: i % 2 == 0 },
            ));
            world.spawn((
                TestComponentD { value: i % 3 == 0 },
                TestComponentE(vec![i as f64, i as f64 + 1.0]),
            ));
            world.spawn((
                TestComponentA { value: -i },
                TestComponentB {
                    value: -i as f32 * 0.3,
                },
                TestComponentC {
                    value: format!("Combo{}", i),
                },
                TestComponentD { value: i % 5 == 0 },
                TestComponentE(vec![0.0; i as usize % 10 + 1]),
                TestComponentF(TestComponentC {
                    value: format!("Nested{}", i),
                }),
            ));
        }

        (world, registry)
    }

    #[test]
    fn test_csv_archetype_snapshot() {
        let (world, registry) = init_world();
        let snapshot = save_world_arch_snapshot(&world, &registry);
        assert_eq!(snapshot.entities.len(), 10 * 5);
        let csv = unsafe { columnar_from_snapshot_unchecked(&snapshot.archetypes[0]) };
        assert_eq!(csv.headers.len(), snapshot.archetypes[0].columns.len());
        println!("CSV Headers: {:?}", csv.headers);
        println!("CSV Row Index: {:?}", csv.row_index);
        println!("CSV Columns: {:?}", csv.columns);

        csv.to_csv_writer(io::stdout()).unwrap();
    }
    #[test]
    fn test_csv_snapshot_roundtrip() {
        let (mut world, registry) = init_world();
        let mut snapshot = save_world_arch_snapshot(&world, &registry);
        let csv = unsafe { columnar_from_snapshot_unchecked(&snapshot.archetypes[0]) };
        let new_snap: ArchetypeSnapshot = (&csv).into();

        assert_eq!(
            new_snap.entities.len(),
            snapshot.archetypes[0].entities.len()
        );
        snapshot.archetypes[0] = new_snap;
        load_world_arch_snapshot(&mut world, &snapshot, &registry);
    }
    #[test]
    fn test_csv_archetype_snapshot_roundtrip() {
        let (world, registry) = init_world();
        let snapshot = save_world_arch_snapshot(&world, &registry);
        assert_eq!(snapshot.entities.len(), 10 * 5);
        let csv = unsafe { columnar_from_snapshot_unchecked(&snapshot.archetypes[0]) };
        assert_eq!(csv.headers.len(), snapshot.archetypes[0].columns.len());
        println!("CSV Headers: {:?}", csv.headers);
        println!("CSV Row Index: {:?}", csv.row_index);
        println!("CSV Columns: {:?}", csv.columns);
        let mut v = Vec::new();
        csv.to_csv_writer(&mut v).unwrap();
        let new_csv = ColumnarCsv::from_csv_reader(v.as_slice()).unwrap();
        let mut nv = Vec::new();
        new_csv.to_csv_writer(&mut nv).unwrap();
        assert_eq!(nv, v);
    }
}
