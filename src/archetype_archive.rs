use bevy_ecs::{
    component::{ComponentId, StorageType},
    entity::EntityRow,
    prelude::*,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, vec};

use crate::{
    bevy_registry::SnapshotMode, bevy_registry::SnapshotRegistry, prelude::DeferredEntityBuilder,
};

use super::entity_archive::{self as archive, *};

pub(crate) trait WorldExt {
    fn iter_entities(&self) -> impl Iterator<Item = Entity> + '_;
}
impl WorldExt for World {
    #[inline]
    fn iter_entities(&self) -> impl Iterator<Item = Entity> + '_ {
        self.archetypes().iter().flat_map(|archetype| {
            archetype
                .entities_with_location()
                .map(|(entity, _location)| entity)
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum StorageTypeFlag {
    /// 直接存储
    #[default]
    Table,
    /// 通过引用存储
    SparseSet,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct ArchetypeSnapshot {
    pub component_types: Vec<String>,         // 顺序确定！
    pub storage_types: Vec<StorageTypeFlag>,  // 与 component_types 对齐
    pub columns: Vec<Vec<serde_json::Value>>, // 每列为一个组件的全部值
    pub entities: Vec<u32>,                   // entity_id → row idx
}
impl ArchetypeSnapshot {
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }
    fn get_column_index_or_err(&self, type_name: &str) -> Result<usize, String> {
        self.get_column_index(type_name)
            .ok_or_else(|| format!("Component '{}' not found", type_name))
    }
    pub fn get_column_index(&self, type_name: &str) -> Option<usize> {
        self.component_types.iter().position(|t| t == type_name)
    }
    pub fn has_component(&self, type_name: &str) -> bool {
        self.get_column_index(type_name).is_some()
    }
    pub fn get_entity(&self, entity: u32) -> Option<Vec<(&str, &Value)>> {
        let row = self.entities.iter().position(|x| x == &entity)?;
        Some(self.get_row(row))
    }
    pub fn get_mut(&mut self, entity_id: u32, type_name: &str) -> Option<&mut Value> {
        let row = self.entities.iter().position(|x| x == &entity_id)?;
        let col = self.component_types.iter().position(|t| t == type_name)?;
        Some(&mut self.columns[col][row])
    }
    pub fn get_row(&self, row: usize) -> Vec<(&str, &Value)> {
        self.component_types
            .iter()
            .zip(self.columns.iter())
            .map(|(t, col)| (t.as_str(), &col[row]))
            .collect()
    }
    pub fn get_column(&self, type_name: &str) -> Option<&Vec<Value>> {
        self.get_column_index(type_name)
            .map(|idx| &self.columns[idx])
    }
    pub fn get_column_mut(&mut self, type_name: &str) -> Option<&mut Vec<Value>> {
        self.get_column_index(type_name)
            .map(|idx| &mut self.columns[idx])
    }
    pub fn entities(&self) -> &Vec<u32> {
        &self.entities
    }
    pub fn insert_component(
        &mut self,
        entity_idx: usize,
        type_name: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        let idx = self.get_column_index_or_err(type_name)?;
        if entity_idx >= self.entities.len() {
            return Err("Invalid entity index".into());
        }
        self.columns[idx][entity_idx] = value;
        Ok(())
    }

    pub fn add_type(&mut self, type_name: &str, storage_type: Option<StorageTypeFlag>) {
        self.component_types.push(type_name.to_string());
        self.columns
            .push(vec![serde_json::Value::Null; self.entities.len()]);
        if let Some(storage_type) = storage_type {
            self.storage_types.push(storage_type);
        } else {
            self.storage_types.push(StorageTypeFlag::Table);
        }
    }
    pub fn remove_type(&mut self, type_name: &str) {
        if let Some(index) = self.get_column_index(type_name) {
            self.component_types.remove(index);
            self.columns.remove(index);
            self.storage_types.remove(index);
        }
    }

    pub fn validate_snapshot(snapshot: &ArchetypeSnapshot) -> Result<(), String> {
        let n_types = snapshot.component_types.len();
        let n_entities = snapshot.entities.len();

        if snapshot.columns.len() != n_types {
            return Err("Component type count mismatch".to_string());
        }

        for (i, col) in snapshot.columns.iter().enumerate() {
            if col.len() != n_entities {
                return Err(format!(
                    "Column {} has length {}, expected {}",
                    i,
                    col.len(),
                    n_entities
                ));
            }
        }

        Ok(())
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorldArchSnapshot {
    pub entities: Vec<u32>,
    pub archetypes: Vec<ArchetypeSnapshot>,
}
impl WorldArchSnapshot {
    pub fn purge_null(&mut self) {
        self.entities.clear();
        self.archetypes.iter().for_each(|x| {
            self.entities.extend_from_slice(x.entities.as_slice());
        });
        //we may want to deduplicate entities here
        self.entities.sort_unstable();
    }
}
pub fn load_world_resource(
    data: &HashMap<String, serde_json::Value>,
    world: &mut World,
    reg: &SnapshotRegistry,
) {
    let loadable_resource = data.keys();
    for res in loadable_resource {
        let factory = reg.get_res_factory(res);
        match factory {
            Some(factory) => {
                (factory.js_value.import)(&data[res], world, Entity::from_raw_u32(0).unwrap())
                    .unwrap();
            }
            None => {
                //may need to emit warnings here
            }
        }
    }
}
pub fn save_world_resource(
    world: &World,
    reg: &SnapshotRegistry,
) -> HashMap<String, serde_json::Value> {
    let mut map = HashMap::new();
    let saveable_resource = reg.resource_entries.keys();
    for res in saveable_resource {
        let value = (reg.get_res_factory(res).unwrap().js_value.export)(
            world,
            Entity::from_raw_u32(0).unwrap(),
        );
        if let Some(value) = value {
            map.insert(res.to_string(), value);
        }
    }
    map
}
pub fn save_world_arch_snapshot(world: &World, reg: &SnapshotRegistry) -> WorldArchSnapshot {
    let mut world_snapshot = WorldArchSnapshot::default();
    world_snapshot.entities = WorldExt::iter_entities(world).map(|e| e.index()).collect();
    world_snapshot.entities.sort_unstable();
    let archetypes = world.archetypes().iter().filter(|x| !x.is_empty());
    let reg_comp_ids: HashMap<ComponentId, &str> = reg
        .type_registry
        .keys()
        .filter_map(|&name| reg.comp_id_by_name(name, &world).map(|cid| (cid, name)))
        .collect();

    let snap = archetypes.map(|archetype| {
        let can_be_stored = archetype
            .components()
            .iter()
            .any(|x| reg_comp_ids.contains_key(&x));
        if !can_be_stored {
            return ArchetypeSnapshot::default();
        }
        let mut archetype_snapshot = ArchetypeSnapshot::default();
        let entities: Vec<_> = archetype
            .entities()
            .iter()
            .map(|x| x.id().index())
            .collect();
        archetype_snapshot.entities.extend(entities.as_slice());
        let iter = entities;
        archetype.components().iter().for_each(|x| {
            if reg_comp_ids.contains_key(&x) {
                let type_name = reg_comp_ids[&x];
                let t = archetype.get_storage_type(*x).map(|x| match x {
                    StorageType::Table => StorageTypeFlag::Table,
                    StorageType::SparseSet => StorageTypeFlag::SparseSet,
                });
                let f = reg.get_factory(type_name).unwrap().js_value.export;
                archetype_snapshot.add_type(type_name, t);
                let col = archetype_snapshot.get_column_mut(type_name).unwrap();
                for (idx, &entity) in iter.iter().enumerate() {
                    let entity = EntityRow::from_raw_u32(entity as u32).unwrap();
                    let entity = world.entities().resolve_from_id(entity).unwrap();
                    let serialized = f(world, entity).unwrap();
                    col[idx] = serialized;
                }
            }
        });

        archetype_snapshot
    });
    world_snapshot.archetypes.extend(snap);

    world_snapshot
}
fn count_entities(snapshot: &WorldArchSnapshot) -> u32 {
    snapshot.entities.last().map(|x| *x).unwrap_or(0) + 1
}
pub fn load_world_arch_snapshot(
    world: &mut World,
    snapshot: &WorldArchSnapshot,
    reg: &SnapshotRegistry,
) {
    world.entities().reserve_entities(count_entities(snapshot));
    world.flush();

    for arch in &snapshot.archetypes {
        let entities = arch.entities();
        for type_name in arch.component_types.iter() {
            // meta info is not strict constraint for loading
            // let storage_type = match arch.storage_types[i] {
            //     StorageTypeFlag::Table => StorageType::Table,
            //     StorageTypeFlag::SparseSet => StorageType::SparseSet,
            // };
            let col = arch.get_column(&type_name).unwrap();
            let un = entities.iter().zip(col.iter());
            for (entity_id, value) in un {
                let entity = Entity::from_row(EntityRow::from_raw_u32(*entity_id as u32).unwrap());
                match reg.get_factory(&type_name).map(|x| x.js_value.import) {
                    Some(func) => {
                        if let Err(e) = func(value, world, entity) {
                            eprintln!(
                                "[ImportError] type='{}', entity={:?}, error={}",
                                type_name, entity, e
                            );
                        }
                    }
                    None => {
                        // eprintln!(
                        //     "[MissingImporter] type='{}', entity={:?}",
                        //     type_name, entity
                        // );
                    }
                }
            }
        }
    }
}

pub fn load_world_arch_snapshot_defragment(
    world: &mut World,
    snapshot: &WorldArchSnapshot,
    reg: &SnapshotRegistry,
) {
    world.entities().reserve_entities(count_entities(snapshot));
    world.flush();

    for arch in &snapshot.archetypes {
        let entities = arch.entities();

        let arch_info: Vec<_> = arch
            .component_types
            .iter()
            .enumerate()
            .filter_map(|(col_idx, type_name)| {
                let Some(factory) = reg.get_factory(&type_name) else {
                    //we can emit warnings here
                    return None;
                };
                let id = reg
                    .comp_id_by_name(type_name.as_str(), world)
                    .or_else(|| Some(reg.reg_by_name(type_name, world)))?;
                let mode = factory.mode;
                Some((col_idx, factory.js_value.dyn_ctor, id, mode))
            })
            .collect();

        let mut bump = bumpalo::Bump::new();
        for (row, entity) in entities.iter().enumerate() {
            let entity = EntityRow::from_raw_u32(*entity).unwrap();
            let current_entity = world.entities().resolve_from_id(entity).unwrap();

            let mut builder = DeferredEntityBuilder::new(world, &bump, current_entity);
            for &(col_idx, ctor, comp_id, mode) in arch_info.iter() {
                let col = &arch.columns[col_idx];
                let (id, comp_ptr) = (comp_id, ctor(&col[row], &bump).unwrap());
                match mode {
                    SnapshotMode::Full => {
                        builder.insert_by_id(id, comp_ptr);
                    }

                    SnapshotMode::EmplaceIfNotExists => {
                        builder.insert_if_new_by_id(id, comp_ptr);
                    }
                }
            }

            builder.commit();
            bump.reset();
        }
    }
}

impl From<&WorldArchSnapshot> for archive::WorldSnapshot {
    fn from(snapshot: &WorldArchSnapshot) -> Self {
        let entities = convert_to_entity_snapshot(&snapshot.archetypes);
        Self { entities }
    }
}

impl From<&archive::WorldSnapshot> for WorldArchSnapshot {
    fn from(snapshot: &archive::WorldSnapshot) -> Self {
        let entities = snapshot.entities.iter().map(|e| e.id as u32).collect();
        Self {
            entities,
            archetypes: convert_to_archetype_snapshot(&snapshot.entities),
        }
    }
}
fn convert_to_archetype_snapshot(entities: &[EntitySnapshot]) -> Vec<ArchetypeSnapshot> {
    // Grouped by component type sets
    let mut archetype_map: HashMap<Vec<String>, ArchetypeSnapshot> = HashMap::new();

    for ent in entities {
        // 先按组件名排序作为分类 key（顺序必须稳定）
        let mut type_names: Vec<String> = ent.components.iter().map(|c| c.r#type.clone()).collect();
        type_names.sort();

        // 获取/创建 archetype snapshot
        let snapshot = archetype_map.entry(type_names.clone()).or_insert_with(|| {
            let mut s = ArchetypeSnapshot::default();
            for type_name in &type_names {
                s.add_type(type_name, None);
            }
            s
        });

        // 当前实体在哪一行？
        snapshot.entities.push(ent.id as u32);

        // 将组件数据填入对应列
        for (type_name, column) in snapshot
            .component_types
            .iter()
            .zip(snapshot.columns.iter_mut())
        {
            // 查找实体中这个组件的值
            if let Some(comp) = ent.components.iter().find(|c| &c.r#type == type_name) {
                column.push(comp.value.clone());
            } else {
                column.push(serde_json::Value::Null); // 不存在该组件，补 Null
            }
        }
    }

    // 转换为 Vec 输出
    archetype_map.into_values().collect()
}
fn convert_to_entity_snapshot(archs: &[ArchetypeSnapshot]) -> Vec<EntitySnapshot> {
    let mut entities = Vec::new();

    for arch in archs {
        for (row_idx, &entity_id) in arch.entities.iter().enumerate() {
            let mut components = Vec::new();

            for (col_idx, type_name) in arch.component_types.iter().enumerate() {
                let value = arch.columns[col_idx][row_idx].clone();
                components.push(ComponentSnapshot {
                    r#type: type_name.clone(),
                    value,
                });
            }

            entities.push(EntitySnapshot {
                id: entity_id as u64,
                components,
            });
        }
    }

    entities
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_ecs::world::World;

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
    fn test_multi_archetype_snapshot() {
        let (world, registry) = init_world();
        let snapshot = save_world_arch_snapshot(&world, &registry);
        assert_eq!(snapshot.entities.len(), 10 * 5);

        // 输出结果片段看看效果吧
        for (idx, arch) in snapshot.archetypes.iter().enumerate() {
            println!("Archetype {}:", idx);
            println!(" - Components: {:?}", arch.component_types);
            println!(" - Entity Count: {}", arch.entities.len());
        }

        // 顺便验证结构一致性
        for arch in &snapshot.archetypes {
            ArchetypeSnapshot::validate_snapshot(arch).unwrap();
        }
        println!("Snapshot validation passed!");
        println!(
            "Snapshot: {}",
            serde_json::to_string_pretty(&snapshot).unwrap()
        );
    }
    #[test]
    fn test_roundtrip_archetype_snapshot() {
        // 第一步：初始化世界
        let (world, registry) = init_world();

        // 第二步：保存快照
        let snapshot_1 = save_world_arch_snapshot(&world, &registry);

        // 第三步：构建空世界并加载
        let mut world_new = World::new();
        load_world_arch_snapshot_defragment(&mut world_new, &snapshot_1, &registry);

        // 第四步：再次保存快照
        let snapshot_2 = save_world_arch_snapshot(&world_new, &registry);

        // 第五步：序列化比较（不直接比较结构体，避免 HashMap/BTreeMap 顺序误差）
        let json_1 = serde_json::to_string_pretty(&snapshot_1).unwrap();
        let json_2 = serde_json::to_string_pretty(&snapshot_2).unwrap();

        println!("Snapshot 1:\n{}\n", json_1);
        println!("Snapshot 2:\n{}\n", json_2);

        assert_eq!(json_1, json_2, "Roundtrip snapshot mismatch!");

        println!("🎉 Roundtrip snapshot test passed!");
    }

    #[test]
    fn test_convert_to_entity_snapshot() {
        let (world, registry) = init_world();
        let snapshot = save_world_arch_snapshot(&world, &registry);
        assert_eq!(snapshot.entities.len(), 10 * 5);
        let entities: archive::WorldSnapshot = (&snapshot).into();
        println!("{}", serde_json::to_string(&entities).unwrap());
    }

    #[test]
    fn test_convert_from_entity_snapshot() {
        let (world, registry) = init_world();
        let snapshot = archive::save_world_snapshot(&world, &registry);
        assert_eq!(snapshot.entities.len(), 10 * 5);
        let entities: WorldArchSnapshot = (&snapshot).into();
        println!("{}", serde_json::to_string(&entities).unwrap());
    }
}
