use std::collections::{BTreeSet, HashSet};

use crate::{archetype_archive::*, flecs_registry::SnapshotRegistry};

use bimap::BiMap;
use flecs_ecs::core::flecs::{Identifier, Name};
use flecs_ecs::prelude::World;
use flecs_ecs::{
    core::{Entity, flecs::Component as EcsComponent},
    prelude::*,
};
use serde::{Deserialize, Serialize};

#[derive(Component, Default)]
#[allow(dead_code)]
struct SerializeTarget(u32);
#[derive(Component, Serialize, Deserialize, Default)]
pub struct NameID(pub String);
fn all_entities(world: &World) -> &[u64] {
    let handle = world.entity_null();
    let all_entities = unsafe {
        let all_entities = flecs_ecs::sys::ecs_get_entities(handle.world_ptr());
        let slice = std::slice::from_raw_parts(all_entities.ids, all_entities.alive_count as usize);
        slice
    };
    all_entities
}
fn derive_type_mapping_cache(
    reg: &SnapshotRegistry,
    world: &World,
) -> (BiMap<String, Entity>, HashSet<Entity>) {
    let q = world.new_query::<(&EcsComponent, &(Identifier, Name))>();
    let mut map = BiMap::new();
    let mut exclude_meta = HashSet::new();
    q.each_entity(|e, _| {
        e.get_name().inspect(|x| {
            exclude_meta.insert(e.id());
            if reg.type_registry.contains_key(x) {
                map.insert(x.to_string(), e.id());
            }
        });
    });
    (map, exclude_meta)
}
pub fn save_world_arch_snapshot(world: &World, reg: &SnapshotRegistry) -> WorldArchSnapshot {
    let mut world_snapshot = WorldArchSnapshot::default();

    let all_entities = all_entities(&world);
    world.component::<NameID>();
    world_snapshot.entities = all_entities.iter().map(|&x| x as u32).collect();
    world_snapshot.entities.sort_unstable();

    let (map, _exclude) = derive_type_mapping_cache(&reg, &world); // do not want meta by default

 
    let mut archs = vec![];

    world
        .query::<()>()
        .with::<flecs::Wildcard>()
        .build()
        .run_iter(|it, _| {
            if it.count() <= 0 {
                return;
            }
            let arch = it.archetype().unwrap();
            let to_be_serialize: BTreeSet<_> = arch
                .as_slice()
                .iter()
                .filter_map(|&x| map.get_by_right(&Entity(*x)))
                .collect();

            if to_be_serialize.is_empty() {
                return;
            }

            let entities: Vec<_> = it.entities().iter().map(|x| x.0 as u32).collect();

            let mut snap = ArchetypeSnapshot::default();
            snap.entities.extend(entities.as_slice());
            //hack name id for entity we can save only
            if it.entity(0).get_name().is_some() {
                let ty = "NameID";
                snap.add_type(ty, None);
                let col = snap.get_column_mut(ty).unwrap();
                for (idx, _eid) in entities.iter().enumerate() {
                    col[idx] = serde_json::to_value(it.entity(idx).get_name().unwrap()).unwrap();
                }
            }

            to_be_serialize
                .iter()
                .for_each(|ty| snap.add_type(ty, None));
            for ty in &to_be_serialize {
                let f = reg.exporters.get(ty.as_str()).unwrap();
                let col = snap.get_column_mut(ty).unwrap();
                for (idx, eid) in entities.iter().enumerate() {
                    col[idx] = f(world, Entity::new(*eid as u64)).unwrap();
                }
            }

            archs.push(snap);
        });

    world_snapshot.archetypes.extend(archs);
    //world.remove_all::<SerializeTarget>();
    world.remove_all::<NameID>();
    world_snapshot
}

pub fn load_world_arch_snapshot(
    world: &mut World,
    snapshot: &WorldArchSnapshot,
    reg: &SnapshotRegistry,
) {
    world.component::<NameID>();
    let max_entities = snapshot.entities.last().unwrap() + 1;
    world.preallocate_entity_count(max_entities as i32);

    for artype in &snapshot.archetypes {
        let functions = artype
            .component_types
            .iter()
            .map(|x| reg.importers[x.as_str()]);
        let entities = artype.entities();
        world.defer_begin();
        artype.columns.iter().zip(functions).for_each(|(col, f)| {
            for (row, &ent) in entities.iter().enumerate() {
                let entity = world.entity_from_id(ent as u64);
                world.make_alive(entity);

                f(&col[row], &world, entity.id()).unwrap();
            }
        });
        world.defer_end();
        world.defer_begin();
        world.new_query::<&NameID>().each_entity(|e, name| {
            e.set_name(name.0.as_str());
        });

        world.remove_all::<NameID>();
        world.defer_end();
    }
}
#[cfg(test)]
mod test {

    use serde::{Deserialize, Serialize};

    use super::*;

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
    struct Inventory(Vec<String>);

    #[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct NestedComponent {
        inner: Position,
        name: String,
    }

    #[derive(Clone, Serialize, Deserialize, Debug, Component)]
    pub struct Vector2([f32; 2]);

    #[derive(Clone, Serialize, Deserialize, Debug)]
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

    #[test]
    fn test_save_world_arch_snapshot() {
        use flecs_ecs::core::World;
        let world = World::new();
        let mut reg = SnapshotRegistry::default();
        reg.register::<Position>();
        reg.register::<NameID>();
        world.entity_named("test").set(Position { x: 1.0, y: 2.0 });
        let all_entities = all_entities(&world);
        let mut world_snapshot = WorldArchSnapshot::default();
        world_snapshot.entities = all_entities.iter().map(|&x| x as u32).collect();
        world_snapshot.entities.sort_unstable();
        world.component::<NameID>();
        let (map, _) = derive_type_mapping_cache(&reg, &world);
        assert_eq!(
            *map.get_by_left("Position").unwrap(),
            world.component_id::<Position>()
        );
        assert_eq!(
            *map.get_by_left("NameID").unwrap(),
            world.component_id::<NameID>()
        );
        let mut w = save_world_arch_snapshot(&world, &reg);

        w.purge_null();
        println!("{}", toml::to_string_pretty(&w).unwrap());
    }

    #[test]
    fn test_world_arch_snapshot_roundtrip() {
        use flecs_ecs::core::World;

        // 1. 初始化原始 world + 注册所有组件
        let mut reg = SnapshotRegistry::default();
        reg.register::<Position>();
        reg.register::<Velocity>();
        reg.register::<Inventory>();
        reg.register::<NestedComponent>();
        reg.register::<Vector2>();
        reg.register::<NameID>();

        let world = World::new();
        println!("Snapshot:\n");
        // 2. 构建 500 个实体，随机分配组件（直接贴入上面 Python 生成的数据）
        for e in 0..500 {
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
                    ent.set(NameID(format!("hero_{}", e)));
                }
                3 => {
                    ent.set(NestedComponent {
                        inner: Position {
                            x: 0.5 * e as f32,
                            y: 0.25 * e as f32,
                        },
                        name: "omega".into(),
                    });
                    ent.set(NameID(format!("boss_{}", e)));
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
                    ent.set(NameID(format!("combo_{}", e)));
                }
                5 => {
                    ent.set(NameID(format!("flagged_{}", e)));
                }
                6 => {
                    ent.set(Vector2([42.0, -42.0]));
                    ent.set(NameID(format!("vec_{}", e)));
                }
                _ => {}
            }
        }

        // 3. 保存快照
        let snapshot = save_world_arch_snapshot(&world, &reg);
        let _serialized = toml::to_string_pretty(&snapshot).unwrap();

        // 4. 重新创建 world
        let mut new_world = World::new();
        load_world_arch_snapshot(&mut new_world, &snapshot, &reg);

        // 5. 简单验证一个实体是否还原成功
        let restored = new_world.try_lookup("entity_0");
        assert!(restored.is_some());

        let p = restored.unwrap().has::<Position>();
        assert!(p);
    }
}
