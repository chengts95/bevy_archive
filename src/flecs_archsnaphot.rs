use crate::{archetype_archive::*, aurora_archive::*, flecs_registry::SnapshotRegistry};
use flecs_ecs::prelude::World;
pub fn save_world_arch_snapshot(world: &World, reg: &SnapshotRegistry) -> WorldArchSnapshot {
    let mut world_snapshot = WorldArchSnapshot::default();
    let storage_type = StorageTypeFlag::Table;

    world_snapshot
}

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeSet, HashMap, HashSet},
        str,
        sync::Arc,
    };

    use flecs_ecs::{
        core::{
            Entity, EntityView,
            flecs::{Any, Component as EcsComponent, Identifier, Name, Wildcard},
        },
        prelude::{
            Builder, Component, IdOperations, IntoTableRange, QueryAPI, QueryBuilder,
            QueryBuilderImpl, SystemAPI, TableOperations, TermBuilderImpl, WorldProvider,
        },
        sys::{ecs_get_table, ecs_sort_table_action_t, ecs_table_column_count},
    };
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
        use flecs_ecs::core::WorldProvider;
        let mut world = World::new();
        let handle = world.entity_null();
        let mut reg = SnapshotRegistry::default();
        reg.register::<Position>();
        world.entity().set(Position { x: 1.0, y: 2.0 });
        let all_entities = all_entities(&world);

        let map = derive_type_mapping_cache(&reg, &world);
        assert_eq!(map["Position"], world.component_id::<Position>());

        let mut recorded_entities = HashSet::new();
        world
            .system::<()>()
            .run_iter(|iter, _| {
                println!("x");
            })
            .run();
        for &key in reg.type_registry.keys() {
            let id = map[key];
            world.each_entity::<&Any>(|x, _| {
                recorded_entities.insert(x.id());
                println!("x");
            });
        }
        // for e in all_entities {
        //     if recorded_entities.contains(e) {
        //         continue;
        //     }

        //     recorded_entities.insert(e);
        // }
    }

    fn all_entities(world: &World) -> &[u64] {
        let handle = world.entity_null();
        let all_entities = unsafe {
            let all_entities = flecs_ecs::sys::ecs_get_entities(handle.world_ptr());
            let slice =
                std::slice::from_raw_parts(all_entities.ids, all_entities.alive_count as usize);
            slice
        };
        all_entities
    }

    fn derive_type_mapping_cache(reg: &SnapshotRegistry, world: &World) -> HashMap<String, Entity> {
        let q = world
            .query::<(&EcsComponent, &(Identifier, Name))>()
            .build();
        let mut map = HashMap::new();
        q.each_entity(|e, _| {
            e.get_name().inspect(|x| {
                if reg.type_registry.contains_key(x) {
                    map.insert(x.to_string(), e.id());
                }
            });
        });
        map
    }
}
