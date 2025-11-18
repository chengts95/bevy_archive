use std::collections::HashMap;

use crate::{
    arrow_snapshot::{ComponentTable, EntityID},
    binary_archive::*,
    flecs_registry::{SnapshotRegistry, snapshot_factory::codec::arrow::SnapshotError},
};
use flecs_ecs::{core::flecs::Wildcard, prelude::*};
impl WorldArrowSnapshot {
    pub fn save_archetypes_flecs(
        world: &World,
        registry: &SnapshotRegistry,
    ) -> Result<Vec<ComponentTable>, SnapshotError> {
        let mut reg_comp_ids = HashMap::new();
        registry.entries.iter().for_each(|(name, f)| {
            let id = (f.comp_id)(world).unwrap();
            reg_comp_ids.insert(id, *name);
        });
        let mut vec = Vec::new();
        world
            .query::<()>()
            .with(Wildcard)
            .build()
            .run(|it| {
                let t = Self::save_archetype_flecs(world, registry, it, &reg_comp_ids);
                vec.push(t);
            });
        vec.into_iter().collect()
    }
    pub fn save_archetype_flecs<'a>(
        world: &'a World,
        registry: &'a SnapshotRegistry,
        archetype: TableIter<'a, true>,
        reg_comp_ids: &HashMap<u64, &'a str>,
    ) -> Result<ComponentTable, SnapshotError> {
        if archetype.count() <= 0 {
            return Ok(ComponentTable::default());
        }
        let arch = archetype.archetype().unwrap();
        let can_be_stored = arch
            .as_slice()
            .iter()
            .any(|x| reg_comp_ids.contains_key(&x));

        if !can_be_stored {
            return Ok(ComponentTable::default());
        }

        let mut archetype_snapshot = ComponentTable::default();
        let entities: Vec<_> = archetype.entities().iter().map(|x| x.clone()).collect();

        let entities_ids: Vec<_> = entities
            .iter()
            .map(|&id| EntityID { id: id.0 as u32 })
            .collect();
        archetype_snapshot.entities.extend(entities_ids);

        for cid in arch.as_slice() {
            if let Some(&type_name) = reg_comp_ids.get(cid) {
                let arrow = registry
                    .get_factory(type_name)
                    .and_then(|f| f.arrow.as_ref())
                    .ok_or_else(|| SnapshotError::MissingFactory(type_name.to_string()))?;

                let column = (arrow.arr_export)(&arrow.schema, world, &entities)?;
                archetype_snapshot.insert_column(type_name, column);
            }
        }

        Ok(archetype_snapshot)
    }
}
