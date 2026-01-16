//! Test aurora-format snapshot roundtrip + archetype integrity
//! Requires `bevy_archive` with Aurora snapshot format enabled

use bevy_archive::{
    archetype_archive::{WorldArchSnapshot, load_world_arch_snapshot},
    prelude::*,
    bevy_cmdbuffer::HarvardCommandBuffer,
};
use bevy_ecs::prelude::*;
use bevy_ecs::ptr::OwningPtr;
use std::ptr::NonNull;
use bumpalo::Bump;
use serde::{Deserialize, Serialize};
use std::fs;

macro_rules! define_test_components {
    ($($name:ident),*) => {
        $(
            #[derive(Component, Serialize, Deserialize, Clone, Debug, PartialEq)]
            struct $name(usize);
        )*

        fn register(reg: &mut SnapshotRegistry) {
            $(reg.register::<$name>();)*
        }
    };
}

define_test_components!(
    TestComp1, TestComp2, TestComp3, TestComp4, TestComp5, TestComp6, TestComp7, TestComp8,
    TestComp9, TestComp10
);
macro_rules! fixed_archetypes {
    ($max:expr) => {{
        let list = vec![
            vec![0, 1, 2, 6],
            vec![1, 2, 3, 3], // duplicated type to verify the override behavior
            vec![0, 3, 4, 5],
            vec![2, 3, 4, 6],
            vec![5, 6, 7, 9],
            vec![6, 7, 8, 5],
            vec![5, 8, 9, 0],
            vec![0, 5, 9, 6],
            vec![1, 6, 8, 2],
            vec![2, 7, 9, 4],
            vec![0, 3, 2, 1], //In the old way, these will trigger extra archtype if we do not merge to bundle.
            vec![0, 1, 2, 3],
            vec![0, 2, 3, 1],
            (0..$max).collect(),
        ];
        list
    }};
}

fn insert_comp<T: Component>(world: &mut World, buffer: &mut HarvardCommandBuffer, entity: Entity, val: T) {
    let comp_id = world.component_id::<T>().unwrap_or_else(|| world.register_component::<T>());
    // Unsafe fix for simultaneous borrow:
    let bump_ptr = buffer.data_bump() as *const Bump;
    let ptr = unsafe { (*bump_ptr).alloc(val) as *mut T };
    let abox = unsafe { ArenaBox::new::<T>(OwningPtr::new(NonNull::new_unchecked(ptr.cast()))) };
    buffer.insert_box(entity, comp_id, abox);
}

// rustfmt::skip
fn build_with_deferred(world: &mut World) {
    let mut buffer = HarvardCommandBuffer::new();
    let archetypes = fixed_archetypes!(10);
    
    // Register components first to get stable IDs (optional but good practice)
    world.register_component::<TestComp1>();
    world.register_component::<TestComp2>();
    world.register_component::<TestComp3>();
    world.register_component::<TestComp4>();
    world.register_component::<TestComp5>();
    world.register_component::<TestComp6>();
    world.register_component::<TestComp7>();
    world.register_component::<TestComp8>();
    world.register_component::<TestComp9>();
    world.register_component::<TestComp10>();

    for i in 0..100 {
        let types = &archetypes[i % 14];
        let entity = world.spawn_empty().id();
        
        for &ty in types {
            match ty {
                0 => insert_comp(world, &mut buffer, entity, TestComp1(i)),
                1 => insert_comp(world, &mut buffer, entity, TestComp2(i)),
                2 => insert_comp(world, &mut buffer, entity, TestComp3(i)),
                3 => insert_comp(world, &mut buffer, entity, TestComp4(i)),
                4 => insert_comp(world, &mut buffer, entity, TestComp5(i)),
                5 => insert_comp(world, &mut buffer, entity, TestComp6(i)),
                6 => insert_comp(world, &mut buffer, entity, TestComp7(i)),
                7 => insert_comp(world, &mut buffer, entity, TestComp8(i)),
                8 => insert_comp(world, &mut buffer, entity, TestComp9(i)),
                9 => insert_comp(world, &mut buffer, entity, TestComp10(i)),
                _ => unreachable!(),
            }
        }
    }
    buffer.apply(world);
}
// rustfmt::skip
fn build_with_commands(world: &mut World) {
    let archetypes = fixed_archetypes!(10);
    let mut cmd = world.commands();
    for i in 0..100 {
        let types = archetypes[i % 14].clone();
        let mut entity = cmd.spawn_empty();

        for ty in types {
            match ty {
                0 => entity.insert(TestComp1(i)),
                1 => entity.insert(TestComp2(i)),
                2 => entity.insert(TestComp3(i)),
                3 => entity.insert(TestComp4(i)),
                4 => entity.insert(TestComp5(i)),
                5 => entity.insert(TestComp6(i)),
                6 => entity.insert(TestComp7(i)),
                7 => entity.insert(TestComp8(i)),
                8 => entity.insert(TestComp9(i)),
                9 => entity.insert(TestComp10(i)),
                _ => unreachable!(),
            };
        }
    }

    world.flush();
}
fn old_load_world_manifest(
    world: &mut World,
    manifest: &AuroraWorldManifest,
    registry: &SnapshotRegistry,
) -> Result<(), String> {
    let snapshot: WorldArchSnapshot = (&manifest.world).into();
    load_world_arch_snapshot(world, &snapshot, registry); //this is old one
    Ok(())
}

fn main() {
    let mut world = World::new();
    let mut registry = SnapshotRegistry::default();
    register(&mut registry);
    build_with_deferred(&mut world);
    println!(
        "✅ Original archetypes (deferred): {}",
        world.archetypes().len()
    );

    let snapshot = save_world_manifest(&world, &registry).unwrap();
    snapshot.to_file("snapshot.toml", None).unwrap();

    let mut new_world = World::new();
    let mut registry2 = SnapshotRegistry::default();
    register(&mut registry2);
    let loaded = AuroraWorldManifest::from_file("snapshot.toml", None).unwrap();
    load_world_manifest(&mut new_world, &loaded, &registry2).unwrap();
    println!(
        "✅ Reloaded archetypes (snapshot): {}",
        new_world.archetypes().len()
    );

    let mut cmd_world = World::new();
    register(&mut registry);
    build_with_commands(&mut cmd_world);
    println!(
        "⚠️  Archetypes with commands.insert(): {}",
        cmd_world.archetypes().len()
    );
    let mut new_world = World::new();
    let mut registry2 = SnapshotRegistry::default();
    register(&mut registry2);
    let loaded = AuroraWorldManifest::from_file("snapshot.toml", None).unwrap();
    old_load_world_manifest(&mut new_world, &loaded, &registry2).unwrap();
    println!(
        "⚠️  Old way reloaded archetypes (snapshot): {}",
        new_world.archetypes().len()
    );
    let _ = fs::remove_file("snapshot.toml");
}
