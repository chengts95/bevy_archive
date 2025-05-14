//! Test aurora-format snapshot roundtrip + archetype integrity
//! Requires `bevy_archive` with Aurora snapshot format enabled

use bevy_archive::{
    archetype_archive::{WorldArchSnapshot, load_world_arch_snapshot},
    prelude::*,
};
use bevy_ecs::prelude::*;
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
        let mut list = vec![
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
            vec![0, 3, 2, 1],
            vec![0, 1, 2, 3],
        ];
        list.push((0..$max).collect());
        list
    }};
}

fn build_with_deferred(world: &mut World) {
    let bump = Bump::new();
    let archetypes = fixed_archetypes!(10);
    for i in 0..100 {
        let types = &archetypes[i % 10];
        let entity = world.spawn_empty().id();
        let mut builder = DeferredEntityBuilder::new(world, &bump, entity);
        for &ty in types {
            match ty {
                0 => builder.insert(TestComp1(i)),
                1 => builder.insert(TestComp2(i)),
                2 => builder.insert(TestComp3(i)),
                3 => builder.insert(TestComp4(i)),
                4 => builder.insert(TestComp5(i)),
                5 => builder.insert(TestComp6(i)),
                6 => builder.insert(TestComp7(i)),
                7 => builder.insert(TestComp8(i)),
                8 => builder.insert(TestComp9(i)),
                9 => builder.insert(TestComp10(i)),
                _ => unreachable!(),
            }
        }
        builder.commit();
    }
}

fn build_with_commands(world: &mut World) {
    let archetypes = fixed_archetypes!(10);
    for i in 0..100 {
        let types = archetypes[i % 10].clone();
        let mut entity = world.spawn_empty();
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
        "⚠️ Old way reloaded archetypes (snapshot): {}",
        new_world.archetypes().len()
    );
    let _ = fs::remove_file("snapshot.toml");
}
