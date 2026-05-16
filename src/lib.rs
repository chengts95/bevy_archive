//! # bevy_archive — Structured ECS World Serialization for Bevy
//!
//! **bevy_archive** is a format-agnostic snapshot and serialization system built on
//! [Bevy ECS](https://bevyengine.org/). It decomposes a [`World`](bevy_ecs::prelude::World)
//! by **archetype**, stores component data in **columnar** form, and writes the result to
//! one of several pluggable backends (JSON, TOML, CSV, MessagePack, Parquet).
//!
//! ## Design Principles
//!
//! 1. **Archetype-centric storage.** Entities are grouped by their component signature
//!    (archetype). Each archetype is a table: rows = entities, columns = component values.
//!    This mirrors how Bevy lays out data internally and avoids the overhead of
//!    per-entity iteration.
//!
//! 2. **Format agnostic via the [`Archive`] trait.** All backends implement the same
//!    five-method interface. Adding a new format requires only implementing
//!    [`Archive`](traits::Archive) — no changes to the core save/load pipeline.
//!
//! 3. **User-controlled entity ID space during merge.** When loading a snapshot into an
//!    existing world, [`apply_with_remap`](traits::Archive::apply_with_remap) accepts a
//!    user-provided [`EntityRemapper`] so that the user, not the engine, decides how old
//!    IDs map to new ones. Bevy's entity generations are transparent to this path.
//!
//! 4. **Zero-allocation writes via [`HarvardCommandBuffer`].** Deserialized component
//!    payloads are placed into a `bumpalo::Bump` arena and batched into a write-combining
//!    command stream before being flushed to the world in a single pass.
//!
//! 5. **Isolation from Bevy API churn.** Volatile Bevy internals (`EntityIndex`, `Entities`,
//!    `EntityAllocator`) are touched only in a few strategic modules. Users interact with
//!    stable wrapper functions like [`entity_to_index`] and [`entity_from_index`].
//!
//! ## Supported Formats
//!
//! | Format | Struct | Feature | Best For |
//! |---|---|---|---|
//! | MessagePack binary | [`MsgPackArchive`](binary_archive::msgpack_archive::MsgPackArchive) | *(default)* | Savegames, network transfer |
//! | Aurora manifest (JSON/TOML + CSV) | [`AuroraWorldManifest`] | *(default)* | Modding, hand-editable assets |
//! | JSON entity dump | [`WorldSnapshot`] | *(default)* | Debugging, introspection |
//! | Apache Arrow / Parquet | [`WorldArrowSnapshot`](binary_archive::WorldArrowSnapshot) | `arrow_rs` | Analytics, large worlds |
//!
//! ## Quick Start
//!
//! ```rust
//! use bevy_archive::prelude::*;
//! use bevy_ecs::prelude::*;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Component, Serialize, Deserialize)]
//! struct Health(f32);
//!
//! // 1. Register components
//! let mut registry = SnapshotRegistry::default();
//! registry.register::<Health>();
//!
//! // 2. Populate the world
//! let mut world = World::new();
//! world.spawn(Health(100.0));
//! world.spawn(Health(60.0));
//!
//! // 3. Save
//! let manifest = save_world_manifest(&world, &registry).unwrap();
//!
//! // 4. Load into a fresh world
//! let mut new_world = World::new();
//! load_world_manifest(&mut new_world, &manifest, &registry).unwrap();
//! ```
//!
//! ## Loading with ID Remapping (Entity Merging)
//!
//! Use [`apply_with_remap`](traits::Archive::apply_with_remap) when loading a snapshot
//! into a world that already contains entities:
//!
//! ```rust
//! # use bevy_archive::prelude::*;
//! # use bevy_ecs::prelude::*;
//! # use std::collections::HashMap;
//! # let (world, registry) = (World::new(), SnapshotRegistry::default());
//! # let archive = WorldSnapshot::create(&world, &registry).unwrap();
//! let mut target_world = World::new();
//! let mut id_registry = IDRemapRegistry::default();
//!
//! // Pre-allocate slots for the incoming entities
//! let old_ids = archive.get_entities();
//! let mut mapper: HashMap<u32, Entity> = HashMap::new();
//! for &old in &old_ids {
//!     mapper.insert(old, target_world.spawn_empty().id());
//! }
//!
//! // Apply with remapping
//! archive.apply_with_remap(&mut target_world, &registry, &id_registry, &mapper).unwrap();
//! ```
//!
//! The mapper gives you **total control** over the new ID space. Bevy's internal
//! entity generations are never inspected or assumed.
//!
//! ## Entity Serialization Helpers
//!
//! Bevy changes its entity API surface between versions. To insulate your code,
//! use the two canonical conversion functions:
//!
//! ```rust
//! use bevy_archive::prelude::*;
//! use bevy_ecs::prelude::*;
//!
//! # let entity = Entity::from_raw_u32(42).unwrap();
//! // Entity → u32 (index only, drops generation)
//! let idx: u32 = entity_to_index(&entity);
//!
//! // u32 → Entity (generation=0, suitable for deserialization)
//! let restored = entity_from_index(idx);
//!
//! // Also usable via serde attribute:
//! // #[serde(with = "entity_serializer")]
//! ```
//!
//! Prefer `entity_to_index` / `entity_from_index` over raw `.index_u32()` or
//! `.from_raw_u32()` in wrapper `From` impls and remap hooks — when the next
//! Bevy release renames these again, only `serde_utils.rs` needs updating.
//!
//! ## Registering Custom Types (Wrappers)
//!
//! When a Bevy component cannot be serialized directly (e.g. it contains an `Entity`
//! handle), register a wrapper type that implements `From<&Component>`:
//!
//! ```rust
//! # use bevy_archive::prelude::*;
//! # use bevy_ecs::prelude::*;
//! # use serde::{Serialize, Deserialize};
//! # #[derive(Component, Clone, Debug)]
//! # struct ChildOf(pub Entity);
//! # #[derive(Serialize, Deserialize, Clone, Debug)]
//! # struct ChildOfWrapper(pub u32);
//! # impl From<&ChildOf> for ChildOfWrapper {
//! #     fn from(c: &ChildOf) -> Self { ChildOfWrapper(entity_to_index(&c.0)) }
//! # }
//! # impl From<ChildOfWrapper> for ChildOf {
//! #     fn from(v: ChildOfWrapper) -> Self { ChildOf(entity_from_index(v.0)) }
//! # }
//! let mut registry = SnapshotRegistry::default();
//! registry.register_with::<ChildOf, ChildOfWrapper>();
//! ```
//!
//! ## Bevy 0.19 Pitfalls
//!
//! Bevy 0.19 introduced several changes that affect snapshot code. `bevy_archive`
//! handles them internally, but awareness helps when extending the library.
//!
//! ### Resources are entities
//!
//! In 0.19 every [`Resource`](bevy_ecs::prelude::Resource) is stored as a real
//! `Entity` tagged with an `IsResource` marker. `World::new()` spawns one in
//! `bootstrap()`. These entities and their archetypes leak into public iterators.
//!
//! `bevy_archive` filters them out at the archetype level with
//! `!arch.contains(bevy_ecs::resource::IS_RESOURCE)` in all four save paths.
//!
//! ### Entity allocation ≠ spawning
//!
//! `EntityAllocator::alloc_many(n)` advances a counter but does **not** extend
//! the entity metadata or spawn entities. `EntityWorldMut` silently returns `Err`
//! for unspawned IDs.
//!
//! `bevy_archive` provides [`reserve_entity_slots`] which wraps `alloc_many` +
//! `spawn_empty_at` to make a contiguous ID range alive before loading.
//!
//! ### `EntityIndex` is a newtype (0.17→0.19)
//!
//! `Entity::index()` now returns `EntityIndex` instead of `u32`. Use
//! `.index_u32()` for raw values, or better yet [`entity_to_index`].
//!
//! ## Module Map
//!
//! | Module | Purpose |
//! |---|---|
//! | [`traits`] | The `Archive` trait — the primary API surface |
//! | [`archetype_archive`] | Core save/load engine: `ArchetypeSnapshot`, `WorldArchSnapshot` |
//! | [`aurora_archive`] | Aurora manifest format (JSON/TOML + CSV embedding) |
//! | [`entity_archive`] | Legacy per-entity JSON snapshot |
//! | [`bevy_registry`] | `SnapshotRegistry`, `IDRemapRegistry`, `reserve_entity_slots` |
//! | [`serde_utils`] | `entity_to_index`, `entity_from_index`, serde helpers |
//! | [`bevy_cmdbuffer`] | `HarvardCommandBuffer` — low-level write engine |
//! | [`binary_archive`] | MessagePack and Arrow/Parquet backends |
//!
//! ## Examples
//!
//! See `examples/` in the repository:
//! - `standard_api_example.rs` — basic save/load with all formats
//! - `id_remap_example.rs` — entity ID remapping (merge)
//! - `aurora_manifest_example.rs` — Aurora manifest with `ChildOf` wrapper
//! - `hybrid_zip.rs` — Parquet + CSV hybrid archive (requires `arrow_rs`)

#![allow(unexpected_cfgs)]
pub mod archetype_archive;
pub mod aurora_archive;
pub mod bevy_registry;
pub mod csv_archive;
pub mod entity_archive;

pub mod binary_archive;
pub mod bevy_cmdbuffer;
pub mod serde_utils;
pub mod traits;

#[cfg(feature = "flecs")]
pub mod flecs_archsnaphot;
#[cfg(feature = "flecs")]
pub mod flecs_registry;

#[cfg(feature = "arrow_rs")]
pub mod arrow_snapshot;

#[cfg(feature = "arrow_rs")]
pub use zip;

pub mod prelude {
    pub use crate::aurora_archive::*;
    pub use crate::bevy_registry::*;
    #[cfg(feature = "flecs")]
    pub use crate::flecs_registry;

    pub use crate::entity_archive::*;
    pub use crate::serde_utils::*;
    pub use crate::traits::*;
}
