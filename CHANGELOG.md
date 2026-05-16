# Changelog

## [0.4.0-dev] - Unreleased

### Bevy 0.19 Upgrade

Bevy 0.19 introduces breaking API changes and internal architectural shifts. This release adapts `bevy_archive` to the new version.

#### Bevy API Renames (0.17 → 0.19)

| Old (0.17) | New (0.19) |
|---|---|
| `EntityRow` | `EntityIndex` |
| `Entity::from_row(row)` | `Entity::from_index(index)` |
| `Entities::resolve_from_id(EntityRow)` | `Entities::resolve_from_index(EntityIndex)` |
| `Entity::index() → u32` | `Entity::index() → EntityIndex`; use `.index_u32()` for raw `u32` |
| `World::resource_id::<T>()` | `World::component_id::<T>()` |
| `World::register_resource::<T>()` | `World::register_component::<T>()` |
| `Entities::reserve_entities(n)` | Removed; use `EntityAllocator::alloc_many(n)` via `world.entity_allocator_mut()` |

#### Bevy 0.19 Internal Quirks & Workarounds

**1. Resources are stored as entities.**

In 0.19, every `Resource` is backed by a real `Entity` with an `IsResource` marker component. `World::new()` calls `bootstrap()` which spawns a `DefaultQueryFilters` resource entity (ID 0). This entity and its archetype are visible to `world.archetypes().iter()` and `iter_entities()`.

*Workaround:* All snapshot save paths now filter archetypes with `!arch.contains(IS_RESOURCE)`. This prevents engine-internal resource entities from polluting user-facing snapshots.

```rust
// Applied in save_world_arch_snapshot, MsgPackArchive::from_world,
// WorldWithAurora::from_world, WorldArrowSnapshot::from_world_reg
archetypes.iter().filter(|x| !x.is_empty() && !x.contains(IS_RESOURCE))
```

**2. Entity allocation is decoupled from metadata.**

`EntityAllocator::alloc_many(n)` only pushes the allocation counter; it does NOT extend the `Entities` metadata vec or spawn entities. Without explicit `spawn_empty_at`, `EntityWorldMut::get_entity_mut()` returns `Err` silently.

*Workaround:* Added `reserve_entity_slots()` in `bevy_registry` that combines `alloc_many` + `spawn_empty_at` to make a range of entity IDs alive before loading snapshots.

```rust
/// Reserve a contiguous range of entity slots and ensure they are alive.
pub fn reserve_entity_slots(world: &mut World, count: u32) {
    world.entity_allocator_mut().alloc_many(count);
    for i in 0..count {
        let _ = world.spawn_empty_at(Entity::from_index(
            EntityIndex::from_raw_u32(i).unwrap()
        ));
    }
}
```

**3. Entity ID deserialization is brittle across versions.**

Each Bevy version changes the public API surface around `Entity`/`EntityIndex`. To insulate user code and examples, we centralize conversions in `serde_utils`:

```rust
// Two canonical conversion functions — use these instead of raw Bevy API
pub fn entity_to_index(entity: &Entity) -> u32 { entity.index_u32() }
pub fn entity_from_index(index: u32) -> Entity { Entity::from_raw_u32(index).unwrap_or(PLACEHOLDER) }
```

#### Added
- `entity_to_index()` / `entity_from_index()` in `serde_utils` — canonical Entity↔u32 conversions.
- `reserve_entity_slots()` in `bevy_registry` — cross-version entity slot reservation.
- `entity_serde_compact` / `entity_serde_full` serde modules (prepared, not yet exported).

#### Changed
- All `WorldExt::iter_entities()` call sites now use `entity_to_index()` instead of raw `.index()`.
- All `example/*.rs` files updated to use the new conversion functions.
- `HarvardCommandBuffer::apply()` now calls `spawn_empty_at` before inserting into entities not yet alive.
- `serde_arrow` bumped from `0.13.6` to `0.14.1`; `arrow` and `parquet` locked at `58.3.0` with `arrow-58` feature.

### [0.3.0] - 2025-12-20
### Architectural Improvements (Aurora Hybrid Pipeline)
- **Direct-to-World Binary I/O:** `binary_archive` now exposes `save_arrow_archetype_from_world` and `load_arrow_archetype_to_world`, allowing `aurora_archive` to perform high-performance binary operations directly against the Bevy World without intermediate conversions.
- **Hybrid Manifest Generation:** `WorldWithAurora::from_guided` now acts as a coordinator. It iterates the ECS World once and dispatches archetype saving to either the Legacy path (Text/JSON via `ArchetypeSnapshot`) or the Binary path (Arrow/Parquet via `ComponentTable`), depending on the `ExportGuidance`. This avoids forcing binary data through the inefficient `serde_json::Value` intermediate representation.
- **Unified Loader with Type Safety:** `load_world_manifest` now uses an internal `LoadedArchetype` enum to handle the heterogeneous list of loaded blobs (Legacy vs Arrow) and dispatches them to their respective optimized loaders.
- **Strict Pipeline Separation:** `ExportFormat::Parquet` is now strictly enforced to use the binary pipeline. Attempting to mix Parquet with the legacy JSON-based `ArchetypeSnapshot` path is prevented to ensure type fidelity and performance.

### Added
- `save_single_archetype_snapshot` exposed in `archetype_archive` to support granular text-based saving.
- `LoadedArchetype` enum in `aurora_archive` to support mixed-format loading.
- `BlobLoader` trait and `ZipBlobLoader` (feature `arrow_rs`) for flexible archive loading.
- `examples/hybrid_zip.rs` demonstrating how to save/load a hybrid (Parquet + CSV) archive in a ZIP file.

### Core Kernel Update: HarvardCommandBuffer
- **Replaced `DeferredEntityBuilder` with `HarvardCommandBuffer`:** A new high-performance transactional kernel for ECS mutations.
- **Why "Harvard"?** Named after the **Harvard Architecture** (physically separate instruction and data memory). This buffer maintains two distinct memory arenas:
  - **Instruction Bus (`meta_bump`):** Stores OpCodes (`ModifyEntity`, `Despawn`) and lightweight arguments.
  - **Data Bus (`data_bump`):** Stores raw component payloads (POD/Drop).
- **Zero-Overhead Write Combining:** Consecutive inserts to the same entity are automatically merged in the instruction stream without extra allocations, mimicking a CPU's write-combining buffer.
- **Dual-Bump Allocation:** Eliminates heap fragmentation by using `bumpalo` for all temporary storage, dropping the entire arena instantly after `apply()`.
- **Safety & correctness:** Implements robust `Drop` safety to clean up unapplied resources and handles duplicate component insertions by keeping the latest value (LIFO override).

## [0.2.1] - 2025-11-20
- Disable Flecs due to API limitations of `Flecs-Rust`.
- Support Bevy 0.17.x. 
- Allow reading empty parquet table without failing.

## [0.2.0] - 2025-11-18
- Disable Flecs due to API limitations of `Flecs-Rust`.
- Support Bevy 0.17.x. 
- Fix memory leak if the struct is not Plain-Old-Data.
- Initial support for Arrow, parquet and binary format.
- Remove placeholder snapshot mode and remove snapshot mode from factories.
  
## [0.1.4] - 2025-05-14
- Initial support for singleton/resource.
- Initial support for merging snapshot.
- Initial support for binary (msgpack) data format.
- Initial support for export options.
  
## [0.1.3] - 2025-05-14

### Fix
- Fix `DeferredEntityBuilder::insert_if_new_by_id` to properly insert the components.

## [0.1.2] - 2025-05-14

### Added
- `DeferredEntityBuilder` and `load_world_arch_snapshot_defragment` now support `insert_if_new`.
- `load_world_arch_snapshot_defragment` now can ignore unknown types in the file.
### Changed
-   Refactored `bevy_registry` to support more operations.


## [0.1.1] - 2025-05-14

### Added
- Integration with bump allocator for temporary component memory.
- `DeferredEntityBuilder` for runtime batch insertion of components.
- Support for insert_by_id(ComponentId, OwningPtr).
- `load_world_arch_snapshot_defragment` to avoid archetype fragments in bevy ECS, **this only happens with bevy**.
- Experimental flecs support: cross-ECS serialization example added, enabling data transfer between bevy and flecs-based runtimes.

- Extended cross-ECS example between bevy and flecs.
### Changed
-   ArchetypeSnapshot entity index storage: migrated from `BTreeMap<Entity, u32> → Vec<u32>` for improved memory locality and faster reconstruction.