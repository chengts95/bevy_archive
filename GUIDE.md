# AI Agent Guide to `bevy_archive`

**Current Version:** 0.4.0-rc.1
**Core Concept:** A high-performance, format-agnostic ECS snapshot and serialization system for Bevy.
**Target Bevy:** 0.19.0-rc.1

## 1. The `Archive` Trait (High-Level API)

This is the primary interface for saving and loading worlds. All archive formats implement this trait.

```rust
pub trait Archive: Sized {
    /// Create an archive from a World.
    fn create(world: &World, registry: &SnapshotRegistry) -> Result<Self, ...>;

    /// Save the archive to disk.
    fn save_to(&self, path: impl AsRef<Path>) -> Result<(), ...>;

    /// Load the archive from disk.
    fn load_from(path: impl AsRef<Path>) -> Result<Self, ...>;

    /// Apply the archive to a World (simple load).
    fn apply(&self, world: &mut World, registry: &SnapshotRegistry) -> Result<(), ...>;

    /// Apply with Entity ID Remapping (for merging into existing worlds).
    fn apply_with_remap(
        &self,
        world: &mut World,
        registry: &SnapshotRegistry,
        id_registry: &IDRemapRegistry,
        mapper: &dyn EntityRemapper,
    ) -> Result<(), ...>;
    
    /// Get list of entity IDs stored in the archive.
    fn get_entities(&self) -> Vec<u32>;
}
```

## 2. Supported Formats

| Format Type | Struct Name | Feature Flag | Description |
| :--- | :--- | :--- | :--- |
| **Binary (Fast)** | `MsgPackArchive` | (default) | Optimized MessagePack blob. Best for networking/savegames. |
| **Manifest (Hybrid)** | `AuroraWorldManifest` | (default) | JSON/TOML manifest pointing to CSV/Arrow/MsgPack blobs. Best for modding/assets. |
| **Binary (Big Data)** | `WorldArrowSnapshot` | `arrow_rs` | Apache Arrow/Parquet backed. Best for analytics or huge worlds. |
| **Legacy (Debug)** | `WorldSnapshot` | (default) | Simple JSON structure. Slow, human-readable. |

## 3. Usage Patterns

### A. Registering Components
Before any operation, register components in `SnapshotRegistry`.

```rust
let mut registry = SnapshotRegistry::default();
registry.register::<Player>();
registry.register::<Transform>();
registry.resource_register::<GameState>();
```

### B. Saving a World
```rust
use bevy_archive::binary_archive::msgpack_archive::MsgPackArchive;

// 1. Create
let archive = MsgPackArchive::create(&world, &registry)?;
// 2. Save
archive.save_to("savegame.msgpack")?;
```

### C. Loading a World (Overwrite/Simple)
```rust
let archive = MsgPackArchive::load_from("savegame.msgpack")?;
// This assumes world is empty or you don't care about ID conflicts (new IDs will be generated)
archive.apply(&mut world, &registry)?;
```

### D. Loading with ID Remapping (Merge)
When loading a snapshot into an existing world, Entity IDs will conflict. Use `apply_with_remap` to shift IDs safely.

1. **Register Hooks:** Tell `bevy_archive` how to update your components' `Entity` fields.
```rust
let mut id_registry = IDRemapRegistry::default();
id_registry.register_remap_hook::<Parent>(|comp, mapper| {
    use bevy_archive::entity_to_index;
    comp.0 = mapper.map(entity_to_index(&comp.0));
});
```

2. **Prepare Mapper:** Create a mapping from Old ID -> New ID.
```rust
let archive = MsgPackArchive::load_from("prefab.msgpack")?;
let old_ids = archive.get_entities();
let mut mapper = HashMap::new();

for old_id in old_ids {
    let new_entity = world.spawn_empty().id();
    mapper.insert(old_id, new_entity);
}
```

3. **Apply:**
```rust
archive.apply_with_remap(&mut world, &registry, &id_registry, &mapper)?;
```

## 4. Internal Architecture: `HarvardCommandBuffer`

Under the hood, `bevy_archive` uses `HarvardCommandBuffer` to perform mutations. 
It is a **Harvard Architecture** (Instruction Stream + Data Stream) command buffer optimized for "Write Combining".

- **Instruction Bus:** Stores `OpHead` (ModifyEntity, BatchInsert).
- **Data Bus:** A linear `bumpalo::Bump` arena storing generic component payloads.
- **Performance:** Achieves 0-allocation deserialization by creating components directly into the Data Bus.

If you need to extend `bevy_archive` or write a custom loader:
```rust
let mut buffer = HarvardCommandBuffer::new();
// ... loop ...
    buffer.insert_box(entity, comp_id, arena_box); // or generic insert<T>
// ... end loop ...
buffer.apply(world);
buffer.reset(); // Reuse memory
```

## 5. Key Examples
- `examples/standard_api_example.rs`: Basic save/load workflow.
- `examples/id_remap_example.rs`: Complex merging with Entity references.
- `examples/hybrid_zip.rs`: Advanced hybrid archive (Parquet + CSV in ZIP).

## 6. Entity Serialization Helpers (`serde_utils`)

Bevy frequently renames `Entity`-related methods between versions. To insulate user code, `bevy_archive` provides canonical conversion functions:

```rust
use bevy_archive::prelude::*;

// Entity → u32 (index only, drops generation)
let idx: u32 = entity_to_index(&entity);

// u32 → Entity (generation = 0, for deserialization)
let entity: Entity = entity_from_index(idx);

// Also available via serde attribute:
// #[serde(with = "entity_serializer")]  ← existing, now backed by entity_to_index
```

**Always use these instead of raw `.index()` or `.index_u32()` in Wrapper `From` impls and remap hooks.** When the next Bevy version renames them again, only `serde_utils.rs` needs updating.

## 7. Bevy 0.19 Compatibility Notes

This section documents Bevy 0.19 behaviors that affect `bevy_archive` and the workarounds in place.

### 7.1 Resources Are Entities

In Bevy 0.19, every `Resource` is stored as a real `Entity` with an `IsResource` marker component. `World::new()` calls `bootstrap()`, which spawns a `DefaultQueryFilters` resource entity (ID 0). This entity and its archetype are visible to `world.archetypes().iter()`.

**Impact:** If unfiltered, resource entities pollute world snapshots with engine-internal data.

**Workaround:** All four save paths filter archetypes with `!arch.contains(IS_RESOURCE)`:
- `archetype_archive::save_world_arch_snapshot`
- `MsgPackArchive::from_world`
- `WorldWithAurora::from_world`
- `WorldArrowSnapshot::from_world_reg`

### 7.2 Entity Allocation ≠ Entity Metadata

`EntityAllocator::alloc_many(n)` only advances the internal counter. It does NOT extend the `Entities` metadata vec or spawn entities. `EntityWorldMut::get_entity_mut()` silently returns `Err` for un-spawned IDs.

**Impact:** Loading snapshots without explicit spawning causes all entities to be silently skipped.

**Workaround:** `reserve_entity_slots()` in `bevy_registry.rs` combines `alloc_many` + `spawn_empty_at`:
```rust
pub fn reserve_entity_slots(world: &mut World, count: u32) {
    world.entity_allocator_mut().alloc_many(count);
    for i in 0..count {
        let _ = world.spawn_empty_at(Entity::from_index(
            EntityIndex::from_raw_u32(i).unwrap()
        ));
    }
}
```

### 7.3 EntityIndex Is a Newtype

`Entity::index()` now returns `EntityIndex` (not `u32`). Use `.index_u32()` for raw values, or use `entity_to_index()` from `serde_utils` for cross-version stability.
