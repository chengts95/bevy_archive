# Usage Scenarios & Best Practices

This document describes the recommended patterns for using `bevy_archive` in
different situations. It reflects the design philosophy of the crate: the user
is in full control of entity identity and the library stays out of business logic.

---

## 1. Full World Snapshot (No Remap)

**When:** You save and load the same world, or replace all components of an
existing world. Entity IDs are preserved exactly.

```rust
use bevy_archive::prelude::*;
use bevy_ecs::prelude::*;

let mut world = World::new();
let mut reg = SnapshotRegistry::default();
reg.register::<MyComponent>();

// ... populate world ...

// Save
let archive = AuroraWorldManifest::create(&world, &reg).unwrap();

// Load into a fresh or reset world
let mut new_world = World::new();
archive.apply(&mut new_world, &reg).unwrap();
```

**Key properties:**
- Entity IDs in the snapshot are reused verbatim (e.g., entity 42 stays 42).
- If the target world already has entities at those IDs, `EmplaceIfNotExists`
  mode can be used to avoid overwriting pre-existing components.
- No mapper is involved.

**Recommended `SnapshotMode`:**
- `Full` — for a completely new world.
- `EmplaceIfNotExists` — when loading into a partially populated world.

---

## 2. Independent Component Groups (Multiple Registries)

**When:** You have logically separate data layers (e.g., world geometry vs.
simulation state vs. output results) that are saved and loaded independently.

```rust
let mut geo_reg = SnapshotRegistry::default();
let mut sim_reg = SnapshotRegistry::default();
let mut out_reg = SnapshotRegistry::default();

geo_reg.register::<Position>();
geo_reg.register::<Velocity>();
sim_reg.register::<PowerFlowState>();
sim_reg.register::<Jacobian>();
out_reg.register::<BusVoltage>();
out_reg.register::<BranchFlow>();
```

**Usage:**
- Each registry tracks a **disjoint set of components**.
- Save/load operates independently per registry:
  ```rust
  save_world_manifest(&world, &sim_reg)?;      // only simulation state
  save_world_manifest(&world, &out_reg)?;      // only outputs
  ```
- Entities without any registered component are silently skipped in that pass.

**Why this works:** `bevy_archive` saves entity IDs across all registries. As
long as the same entities exist in the target world and have the same component
signature, loading each registry independently will correctly place components
onto the right entities.

---

## 3. Incremental Remap (Segmented Address Space)

**When:** You are merging snapshots from multiple source worlds, or loading a
snapshot alongside existing entities, and you don't need one-to-one entity
correspondence. You control the target ID space.

```rust
use std::collections::HashMap;

let archive = MsgPackArchive::load_from("prefab.msgpack").unwrap();
let old_ids = archive.get_entities();

let mut target = World::new();
let mut id_registry = IDRemapRegistry::default();
let mut mapper: HashMap<u32, Entity> = HashMap::new();

// Allocate a contiguous block for the incoming entities,
// offset by 10_000 to avoid any collision.
for &old_id in &old_ids {
    let new = target.spawn_empty().id();
    mapper.insert(old_id, new);
}

archive.apply_with_remap(&mut target, &reg, &id_registry, &mapper).unwrap();
```

**Key properties:**
- The user allocates target entities however they want. Bevy's entity
  generations are never inspected.
- Multiple archives can be loaded side-by-side by assigning non-overlapping ID
  ranges.
- The mapper is a trait (`EntityRemapper`) — any key-value store works.
  `HashMap<u32, Entity>` is the simplest; a `BTreeMap` or a bespoke allocator
  are all valid.

**Why this beats UUID-based schemes:** UUIDs require a distributed consensus
mechanism. In a single-process ECS, you already have a reliable 32-bit ID
space. Assigning non-overlapping segments is simpler and faster than hashing
UUIDs on every entity lookup.

---

## 4. Global Unique ID / UUID Mapping

**When:** You need cross-instance or cross-session entity identity
(save/load across runs, networked state, distributed systems). You maintain
your own global ID scheme and map it onto Bevy entities.

```rust
use std::collections::HashMap;

struct GlobalId(u64); // your custom UUID-like identifier

// Build mapper: GlobalId → (target Bevy Entity)
let mut mapper: HashMap<u32, Entity> = HashMap::new();
for item in &load_manifest {
    let bevy_entity = target.spawn_empty().id();
    // Store the association however you like (e.g., GlobalId → Entity)
    mapper.insert(item.old_entity_index, bevy_entity);
}

archive.apply_with_remap(&mut target, &reg, &id_registry, &mapper).unwrap();
```

**Why `bevy_archive` doesn't handle UUIDs natively:**
- UUID allocation is a business-logic concern. The library only needs an
  `EntityRemapper` trait impl.
- `HashMap<u32, Entity>` is the general-purpose zero-cost tool; if you need
  UUIDs, wrap a `HashMap<Uuid, Entity>` that implements `EntityRemapper`.

---

## 5. Entity Generations (Version) Not Stored

**Design decision:** `bevy_archive` stores only the **entity index** (u32),
not the generation counter.

**Rationale:**

1. **Index is sufficient for uniqueness within a snapshot.** Two entities
   simultaneously alive in the same world cannot share the same index. The
   generation counter exists to detect use-after-despawn, not to identify
   entities.

2. **A generation mismatch means the snapshot is already broken.** If an
   archive references an entity whose generation has wrapped, the entity
   was despawned before the archive was created. This is a data integrity
   issue that belongs in the application's validation layer, not in the
   serialization format.

3. **Avoiding opinionated behavior.** Some users may want to validate
   generations; others may not care. The library does not impose a
   validation strategy. If you need generation information:

   ```rust
   // Scan the world before archiving and build a generation map.
   let mut gen_map: HashMap<u32, u32> = HashMap::new();
   for entity in world.iter_entities() {
       gen_map.insert(entity.index(), entity.generation());
   }
   // Store gen_map alongside the archive.
   // On load, compare generations and decide your own retry/error policy.
   ```

**Summary:** `bevy_archive` treats entity generations as an application-level
concern. The library gives you the raw index; you decide how (and whether) to
validate it.

---

## Quick Reference: Which Pattern to Use

| Scenario | Registry setup | Load method | Mapper |
|---|---|---|---|
| Save/load same world | One registry, all components | `apply()` | None |
| Independent data layers | Multiple registries, disjoint components | `save_world_manifest(w, &layer_reg)` | None |
| Merge into existing world | One registry | `apply_with_remap()` | Offset allocation |
| Cross-session / network | One registry + app-level UUID table | `apply_with_remap()` | `HashMap<u32, Entity>` or UUID wrapper |
| Generation validation | App code scans pre-save | Either | App-provided validation |
