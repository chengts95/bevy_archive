# bevy_archive

**bevy_archive** is an experimental project for exploring ECS world storage and loading strategies based on archetype decomposition and structured serialization.

It uses a combination of serialization formats to balance:
- **flexibility** (via JSON as a common object model),
- **machine-read/write efficiency** (via CSV), and
- **human readability/editability** (via TOML manifests).

Currently implemented on top of [Bevy ECS](https://bevyengine.org/), but designed with the potential for cross-ECS engine compatibility in mind.

---

## ✨ Goals

- Represent ECS state in a fully structured, archetype-separated format
- Support full world snapshot roundtrip (save → serialize → reload)
- Enable patching, diffing, and composable storage segments
- Treat `Entity` references as serializable via wrappers (e.g., `ChildOfWrapper`)
- Organize data in layers: entity IDs → archetypes → CSV-encoded columns

---

## 📦 Format Overview ("Aurora")

Aurora is the internal name for the snapshot format used in this project. It is not a rigid standard, but a set of conventions designed to:

- Encode **archetype layout** in manifest
- Encode **component data** in columnar CSV or structured JSON
- Embed or reference data via paths/URIs (`embed://`, `file://`, etc.)
- Save and load data from multiple data files (not tested yet)

### Aurora TOML Manifest Example
```toml
name = "MyWorld"

[world]
version = "0.1"

[[world.archetypes]]
name = "arch_0"
components = ["Position", "Velocity"]
storage = ["Table", "Table"]
source = "embed://arch_0"

[world.embed.arch_0]
format = "csv"
data = '''
id,Position.x,Position.y,Velocity.dx,Velocity.dy
0,1.0,2.0,0.1,-0.2
'''
```

---

## 📂 Examples
### 🔁 Minimal Snapshot Example

```rust
// Full roundtrip: save → serialize → load

use bevy_archive::prelude::*;
use bevy_ecs::prelude::*;
use serde::{Serialize, Deserialize};

#[derive(Component, Serialize, Deserialize)]
struct Health(f32);

fn main() {
    // Setup world and registry
    let mut world = World::new();
    let mut registry = SnapshotRegistry::default();
    registry.register::<Health>();

    // Spawn some test data
    world.spawn(Health(100.0));
    world.spawn(Health(75.0));

    // Save snapshot
    let manifest = save_world_manifest(&world, &registry).unwrap();
    manifest.to_file("my_world.toml", None).unwrap();

    // Load into new world
    let loaded = AuroraWorldManifest::from_file("my_world.toml", None).unwrap();
    let mut new_world = World::new();
    registry.register::<Health>();
    load_world_manifest(&mut new_world, &loaded, &registry).unwrap();
}
```
- `aurora_manifest_example.rs`: save/load a Bevy world with children using a structured manifest
- `entity_snapshot.rs`: legacy entity-based archive with per-entity serialization

---

Custom type wrappers can be added via:
```rust
//Vector2Wrapper need to implement From<&Vector> and Into<Vector2> for Vector2Wrapper
registry.register_with::<Vector2, Vector2Wrapper>(); 

```


---

## 🔧 Future Directions
- global singleton support
- Patch and delta snapshot calculation
- Archetype merge and dependency resolution
- Cross-engine (e.g., Flecs, EnTT) structure mapping
- Streamed and binary snapshotting

---

## 🪐 Philosophy

This project does not aim to lock into a fixed standard. Instead, it provides a practical, iteratively refined data flow that is:

- observable,
- debuggable,
- serializable,
- and extensible.

> ECS is not about objects — it's about structure, state, and change. With Aurora, composable components can form a flexible patching system that updates only the key state information, allowing targeted restoration or transformation of an ECS world.

> 
> Note: This project is **not affiliated with the official Bevy organization**. It is an experimental toolset built on top of the Bevy ECS framework.
> 
---

## License

MPL 2.0 License

