use bevy_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
#[derive(Debug, Deserialize)]
pub struct SnapshotFile {
    #[serde(rename = "entity")]
    pub entities: Vec<EntitySnapshot>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ComponentSnapshot {
    pub r#type: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub value: serde_json::Value,
}
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EntitySnapshot {
    pub id: u64,

    pub components: Vec<ComponentSnapshot>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorldSnapshot {
    pub entities: Vec<EntitySnapshot>,
}
impl WorldSnapshot {
    pub fn purge_null(&mut self) {
        self.entities.retain(|c| !c.components.is_empty());
    }
}

use serde_json::Value as JsonValue;
use toml::Value as TomlValue;

use crate::bevy_registry::SnapshotRegistry;

/// JSON → TOML
pub fn json_to_toml(json: &JsonValue) -> Result<TomlValue, String> {
    toml::Value::try_from(json).map_err(|e| format!("to_toml failed: {}", e))
}

/// TOML → JSON
pub fn toml_to_json(toml: &TomlValue) -> Result<JsonValue, String> {
    toml.serialize(serde_json::value::Serializer)
        .map_err(|e| format!("to_json failed: {}", e))
}

pub fn save_world_snapshot(world: &World, reg: &SnapshotRegistry) -> WorldSnapshot {
    let mut entities_snapshot = Vec::new();
    for e in world.iter_entities() {
        let mut es = EntitySnapshot::default();
        es.id = e.id().index() as u64;
        for key in reg.type_registry.keys() {
            if let Some(func) = reg.exporters.get(key) {
                if let Some(value) = func(world, e.id()) {
                    es.components.push(ComponentSnapshot {
                        r#type: key.to_string(),
                        value,
                    });
                }
            }
        }
        entities_snapshot.push(es);
    }
    WorldSnapshot {
        entities: entities_snapshot,
    }
}

pub fn load_world_snapshot(world: &mut World, snapshot: &WorldSnapshot, reg: &SnapshotRegistry) {
    let mut max_id = 0;
    for e in &snapshot.entities {
        max_id = max_id.max(e.id);
    }
    world.entities().reserve_entities((max_id + 1) as u32);
    world.flush();
    for e in &snapshot.entities {
        let entity = Entity::from_raw(e.id as u32);
        for c in &e.components {
            reg.importers
                .get(&c.r#type.as_str())
                .and_then(|f| Some(f(&c.value, world, entity).unwrap()))
                .unwrap()
        }
    }
}

pub fn save_snapshot_to_file<P: AsRef<Path>>(
    snapshot: &WorldSnapshot,
    path: P,
) -> Result<(), std::io::Error> {
    let content = serde_json::to_value(snapshot).unwrap();

    fs::write(path, content.to_string())
}
pub fn save_snapshot_to_file_toml<P: AsRef<Path>>(
    snapshot: &WorldSnapshot,
    path: P,
) -> Result<(), std::io::Error> {
    let mut content = json_to_toml(&serde_json::to_value(snapshot).unwrap()).unwrap();
    let t = content.as_table_mut().unwrap();

    fs::write(path, t.to_string())
}
pub fn load_snapshot_from_file<P: AsRef<Path>>(path: P) -> Result<WorldSnapshot, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("I/O error: {}", e))?;
    serde_json::from_str(&content).map_err(|e| format!("Deserialization error: {}", e))
}

pub fn load_snapshot_from_file_toml<P: AsRef<Path>>(path: P) -> Result<WorldSnapshot, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("I/O error: {}", e))?;
    toml::from_str(&content).map_err(|e| format!("Deserialization error: {}", e))
}
#[cfg(test)]
mod tests {
    use crate::bevy_registry::SnapshotRegistry;

    use super::*;

    use serde_json::json;
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponent {
        pub value: i32,
    }
    #[derive(Serialize, Deserialize, Component)]
    struct Admittance(pub f64);
    #[derive(Serialize, Deserialize, Component)]
    struct Resistor(pub f64);
    #[derive(Serialize, Deserialize, Component)]
    struct Port2(pub [i32; 2]);
    #[test]
    fn test_snapshot_registry_world() {
        let mut registry = SnapshotRegistry::default();
        let mut world = World::default();
        registry.register::<Admittance>();
        registry.register::<Resistor>();
        registry.register::<Port2>();
        let a: Vec<_> = world.entities().reserve_entities(10).collect();
        world.flush();
        a.into_iter().enumerate().for_each(|(i, x)| {
            world
                .entity_mut(x)
                .insert((Resistor(1.0), Port2([0, i as i32]), Admittance(1.0)));
        });

        let _w = save_world_snapshot(&world, &registry);
    }

    #[test]
    fn test_parse_pretty_toml() {
        let input = r#"[[entities]]
id = 0
components = [
  { type = "Resistor", value = 1.0 },
  { type = "Admittance", value = 0.0 },
  { type = "Port2", value = [0, 0] }
]
                        "#;
        let mut registry = SnapshotRegistry::default();
        let mut world = World::default();
        registry.register::<Admittance>();
        registry.register::<Resistor>();
        registry.register::<Port2>();

        let parsed: TomlValue = toml::from_str(input).expect("Failed to parse TOML");
        let snapshot: WorldSnapshot = parsed.try_into().unwrap();
        load_world_snapshot(&mut world, &snapshot, &registry);
    }

    #[test]
    fn test_snapshot_registry() {
        let mut registry = SnapshotRegistry::default();
        registry.register::<TestComponent>();

        let component = TestComponent { value: 42 };
        let mut world = World::default();
        let entity = world.spawn(component.clone()).id();

        // Export
        let exported = registry.exporters.get("TestComponent").unwrap()(&world, entity);
        assert!(exported.is_some());
        let exported_value = exported.unwrap();
        assert_eq!(exported_value, json!({"value": 42}));
        assert_eq!(exported_value.get("value").unwrap().as_i64().unwrap(), 42);
        println!("Exported JSON: {}", exported_value);
        println!(
            "Exported JSON as TOML: {}",
            json_to_toml(&exported_value).unwrap()
        );
        println!(
            "Exported JSON as TOML as JSON: {}",
            toml_to_json(&json_to_toml(&exported_value).unwrap()).unwrap()
        );
    }
}
