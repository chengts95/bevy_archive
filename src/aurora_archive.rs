//! This module contains the full Aurora manifest system for BevyArchive.
//!
//! It provides:
//! - AuroraLocation / AuroraFormat
//! - WorldWithAurora and manifest types
//! - Blob loading, embedding, parsing
//! - Roundtrip serialization to file

use bevy_ecs::world::World;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use crate::archetype_archive::{
    ArchetypeSnapshot, StorageTypeFlag, WorldArchSnapshot, load_world_arch_snapshot,
    save_world_arch_snapshot,
};
use crate::bevy_registry::SnapshotRegistry;
use crate::csv_archive::ColumnarCsv;
use crate::csv_archive::columnar_from_snapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuroraLocation {
    File(String),
    Embed(String),
    Unknown(String),
}

impl From<&str> for AuroraLocation {
    fn from(s: &str) -> Self {
        if let Some(rest) = s.strip_prefix("file://") {
            Self::File(rest.to_string())
        } else if let Some(rest) = s.strip_prefix("embed://") {
            Self::Embed(rest.to_string())
        } else {
            Self::Unknown(s.to_string())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuroraFormat {
    Csv,
    Json,
    Binary,
    Unknown,
}

impl AuroraFormat {
    pub fn from_path(path: &str) -> Self {
        if path.ends_with(".csv") {
            Self::Csv
        } else if path.ends_with(".json") {
            Self::Json
        } else {
            Self::Unknown
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "csv" => Self::Csv,
            "json" => Self::Json,
            _ => Self::Unknown,
        }
    }
}

pub struct LoadedBlob {
    pub format: AuroraFormat,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum AuroraInternalFormat {
    ColumnarCsv(ColumnarCsv),
    ArchetypeSnapshot(ArchetypeSnapshot),
}

pub fn load_blob_from_location(
    loc: &AuroraLocation,
    embed_map: &HashMap<String, EmbeddedBlob>,
) -> Result<LoadedBlob, String> {
    match loc {
        AuroraLocation::File(path) => {
            let bytes = fs::read(path).map_err(|e| e.to_string())?;
            let format = AuroraFormat::from_path(path);
            Ok(LoadedBlob { format, bytes })
        }
        AuroraLocation::Embed(name) => {
            let blob = embed_map.get(name).ok_or("Missing embed")?;
            let format = AuroraFormat::from_str(&blob.format);
            Ok(LoadedBlob {
                format,
                bytes: blob.data.as_bytes().to_vec(),
            })
        }
        _ => Err("Unsupported".into()),
    }
}

fn parse_blob(blob: &LoadedBlob) -> Result<AuroraInternalFormat, String> {
    match &blob.format {
        AuroraFormat::Csv => ColumnarCsv::from_csv_reader(&blob.bytes[..])
            .map(AuroraInternalFormat::ColumnarCsv)
            .map_err(|e| e.to_string()),
        AuroraFormat::Json => serde_json::from_slice(&blob.bytes)
            .map(AuroraInternalFormat::ArchetypeSnapshot)
            .map_err(|e| e.to_string()),
        _ => Err("Cannot parse unknown format".into()),
    }
}

pub fn load_and_parse(
    loc: &AuroraLocation,
    embed: &HashMap<String, EmbeddedBlob>,
) -> Result<AuroraInternalFormat, String> {
    let blob = load_blob_from_location(loc, embed)?;
    parse_blob(&blob)
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Url(pub String);

#[derive(Deserialize, Serialize, Debug)]
pub struct ArchetypeSpec {
    #[serde(default)]
    pub name: Option<String>,
    pub components: Vec<String>,
    pub storage: Option<Vec<StorageTypeFlag>>,
    pub source: Url,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EmbeddedBlob {
    pub format: String,
    pub data: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorldWithAurora {
    pub version: String,
    pub name: Option<String>,
    pub archetypes: Vec<ArchetypeSpec>,
    #[serde(default)]
    pub embed: HashMap<String, EmbeddedBlob>,
}

impl WorldWithAurora {
    pub fn from_file(path: &str) -> Result<Self, String> {
        let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
        serde_json::from_str(&content).map_err(|e| e.to_string())
    }

    pub fn to_file(&self, path: &str) -> Result<(), String> {
        let content = serde_json::to_string(self).map_err(|e| e.to_string())?;
        fs::write(path, content).map_err(|e| e.to_string())
    }
}

impl From<&WorldArchSnapshot> for WorldWithAurora {
    fn from(world: &WorldArchSnapshot) -> Self {
        let mut archetypes = Vec::new();
        let mut embed = HashMap::new();

        for (i, arch) in world.archetypes.iter().enumerate() {
            if arch.is_empty() {
                continue;
            }
            let name = Some(format!("arch_{}", i));
            let source = Url(format!("embed://arch_{}", i));

            let csv = columnar_from_snapshot(arch);
            let mut data = Vec::new();
            csv.to_csv_writer(&mut data).unwrap();

            let blob = EmbeddedBlob {
                format: "csv".to_string(),
                data: String::from_utf8(data).unwrap(),
            };

            embed.insert(format!("arch_{}", i), blob);

            archetypes.push(ArchetypeSpec {
                name,
                components: arch.component_types.clone(),
                //storage: Some(arch.storage_types.clone()),
                storage: None, //disable this  for now
                source,
            });
        }

        Self {
            version: "0.1".into(),
            archetypes,
            embed,
            name: None,
        }
    }
}

impl From<&WorldWithAurora> for WorldArchSnapshot {
    fn from(world: &WorldWithAurora) -> Self {
        let mut archetypes = Vec::new();
        let mut all_entities: BTreeSet<u32> = BTreeSet::new();

        for arch in &world.archetypes {
            let loc = AuroraLocation::from(arch.source.0.as_str());
            let blob = load_blob_from_location(&loc, &world.embed).unwrap();
            let parsed = parse_blob(&blob).unwrap();

            let snapshot = match parsed {
                AuroraInternalFormat::ColumnarCsv(csv) => {
                    let mut snap: ArchetypeSnapshot = (&csv).into();
                    snap.storage_types = arch
                        .storage
                        .clone()
                        .unwrap_or(vec![StorageTypeFlag::Table; snap.entities.len()]);
                    snap
                }
                AuroraInternalFormat::ArchetypeSnapshot(data) => data,
            };

            all_entities.extend(snapshot.entities.clone());
            archetypes.push(snapshot);
        }

        WorldArchSnapshot {
            entities: all_entities.into_iter().collect(),
            archetypes,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AuroraWorldManifest {
    pub metadata: Option<HashMap<String, Value>>,
    pub world: WorldWithAurora,
}
#[derive(Default)]
pub enum ManifestOutputFormat {
    Json,
    #[default]
    Toml,
}
impl AuroraWorldManifest {
    /// Save the manifest to a file.
    ///
    /// # Parameters
    /// - `path`: Destination path to write the manifest file.
    /// - `format`: Optional format override (`Json` or `Toml`). If `None`, TOML is used.
    ///
    /// # Returns
    /// Returns `Ok(())` on success, or an error message string.
    pub fn to_file(&self, path: &str, format: Option<ManifestOutputFormat>) -> Result<(), String> {
        write_manifest_to_file(self, path, format.unwrap_or_default())
    }

    /// Load a manifest from a file on disk.
    ///
    /// # Parameters
    /// - `path`: File path of the TOML/JSON world manifest.
    /// - `format`: Optional format hint. If not provided, guessed from file extension.
    ///
    /// # Returns
    /// The loaded `AuroraWorldManifest` structure.
    pub fn from_file(path: &str, format: Option<ManifestOutputFormat>) -> Result<Self, String> {
        read_manifest_from_file(path, format)
    }
}

/// Save a snapshot of the ECS `World` into an `AuroraWorldManifest`, which includes
/// archetypes and optionally embedded data.
///
/// This serves as a serializable container that can be persisted or diffed later.
///
/// # Parameters
/// - `world`: The Bevy ECS world to capture.
/// - `registry`: Snapshot registry for (de)serialization logic.
///
/// # Returns
/// A fully structured `AuroraWorldManifest`.
pub fn save_world_manifest(
    world: &World,
    registry: &SnapshotRegistry,
) -> Result<AuroraWorldManifest, String> {
    let snapshot = save_world_arch_snapshot(world, registry);
    let world_with_aurora = WorldWithAurora::from(&snapshot);

    Ok(AuroraWorldManifest {
        metadata: None,
        world: world_with_aurora,
    })
}

/// Load an ECS world from a manifest structure.
///
/// Converts the manifest into internal snapshot data and inserts the data into a world.
///
/// # Parameters
/// - `world`: A mutable ECS world to populate.
/// - `manifest`: The manifest to load from.
/// - `registry`: Component (de)serialization registry.
///
/// # Returns
/// Ok on success, or a string describing the failure.
pub fn load_world_manifest(
    world: &mut World,
    manifest: &AuroraWorldManifest,
    registry: &SnapshotRegistry,
) -> Result<(), String> {
    let snapshot: WorldArchSnapshot = (&manifest.world).into();
    load_world_arch_snapshot(world, &snapshot, registry);
    Ok(())
}

/// Write a manifest to a file in a specified format.
///
/// # Parameters
/// - `manifest`: The manifest structure to save.
/// - `path`: Destination path to write.
/// - `format`: Desired serialization format (JSON or TOML).
///
/// # Returns
/// Ok if written successfully, or a string with error message.
pub fn write_manifest_to_file<P: AsRef<Path>>(
    manifest: &AuroraWorldManifest,
    path: P,
    format: ManifestOutputFormat,
) -> Result<(), String> {
    let content = match format {
        ManifestOutputFormat::Json => {
            serde_json::to_string_pretty(manifest).map_err(|e| e.to_string())?
        }
        ManifestOutputFormat::Toml => {
            toml::to_string_pretty(manifest).map_err(|e| e.to_string())?
        }
    };
    fs::write(path, content).map_err(|e| e.to_string())
}

/// Load a manifest from a file on disk and parse it.
///
/// This function will try to guess the format from the extension if none is provided.
///
/// # Parameters
/// - `path`: Path to the manifest file.
/// - `format_hint`: Optional explicit format. If not provided, guesses from extension.
///
/// # Returns
/// A parsed `AuroraWorldManifest`, or an error message.
///
/// # Supported Extensions
/// - `.toml` → `TOML`
/// - `.json` → `JSON`
pub fn read_manifest_from_file<P: AsRef<Path>>(
    path: P,
    format_hint: Option<ManifestOutputFormat>,
) -> Result<AuroraWorldManifest, String> {
    let content = fs::read_to_string(&path).map_err(|e| e.to_string())?;

    let format = match format_hint {
        Some(f) => f,
        None => {
            let ext = path
                .as_ref()
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();
            match ext.as_str() {
                "json" => ManifestOutputFormat::Json,
                "toml" => ManifestOutputFormat::Toml,
                _ => return Err(format!("Cannot guess format from extension: {}", ext)),
            }
        }
    };

    match format {
        ManifestOutputFormat::Json => serde_json::from_str(&content).map_err(|e| e.to_string()),
        ManifestOutputFormat::Toml => toml::from_str(&content).map_err(|e| e.to_string()),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::archetype_archive::load_world_arch_snapshot;
    use crate::archetype_archive::save_world_arch_snapshot;
    use bevy_ecs::prelude::*;
    use serde::Deserialize;
    use serde::Serialize;
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentA {
        pub value: i32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentB {
        pub value: f32,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentC {
        pub value: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentD {
        pub value: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentE(Vec<f64>);
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Component)]
    struct TestComponentF(TestComponentC);
    fn init_world() -> (World, SnapshotRegistry) {
        let mut world = World::new();
        let mut registry = SnapshotRegistry::default();

        // 注册组件类型
        registry.register::<TestComponentA>();
        registry.register::<TestComponentB>();
        registry.register::<TestComponentC>();
        registry.register::<TestComponentD>();
        registry.register::<TestComponentE>();
        registry.register::<TestComponentF>();
        // 构建不同组合的 archetype
        for i in 0..10 {
            world.spawn((
                TestComponentA { value: i },
                TestComponentB {
                    value: i as f32 * 0.1,
                },
            ));
            world.spawn((
                TestComponentB {
                    value: i as f32 * 0.2,
                },
                TestComponentC {
                    value: format!("EntityC{}", i),
                },
            ));
            world.spawn((
                TestComponentA { value: i * 2 },
                TestComponentC {
                    value: format!("EntityAC{}", i),
                },
                TestComponentD { value: i % 2 == 0 },
            ));
            world.spawn((
                TestComponentD { value: i % 3 == 0 },
                TestComponentE(vec![i as f64, i as f64 + 1.0]),
            ));
            world.spawn((
                TestComponentA { value: -i },
                TestComponentB {
                    value: -i as f32 * 0.3,
                },
                TestComponentC {
                    value: format!("Combo{}", i),
                },
                TestComponentD { value: i % 5 == 0 },
                TestComponentE(vec![0.0; i as usize % 10 + 1]),
                TestComponentF(TestComponentC {
                    value: format!("Nested{}", i),
                }),
            ));
        }

        (world, registry)
    }

    #[test]
    fn test_aurora_snapshot_roundtrip() {
        let (world, registry) = init_world();
        let snapshot = save_world_arch_snapshot(&world, &registry);
        let table = WorldWithAurora::from(&snapshot);

        let data = toml::to_string_pretty(&table).unwrap();
        println!("Serialized data: {}", data);

        let deserialized: WorldWithAurora = toml::from_str(&data).unwrap();

        let mut world2 = World::new();
        load_world_arch_snapshot(&mut world2, &((&deserialized).into()), &registry);
    }

    #[test]
    fn test_aurora_manifest_snapshot_roundtrip() {
        let (world, registry) = init_world();
        let snapshot = save_world_manifest(&world, &registry).unwrap();
        let toml = toml::to_string_pretty(&snapshot).unwrap();
        let deserialized: AuroraWorldManifest =
            toml::from_str(&toml).expect("Failed to deserialize TOML");

        let mut world2 = World::new();
        load_world_manifest(&mut world2, &snapshot, &registry).unwrap();
        load_world_manifest(&mut world2, &deserialized, &registry).unwrap();
    }
}
