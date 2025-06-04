//! This module contains the full Aurora manifest system for BevyArchive.
//!
//! It provides:
//! - AuroraLocation / AuroraFormat
//! - WorldWithAurora and manifest types
//! - Blob loading, embedding, parsing
//! - Roundtrip serialization to file

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bevy_ecs::world::World;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use crate::archetype_archive::{
    ArchetypeSnapshot, StorageTypeFlag, WorldArchSnapshot,
    load_world_arch_snapshot_defragment as load_world_arch_snapshot, load_world_resource,
    save_world_arch_snapshot, save_world_resource,
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
    MsgPack, // msgpack
    Unknown,
}

impl AuroraFormat {
    pub fn from_path(path: &str) -> Self {
        if path.ends_with(".csv") {
            Self::Csv
        } else if path.ends_with(".json") {
            Self::Json
        } else if path.ends_with(".msgpack") {
            Self::MsgPack
        } else {
            Self::Unknown
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "csv" => Self::Csv,
            "json" => Self::Json,
            "msgpack" => Self::MsgPack,
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
/// Load a blob from a specified `AuroraLocation`, resolving relative paths with a base directory.
///
/// # Parameters
/// - `loc`: The logical location (file path, embedded, etc.)
/// - `embed_map`: Embedded blob map for `embed://` references.
/// - `base_dir`: Base directory used to resolve `file://` relative paths.
///
/// # Returns
/// A `LoadedBlob` with its bytes and format.
pub fn load_blob_from_location_with_base(
    loc: &AuroraLocation,
    embed_map: &HashMap<String, EmbeddedBlob>,
    base_dir: &Path,
) -> Result<LoadedBlob, String> {
    match loc {
        AuroraLocation::File(raw_path) => {
            let relative_path = Path::new(raw_path);
            let full_path = if relative_path.is_absolute() {
                relative_path.to_path_buf()
            } else {
                base_dir.join(relative_path)
            };

            let bytes = fs::read(&full_path)
                .map_err(|e| format!("Failed to read {}: {}", full_path.display(), e))?;

            let format = AuroraFormat::from_path(
                full_path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
            );

            Ok(LoadedBlob { format, bytes })
        }

        AuroraLocation::Embed(name) => {
            let blob = embed_map.get(name).ok_or_else(|| {
                format!(
                    "Embedded blob '{}' not found in manifest embed section.",
                    name
                )
            })?;

            let format = AuroraFormat::from_str(&blob.format);

            let bytes = match format {
                AuroraFormat::MsgPack => BASE64_STANDARD
                    .decode(&blob.data)
                    .map_err(|e| format!("Base64 decode failed: {}", e))?,
                _ => blob.data.as_bytes().to_vec(),
            };

            Ok(LoadedBlob { format, bytes })
        }

        AuroraLocation::Unknown(s) => Err(format!("Unknown location type: {}", s)),
    }
}

pub fn load_blob_from_location(
    loc: &AuroraLocation,
    embed_map: &HashMap<String, EmbeddedBlob>,
) -> Result<LoadedBlob, String> {
    load_blob_from_location_with_base(loc, embed_map, Path::new("."))
}

fn parse_blob(blob: &LoadedBlob) -> Result<AuroraInternalFormat, String> {
    match &blob.format {
        AuroraFormat::Csv => ColumnarCsv::from_csv_reader(&blob.bytes[..])
            .map(AuroraInternalFormat::ColumnarCsv)
            .map_err(|e| e.to_string()),
        AuroraFormat::Json => serde_json::from_slice(&blob.bytes)
            .map(AuroraInternalFormat::ArchetypeSnapshot)
            .map_err(|e| e.to_string()),
        AuroraFormat::MsgPack => rmp_serde::from_slice(&blob.bytes)
            .map(AuroraInternalFormat::ArchetypeSnapshot)
            .map_err(|e| e.to_string()),
        _ => Err("Cannot parse unknown format".into()),
    }
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

#[derive(Clone)]
pub enum ExportFormat {
    Csv,
    Json,
    MsgPack,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EmbeddedBlob {
    pub format: String,
    pub data: String,
}
#[derive(Clone)]
pub enum OutputStrategy {
    Embed(ExportFormat),

    File(ExportFormat, std::path::PathBuf),
}

#[derive(Clone)]
pub struct ExportGuidance {
    pub default: OutputStrategy,

    pub per_arch: HashMap<usize, OutputStrategy>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorldWithAurora {
    pub version: String,
    pub name: Option<String>,
    pub archetypes: Vec<ArchetypeSpec>,
    #[serde(default)]
    pub embed: HashMap<String, EmbeddedBlob>,
    pub resources: HashMap<String, serde_json::Value>,
}
impl WorldWithAurora {
    pub fn from_guided(world: &WorldArchSnapshot, guidance: &ExportGuidance) -> Self {
        let mut archetypes = Vec::new();
        let mut embed = HashMap::new();

        for (i, arch) in world.archetypes.iter().enumerate() {
            if arch.is_empty() {
                continue;
            }

            let strat = guidance
                .per_arch
                .get(&i)
                .cloned()
                .unwrap_or_else(|| guidance.default.clone());

            let (source, blob_opt) = match strat {
                OutputStrategy::Embed(fmt) => {
                    let blob = match fmt {
                        ExportFormat::Csv => {
                            let csv = columnar_from_snapshot(arch);
                            let mut data = Vec::new();
                            csv.to_csv_writer(&mut data).unwrap();
                            EmbeddedBlob {
                                format: "csv".into(),
                                data: String::from_utf8(data).unwrap(),
                            }
                        }
                        ExportFormat::MsgPack => {
                            let bytes = rmp_serde::to_vec(arch).unwrap();
                            EmbeddedBlob {
                                format: "msgpack".into(),
                                data: BASE64_STANDARD.encode(&bytes),
                            }
                        }
                        ExportFormat::Json => {
                            let json = serde_json::to_string(arch).unwrap();
                            EmbeddedBlob {
                                format: "json".into(),
                                data: json,
                            }
                        }
                    };
                    (Url(format!("embed://arch_{}", i)), Some(blob))
                }

                OutputStrategy::File(fmt, ref base_path) => {
                    let ext = match fmt {
                        ExportFormat::Csv => "csv",
                        ExportFormat::MsgPack => "msgpack",
                        ExportFormat::Json => "json",
                    };
                    let file_path = base_path.join(format!("arch_{}.{}", i, ext));
                    if let Some(parent) = file_path.parent() {
                        std::fs::create_dir_all(parent).unwrap();
                    }

                    match fmt {
                        ExportFormat::Csv => {
                            let csv = columnar_from_snapshot(arch);
                            let mut data = Vec::new();
                            csv.to_csv_writer(&mut data).unwrap();
                            std::fs::write(&file_path, data).unwrap();
                        }
                        ExportFormat::MsgPack => {
                            let data = rmp_serde::to_vec(arch).unwrap();
                            std::fs::write(&file_path, &data).unwrap();
                        }
                        ExportFormat::Json => {
                            let json = serde_json::to_string(arch).unwrap();
                            std::fs::write(&file_path, &json).unwrap();
                        }
                    }

                    (Url(format!("file://{}", file_path.display())), None)
                }
            };

            archetypes.push(ArchetypeSpec {
                name: Some(format!("arch_{}", i)),
                components: arch.component_types.clone(),
                storage: None,
                source,
            });

            if let Some(blob) = blob_opt {
                embed.insert(format!("arch_{}", i), blob);
            }
        }

        Self {
            version: "0.1".into(),
            archetypes,
            embed,
            name: None,
            resources: HashMap::new(),
        }
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
            resources: HashMap::new(),
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

impl ExportGuidance {
    pub fn embed_all(format: ExportFormat) -> Self {
        Self {
            default: OutputStrategy::Embed(format),
            per_arch: HashMap::new(),
        }
    }

    pub fn file_all(format: ExportFormat, base_path: impl Into<PathBuf>) -> Self {
        let base = base_path.into();
        Self {
            default: OutputStrategy::File(format.clone(), base),
            per_arch: HashMap::new(),
        }
    }

    /// 设置某个 Archetype 的导出策略
    pub fn set_strategy_for(&mut self, index: usize, strategy: OutputStrategy) -> &mut Self {
        self.per_arch.insert(index, strategy);
        self
    }

    pub fn embed_as(&mut self, index: usize, fmt: ExportFormat) -> &mut Self {
        self.set_strategy_for(index, OutputStrategy::Embed(fmt))
    }

    pub fn file_as(
        &mut self,
        index: usize,
        fmt: ExportFormat,
        path: impl Into<PathBuf>,
    ) -> &mut Self {
        self.set_strategy_for(index, OutputStrategy::File(fmt, path.into()))
    }

    pub fn get_strategy(&self, index: usize) -> OutputStrategy {
        self.per_arch
            .get(&index)
            .cloned()
            .unwrap_or_else(|| self.default.clone())
    }
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
    let mut world_with_aurora = WorldWithAurora::from(&snapshot);
    world_with_aurora.resources = save_world_resource(world, registry);
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
    let resource = &manifest.world.resources;
    let snapshot: WorldArchSnapshot = (&manifest.world).into();
    load_world_resource(resource, world, registry);
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

pub fn save_world_manifest_with_guidance(
    world: &World,
    registry: &SnapshotRegistry,
    guidance: &ExportGuidance,
) -> Result<AuroraWorldManifest, String> {
    let snapshot = save_world_arch_snapshot(world, registry);
    let mut world_with_aurora = WorldWithAurora::from_guided(&snapshot, guidance);
    world_with_aurora.resources = save_world_resource(world, registry);
    Ok(AuroraWorldManifest {
        metadata: None,
        world: world_with_aurora,
    })
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

    #[test]
    fn test_msgpack_manifest_snapshot_roundtrip() {
        let path = "test.toml";

        let (world, registry) = init_world();
        let guide = ExportGuidance::embed_all(ExportFormat::MsgPack);

        let snapshot = save_world_manifest_with_guidance(&world, &registry, &guide).unwrap();
        snapshot.to_file(path, None).unwrap();

        assert!(Path::new(path).exists(), "File not written");

        let toml = fs::read_to_string(path).unwrap();
        let deserialized: AuroraWorldManifest =
            toml::from_str(&toml).expect("Failed to deserialize TOML");

        let mut world2 = World::new();
        load_world_manifest(&mut world2, &snapshot, &registry).unwrap();
        load_world_manifest(&mut world2, &deserialized, &registry).unwrap();

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_msgpack_manifest_snapshot_roundtrip_file() {
        let path = "test.toml";
        let arch_type_path = "arch_default";
        let (world, registry) = init_world();
        let guide = ExportGuidance::file_all(ExportFormat::Csv, arch_type_path);

        let snapshot = save_world_manifest_with_guidance(&world, &registry, &guide).unwrap();
        snapshot.to_file(path, None).unwrap();

        assert!(Path::new(path).exists(), "File not written");

        let toml = fs::read_to_string(path).unwrap();
        let deserialized: AuroraWorldManifest =
            toml::from_str(&toml).expect("Failed to deserialize TOML");

        let mut world2 = World::new();
        load_world_manifest(&mut world2, &snapshot, &registry).unwrap();
        load_world_manifest(&mut world2, &deserialized, &registry).unwrap();

        fs::remove_file(path).ok();
        fs::remove_dir_all(arch_type_path).ok();
    }
}
