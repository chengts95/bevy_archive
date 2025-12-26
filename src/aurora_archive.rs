//! This module contains the full Aurora manifest system for BevyArchive.
//!
//! It provides:
//! - AuroraLocation / AuroraFormat
//! - WorldWithAurora and manifest types
//! - Blob loading, embedding, parsing
//! - Roundtrip serialization to file

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bevy_ecs::component::ComponentId;
use bevy_ecs::world::World;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use crate::archetype_archive::{
    ArchetypeSnapshot, StorageTypeFlag, WorldArchSnapshot,
    load_world_arch_snapshot_defragment as load_world_arch_snapshot, load_world_resource,
    save_world_arch_snapshot, save_world_resource, load_world_arch_snapshot_with_remap,
};
#[cfg(feature = "arrow_rs")]
use crate::arrow_snapshot::ComponentTable;
use crate::bevy_registry::{SnapshotRegistry, IDRemapRegistry, EntityRemapper};
use crate::csv_archive::ColumnarCsv;
use crate::csv_archive::columnar_from_snapshot;
use crate::traits::Archive;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuroraLocation {
    File(String),
    Embed(String),
    Unknown(String),
}

impl Archive for AuroraWorldManifest {
    fn create(
        world: &World,
        registry: &SnapshotRegistry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        save_world_manifest(world, registry).map_err(|e| e.into())
    }

    fn apply(
        &self,
        world: &mut World,
        registry: &SnapshotRegistry,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        load_world_manifest(world, self, registry).map_err(|e| e.into())
    }

    fn apply_with_remap(
        &self,
        world: &mut World,
        registry: &SnapshotRegistry,
        id_registry: &IDRemapRegistry,
        mapper: &dyn EntityRemapper,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snap: WorldArchSnapshot = self.into();
        load_world_arch_snapshot_with_remap(world, &snap, registry, id_registry, mapper);
        load_world_resource(&self.world.resources, world, registry);
        Ok(())
    }

    fn get_entities(&self) -> Vec<u32> {
        let snap: WorldArchSnapshot = self.into();
        snap.entities
    }

    fn load_resources(
        &self,
        world: &mut World,
        registry: &SnapshotRegistry,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        load_world_resource(&self.world.resources, world, registry);
        Ok(())
    }

    fn save_to(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let p = path.as_ref().to_str().ok_or("Invalid path")?;
        
        let ext = path
            .as_ref()
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
            
        let format = match ext.as_str() {
            "json" => Some(ManifestOutputFormat::Json),
            "toml" => Some(ManifestOutputFormat::Toml),
            _ => None,
        };

        self.to_file(p, format).map_err(|e| e.into())
    }

    fn load_from(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let p = path.as_ref().to_str().ok_or("Invalid path")?;
        Self::from_file(p, None).map_err(|e| e.into())
    }
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
    MsgPack,    // msgpack
    CsvMsgPack, // csv in msgpack
    #[cfg(feature = "arrow_rs")]
    Parquet,
    Unknown,
}

impl AuroraFormat {
    pub fn from_path(path: &str) -> Self {
        if path.ends_with(".csv") {
            Self::Csv
        } else if path.ends_with(".json") {
            Self::Json
        } else if path.ends_with(".csv.msgpack") {
            Self::CsvMsgPack
        } else if path.ends_with(".msgpack") {
            Self::MsgPack
        } else {
            #[cfg(feature = "arrow_rs")]
            {
                if path.ends_with(".parquet") {
                    return Self::Parquet;
                }
            }
            Self::Unknown
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "csv" => Self::Csv,
            "json" => Self::Json,
            "msgpack" => Self::MsgPack,
            "csv.msgpack" => Self::CsvMsgPack,
            #[cfg(feature = "arrow_rs")]
            "parquet" => Self::Parquet,
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
    #[cfg(feature = "arrow_rs")]
    ArrowComponentTable(ComponentTable),
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
                AuroraFormat::MsgPack | AuroraFormat::CsvMsgPack => BASE64_STANDARD
                    .decode(&blob.data)
                    .map_err(|e| format!("Base64 decode failed: {}", e))?,
                #[cfg(feature = "arrow_rs")]
                AuroraFormat::Parquet => BASE64_STANDARD
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
        AuroraFormat::CsvMsgPack => rmp_serde::from_slice(&blob.bytes)
            .map(AuroraInternalFormat::ColumnarCsv)
            .map_err(|e| e.to_string()),
        #[cfg(feature = "arrow_rs")]
        AuroraFormat::Parquet => ComponentTable::from_parquet_u8(&blob.bytes)
            .map(AuroraInternalFormat::ArrowComponentTable)
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
    CsvMsgPack,
    #[cfg(feature = "arrow_rs")]
    Parquet,
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
    /// Returns the bytes in `external_payloads` instead of writing to disk,
    /// setting the source to the provided virtual path.
    Return(ExportFormat, String),
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
    #[serde(skip)]
    pub external_payloads: HashMap<String, Vec<u8>>,
    pub resources: HashMap<String, serde_json::Value>,
}
fn serialize_arch_data(arch: &ArchetypeSnapshot, fmt: &ExportFormat) -> (Vec<u8>, &'static str) {
    match fmt {
        ExportFormat::Csv => {
            let csv = columnar_from_snapshot(arch);
            let mut data = Vec::new();
            csv.to_csv_writer(&mut data).unwrap();
            (data, "csv")
        }
        ExportFormat::Json => (serde_json::to_vec(arch).unwrap(), "json"),
        ExportFormat::MsgPack => (rmp_serde::to_vec(arch).unwrap(), "msgpack"),
        ExportFormat::CsvMsgPack => {
            let csv = columnar_from_snapshot(arch);
            (rmp_serde::to_vec(&csv).unwrap(), "csv.msgpack")
        }
        #[cfg(feature = "arrow_rs")]
        ExportFormat::Parquet => {
            panic!("Parquet should utilize the binary pipeline, not ArchetypeSnapshot")
        }
    }
}

impl WorldWithAurora {
    pub fn from_guided(
        world: &World,
        registry: &SnapshotRegistry,
        guidance: &ExportGuidance,
    ) -> Self {
        let mut archetypes = Vec::new();
        let mut embed = HashMap::new();
        let mut external_payloads: HashMap<String, Vec<u8>> = HashMap::new();

        let reg_comp_ids: HashMap<ComponentId, &str> = registry
            .type_registry
            .keys()
            .filter_map(|&name| registry.comp_id_by_name(name, world).map(|cid| (cid, name)))
            .collect();

        for (i, arch) in world.archetypes().iter().enumerate() {
            if arch.is_empty() {
                continue;
            }
            if !arch
                .components()
                .iter()
                .any(|x| reg_comp_ids.contains_key(x))
            {
                continue;
            }

            let strat = guidance.per_arch.get(&i).unwrap_or(&guidance.default);

            let (fmt, base_path, virtual_path) = match strat {
                OutputStrategy::Embed(f) => (f, None, None),
                OutputStrategy::File(f, p) => (f, Some(p), None),
                OutputStrategy::Return(f, v) => (f, None, Some(v.clone())),
            };

            let (bytes, ext) = match fmt {
                #[cfg(feature = "arrow_rs")]
                ExportFormat::Parquet => {
                    let table = crate::binary_archive::save_arrow_archetype_from_world(
                        world,
                        registry,
                        arch,
                        &reg_comp_ids,
                    )
                    .unwrap();
                    (table.to_parquet().unwrap(), "parquet")
                }
                _ => {
                    let snap = crate::archetype_archive::save_single_archetype_snapshot(
                        world,
                        arch,
                        registry,
                        &reg_comp_ids,
                    );
                    serialize_arch_data(&snap, fmt)
                }
            };

            let arch_name = format!("arch_{}", i);

            let (source, blob_opt) = if let Some(base) = base_path {
                let filename = format!("{}.{}", arch_name, ext);
                let file_path = base.join(filename);
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&file_path, &bytes).unwrap();
                (Url(format!("file://{}", file_path.display())), None)
            } else if let Some(v_path) = virtual_path {
                let filename = format!("{}.{}", arch_name, ext);
                let full_path = if v_path.ends_with('/') || v_path.is_empty() {
                    format!("{}{}", v_path, filename)
                } else {
                    format!("{}/{}", v_path, filename)
                };

                external_payloads.insert(full_path.clone(), bytes);
                (Url(format!("file://{}", full_path)), None)
            } else {
                let data_str = match fmt {
                    ExportFormat::Csv | ExportFormat::Json => String::from_utf8(bytes).unwrap(),
                    ExportFormat::MsgPack | ExportFormat::CsvMsgPack => {
                        BASE64_STANDARD.encode(&bytes)
                    }
                    #[cfg(feature = "arrow_rs")]
                    ExportFormat::Parquet => BASE64_STANDARD.encode(&bytes),
                };
                let blob = EmbeddedBlob {
                    format: ext.to_string(),
                    data: data_str,
                };
                (Url(format!("embed://{}", arch_name)), Some(blob))
            };

            let components: Vec<String> = arch
                .components()
                .iter()
                .filter_map(|id| reg_comp_ids.get(id).map(|s| s.to_string()))
                .collect();

            archetypes.push(ArchetypeSpec {
                name: Some(arch_name.clone()),
                components,
                storage: None,
                source,
            });

            if let Some(blob) = blob_opt {
                embed.insert(arch_name, blob);
            }
        }

        Self {
            version: "0.1".into(),
            archetypes,
            embed,
            external_payloads,
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

            let (bytes, _ext) = serialize_arch_data(arch, &ExportFormat::Csv);
            let blob = EmbeddedBlob {
                format: "csv".to_string(),
                data: String::from_utf8(bytes).unwrap(),
            };

            embed.insert(format!("arch_{}", i), blob);

            archetypes.push(ArchetypeSpec {
                name,
                components: arch.component_types.clone(),
                storage: None,
                source,
            });
        }

        Self {
            version: "0.1".into(),
            archetypes,
            embed,
            external_payloads: HashMap::new(),
            name: None,
            resources: HashMap::new(),
        }
    }
}

impl From<&AuroraWorldManifest> for WorldArchSnapshot {
    fn from(manifest: &AuroraWorldManifest) -> Self {
        (&manifest.world).into()
    }
}

impl From<&WorldWithAurora> for WorldArchSnapshot {
    #[allow(unreachable_patterns)]
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
                    snap.storage_types =
                        arch.storage
                            .clone()
                            .unwrap_or(vec![StorageTypeFlag::Table; snap.component_types.len()]);
                    snap
                }
                AuroraInternalFormat::ArchetypeSnapshot(data) => data,
                _ => panic!("not supported"),
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

enum LoadedArchetype {
    Legacy(ArchetypeSnapshot),
    #[cfg(feature = "arrow_rs")]
    Arrow(ComponentTable),
}

/// Trait for abstracting blob loading (Filesystem, Zip, Memory, etc.)
pub trait BlobLoader {
    fn load_blob(&mut self, path: &str) -> Result<Vec<u8>, String>;
}

/// Default filesystem loader
pub struct FsBlobLoader {
    pub base_dir: PathBuf,
}
impl BlobLoader for FsBlobLoader {
    fn load_blob(&mut self, path: &str) -> Result<Vec<u8>, String> {
        let relative_path = Path::new(path);
        let full_path = if relative_path.is_absolute() {
            relative_path.to_path_buf()
        } else {
            self.base_dir.join(relative_path)
        };
        fs::read(&full_path).map_err(|e| format!("Failed to read {}: {}", full_path.display(), e))
    }
}

#[cfg(feature = "arrow_rs")]
pub struct ZipBlobLoader<R: std::io::Read + std::io::Seek> {
    pub archive: zip::ZipArchive<R>,
}

#[cfg(feature = "arrow_rs")]
impl<R: std::io::Read + std::io::Seek> BlobLoader for ZipBlobLoader<R> {
    fn load_blob(&mut self, path: &str) -> Result<Vec<u8>, String> {
        use std::io::Read;
        let mut file = self.archive.by_name(path).map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).map_err(|e| e.to_string())?;
        Ok(buf)
    }
}

/// Load an ECS world from a manifest structure using a specific blob loader.
pub fn load_world_manifest_with_loader<L: BlobLoader>(
    world: &mut World,
    manifest: &AuroraWorldManifest,
    registry: &SnapshotRegistry,
    loader: &mut L,
) -> Result<(), String> {
    let resource = &manifest.world.resources;
    load_world_resource(resource, world, registry);

    // Parse all blobs first
    let mut loaded_archetypes = Vec::new();
    for arch in &manifest.world.archetypes {
        let loc = AuroraLocation::from(arch.source.0.as_str());

        // Resolve blob
        let blob = match loc {
            AuroraLocation::File(path) => {
                let bytes = loader.load_blob(&path)?;
                let format = AuroraFormat::from_path(&path);
                LoadedBlob { format, bytes }
            }
            AuroraLocation::Embed(name) => {
                let blob =
                    manifest.world.embed.get(&name).ok_or_else(|| {
                        format!("Embedded blob '{}' not found in manifest.", name)
                    })?;
                let format = AuroraFormat::from_str(&blob.format);
                let bytes = match format {
                    AuroraFormat::MsgPack | AuroraFormat::CsvMsgPack => BASE64_STANDARD
                        .decode(&blob.data)
                        .map_err(|e| format!("Base64 decode failed: {}", e))?,
                    #[cfg(feature = "arrow_rs")]
                    AuroraFormat::Parquet => BASE64_STANDARD
                        .decode(&blob.data)
                        .map_err(|e| format!("Base64 decode failed: {}", e))?,
                    _ => blob.data.as_bytes().to_vec(),
                };
                LoadedBlob { format, bytes }
            }
            AuroraLocation::Unknown(s) => return Err(format!("Unknown location: {}", s)),
        };

        let parsed = parse_blob(&blob).unwrap();

        match parsed {
            AuroraInternalFormat::ColumnarCsv(csv) => {
                let mut snap: ArchetypeSnapshot = (&csv).into();
                snap.storage_types = arch
                    .storage
                    .clone()
                    .unwrap_or(vec![StorageTypeFlag::Table; snap.component_types.len()]);
                loaded_archetypes.push(LoadedArchetype::Legacy(snap));
            }
            AuroraInternalFormat::ArchetypeSnapshot(data) => {
                loaded_archetypes.push(LoadedArchetype::Legacy(data));
            }
            #[cfg(feature = "arrow_rs")]
            AuroraInternalFormat::ArrowComponentTable(table) => {
                loaded_archetypes.push(LoadedArchetype::Arrow(table));
            }
        }
    }

    // Reserve entities
    let mut max_entity = 0;
    for arch in &loaded_archetypes {
        let max = match arch {
            LoadedArchetype::Legacy(s) => s.entities.iter().max().copied().unwrap_or(0),
            #[cfg(feature = "arrow_rs")]
            LoadedArchetype::Arrow(t) => t.entities.iter().map(|e| e.id).max().unwrap_or(0),
        };
        if max > max_entity {
            max_entity = max;
        }
    }
    world.entities().reserve_entities(max_entity + 1);
    world.flush();

    // Load data
    #[cfg(feature = "arrow_rs")]
    let mut bump = bumpalo::Bump::new();

    for arch in loaded_archetypes {
        match arch {
            LoadedArchetype::Legacy(snap) => {
                let temp_snap = WorldArchSnapshot {
                    entities: vec![], // Not used by defragment loader for reservation if we did it already
                    archetypes: vec![snap],
                };
                load_world_arch_snapshot(world, &temp_snap, registry);
            }
            #[cfg(feature = "arrow_rs")]
            LoadedArchetype::Arrow(table) => {
                crate::binary_archive::load_arrow_archetype_to_world(
                    world, &registry, &table, &mut bump,
                )
                .map_err(|e| e.to_string())?;
            }
        }
    }

    Ok(())
}

/// Load an ECS world from a manifest structure using default filesystem loading.
///
/// This is a convenience wrapper around `load_world_manifest_with_loader`.
pub fn load_world_manifest(
    world: &mut World,
    manifest: &AuroraWorldManifest,
    registry: &SnapshotRegistry,
) -> Result<(), String> {
    let mut loader = FsBlobLoader {
        base_dir: Path::new(".").to_path_buf(),
    };
    load_world_manifest_with_loader(world, manifest, registry, &mut loader)
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
    let mut world_with_aurora = WorldWithAurora::from_guided(world, registry, guidance);
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
        let path = "test_msgpack.toml";

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
        let arch_type_path = "arch_default_msgpack";
        let (world, registry) = init_world();
        let guide = ExportGuidance::file_all(ExportFormat::MsgPack, arch_type_path);

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
    #[test]
    fn test_csv_msgpack_manifest_snapshot_roundtrip() {
        let path = "test_csvmsgpack.toml";

        let (world, registry) = init_world();
        let guide = ExportGuidance::embed_all(ExportFormat::CsvMsgPack);

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
    fn test_csv_msgpack_manifest_snapshot_roundtrip_file() {
        let path = "test_csvmsgpack_file.toml";
        let arch_type_path = "arch_default_csvmsgpack";
        let (world, registry) = init_world();
        let guide = ExportGuidance::file_all(ExportFormat::CsvMsgPack, arch_type_path);

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

    #[test]
    #[cfg(feature = "arrow_rs")]
    fn test_parquet_manifest_snapshot_roundtrip() {
        let path = "test_parquet.toml";
        let (world, registry) = init_world();
        let guide = ExportGuidance::embed_all(ExportFormat::Parquet);

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
}
