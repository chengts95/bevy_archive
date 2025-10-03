use std::collections::HashMap;
use std::error::Error;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use zip::ZipArchive;
use zip::{ZipWriter, write::SimpleFileOptions};

use crate::arrow_snapshot::ComponentTable;
use crate::binary_archive::BinBlob;
use crate::binary_archive::WorldArrowSnapshot;
use crate::binary_archive::world_snapshot::sparse_entitiy_list;
use crate::binary_archive::world_snapshot::sparse_entitiy_list::SparseU32List;
use crate::prelude::vec_snapshot_factory::SnapshotError;
// === Magic string 常量区（全局唯一入口） ===
const META_TOML: &str = "meta.toml";
const ENTITIES_MSGPACK: &str = "entities.msgpack";
const RESOURCES_PREFIX: &str = "resources/";
const RESOURCES_SUFFIX: &str = ".msgpack";
const ARCHETYPES_PREFIX: &str = "archetypes/";
const ARCHETYPES_SUFFIX: &str = ".parquet";

// 工具函数，读写都调它，不再拼字符串
#[inline]
fn resource_path(key: &str) -> String {
    format!("{RESOURCES_PREFIX}{key}{RESOURCES_SUFFIX}")
}
#[inline]
fn archetype_path(idx: usize) -> String {
    format!("{ARCHETYPES_PREFIX}arch_{idx}{ARCHETYPES_SUFFIX}")
}
#[inline]
fn parse_resource_key(path: &str) -> Option<&str> {
    path.strip_prefix(RESOURCES_PREFIX)?
        .strip_suffix(RESOURCES_SUFFIX)
}
#[inline]
fn parse_archetype_idx(path: &str) -> Option<usize> {
    path.strip_prefix(ARCHETYPES_PREFIX)?
        .strip_prefix("arch_")?
        .strip_suffix(ARCHETYPES_SUFFIX)?
        .parse()
        .ok()
}

impl WorldArrowSnapshot {
    pub fn to_zip(&self, level: Option<i64>) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let mut zip = ZipWriter::new(cursor);

        let options = SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .compression_level(level);

        // 1. meta
        let meta_toml = toml::to_string(&self.meta)
            .map_err(|e| SnapshotError::Generic(format!("toml encode error: {e}")))?;
        zip.start_file(META_TOML, options)?;
        zip.write_all(meta_toml.as_bytes())?;

        // 2. entities
        let entity_bytes = sparse_entitiy_list::SparseU32List::from_unsorted(self.entities.clone());
        zip.start_file(ENTITIES_MSGPACK, options)?;
        zip.write_all(&rmp_serde::to_vec(&entity_bytes)?)?;

        // 3. resources
        for (key, blob) in &self.resources {
            let path = resource_path(key);
            zip.start_file(&path, options)?;
            zip.write_all(&blob.0)?;
        }

        // 4. archetypes
        for (idx, arch) in self.archetypes.iter().enumerate() {
            let name = archetype_path(idx);
            zip.start_file(&name, options)?;
            let parquet_data = arch.to_parquet()?;
            zip.write_all(&parquet_data)?;
        }

        zip.finish()?; // flush everything

        Ok(buffer)
    }
}

impl WorldArrowSnapshot {
    pub fn from_zip(zip_data: &[u8]) -> Result<Self, SnapshotError> {
        let cursor = Cursor::new(zip_data);
        let mut zip = ZipArchive::new(cursor)
            .map_err(|e| SnapshotError::Generic(format!("zip decode error: {e}")))?;

        let mut meta = None;
        let mut entities: Option<Vec<u32>> = None;
        let mut resources = HashMap::new();
        let mut archetypes = vec![];

        for i in 0..zip.len() {
            let mut file = zip
                .by_index(i)
                .map_err(|x| SnapshotError::Generic(x.to_string()))?;
            let name = file.name().to_owned();

            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .map_err(|x| SnapshotError::Generic(x.to_string()))?;

            if name == META_TOML {
                meta = Some(
                    toml::from_str(std::str::from_utf8(&buf).unwrap())
                        .map_err(|e| SnapshotError::Generic(format!("toml decode error: {e}")))?,
                );
            } else if name == ENTITIES_MSGPACK {
                let raw: &[u8] = &buf;
                let ent: SparseU32List = rmp_serde::from_slice(raw)
                    .map_err(|x| SnapshotError::Generic(format!("msgpack decode error: {x}")))?;
                entities = Some(ent.to_vec().iter().copied().collect());
            } else if let Some(key) = parse_resource_key(&name) {
                resources.insert(key.to_string(), BinBlob(buf));
            } else if let Some(_idx) = parse_archetype_idx(&name) {
                let table = ComponentTable::from_parquet_u8(&buf)?;
                archetypes.push(table);
            } else {
                println!("unrecognized file in snapshot zip: {name}");
            }
        }
        let entities: Vec<u32> = entities.unwrap_or_default();
        Ok(WorldArrowSnapshot {
            meta: meta.unwrap_or_default(),
            entities,
            resources,
            archetypes,
        })
    }
}
