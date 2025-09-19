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
use crate::prelude::vec_snapshot_factory::SnapshotError;

impl WorldArrowSnapshot {
    pub fn to_zip(&self, level: Option<i64>) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let mut zip = ZipWriter::new(cursor);

        let options = SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .compression_level(level);
        // 1. 写 meta.toml
        let meta_toml = toml::to_string(&self.meta)
            .map_err(|e| SnapshotError::Generic(format!("toml encode error: {e}")))?;
        zip.start_file("meta.toml", options)?;
        zip.write_all(meta_toml.as_bytes())?;

        // 2. 写 entities
        let entity_bytes = sparse_entitiy_list::SparseU32List::from_unsorted(self.entities.clone());
        zip.start_file("entities.msgpack", options)?;
        zip.write_all(&rmp_serde::to_vec(&entity_bytes)?)?;

        // 3. 写资源
        for (key, blob) in &self.resources {
            let path = format!("resources/{key}.msgpack");
            zip.start_file(path, options)?;
            zip.write_all(&blob.0)?;
        }

        // 4. 写 archetypes
        for (idx, arch) in self.archetypes.iter().enumerate() {
            let name = format!("archetypes/arch_{idx}.parquet");
            zip.start_file(name, options)?;
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
            let name = file.name().to_string();

            let mut buf = vec![];
            file.read(&mut buf)
                .map_err(|x| SnapshotError::Generic(x.to_string()))?;

            if name == "meta.toml" {
                meta = Some(
                    toml::from_str(unsafe { str::from_utf8_unchecked(&buf) })
                        .map_err(|e| SnapshotError::Generic(format!("toml decode error: {e}")))?,
                );
            } else if name == "entities.msgpack" {
                let raw: &[u8] = &buf;
                let ent: Vec<u32> = rmp_serde::from_slice(raw)
                    .map_err(|x| SnapshotError::Generic(x.to_string()))?;
                entities = Some(ent.iter().copied().collect());
            } else if name.starts_with("resources/") && name.ends_with(".msg") {
                let key = name
                    .trim_start_matches("resources/")
                    .trim_end_matches(".msgpack");
                resources.insert(key.to_string(), BinBlob(buf));
            } else if name.starts_with("archetypes/") && name.ends_with(".parquet") {
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
