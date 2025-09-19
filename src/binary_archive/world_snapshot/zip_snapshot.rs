use std::error::Error;
use std::io::Cursor;
use std::io::Write;
use zip::{ZipWriter, write::SimpleFileOptions};

use crate::binary_archive::WorldArrowSnapshot;
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
        let entity_bytes = bytemuck::cast_slice(&self.entities);
        zip.start_file("entities.bin", options)?;
        zip.write_all(entity_bytes)?;

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
