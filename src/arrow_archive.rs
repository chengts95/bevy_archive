use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::Arc;

use crate::archetype_archive::ArchetypeSnapshot;
use crate::prelude::vec_snapshot_factory::ArrowColumn;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;
use serde::{Deserialize, Serialize};
#[derive(Debug, Default, Clone)]
pub struct ComponentTable {
    pub columns: BTreeMap<String, ArrowColumn>,
    pub entities: Vec<EntityID>,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct EntityID {
    pub id: u32,
}

impl ComponentTable {
    pub fn to_record_batch(&self) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        let mut type_map = HashMap::new();

        let ent = ArrowColumn::from_slice(&self.entities).unwrap();
        type_map.insert("id".to_string(), vec!["id".to_string()]);
        for f in &ent.fields {
            fields.push(f.clone());
            arrays.extend(ent.data.clone());
        }
        for (type_name, col) in &self.columns {
            let mut str_fields = Vec::with_capacity(col.fields.len());
            for f in &col.fields {
                let mut meta = f.metadata().clone();
                let mut f = (**f).clone();

                if f.name() == "" {
                    f = f.with_name(format!("{}", type_name));
                } else {
                    let name = format!("{}.{}", type_name, f.name());
                    f = f.with_name(name);
                    meta.insert("prefix".to_string(), type_name.clone());
                }

                str_fields.push(f.name().to_owned());
                fields.push(Arc::new(f.with_metadata(meta)));
            }
            type_map.insert(type_name.to_owned(), str_fields);
            let arrow_arrays = col.data.clone();
            arrays.extend(arrow_arrays);
        }
        let mut schema = arrow::datatypes::Schema::new(fields);

        schema.metadata.insert(
            "type_mapping".to_string(),
            serde_json::to_string(&type_map)?,
        );
        let record_batch = arrow::array::RecordBatch::try_new(Arc::new(schema), arrays);
        Ok(record_batch?)
    }
}
impl ComponentTable {
 
    pub fn insert_column(&mut self, name: &str, column: ArrowColumn) {
        self.columns.insert(name.to_string(), column);
    }
    pub fn remove_column(&mut self, name: &str) {
        self.columns.remove(name);
    }
    pub fn get_column_mut(&mut self, name: &str) -> Option<&mut ArrowColumn> {
        self.columns.get_mut(name)
    }
    pub fn get_column(&self, name: &str) -> Option<&ArrowColumn> {
        self.columns.get(name)
    }
}

impl ComponentTable {
    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self, Box<dyn std::error::Error>> {
        let mut new_table = ComponentTable::default();
        let fields = batch.schema().fields().clone();
        let mut table_builder = HashMap::new();

        for field in fields.iter() {
            let prefix = field.metadata().get("prefix").map_or(field.name(), |v| v);
            let column = batch.column_by_name(field.name()).unwrap();
            let final_name = field
                .name()
                .strip_prefix(&format!("{}.", prefix))
                .unwrap_or(field.name());
            let renamed_field = (**field).clone().with_name(final_name);
            table_builder
                .entry(prefix.to_string())
                .or_insert(Vec::new())
                .push((Arc::new(renamed_field), column.clone()));
        }

        for (name, data) in table_builder {
            let column = ArrowColumn {
                fields: data.iter().map(|(f, _)| f.clone()).collect(),
                data: data.iter().map(|(_, a)| a.clone()).collect(),
            };
            if name == "id" {
                new_table.entities = column.to_vec::<EntityID>()?;
            } else {
                new_table.insert_column(&name, column);
            }
        }

        Ok(new_table)
    }
}

impl ComponentTable {
    pub fn from_parquet_u8(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = bytes::Bytes::from_iter(buffer.iter().cloned());
        Self::from_parquet(bytes)
    }
    pub fn to_parquet(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let record_batch = self.to_record_batch()?;
        let mut buffer = Vec::new();
        {
            let mut arrow_writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
            arrow_writer.write(&record_batch)?;
            arrow_writer.close()?;
        }
        Ok(buffer)
    }
    ///! Caution: this does not support nested struct due to arrow limitations.
    pub fn to_csv(&self) -> Result<String, Box<dyn std::error::Error>> {
        let record_batch = self.to_record_batch()?;
        let buffer = Cursor::new(Vec::new());

        let data = arrow::csv::WriterBuilder::new();
        let data = data.with_header(true);
        let mut w = data.build(buffer);
        w.write(&record_batch)?;
        let buffer = w.into_inner();
        Ok(String::from_utf8(buffer.into_inner())?)
    }
    pub fn from_parquet<T>(reader: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: ChunkReader + 'static,
    {
        let reader = ParquetRecordBatchReaderBuilder::try_new(reader)?
            .with_batch_size(8192)
            .build()?;

        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        let schema = batches[0].schema();
        let batch = concat_batches(&schema, &batches)?;

        Self::from_record_batch(&batch)
    }
}
pub struct ArrowTableConverstion;
pub struct ArchetypeSnapshotCtx<'a, 'w> {
    pub arch: &'a ArchetypeSnapshot,
    pub reg: &'w ArrowTableConverstion,
}
// impl From<&ArchetypeSnapshot> for ComponentTable {
//     fn from(comp: &ArchetypeSnapshot) -> Self {
//         let mut table = ComponentTable::default();
//         table.entities = comp.entities.iter().map(|x| EntityID { id: *x }).collect();
//         table.columns.insert(key, value);
//         table
//     }
// }

// impl From<&ComponentTable> for ArchetypeSnapshot {
//     fn from(comp: &ComponentTable) -> Self {
//         let mut arch = ArchetypeSnapshot::default();
//     }
// }
