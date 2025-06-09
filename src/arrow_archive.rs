use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{archetype_archive::StorageTypeFlag, prelude::vec_snapshot_factory::ArrowColumn};
// #[derive(Default, Clone, Debug, Serialize, Deserialize)]
// pub struct ArrowArray(pub RecordBatch);
// #[derive(Default, Clone, Debug,Serialize,Deserialize)]
// pub struct ArrowSnapshot {
//     pub component_types: Vec<String>,         // 顺序确定！
//     pub storage_types: Vec<StorageTypeFlag>,  // 与 component_types 对齐
//     pub columns: Vec<ArrowArray>, 
//     pub entities: Vec<u32>,                   // entity_id → row idx
// }