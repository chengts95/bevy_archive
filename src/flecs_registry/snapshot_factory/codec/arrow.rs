

use crate::binary_archive::arrow_column::ArrowColumn;
use flecs_ecs::prelude::*;
use std::ptr::NonNull;

use crate::prelude::{SnapshotMode, vec_snapshot_factory::*};
use arrow::{array::Array, datatypes::FieldRef};
use serde::de::DeserializeOwned;
use serde_arrow::marrow;
 
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("missing factory for component/resource: {0}")]
    MissingFactory(String),
    #[error("serde error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("arrow error: {0}")]
    ArrowError(#[from] marrow::error::MarrowError),
    #[error("parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),
    #[error("failed to resolve entity id: {0}")]
    InvalidEntityID(u32),
    #[error("unexpected null component {0}")]
    MissingComponent(String),
    #[error("generic error: {0}")]
    Generic(String),
    #[error("generic error: {0}")]
    GenericBox(#[from] Box<dyn std::error::Error>),
}

pub type ArrExportFn = fn(&[FieldRef], &World, &[Entity]) -> Result<ArrowColumn, SnapshotError>;
pub type ArrImportFn = fn(&ArrowColumn, &mut World, &[Entity]) -> Result<(), SnapshotError>;
  
#[derive(Clone, Debug)]
pub struct ArrowSnapshotFactory {
    pub arr_export: ArrExportFn,
    pub arr_import: ArrImportFn, 
    pub schema: Vec<FieldRef>,
}
 