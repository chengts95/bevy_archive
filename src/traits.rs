use bevy_ecs::prelude::World;
use std::path::Path;

use crate::bevy_registry::{SnapshotRegistry, IDRemapRegistry, EntityRemapper};

/// A common trait for all Bevy archive formats.
pub trait Archive: Sized {
    /// Create an in-memory archive from the World.
    fn create(
        world: &World,
        registry: &SnapshotRegistry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>;

    /// Apply the archive content to the World.
    fn apply(
        &self,
        world: &mut World,
        registry: &SnapshotRegistry,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Apply the archive content with entity remapping.
    fn apply_with_remap(
        &self,
        _world: &mut World,
        _registry: &SnapshotRegistry,
        _id_registry: &IDRemapRegistry,
        _mapper: &dyn EntityRemapper,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("Remapping not implemented for this archive format".into())
    }

    /// Save the archive to a file.
    fn save_to(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Load the archive from a file.
    fn load_from(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>;
}
