#![allow(unexpected_cfgs)]
pub mod archetype_archive;
pub mod aurora_archive;
pub mod bevy_registry;
pub mod csv_archive;
pub mod entity_archive;

pub mod binary_archive;
pub mod bevy_cmdbuffer;
pub mod serde_utils;
pub mod traits;

#[cfg(feature = "flecs")]
pub mod flecs_archsnaphot;
#[cfg(feature = "flecs")]
pub mod flecs_registry;

#[cfg(feature = "arrow_rs")]
pub mod arrow_snapshot;

#[cfg(feature = "arrow_rs")]
pub use zip;

pub mod prelude {
    pub use crate::aurora_archive::*;
    pub use crate::bevy_registry::*;
    #[cfg(feature = "flecs")]
    pub use crate::flecs_registry;

    pub use crate::entity_archive::*;
    pub use crate::serde_utils::*;
    pub use crate::traits::*;
}
