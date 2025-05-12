pub mod archetype_archive;
pub mod aurora_archive;
pub mod bevy_registry;
pub mod csv_archive;
pub mod entity_archive;
pub mod flecs_archsnaphot;
#[cfg(feature = "flecs")]
pub mod flecs_registry;

pub mod prelude {
    pub use crate::aurora_archive::*;
    pub use crate::bevy_registry::*;
    #[cfg(feature = "flecs")]
    pub use crate::flecs_registry as flecs_registry;

    pub use crate::entity_archive::*;
}
