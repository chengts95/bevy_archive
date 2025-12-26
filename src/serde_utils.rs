use bevy_ecs::entity::Entity;
use serde::{Deserialize, Deserializer, Serializer};

pub mod entity_serializer {
    use super::*;

    pub fn serialize<S>(entity: &Entity, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(entity.index())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Entity, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = u32::deserialize(deserializer)?;
        Ok(Entity::from_raw_u32(id).unwrap_or(Entity::PLACEHOLDER))
    }
}
