use bevy_ecs::entity::Entity;
use serde::{Deserialize, Deserializer, Serializer};

/// Entity → u32 index（去掉 generation，仅保留 index）
#[inline]
pub fn entity_to_index(entity: &Entity) -> u32 {
    entity.index_u32()
}

/// u32 index → Entity（generation = 0，用于反序列化恢复）
#[inline]
pub fn entity_from_index(index: u32) -> Entity {
    Entity::from_raw_u32(index).unwrap_or(Entity::PLACEHOLDER)
}

pub mod entity_serializer {
    use super::*;

    pub fn serialize<S>(entity: &Entity, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(entity_to_index(entity))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Entity, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = u32::deserialize(deserializer)?;
        Ok(entity_from_index(id))
    }
}
