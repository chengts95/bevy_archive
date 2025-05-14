# Changelog

## [0.1.1] - 2024-05-14

### Added
- Integration with bump allocator for temporary component memory.
- `DeferredEntityBuilder` for runtime batch insertion of components.
- Support for insert_by_id(ComponentId, OwningPtr).
- `load_world_arch_snapshot_defragment` to avoid archetype fragments in bevy ECS
- Experimental flecs support: cross-ECS serialization example added, enabling data transfer between bevy and flecs-based runtimes.

- Extended cross-ECS example between bevy and flecs.
### Changed
-   ArchetypeSnapshot entity index storage: migrated from `BTreeMap<Entity, u32> → Vec<u32>` for improved memory locality and faster reconstruction.