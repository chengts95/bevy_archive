# Changelog
## [0.2.0] - 2024-10-10
- Initial support for Arrow, parquet and binary format.
- Remove placeholder snapshot mode and remove snapshot mode from factories.
## [0.1.4] - 2024-05-14
- Initial support for singleton/resource.
- Initial support for merging snapshot.
- Initial support for binary (msgpack) data format.
- Initial support for export options.
  
## [0.1.3] - 2024-05-14

### Fix
- Fix `DeferredEntityBuilder::insert_if_new_by_id` to properly insert the components.

## [0.1.2] - 2024-05-14

### Added
- `DeferredEntityBuilder` and `load_world_arch_snapshot_defragment` now support `insert_if_new`.
- `load_world_arch_snapshot_defragment` now can ignore unknown types in the file.
### Changed
-   Refactored `bevy_registry` to support more operations.


## [0.1.1] - 2024-05-14

### Added
- Integration with bump allocator for temporary component memory.
- `DeferredEntityBuilder` for runtime batch insertion of components.
- Support for insert_by_id(ComponentId, OwningPtr).
- `load_world_arch_snapshot_defragment` to avoid archetype fragments in bevy ECS, **this only happens with bevy**.
- Experimental flecs support: cross-ECS serialization example added, enabling data transfer between bevy and flecs-based runtimes.

- Extended cross-ECS example between bevy and flecs.
### Changed
-   ArchetypeSnapshot entity index storage: migrated from `BTreeMap<Entity, u32> → Vec<u32>` for improved memory locality and faster reconstruction.