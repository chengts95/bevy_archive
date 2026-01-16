use bevy_ecs::prelude::*;
use bevy_ecs::ptr::{Aligned, OwningPtr};
use bevy_ecs::component::ComponentId;
use bumpalo::Bump;
use std::alloc::Layout;
use std::ptr::NonNull;
use crate::prelude::ArenaBox;

// Safety: Must be called with a pointer to the correct type T.
pub type DropFn = unsafe fn(OwningPtr<'_, Aligned>);

#[derive(Clone, Copy, Debug)]
pub struct ArgMeta {
    pub comp_id: ComponentId,
    pub payload_ptr: NonNull<u8>,
    pub drop_fn: Option<DropFn>,
}

#[derive(Clone, Copy, Debug)]
pub enum OpHead {
    ModifyEntity {
        entity: Entity,
        args_ptr: NonNull<ArgMeta>,
        count: u16,
    },
    BatchInsert {
        entities_ptr: NonNull<Entity>,
        payload_ptr: NonNull<u8>,
        count: u32,
        comp_id: ComponentId,
        stride: usize,
        drop_fn: Option<DropFn>,
    },
    RemoveComponents {
        entity: Entity,
        ids_ptr: NonNull<ComponentId>,
        count: u16,
    },
    Despawn(Entity),
}

pub struct HarvardCommandBuffer {
    ops: Vec<OpHead>,
    meta_bump: Bump,
    data_bump: Bump,
    
    // Staging for ModifyEntity Write Combining
    pending_entity: Option<Entity>,
    pending_args: Vec<ArgMeta>,
}

impl Default for HarvardCommandBuffer {
    fn default() -> Self {
        Self {
            ops: Vec::new(),
            meta_bump: Bump::new(),
            data_bump: Bump::new(),
            pending_entity: None,
            pending_args: Vec::new(),
        }
    }
}

impl Drop for HarvardCommandBuffer {
    fn drop(&mut self) {
        // If the buffer is dropped without being applied (or cleared),
        // we must run the destructors for the payloads.
        
        // 1. Pending args
        for arg in &self.pending_args {
            if let Some(drop_fn) = arg.drop_fn {
                // Safety: We still own the data in data_bump (it hasn't been dropped yet)
                // and we haven't applied it to the world.
                let ptr = unsafe { OwningPtr::new(arg.payload_ptr) };
                unsafe { drop_fn(ptr) };
            }
        }
        
        // 2. Ops
        for op in &self.ops {
            match op {
                OpHead::ModifyEntity { args_ptr, count, .. } => {
                    let args = unsafe { std::slice::from_raw_parts(args_ptr.as_ptr(), *count as usize) };
                    for arg in args {
                        if let Some(drop_fn) = arg.drop_fn {
                             let ptr = unsafe { OwningPtr::new(arg.payload_ptr) };
                             unsafe { drop_fn(ptr) };
                        }
                    }
                }
                OpHead::BatchInsert { payload_ptr, count, stride, drop_fn, .. } => {
                    if let Some(drop_fn) = drop_fn {
                        let mut ptr = payload_ptr.as_ptr();
                        for _ in 0..*count {
                            let owning_ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr)) };
                            unsafe { drop_fn(owning_ptr) };
                            ptr = unsafe { ptr.add(*stride) };
                        }
                    }
                }
                // RemoveComponents and Despawn don't hold owned payloads that need dropping.
                _ => {}
            }
        }
    }
}

impl HarvardCommandBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn data_bump(&self) -> &Bump {
        &self.data_bump
    }

    fn flush(&mut self) {
        if let Some(entity) = self.pending_entity.take() {
            if !self.pending_args.is_empty() {
                // Deduplicate pending_args to support "Write Combining" where a later insert overwrites an earlier one.
                // We must drop the payloads of the overwritten components.
                let mut i = 0;
                while i < self.pending_args.len() {
                    let id = self.pending_args[i].comp_id;
                    let mut overwritten = false;
                    // Check if this ID appears later in the list
                    for j in (i + 1)..self.pending_args.len() {
                        if self.pending_args[j].comp_id == id {
                            overwritten = true;
                            break;
                        }
                    }

                    if overwritten {
                        // This arg is overwritten by a later one. Drop its payload.
                        if let Some(drop_fn) = self.pending_args[i].drop_fn {
                            let ptr = unsafe { OwningPtr::new(self.pending_args[i].payload_ptr) };
                            unsafe { drop_fn(ptr) };
                        }
                        // Remove from list. swap_remove is efficient and order doesn't strictly matter for insert_by_ids.
                        self.pending_args.swap_remove(i);
                        // Do not increment i, as we swapped in a new element to check.
                    } else {
                        i += 1;
                    }
                }

                if !self.pending_args.is_empty() {
                    let slice = self.meta_bump.alloc_slice_copy(&self.pending_args);
                    let count = slice.len() as u16;
                    let args_ptr = unsafe { NonNull::new_unchecked(slice.as_mut_ptr()) };
                    self.ops.push(OpHead::ModifyEntity {
                        entity,
                        args_ptr,
                        count,
                    });
                }
                self.pending_args.clear();
            }
        }
    }

    pub fn insert_generic<T: Component>(&mut self, world: &World, entity: Entity, component: T) {
        let comp_id = world.component_id::<T>().unwrap_or_else(|| {
             // This is technically unsafe if we are in a read-only context where register is not allowed?
             // But usually buffer construction assumes we can get IDs.
             // If component is not registered, we can't get ID without mutable world access to register it.
             // `world.component_id` is read-only.
             // If we must register, we need `&mut World`.
             // But this method takes `&World`.
             // So we panic if not registered? Or user must ensure registration.
             // The prompt says "get it done", I will panic or assume it exists.
             // Actually, `world.component_id` returns Option.
             // I'll assume registered for now to keep signature `&World`.
             // To be safe, I should probably take `&mut World`? 
             // Bevy Commands use &mut World in apply.
             // Here we are recording.
             panic!("Component {} not registered", std::any::type_name::<T>())
        });
        
        let ptr = self.data_bump.alloc(component) as *mut T;
        // Safety: ptr is from bump, aligned.
        let payload_ptr = unsafe { NonNull::new_unchecked(ptr as *mut u8) };
        let drop_fn: DropFn = |ptr| unsafe { ptr.drop_as::<T>() };
        
        self.insert_raw(entity, comp_id, payload_ptr, Some(drop_fn));
    }

    // Renamed existing insert to insert_raw (or keeping as insert but with different sig? No, I'll keep insert taking ArenaBox for compatibility)
    // Actually, I can keep `insert` taking `ArenaBox` for compatibility and add `insert_generic`?
    // User asked for `insert<T>`.
    
    pub fn insert<T: Component>(&mut self, world: &World, entity: Entity, component: T) {
        let comp_id = world.component_id::<T>().expect("Component not registered");
        let ptr = self.data_bump.alloc(component) as *mut T;
        let payload_ptr = unsafe { NonNull::new_unchecked(ptr as *mut u8) };
        let drop_fn: DropFn = |ptr| unsafe { ptr.drop_as::<T>() };
        
        self.insert_raw(entity, comp_id, payload_ptr, Some(drop_fn));
    }

    pub fn remove<T: Component>(&mut self, world: &World, entity: Entity) {
        let comp_id = world.component_id::<T>().expect("Component not registered");
        self.remove_raw(entity, &[comp_id]);
    }

    pub fn insert_batch<T: Component, I>(&mut self, world: &World, entities: &[Entity], components: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        self.flush(); // Must flush pending single inserts first.

        let comp_id = world.component_id::<T>().expect("Component not registered");
        let components_iter = components.into_iter();
        
        // We need contiguous memory.
        // Bumpalo `alloc_slice_fill_iter` is what we want!
        let slice = self.data_bump.alloc_slice_fill_iter(components_iter);
        let count = slice.len();
        
        if count != entities.len() {
            panic!("Batch insert mismatch: {} entities vs {} components", entities.len(), count);
        }
        
        if count == 0 {
            return;
        }

        let payload_ptr = unsafe { NonNull::new_unchecked(slice.as_mut_ptr() as *mut u8) };
        let drop_fn: DropFn = |ptr| unsafe { ptr.drop_as::<T>() };
        
        // Copy entities to meta_bump
        let entities_slice = self.meta_bump.alloc_slice_copy(entities);
        let entities_ptr = unsafe { NonNull::new_unchecked(entities_slice.as_mut_ptr()) };

        self.ops.push(OpHead::BatchInsert {
            entities_ptr,
            payload_ptr,
            count: count as u32,
            comp_id,
            stride: std::mem::size_of::<T>(),
            drop_fn: Some(drop_fn),
        });
    }

    // Original insert with ArenaBox support (renamed or kept?)
    // I will rename `insert` to `insert_box` or overload? Rust doesn't support overload.
    // I will rename the old `insert` to `insert_raw_box` and update callers in next step?
    // Or I can keep `insert` as is and name generic one `add_component`?
    // User asked for `insert<T>`.
    // I'll rename the old `insert` to `insert_box`.
    
    pub fn insert_box(&mut self, entity: Entity, comp_id: ComponentId, payload: ArenaBox<'_>) {
        let ptr = payload.ptr;
        let drop_fn = unsafe { std::mem::transmute::<_, DropFn>(payload.drop_fn) };
        let payload_ptr = NonNull::new(ptr.as_ptr() as *mut u8).expect("ArenaBox ptr is null");
        self.insert_raw(entity, comp_id, payload_ptr, Some(drop_fn));
    }

    // Helper for low-level insert
    fn insert_raw(&mut self, entity: Entity, comp_id: ComponentId, payload_ptr: NonNull<u8>, drop_fn: Option<DropFn>) {
        if self.pending_entity != Some(entity) {
            self.flush();
            self.pending_entity = Some(entity);
        }
        self.pending_args.push(ArgMeta {
            comp_id,
            payload_ptr,
            drop_fn,
        });
    }

    // Renamed remove to remove_raw
    pub fn remove_raw(&mut self, entity: Entity, components: &[ComponentId]) {
        self.flush();
        if components.is_empty() {
            return;
        }
        let slice = self.meta_bump.alloc_slice_copy(components);
        let ids_ptr = unsafe { NonNull::new_unchecked(slice.as_mut_ptr()) };
        self.ops.push(OpHead::RemoveComponents {
            entity,
            ids_ptr,
            count: slice.len() as u16,
        });
    }

    pub fn despawn(&mut self, entity: Entity) {
        self.flush();
        self.ops.push(OpHead::Despawn(entity));
    }

    pub fn apply(&mut self, world: &mut World) {
        self.flush();

        for op in &self.ops {
            match op {
                OpHead::ModifyEntity { entity, args_ptr, count } => {
                    let args = unsafe { std::slice::from_raw_parts(args_ptr.as_ptr(), *count as usize) };
                    
                    let ids: Vec<ComponentId> = args.iter().map(|a| a.comp_id).collect();
                    let ptrs = args.iter().map(|a| unsafe { OwningPtr::new(a.payload_ptr) });
                    
                    if let Ok(mut entity_mut) = world.get_entity_mut(*entity) {
                        unsafe { entity_mut.insert_by_ids(&ids, ptrs) };
                    }
                }
                OpHead::BatchInsert { entities_ptr, payload_ptr, count, comp_id, stride, .. } => {
                    let entities = unsafe { std::slice::from_raw_parts(entities_ptr.as_ptr(), *count as usize) };
                    // We need to iterate entities and payload simultaneously.
                    // world.insert_batch expects IntoIterator<Item=(Entity, Bundle)>.
                    // Here Bundle is a single component (OwningPtr).
                    // But insert_batch takes `Bundle`, not `OwningPtr`.
                    // Does `OwningPtr` implement `Bundle`? No.
                    // We need `unsafe { world.insert_batch_by_id(comp_id, iterator) }`?
                    // Bevy doesn't have `insert_batch_by_id`.
                    // It has `insert_batch` which takes `Bundle`.
                    // But we have raw data.
                    // We can use `world.resource_scope` or similar hacks?
                    // Actually, for batch insertion of dynamic components, Bevy usually requires `InsertBatch` command or similar.
                    // If we can't use `insert_batch` with raw pointers easily, we loop.
                    // BUT, `BatchInsert` op was supposed to be optimized.
                    // If we loop here, we save on `OpHead` overhead but still pay `get_entity_mut` cost?
                    // No, `world.insert_batch` optimizes archetype moves.
                    
                    // How to do `insert_batch` with raw pointers?
                    // We might need to rely on the fact we know T.
                    // But `apply` is not generic over T.
                    // So we are stuck with type-erased data.
                    // Bevy's `insert_batch` relies on `I::Item` to know the type.
                    
                    // Workaround: We loop. 
                    // `world.entity_mut(e).insert_by_id(id, ptr)`.
                    // This is not the "Nuclear Weapon" batching I promised, but it's what we can do without modifying Bevy internals
                    // or using specialized unsafe Bevy APIs that might not exist publicly.
                    // Wait, `insert_batch` IS generic.
                    // To use it, we need T.
                    // But `apply` doesn't know T.
                    // So we MUST loop.
                    
                    let mut ptr = payload_ptr.as_ptr();
                    for &entity in entities {
                        let owning_ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr)) };
                        if let Ok(mut entity_mut) = world.get_entity_mut(entity) {
                            unsafe { entity_mut.insert_by_id(*comp_id, owning_ptr) };
                        }
                        ptr = unsafe { ptr.add(*stride) };
                    }
                }
                OpHead::RemoveComponents { entity, ids_ptr, count } => {
                    let ids = unsafe { std::slice::from_raw_parts(ids_ptr.as_ptr(), *count as usize) };
                    if let Ok(mut entity_mut) = world.get_entity_mut(*entity) {
                        entity_mut.remove_by_ids(ids);
                    }
                }
                OpHead::Despawn(entity) => {
                     world.despawn(*entity);
                }
            }
        }

        self.ops.clear();
        self.pending_args.clear();
        self.pending_entity = None;
    }

    pub fn reset(&mut self) {
        // Drop unapplied data
        for arg in &self.pending_args {
            if let Some(drop_fn) = arg.drop_fn {
                let ptr = unsafe { OwningPtr::new(arg.payload_ptr) };
                unsafe { drop_fn(ptr) };
            }
        }
        
        for op in &self.ops {
            match op {
                OpHead::ModifyEntity { args_ptr, count, .. } => {
                    let args = unsafe { std::slice::from_raw_parts(args_ptr.as_ptr(), *count as usize) };
                    for arg in args {
                        if let Some(drop_fn) = arg.drop_fn {
                             let ptr = unsafe { OwningPtr::new(arg.payload_ptr) };
                             unsafe { drop_fn(ptr) };
                        }
                    }
                }
                OpHead::BatchInsert { payload_ptr, count, stride, drop_fn, .. } => {
                    if let Some(drop_fn) = drop_fn {
                        let mut ptr = payload_ptr.as_ptr();
                        for _ in 0..*count {
                            let owning_ptr = unsafe { OwningPtr::new(NonNull::new_unchecked(ptr)) };
                            unsafe { drop_fn(owning_ptr) };
                            ptr = unsafe { ptr.add(*stride) };
                        }
                    }
                }
                _ => {}
            }
        }

        self.ops.clear();
        self.pending_args.clear();
        self.pending_entity = None;
        self.meta_bump.reset();
        self.data_bump.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Component)]
    struct A(u32);

    #[derive(Component)]
    struct B(String);

    #[test]
    fn test_reset_and_reuse() {
        let mut world = World::new();
        let e1 = world.spawn_empty().id();
        let comp_id_a = world.register_component::<A>();
        
        let mut buffer = HarvardCommandBuffer::new();
        
        // Cycle 1
        let a_val = A(10);
        let ptr = buffer.data_bump.alloc(a_val) as *mut A;
        let abox = unsafe { ArenaBox::new::<A>(OwningPtr::new(NonNull::new(ptr as *mut u8).unwrap())) };
        buffer.insert_box(e1, comp_id_a, abox);
        
        buffer.apply(&mut world);
        buffer.reset(); // Should be safe and reuse memory

        // Cycle 2
        let a_val = A(20);
        let ptr = buffer.data_bump.alloc(a_val) as *mut A;
        let abox = unsafe { ArenaBox::new::<A>(OwningPtr::new(NonNull::new(ptr as *mut u8).unwrap())) };
        buffer.insert_box(e1, comp_id_a, abox);
        
        buffer.apply(&mut world);
        
        let a = world.entity(e1).get::<A>().unwrap();
        assert_eq!(a.0, 20);
    }

    #[test]
    fn test_generic_apis() {
        let mut world = World::new();
        world.register_component::<A>();
        world.register_component::<B>();
        
        let e1 = world.spawn_empty().id();
        let e2 = world.spawn_empty().id();
        
        let mut buffer = HarvardCommandBuffer::new();
        
        // Test insert<T>
        buffer.insert(&world, e1, A(100));
        buffer.insert(&world, e1, B("Generic".to_string()));
        
        // Test insert_batch<T>
        let entities = vec![e1, e2];
        let components = vec![A(200), A(300)]; // e1 gets 200 (overwrite), e2 gets 300
        buffer.insert_batch(&world, &entities, components);
        
        buffer.apply(&mut world);
        
        let a1 = world.entity(e1).get::<A>().unwrap();
        assert_eq!(a1.0, 200); // Batch insert should happen after/overwrite
        
        let b1 = world.entity(e1).get::<B>().unwrap();
        assert_eq!(b1.0, "Generic");
        
        let a2 = world.entity(e2).get::<A>().unwrap();
        assert_eq!(a2.0, 300);
        
        // Test remove<T>
        buffer.reset();
        buffer.remove::<A>(&world, e1);
        buffer.apply(&mut world);
        
        assert!(world.entity(e1).get::<A>().is_none());
        assert!(world.entity(e1).get::<B>().is_some());
    }

    #[test]
    fn test_drop_safety() {
        use std::sync::atomic::{AtomicU32, Ordering};

        static DROP_COUNT: AtomicU32 = AtomicU32::new(0);

        #[derive(Component)]
        struct Droppable {
            _data: u32,
        }
        impl Drop for Droppable {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut world = World::new();
        let comp_id = world.register_component::<Droppable>();
        let e1 = world.spawn_empty().id();

        {
            let mut buffer = HarvardCommandBuffer::new();
            
            let val = Droppable { _data: 1 };
            let ptr = buffer.data_bump.alloc(val) as *mut Droppable;
            let abox = unsafe { ArenaBox::new::<Droppable>(OwningPtr::new(NonNull::new(ptr as *mut u8).unwrap())) };
            
            buffer.insert_box(e1, comp_id, abox);
            
            // buffer dropped here without apply
        }

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }
}