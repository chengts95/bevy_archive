use bevy_ecs::prelude::*;
use bevy_ecs::ptr::{Aligned, OwningPtr};
use bevy_ecs::component::ComponentId;
use bumpalo::Bump;
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
            if let OpHead::ModifyEntity { args_ptr, count, .. } = op {
                let args = unsafe { std::slice::from_raw_parts(args_ptr.as_ptr(), *count as usize) };
                for arg in args {
                    if let Some(drop_fn) = arg.drop_fn {
                         let ptr = unsafe { OwningPtr::new(arg.payload_ptr) };
                         unsafe { drop_fn(ptr) };
                    }
                }
            }
            // RemoveComponents and Despawn don't hold owned payloads that need dropping.
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

    pub fn insert(&mut self, entity: Entity, comp_id: ComponentId, payload: ArenaBox<'_>) {
        if self.pending_entity != Some(entity) {
            self.flush();
            self.pending_entity = Some(entity);
        }

        let ptr = payload.ptr;
        // Cast drop_fn to the HRTB signature we need. 
        // Safety: The drop function logic (ptr.drop_as::<T>()) is valid for any lifetime of ptr.
        let drop_fn = unsafe { std::mem::transmute::<_, DropFn>(payload.drop_fn) };
        
        // ArenaBox does not implement Drop, so we don't need to forget it.
        // We have extracted the ptr and drop_fn and taken responsibility for cleanup.

        let payload_ptr = NonNull::new(ptr.as_ptr() as *mut u8).expect("ArenaBox ptr is null");
        
        self.pending_args.push(ArgMeta {
            comp_id,
            payload_ptr,
            drop_fn: Some(drop_fn),
        });
    }

    pub fn remove(&mut self, entity: Entity, components: &[ComponentId]) {
        self.flush();
        if components.is_empty() {
            return;
        }
        let slice = self.meta_bump.alloc_slice_copy(components);
        let count = slice.len() as u16;
        let ids_ptr = unsafe { NonNull::new_unchecked(slice.as_mut_ptr()) };
        self.ops.push(OpHead::RemoveComponents {
            entity,
            ids_ptr,
            count,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Component)]
    struct A(u32);

    #[derive(Component)]
    struct B(String);

    #[test]
    fn test_harvard_buffer() {
        let mut world = World::new();
        let e1 = world.spawn_empty().id();
        let e2 = world.spawn_empty().id();
        
        let mut buffer = HarvardCommandBuffer::new();
        
        let comp_id_a = world.register_component::<A>();
        let comp_id_b = world.register_component::<B>();

        // Create ArenaBox for A(10) on e1
        let a_val = A(10);
        let ptr = buffer.data_bump.alloc(a_val) as *mut A;
        let abox = unsafe { ArenaBox::new::<A>(OwningPtr::new(NonNull::new(ptr as *mut u8).unwrap())) };
        
        buffer.insert(e1, comp_id_a, abox);

        // Create ArenaBox for B("hello") on e1 (should combine)
        let b_val = B("hello".to_string());
        let ptr = buffer.data_bump.alloc(b_val) as *mut B;
        let abox = unsafe { ArenaBox::new::<B>(OwningPtr::new(NonNull::new(ptr as *mut u8).unwrap())) };
        
        buffer.insert(e1, comp_id_b, abox);

        // Create ArenaBox for A(20) on e2
        let a_val2 = A(20);
        let ptr = buffer.data_bump.alloc(a_val2) as *mut A;
        let abox = unsafe { ArenaBox::new::<A>(OwningPtr::new(NonNull::new(ptr as *mut u8).unwrap())) };
        
        buffer.insert(e2, comp_id_a, abox);

        // Check internal state (staging)
        assert_eq!(buffer.pending_entity, Some(e2));
        assert_eq!(buffer.pending_args.len(), 1);
        assert_eq!(buffer.ops.len(), 1); // e1 op is flushed

        // Apply
        buffer.apply(&mut world);

        // Verify world
        let a1 = world.entity(e1).get::<A>();
        assert!(a1.is_some());
        assert_eq!(a1.unwrap().0, 10);
        
        let b1 = world.entity(e1).get::<B>();
        assert!(b1.is_some());
        assert_eq!(b1.unwrap().0, "hello");

        let a2 = world.entity(e2).get::<A>();
        assert!(a2.is_some());
        assert_eq!(a2.unwrap().0, 20);

        // Verify buffer is clear
        assert!(buffer.ops.is_empty());
        assert!(buffer.pending_args.is_empty());
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
            
            buffer.insert(e1, comp_id, abox);
            
            // buffer dropped here without apply
        }

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }
}
