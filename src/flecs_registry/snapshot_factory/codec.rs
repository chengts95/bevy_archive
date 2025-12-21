pub mod json;

#[cfg(feature = "arrow_rs")]
pub mod arrow;
use flecs_ecs::{prelude::*, sys};
use std::ptr::NonNull;
// 🌟 trait 封装：统一访问接口
pub trait ComponentAccess {
    unsafe fn get_data_ptr<T: ComponentId>(&self, entity: Entity) -> Option<*const T>;
    unsafe fn get_data_ptr_mut<T: ComponentId>(&self, entity: Entity) -> Option<*mut T>;

    fn get_data_ref<T: ComponentId>(&self, entity: Entity) -> Option<&T> {
        unsafe { self.get_data_ptr::<T>(entity).and_then(|x| Some(&*x)) }
    }

    fn get_data_mut<T: ComponentId>(&self, entity: Entity) -> Option<&mut T> {
        unsafe {
            self.get_data_ptr_mut::<T>(entity)
                .and_then(|x| Some(&mut *x))
        }
    }
}

impl ComponentAccess for World {
    unsafe fn get_data_ptr<T: ComponentId>(&self, entity: Entity) -> Option<*const T> {
        let ptr = unsafe { sys::ecs_get_id(self.world_ptr(), *entity, T::get_id(self)) };
        NonNull::new(ptr as *mut T).map(|nn| nn.as_ptr() as *const T)
    }

    unsafe fn get_data_ptr_mut<T: ComponentId>(&self, entity: Entity) -> Option<*mut T> {
        let ptr = unsafe { sys::ecs_get_mut_id(self.world_ptr(), *entity, T::get_id(self)) };
        NonNull::new(ptr as *mut T).map(|nn| nn.as_ptr())
    }
}
