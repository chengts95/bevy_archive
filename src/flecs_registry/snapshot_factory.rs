use bevy_ecs::{component::ComponentId, prelude::*};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};


 
pub mod codec;
use  codec::json::JsonValueCodec;

pub type CompIdFn = fn(&World) -> Option<ComponentId>;
pub type CompRegFn = fn(&mut World) -> ComponentId;

pub fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SnapshotMode {
    #[default]
    Full,
    Placeholder,
    PlaceholderEmplaceIfNotExists,
}

#[derive(Clone, Debug)]
pub struct SnapshotFactory {
    pub js_value: JsonValueCodec,
    #[cfg(feature = "arrow_rs")]
    pub arrow: Option<ArrowSnapshotFactory>,
    pub comp_id: CompIdFn,
    pub register: CompRegFn,
    pub mode: SnapshotMode,
}

#[cfg(feature = "arrow_rs")]
macro_rules! arrow_ext {
    ($text:ty) => {
        $text
    };
}

#[cfg(not(feature = "arrow_rs"))]
macro_rules! arrow_ext {
    ($text:ty) => {
        ()
    };
}

macro_rules! feature_expr {
    ($feature:literal, $expr:expr) => {{
        #[cfg(feature = $feature)]
        {
            $expr
        }
        #[cfg(not(feature = $feature))]
        {
            ()
        }
    }};
}
type SnapshotTuple = (JsonValueCodec, arrow_ext!(Option<ArrowSnapshotFactory>));
impl SnapshotFactory {
    #[inline]
    #[allow(unused_variables)]
    fn from_mode_tuple(
        mode: SnapshotMode,
        comp_id: CompIdFn,
        register: CompRegFn,
        parts: SnapshotTuple,
    ) -> Self {
        let (js_value, arrow) = parts;
        SnapshotFactory {
            js_value,
            #[cfg(feature = "arrow_rs")]
            arrow,
            mode,
            comp_id,
            register,
        }
    }
}

macro_rules! build_common {
    ($t:ty ) => {
        (SnapshotFactory::component_id::<$t>, |world| {
            world.register_component::<$t>()
        })
    };
}

impl SnapshotFactory {
    #[inline]
    fn component_id<T: Component>(world: &World) -> Option<ComponentId> {
        world.component_id::<T>()
    }

    pub fn new_with<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        Self::with_mode::<T>(mode)
    }
    pub fn new<T>() -> Self
    where
        T: Serialize + DeserializeOwned + Component + 'static,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::new::<T>();
        let arrow = feature_expr!("arrow_rs", Some(ArrowSnapshotFactory::new::<T>()));
        SnapshotFactory::from_mode_tuple(SnapshotMode::Full, comp_id, register, (js, arrow))
    }
    pub fn new_with_wrapper_full<T, T1>() -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T> + Into<T>,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);

        let js = JsonValueCodec::new_with_wrapper_full::<T, T1>();
        let arrow = feature_expr!(
            "arrow_rs",
            Some(ArrowSnapshotFactory::new_with_wrapper_full::<T, T1>())
        );
        return SnapshotFactory::from_mode_tuple(
            SnapshotMode::Full,
            comp_id,
            register,
            (js, arrow),
        );
    }
    pub fn new_with_wrapper<T, T1>(mode: SnapshotMode) -> Self
    where
        T: Component,
        T1: Serialize + DeserializeOwned + Default + for<'a> From<&'a T> + Into<T>,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::new_with_wrapper::<T, T1>(mode);
        let arrow = feature_expr!(
            "arrow_rs",
            Some(ArrowSnapshotFactory::new_with_wrapper::<T, T1>(mode))
        );
        return SnapshotFactory::from_mode_tuple(mode, comp_id, register, (js, arrow));
    }

    pub fn with_mode<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + Default + 'static,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::with_mode::<T>(mode);

        let arrow = feature_expr!("arrow_rs", Some(ArrowSnapshotFactory::with_mode::<T>(mode)));
        return SnapshotFactory::from_mode_tuple(mode, comp_id, register, (js, arrow));
    }
}
