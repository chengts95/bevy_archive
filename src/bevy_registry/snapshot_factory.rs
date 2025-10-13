use bevy_ecs::{component::ComponentId, prelude::*};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::prelude::codec::JsonValueCodec;
#[cfg(feature = "arrow_rs")]
use crate::prelude::vec_snapshot_factory::ArrowSnapshotFactory;
pub mod codec;

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
    EmplaceIfNotExists,
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

    pub fn new<T>(mode: SnapshotMode) -> Self
    where
        T: Serialize + DeserializeOwned + Component + 'static,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);
        let js = JsonValueCodec::new::<T>();
        let arrow = feature_expr!("arrow_rs", Some(ArrowSnapshotFactory::new::<T>()));
        SnapshotFactory::from_mode_tuple(mode, comp_id, register, (js, arrow))
    }
    pub fn new_with_wrapper<T, T1>(mode: SnapshotMode) -> Self
    where
        T: Component + From<T1>,
        T1: Serialize + DeserializeOwned + for<'a> From<&'a T>,
    {
        let (comp_id, register): (CompIdFn, CompRegFn) = build_common!(T);

        let js = JsonValueCodec::new_with::<T, T1>();
        let arrow = feature_expr!("arrow_rs", Some(ArrowSnapshotFactory::new_with::<T, T1>()));
        return SnapshotFactory::from_mode_tuple(mode, comp_id, register, (js, arrow));
    }
}
