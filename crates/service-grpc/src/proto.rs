#![allow(unused_qualifications)]

include!(concat!(env!("OUT_DIR"), "/proto.rs"));

macro_rules! map_enum {
    ($core:path,$proto:path,[$($variant:ident,)*]) => {
        impl From<$core> for $proto {
            fn from(core: $core) -> Self {
                match core {
                    $(
                        <$core>::$variant => <$proto>::$variant,
                    )*
                }
            }
        }

        impl From<&$core> for $proto {
            fn from(core: &$core) -> Self {
                match core {
                    $(
                        <$core>::$variant => <$proto>::$variant,
                    )*
                }
            }
        }

        impl $proto {
            pub fn to_core(self) -> Option<$core> {
                match self {
                    $(
                        <$proto>::$variant => Some(<$core>::$variant),
                    )*
                    _ => None,
                }
            }
        }
    };
}

map_enum!(service_core::Service, common::Service, [Summit, Avalanche, Vessel,]);
