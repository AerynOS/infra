use service_core::auth;

#[allow(unused_qualifications)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

pub use proto::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Account(account::Method),
    Endpoint(endpoint::Method),
    Summit(summit::Method),
    Vessel(vessel::Method),
}

impl Method {
    pub fn from_path(path: &str) -> Option<Self> {
        account::Method::from_path(path)
            .map(Self::Account)
            .or_else(|| endpoint::Method::from_path(path).map(Self::Endpoint))
            .or_else(|| summit::Method::from_path(path).map(Self::Summit))
            .or_else(|| vessel::Method::from_path(path).map(Self::Vessel))
    }

    pub fn flags(self) -> auth::Flags {
        match self {
            Method::Account(method) => method.flags(),
            Method::Endpoint(method) => method.flags(),
            Method::Summit(method) => method.flags(),
            Method::Vessel(method) => method.flags(),
        }
    }

    pub fn permission(self) -> Option<auth::Permission> {
        match self {
            Method::Account(method) => method.permission(),
            Method::Endpoint(method) => method.permission(),
            Method::Summit(method) => method.permission(),
            Method::Vessel(method) => method.permission(),
        }
    }
}
