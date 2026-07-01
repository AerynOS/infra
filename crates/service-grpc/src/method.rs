pub use crate::proto::auth::Method as AuthMethod;
pub use crate::proto::summit::Method as SummitMethod;
pub use crate::proto::vessel::Method as VesselMethod;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Auth(AuthMethod),
    Summit(SummitMethod),
    Vessel(VesselMethod),
}

impl Method {
    pub fn from_path(path: &str) -> Option<Self> {
        AuthMethod::from_path(path)
            .map(Self::Auth)
            .or_else(|| SummitMethod::from_path(path).map(Self::Summit))
            .or_else(|| VesselMethod::from_path(path).map(Self::Vessel))
    }

    pub fn flags(self) -> service_core::auth::Flags {
        match self {
            Method::Auth(method) => method.flags(),
            Method::Summit(method) => method.flags(),
            Method::Vessel(method) => method.flags(),
        }
    }

    pub fn permission(self) -> Option<service_core::auth::Permission> {
        match self {
            Method::Auth(method) => method.permission(),
            Method::Summit(method) => method.permission(),
            Method::Vessel(method) => method.permission(),
        }
    }
}
