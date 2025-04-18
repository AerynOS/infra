use service_core::auth;

include!(concat!(env!("OUT_DIR"), "/proto.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Account(account::Method),
    Endpoint(endpoint::Method),
    Summit(summit::Method),
    Vessel(vessel::Method),
    Avalanche(avalanche::Method),
}

impl Method {
    pub fn from_path(path: &str) -> Option<Self> {
        account::Method::from_path(path)
            .map(Self::Account)
            .or_else(|| endpoint::Method::from_path(path).map(Self::Endpoint))
            .or_else(|| summit::Method::from_path(path).map(Self::Summit))
            .or_else(|| vessel::Method::from_path(path).map(Self::Vessel))
            .or_else(|| avalanche::Method::from_path(path).map(Self::Avalanche))
    }

    pub fn flags(self) -> auth::Flags {
        match self {
            Method::Account(method) => method.flags(),
            Method::Endpoint(method) => method.flags(),
            Method::Summit(method) => method.flags(),
            Method::Vessel(method) => method.flags(),
            Method::Avalanche(method) => method.flags(),
        }
    }
}
