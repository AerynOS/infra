use serde::{Deserialize, Serialize};

/// Services defined by this infrastructure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display, strum::IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Service {
    /// Hub & coordinator service
    Summit,
    /// Builder service
    Avalanche,
    /// Respository manager service
    Vessel,
}

impl Service {
    /// Service name
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
