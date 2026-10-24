//! One classifier drives retry and credential policy (audit gate G08).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    CredentialAuth,
    RateLimited,
    ProviderTransient,
    TransportFailure,
    CallerRejected,
}

pub fn classify(status: Option<u16>) -> Outcome {
    match status {
        Some(200..=299) => Outcome::Success,
        Some(401 | 403) => Outcome::CredentialAuth,
        Some(429) => Outcome::RateLimited,
        Some(500..=599) => Outcome::ProviderTransient,
        None => Outcome::TransportFailure,
        Some(_) => Outcome::CallerRejected,
    }
}

impl Outcome {
    pub fn quarantine_credential(self) -> bool {
        matches!(self, Self::CredentialAuth | Self::RateLimited)
    }

    pub fn blame_provider(self) -> bool {
        matches!(self, Self::ProviderTransient)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classify_once_for_all_consumers() {
        let cases = [
            (None, Outcome::TransportFailure, false, false),
            (Some(200), Outcome::Success, false, false),
            (Some(400), Outcome::CallerRejected, false, false),
            (Some(401), Outcome::CredentialAuth, true, false),
            (Some(403), Outcome::CredentialAuth, true, false),
            (Some(404), Outcome::CallerRejected, false, false),
            (Some(422), Outcome::CallerRejected, false, false),
            (Some(429), Outcome::RateLimited, true, false),
            (Some(500), Outcome::ProviderTransient, false, true),
            (Some(502), Outcome::ProviderTransient, false, true),
        ];
        for (status, expected, key, model) in cases {
            let o = classify(status);
            assert_eq!(o, expected, "{status:?}");
            assert_eq!(o.quarantine_credential(), key, "{status:?}");
            assert_eq!(o.blame_provider(), model, "{status:?}");
        }
    }
}
