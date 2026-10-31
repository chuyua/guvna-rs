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
        // Redirects are deliberately never followed by the upstream client
        // (Bearer credentials must not reach a Location target). A redirect
        // therefore cannot be a successful model answer or a caller error:
        // treat a broken provider endpoint as transient for circuit recovery.
        Some(300..=399) => Outcome::ProviderTransient,
        // Upstream 408 means the *provider* timed out handling this HTTP
        // request. Treat it like a transient 5xx, not a bad client prompt:
        // fail over, feed the circuit breaker and avoid repeated slow calls.
        Some(408) => Outcome::ProviderTransient,
        Some(500..=599) => Outcome::ProviderTransient,
        // 1xx is not a final model response; nonstandard 6xx+ errors are
        // also not evidence of a malformed caller request. In both cases
        // fail over and let the existing provider circuit recover.
        Some(400..=499) => Outcome::CallerRejected,
        None => Outcome::TransportFailure,
        Some(_) => Outcome::ProviderTransient,
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
            (Some(103), Outcome::ProviderTransient, false, true),
            (Some(301), Outcome::ProviderTransient, false, true),
            (Some(302), Outcome::ProviderTransient, false, true),
            (Some(307), Outcome::ProviderTransient, false, true),
            (Some(308), Outcome::ProviderTransient, false, true),
            (Some(400), Outcome::CallerRejected, false, false),
            (Some(401), Outcome::CredentialAuth, true, false),
            (Some(403), Outcome::CredentialAuth, true, false),
            (Some(404), Outcome::CallerRejected, false, false),
            (Some(408), Outcome::ProviderTransient, false, true),
            (Some(409), Outcome::CallerRejected, false, false),
            (Some(422), Outcome::CallerRejected, false, false),
            (Some(429), Outcome::RateLimited, true, false),
            (Some(500), Outcome::ProviderTransient, false, true),
            (Some(502), Outcome::ProviderTransient, false, true),
            (Some(600), Outcome::ProviderTransient, false, true),
            (Some(999), Outcome::ProviderTransient, false, true),
        ];
        for (status, expected, key, model) in cases {
            let o = classify(status);
            assert_eq!(o, expected, "{status:?}");
            assert_eq!(o.quarantine_credential(), key, "{status:?}");
            assert_eq!(o.blame_provider(), model, "{status:?}");
        }
    }
}
