//! Generic models.dev-style pricing fallback.
//!
//! Serves as the last resort in the pricing pipeline: when neither the local
//! catalog nor the GitHub Copilot pricing page has a rate for a model, this
//! module supplies indicative list prices for well-known models.
//!
//! The catalog is data-oriented: one immutable `const` holding three parallel
//! `&'static` slices (structure-of-arrays).  Lookup is a bounded linear scan —
//! no allocation, no hashing, deterministic, ASCII case-insensitive, and
//! tolerant of effort suffixes (see [`EFFORT_SUFFIXES`]).
//! [`ModelsDevFallback`] implements the system's pricing-provider trait
//! [`PricingSource`](crate::pricing::PricingSource), so the fallback plugs
//! into [`load_pricing`](crate::pricing::load_pricing) as one more provider
//! in the chain.
//!
//! Cache rates are unknown for generic list prices, so the fallback maps a
//! catalog hit to `ModelRates` with input/output set and both cache fields
//! `None`.

use crate::data::ModelRates;
use crate::pricing::PricingSource;

// ---------------------------------------------------------------------------
// PriceInfo — plain-data value
// ---------------------------------------------------------------------------

/// Per-1M-token model pricing, USD.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PriceInfo {
    /// USD per one million input tokens.
    pub(crate) input_per_mtok: f64,
    /// USD per one million output tokens.
    pub(crate) output_per_mtok: f64,
}

// ---------------------------------------------------------------------------
// Effort suffixes — model-id variants share their base price
// ---------------------------------------------------------------------------

/// Effort-variant suffixes stripped before catalog lookup.
///
/// OpenCode model ids carry these suffixes (e.g. `gpt-4o-high`); the base
/// id's list price is the best generic approximation.
pub(crate) const EFFORT_SUFFIXES: [&str; 5] = ["-xhigh", "-high", "-medium", "-low", "-none"];

// ---------------------------------------------------------------------------
// ModelCatalog — dense SoA static lookup
// ---------------------------------------------------------------------------

/// Dense, static, SoA model pricing table.
///
/// Three parallel `&'static` slices; every slice has the same length as the
/// id slice, checked at compile time by a const assertion inside the
/// constructor.  Constructed only as immutable `const` data (see
/// [`GENERIC_CATALOG`]) — there is no public construction path.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ModelCatalog {
    ids: &'static [&'static str],
    input_per_mtok: &'static [f64],
    output_per_mtok: &'static [f64],
}

impl ModelCatalog {
    const fn new(
        ids: &'static [&'static str],
        input_per_mtok: &'static [f64],
        output_per_mtok: &'static [f64],
    ) -> Self {
        assert!(
            ids.len() == input_per_mtok.len() && ids.len() == output_per_mtok.len(),
            "catalog arrays must be parallel"
        );

        Self {
            ids,
            input_per_mtok,
            output_per_mtok,
        }
    }

    /// Look up pricing for `model` (ASCII case-insensitive).
    ///
    /// Returns `None` when the catalog has no entry.  The scan is a bounded
    /// linear pass over the static arrays: no allocation, deterministic.
    /// An exact id match always wins; an effort suffix is stripped only when
    /// the full id misses, so a catalog entry named `gpt-4o-low` stays
    /// reachable even though `-low` is a suffix.
    pub(crate) fn pricing(&self, model: &str) -> Option<PriceInfo> {
        let index = self.index_of(model).or_else(|| {
            EFFORT_SUFFIXES
                .iter()
                .find_map(|suffix| model.strip_suffix(suffix))
                .and_then(|base| self.index_of(base))
        })?;

        let input = *self.input_per_mtok.get(index)?;
        let output = *self.output_per_mtok.get(index)?;

        Some(PriceInfo {
            input_per_mtok: input,
            output_per_mtok: output,
        })
    }

    /// Index of the first id equal to `model`, case-insensitively.
    fn index_of(&self, model: &str) -> Option<usize> {
        self.ids
            .iter()
            .position(|id| id.eq_ignore_ascii_case(model))
    }
}

// ---------------------------------------------------------------------------
// Generic catalog — static fallback data
// ---------------------------------------------------------------------------

/// Generic models.dev-style fallback catalog.
///
/// Indicative public list prices (USD per 1M tokens) for well-known models,
/// used only when no authoritative API pricing is available.  Not a
/// replacement for provider documentation.
pub(crate) const GENERIC_CATALOG: ModelCatalog = ModelCatalog::new(
    &[
        "gpt-4o",
        "gpt-4o-mini",
        "gpt-4-turbo",
        "gpt-4.1",
        "claude-3-5-sonnet",
        "claude-3-5-haiku",
        "claude-3-opus",
        "gemini-1.5-pro",
        "gemini-1.5-flash",
        "llama-3.1-405b-instruct",
        "llama-3.1-70b-instruct",
        "mistral-large-latest",
        "mistral-small-latest",
        "text-embedding-3-small",
        "text-embedding-3-large",
    ],
    &[
        2.50, 0.15, 10.00, 2.00, 3.00, 0.80, 15.00, 1.25, 0.075, 3.00, 0.70, 2.00, 0.20, 0.02, 0.13,
    ],
    &[
        10.00, 0.60, 30.00, 8.00, 15.00, 4.00, 75.00, 5.00, 0.30, 3.00, 0.70, 6.00, 0.60, 0.02,
        0.13,
    ],
);

// ---------------------------------------------------------------------------
// ModelsDevFallback — zero-state fallback provider
// ---------------------------------------------------------------------------

/// Fallback pricing provider serving the generic [`GENERIC_CATALOG`].
///
/// Provider-agnostic: lookup is by model id only, so the provider argument
/// is ignored.  Zero-field unit struct with no state.
pub(crate) struct ModelsDevFallback;

impl PricingSource for ModelsDevFallback {
    fn rates(&self, _provider: &str, model: &str) -> Option<ModelRates> {
        GENERIC_CATALOG.pricing(model).map(|info| ModelRates {
            input: Some(info.input_per_mtok),
            cache_read: None,
            cache_write: None,
            output: Some(info.output_per_mtok),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_arrays_are_parallel() {
        let catalog = GENERIC_CATALOG;

        assert_eq!(catalog.ids.len(), catalog.input_per_mtok.len());
        assert_eq!(catalog.ids.len(), catalog.output_per_mtok.len());
    }

    #[test]
    fn catalog_ids_are_unique() {
        let ids = GENERIC_CATALOG.ids;

        let mut lowered: Vec<String> = ids.iter().map(|id| id.to_ascii_lowercase()).collect();
        lowered.sort();
        lowered.dedup();

        assert_eq!(lowered.len(), ids.len(), "duplicate catalog ids present");
    }

    #[test]
    fn catalog_lookup_hits_known_models() {
        let gpt4o = GENERIC_CATALOG.pricing("gpt-4o");

        assert_eq!(
            gpt4o,
            Some(PriceInfo {
                input_per_mtok: 2.50,
                output_per_mtok: 10.00,
            })
        );

        let embedding = GENERIC_CATALOG.pricing("text-embedding-3-small");

        assert_eq!(
            embedding,
            Some(PriceInfo {
                input_per_mtok: 0.02,
                output_per_mtok: 0.02,
            })
        );
    }

    #[test]
    fn catalog_lookup_is_case_insensitive() {
        let upper = GENERIC_CATALOG.pricing("GPT-4O");
        let mixed = GENERIC_CATALOG.pricing("Claude-3-5-Sonnet");

        assert_eq!(upper, GENERIC_CATALOG.pricing("gpt-4o"));
        assert_eq!(mixed, GENERIC_CATALOG.pricing("claude-3-5-sonnet"));
    }

    #[test]
    fn catalog_lookup_strips_effort_suffixes() {
        let base = GENERIC_CATALOG.pricing("gpt-4o");
        let suffixed = GENERIC_CATALOG.pricing("gpt-4o-high");

        assert_eq!(base, suffixed);
    }

    #[test]
    fn catalog_prefers_exact_match_over_suffix_strip() {
        let catalog = ModelCatalog::new(&["gpt-4o", "gpt-4o-low"], &[1.0, 2.0], &[3.0, 4.0]);

        assert_eq!(
            catalog.pricing("gpt-4o-low"),
            Some(PriceInfo {
                input_per_mtok: 2.0,
                output_per_mtok: 4.0,
            })
        );
        assert_eq!(
            catalog.pricing("gpt-4o"),
            Some(PriceInfo {
                input_per_mtok: 1.0,
                output_per_mtok: 3.0,
            })
        );
    }

    #[test]
    fn catalog_lookup_misses_unknown_model() {
        assert_eq!(GENERIC_CATALOG.pricing("nonexistent-model-9000"), None);
    }

    #[test]
    fn catalog_is_not_empty() {
        assert!(!GENERIC_CATALOG.ids.is_empty());
    }

    #[test]
    fn fallback_serves_generic_catalog() {
        let fallback = ModelsDevFallback;

        assert_eq!(
            fallback.rates("openai", "gpt-4o"),
            Some(ModelRates {
                input: Some(2.50),
                cache_read: None,
                cache_write: None,
                output: Some(10.00),
            })
        );
    }

    #[test]
    fn fallback_ignores_provider() {
        let fallback = ModelsDevFallback;

        let via_unknown_provider = fallback.rates("some-other-provider", "gpt-4o");
        let via_openai = fallback.rates("openai", "gpt-4o");

        assert_eq!(via_unknown_provider, via_openai);
    }

    #[test]
    fn fallback_misses_unknown_model() {
        let fallback = ModelsDevFallback;

        assert_eq!(fallback.rates("openai", "unknown-model-9000"), None);
    }
}
