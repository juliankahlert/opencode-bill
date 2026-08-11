//! Model pricing: local catalog, then the GitHub Copilot pricing page, then
//! the generic models.dev fallback.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use log::warn;
use rust_decimal::Decimal;
use rusty_money::{Money, Round};
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::data::{
    Cost, Interner, MICRO_PER_DOLLAR, MICRO_USD, ModelKey, ModelRates, Tokens, cost_add,
    cost_from_micros, cost_to_micros,
};
use crate::error::BillError;
use crate::models_dev::{EFFORT_SUFFIXES, ModelsDevFallback};
use crate::parse::read_json;

/// GitHub documentation page that lists Copilot model rates.
const GITHUB_COPILOT_PRICING_URL: &str = "https://docs.github.com/api/article/body?pathname=/en/copilot/reference/copilot-billing/models-and-pricing";
/// Network timeout for pricing fetches.
const HTTP_TIMEOUT_SECONDS: u64 = 5;
/// HTTP user agent for pricing fetches.
const USER_AGENT: &str = concat!("opencode-bill/", env!("CARGO_PKG_VERSION"));
/// Whether the pricing page may be fetched over the network.
///
/// Unit tests stay hermetic: the page parser is exercised with pre-seeded
/// markdown (see `GithubSource::with_markdown`), so test builds never
/// attempt a network fetch.
const NETWORK_FETCH_ALLOWED: bool = cfg!(not(test));

/// Looks up pricing for a provider/model pair.
///
/// Implementations are read-only: `rates` never mutates the source, so the
/// trait stays a pure behavior contract.
pub(crate) trait PricingSource {
    /// Returns the per-million-token rates, or `None` when unknown.
    fn rates(&self, provider: &str, model: &str) -> Option<ModelRates>;
}

/// Pricing read from a local models.json catalog.
pub(crate) struct CatalogSource {
    catalog: Option<Value>,
}

impl CatalogSource {
    pub(crate) fn new(path: Option<&Path>) -> Result<Self, BillError> {
        let catalog = match path {
            Some(path) if path.is_file() => Some(read_json(path)?),
            _ => None,
        };

        Ok(Self { catalog })
    }
}

impl PricingSource for CatalogSource {
    fn rates(&self, provider: &str, model: &str) -> Option<ModelRates> {
        let catalog = self.catalog.as_ref()?;

        catalog
            .get(provider)
            .and_then(|value| value.get("models"))
            .and_then(|value| value.get(model))
            .and_then(|value| value.get("cost"))
            .and_then(model_rates)
    }
}

/// Pricing fetched from GitHub's Copilot pricing page.
///
/// Fetch-once state lives in the struct as a `tokio::sync::OnceCell`; the
/// `PricingSource` impl itself stays behavior-only.  The pipeline fetches
/// only when the bill uses `github-copilot`, and awaits
/// [`GithubSource::ensure_fetched`] before the pricing phase.  A failed
/// attempt is remembered (`Err(())` is stored), so the network is never hit
/// twice in one process.
#[derive(Default)]
pub(crate) struct GithubSource {
    markdown: OnceCell<Result<String, ()>>,
}

impl PricingSource for GithubSource {
    fn rates(&self, provider: &str, model: &str) -> Option<ModelRates> {
        if provider != "github-copilot" {
            return None;
        }

        let markdown = self.markdown_now()?;

        github_model_pricing(markdown, model)
    }
}

impl GithubSource {
    /// Fetches the pricing page at most once, waiting for an in-flight fetch.
    ///
    /// Returns the page markdown when the fetch succeeded.  A failed attempt
    /// is stored, so later calls return `None` without touching the network.
    pub(crate) async fn ensure_fetched(&self) -> Option<&str> {
        self.markdown
            .get_or_init(|| async {
                if !NETWORK_FETCH_ALLOWED {
                    return Err(());
                }

                fetch_github_pricing().await.map_err(|failure| {
                    warn!("could not fetch GitHub Copilot pricing: {failure}");
                })
            })
            .await
            .as_ref()
            .ok()
            .map(String::as_str)
    }

    /// The fetched page markdown, without waiting for a fetch in flight.
    fn markdown_now(&self) -> Option<&str> {
        self.markdown
            .get()
            .and_then(|result| result.as_ref().ok())
            .map(String::as_str)
    }

    /// Rates for a model from the pricing page, regardless of provider.
    ///
    /// Last resort of the pricing chain: the page lists current list prices
    /// for well-known models, so a model used under any provider can reuse
    /// them when nothing more specific is known.
    pub(crate) fn rates_for_model(&self, model: &str) -> Option<ModelRates> {
        let markdown = self.markdown_now()?;

        github_model_pricing(markdown, model)
    }

    /// A source pre-seeded with page markdown, for tests.
    #[cfg(test)]
    fn with_markdown(markdown: &str) -> Self {
        let source = Self::default();

        let _ = source.markdown.set(Ok(markdown.to_owned()));
        source
    }
}

/// Collects rates for the requested models: catalog, GitHub, then generic.
///
/// `github` is the shared fetch-once source; the pipeline already awaited
/// `ensure_fetched` when the bill needs it, so `rates` reads the page
/// without touching the network.
pub(crate) fn load_pricing<'a>(
    models_file: Option<&Path>,
    keys: impl Iterator<Item = &'a ModelKey>,
    interner: &Interner,
    github: &GithubSource,
) -> Result<BTreeMap<ModelKey, ModelRates>, BillError> {
    let catalog = CatalogSource::new(models_file)?;
    let keys = keys.collect::<Vec<_>>();
    let pricing = keys
        .into_iter()
        .filter_map(|key| rates_for_key(key, interner, &catalog, github).map(|rates| (*key, rates)))
        .collect();

    Ok(pricing)
}

/// Rates for one model key through the shared chain: catalog, GitHub,
/// generic fallback, then the GitHub page as last resort.
///
/// The streaming pipeline resolves keys lazily as rows arrive, so the chain
/// lives here once: a streamed key resolves exactly like a whole-window
/// [`load_pricing`] lookup.
pub(crate) fn rates_for_key(
    key: &ModelKey,
    interner: &Interner,
    catalog: &CatalogSource,
    github: &GithubSource,
) -> Option<ModelRates> {
    let provider = interner.resolve_provider(key.0);
    let model = interner.resolve_model(key.1);

    catalog
        .rates(provider, model)
        .or_else(|| github.rates(provider, model))
        .or_else(|| ModelsDevFallback.rates(provider, model))
        .or_else(|| github.rates_for_model(model))
}

/// Estimated micro-dollar cost of one call from token usage and rates.
///
/// Reasoning tokens bill at the output rate; missing rates contribute
/// nothing.  Used to price calls whose recorded cost is unknown (zero).
/// The per-term products and the sum run through [`rusty_money`] decimal
/// arithmetic, rounded ties-to-even at the micro-dollar.  The old `f64`
/// path was noise-dependent at half-micro ties; the exact decimal path
/// rounds the true sum, which can differ from the old binary result by
/// one micro on ties.
pub(crate) fn estimated_cost_micros(tokens: &Tokens, rates: &ModelRates) -> i64 {
    let output_tokens = tokens.output.saturating_add(tokens.reasoning);
    let cost = [
        term(tokens.input, rates.input),
        term(tokens.cache_read, rates.cache_read),
        term(tokens.cache_write, rates.cache_write),
        term(output_tokens, rates.output),
    ]
    .into_iter()
    .fold(cost_from_micros(0), cost_add);

    cost_to_micros(cost.round(6, Round::HalfEven))
}

/// Cost of `tokens` at a USD-per-million rate, or zero when the rate is
/// absent.  Extreme token counts clamp to the `i64` dollar range so the
/// exact decimal arithmetic stays inside the 96-bit mantissa.
fn term(tokens: u64, rate: Option<f64>) -> Cost {
    let Some(rate) = rate else {
        return cost_from_micros(0);
    };
    let rate = match Decimal::try_from(rate) {
        Ok(rate) => rate,
        Err(_) => {
            warn!("non-decimal model rate {rate}; billing the call at the cap");
            return cost_from_micros(i64::MAX);
        }
    };
    let tokens = tokens.min(i64::MAX as u64) as i64;
    let dollars = Money::from_major(tokens, &MICRO_USD).mul(rate);

    // Every rate source filters non-negative values, so only a decimal
    // overflow can reach the error arm; the negative guard is defensive.
    match dollars.and_then(|dollars| dollars.div(MICRO_PER_DOLLAR)) {
        Ok(cost) => cost,
        Err(_) if rate.is_sign_negative() => cost_from_micros(i64::MIN),
        Err(_) => cost_from_micros(i64::MAX),
    }
}

/// Reads the four rates out of a catalog `cost` object.
fn model_rates(cost: &Value) -> Option<ModelRates> {
    let input = cost.get("input").and_then(valid_rate);
    let output = cost.get("output").and_then(valid_rate);
    let cache_read = cost.get("cache_read").and_then(valid_rate);
    let cache_write = cost.get("cache_write").and_then(valid_rate);

    if input.is_none() && output.is_none() && cache_read.is_none() && cache_write.is_none() {
        return None;
    }

    Some(ModelRates {
        input,
        cache_read,
        cache_write,
        output,
    })
}

/// A rate that is finite and non-negative.
fn valid_rate(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .filter(|rate| rate.is_finite() && *rate >= 0.0)
}

/// Downloads the GitHub Copilot pricing page as markdown.
async fn fetch_github_pricing() -> Result<String, reqwest::Error> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .user_agent(USER_AGENT)
        .build()?;

    client
        .get(GITHUB_COPILOT_PRICING_URL)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
}

/// Rates for one model from the pricing page's markdown table.
fn github_model_pricing(markdown: &str, model: &str) -> Option<ModelRates> {
    let wanted = normalized_model_id(model);
    let mut headers = Vec::new();

    markdown.lines().find_map(|line| {
        let cells = markdown_cells(line);

        if cells.iter().any(|cell| cell == "Model") && cells.iter().any(|cell| cell == "Input") {
            headers = cells;

            return None;
        }

        if headers.is_empty() || cells.len() != headers.len() {
            return None;
        }

        let fields = headers
            .iter()
            .map(String::as_str)
            .zip(cells.iter().map(String::as_str))
            .collect::<BTreeMap<_, _>>();
        let name = fields.get("Model")?;

        if normalize_model_name(name) != wanted || !is_default_tier(&fields) {
            return None;
        }

        let input = markdown_rate(&fields, "Input");
        let output = markdown_rate(&fields, "Output");
        let cache_read = markdown_rate(&fields, "Cached input");
        let cache_write = markdown_rate(&fields, "Cache write");

        if input.is_some() && output.is_some() {
            Some(ModelRates {
                input,
                cache_read,
                cache_write,
                output,
            })
        } else {
            None
        }
    })
}

/// Splits one markdown table row into trimmed cells.
fn markdown_cells(line: &str) -> Vec<String> {
    let trimmed = line.trim();

    if !trimmed.starts_with('|') || !trimmed.ends_with('|') {
        return Vec::new();
    }

    trimmed
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_owned())
        .collect()
}

/// Parses a `$5.00`-style cell as a rate.
fn markdown_rate(fields: &BTreeMap<&str, &str>, name: &str) -> Option<f64> {
    fields
        .get(name)
        .and_then(|value| value.trim().trim_start_matches('$').parse().ok())
        .filter(|rate: &f64| rate.is_finite() && *rate >= 0.0)
}

/// Whether the row belongs to the default pricing tier.
fn is_default_tier(fields: &BTreeMap<&str, &str>) -> bool {
    fields
        .get("Tier")
        .is_none_or(|tier| tier.is_empty() || *tier == "Default")
}

/// Normalizes a model ID, stripping effort suffixes like `-high`.
fn normalized_model_id(model: &str) -> String {
    let base = EFFORT_SUFFIXES
        .iter()
        .find_map(|suffix| model.strip_suffix(suffix))
        .unwrap_or(model);

    normalize_model_name(base)
}

/// Collapses a model name to lowercase alphanumerics separated by `-`.
fn normalize_model_name(name: &str) -> String {
    let normalized = name.chars().flat_map(char::to_lowercase).fold(
        String::new(),
        |mut normalized, character| {
            if character.is_ascii_alphanumeric() {
                normalized.push(character);
            } else if !normalized.ends_with('-') && !normalized.is_empty() {
                normalized.push('-');
            }

            normalized
        },
    );

    normalized.trim_end_matches('-').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Tokens;

    const PAGE_MARKDOWN: &str = r#"
| Model         | Release status | Category | Tier         | Threshold | Input | Cached input | Output |
| ------------- | -------------- | -------- | ------------ | --------- | ----: | -----------: | -----: |
| GPT-5.6 Sol   | GA             | Powerful | Default      | <= 272K   | $5.00 |        $0.50 | $30.00 |
| GPT-5.6 Sol   | GA             | Powerful | Long context | > 272K    | $10.00 |        $1.00 | $45.00 |
"#;

    #[test]
    fn parses_github_pricing_for_effort_variant() {
        assert_eq!(
            github_model_pricing(PAGE_MARKDOWN, "gpt-5.6-sol-low"),
            Some(ModelRates {
                input: Some(5.0),
                cache_read: Some(0.5),
                cache_write: None,
                output: Some(30.0),
            })
        );
    }

    #[test]
    fn rates_for_model_ignores_provider() {
        let github = GithubSource::with_markdown(PAGE_MARKDOWN);

        assert_eq!(
            github.rates_for_model("gpt-5.6-sol-medium"),
            Some(ModelRates {
                input: Some(5.0),
                cache_read: Some(0.5),
                cache_write: None,
                output: Some(30.0),
            })
        );
    }

    #[test]
    fn rates_for_model_misses_unknown_model() {
        let github = GithubSource::with_markdown(PAGE_MARKDOWN);

        assert_eq!(github.rates_for_model("nonexistent-model-9000"), None);
    }

    #[test]
    fn estimated_cost_bills_usage_at_rates() {
        let tokens = Tokens {
            input: 1_000_000,
            output: 1_000_000,
            reasoning: 500_000,
            cache_read: 2_000_000,
            cache_write: 500_000,
        };
        let rates = ModelRates {
            input: Some(1.0),
            cache_read: Some(0.5),
            cache_write: Some(2.0),
            output: Some(3.0),
        };

        assert_eq!(estimated_cost_micros(&tokens, &rates), 7_500_000);
    }

    #[test]
    fn estimated_cost_ignores_missing_rates() {
        let tokens = Tokens {
            input: 1_000_000,
            output: 1_000_000,
            reasoning: 0,
            cache_read: 0,
            cache_write: 0,
        };
        let rates = ModelRates {
            input: Some(2.0),
            cache_read: None,
            cache_write: None,
            output: None,
        };

        assert_eq!(estimated_cost_micros(&tokens, &rates), 2_000_000);
    }

    #[test]
    fn estimated_cost_rounds_half_micro_ties_to_even() {
        let rates = ModelRates {
            input: Some(0.5),
            cache_read: None,
            cache_write: None,
            output: None,
        };

        // 3 * 0.5 / 1e6 = 1.5 micros, an exact tie -> rounds to even (2).
        let tokens = Tokens {
            input: 3,
            ..Tokens::default()
        };
        assert_eq!(estimated_cost_micros(&tokens, &rates), 2);

        // 1 * 0.5 / 1e6 = 0.5 micros, an exact tie -> rounds to even (0).
        let tokens = Tokens {
            input: 1,
            ..Tokens::default()
        };
        assert_eq!(estimated_cost_micros(&tokens, &rates), 0);
    }

    #[test]
    fn load_pricing_falls_back_to_generic_catalog() {
        let mut interner = Interner::default();
        let keys = [
            (
                interner.intern_provider("openai"),
                interner.intern_model("gpt-4o"),
            ),
            (
                interner.intern_provider("openai"),
                interner.intern_model("unknown-model-9000"),
            ),
        ];

        let pricing = load_pricing(None, keys.iter(), &interner, &GithubSource::default()).unwrap();

        assert_eq!(pricing.len(), 1);
        assert_eq!(
            pricing[&(
                interner.intern_provider("openai"),
                interner.intern_model("gpt-4o")
            )],
            ModelRates {
                input: Some(2.50),
                cache_read: None,
                cache_write: None,
                output: Some(10.00),
            }
        );
    }

    #[test]
    fn load_pricing_prefers_catalog_over_fallback() {
        use std::fs;

        let temporary = tempfile::TempDir::new().unwrap();
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-4o":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}}}"#,
        )
        .unwrap();

        let mut interner = Interner::default();
        let keys = [(
            interner.intern_provider("openai"),
            interner.intern_model("gpt-4o"),
        )];

        let pricing = load_pricing(
            Some(&models_file),
            keys.iter(),
            &interner,
            &GithubSource::default(),
        )
        .unwrap();

        assert_eq!(
            pricing[&(
                interner.intern_provider("openai"),
                interner.intern_model("gpt-4o")
            )],
            ModelRates {
                input: Some(1.25),
                cache_read: Some(0.125),
                cache_write: None,
                output: Some(5.00),
            }
        );
    }

    #[tokio::test]
    async fn fetch_once_remembers_failure_without_network() {
        let github = GithubSource::default();

        // Test builds never fetch: the cell stores the failure on first
        // call and answers from memory afterwards.
        assert_eq!(github.ensure_fetched().await, None);
        assert_eq!(github.ensure_fetched().await, None);
        assert!(github.markdown.get().is_some());
        assert_eq!(github.rates("github-copilot", "gpt-5.6-sol"), None);
    }

    #[tokio::test]
    #[ignore = "network test; run manually with --ignored"]
    async fn fetches_live_github_pricing_page() {
        let markdown = fetch_github_pricing().await.expect("live fetch failed");

        assert!(
            markdown.contains('|'),
            "pricing page should be a markdown table"
        );
    }
}
