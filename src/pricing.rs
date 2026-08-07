//! Model pricing: local catalog, then the GitHub Copilot pricing page, then
//! the generic models.dev fallback.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use log::warn;
use serde_json::Value;

use crate::data::{ModelKey, ModelRates};
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
pub(crate) trait PricingSource {
    /// Returns the per-million-token rates, or `None` when unknown.
    fn rates(&mut self, provider: &str, model: &str) -> Option<ModelRates>;
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
    fn rates(&mut self, provider: &str, model: &str) -> Option<ModelRates> {
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
#[derive(Default)]
pub(crate) struct GithubSource {
    fetched: bool,
    markdown: Option<String>,
}

impl PricingSource for GithubSource {
    fn rates(&mut self, provider: &str, model: &str) -> Option<ModelRates> {
        if provider != "github-copilot" {
            return None;
        }

        let markdown = self.markdown()?;

        github_model_pricing(markdown, model)
    }
}

impl GithubSource {
    /// Fetches the pricing page at most once, warning on failure.
    fn markdown(&mut self) -> Option<&str> {
        if !self.fetched {
            self.fetched = true;

            if NETWORK_FETCH_ALLOWED {
                self.markdown = match fetch_github_pricing() {
                    Ok(markdown) => Some(markdown),
                    Err(failure) => {
                        warn!("could not fetch GitHub Copilot pricing: {failure}");
                        None
                    }
                };
            }
        }

        self.markdown.as_deref()
    }

    /// Rates for a model from the pricing page, regardless of provider.
    ///
    /// Last resort of the pricing chain: the page lists current list prices
    /// for well-known models, so a model used under any provider can reuse
    /// them when nothing more specific is known.
    pub(crate) fn rates_for_model(&mut self, model: &str) -> Option<ModelRates> {
        let markdown = self.markdown()?;

        github_model_pricing(markdown, model)
    }

    /// A source pre-seeded with page markdown, for tests.
    #[cfg(test)]
    fn with_markdown(markdown: &str) -> Self {
        Self {
            fetched: true,
            markdown: Some(markdown.to_owned()),
        }
    }
}

/// Collects rates for the requested models: catalog, GitHub, then generic.
pub(crate) fn load_pricing<'a>(
    models_file: Option<&Path>,
    keys: impl Iterator<Item = &'a ModelKey>,
) -> Result<BTreeMap<ModelKey, ModelRates>, BillError> {
    let mut catalog = CatalogSource::new(models_file)?;
    let mut github = GithubSource::default();
    let mut fallback = ModelsDevFallback;
    let pricing = keys
        .filter_map(|key| {
            catalog
                .rates(&key.0, &key.1)
                .or_else(|| github.rates(&key.0, &key.1))
                .or_else(|| fallback.rates(&key.0, &key.1))
                .or_else(|| github.rates_for_model(&key.1))
                .map(|rates| (key.clone(), rates))
        })
        .collect();

    Ok(pricing)
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
fn fetch_github_pricing() -> Result<String, reqwest::Error> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .user_agent(USER_AGENT)
        .build()?
        .get(GITHUB_COPILOT_PRICING_URL)
        .send()?
        .error_for_status()?
        .text()
}

/// Rates for one model from the pricing page's markdown table.
fn github_model_pricing(markdown: &str, model: &str) -> Option<ModelRates> {
    let wanted = normalized_model_id(model);
    let mut headers = Vec::new();

    for line in markdown.lines() {
        let cells = markdown_cells(line);

        if cells.iter().any(|cell| cell == "Model") && cells.iter().any(|cell| cell == "Input") {
            headers = cells;
            continue;
        }

        if headers.is_empty() || cells.len() != headers.len() {
            continue;
        }

        let fields = headers
            .iter()
            .map(String::as_str)
            .zip(cells.iter().map(String::as_str))
            .collect::<BTreeMap<_, _>>();
        let name = fields.get("Model")?;

        if normalize_model_name(name) != wanted || !is_default_tier(&fields) {
            continue;
        }

        let input = markdown_rate(&fields, "Input");
        let output = markdown_rate(&fields, "Output");
        let cache_read = markdown_rate(&fields, "Cached input");
        let cache_write = markdown_rate(&fields, "Cache write");

        if input.is_some() && output.is_some() {
            return Some(ModelRates {
                input,
                cache_read,
                cache_write,
                output,
            });
        }
    }

    None
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
    let mut normalized = String::new();
    let mut separator = false;

    for character in name.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            normalized.push(character);
            separator = false;
        } else if !separator && !normalized.is_empty() {
            normalized.push('-');
            separator = true;
        }
    }

    normalized.trim_end_matches('-').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut github = GithubSource::with_markdown(PAGE_MARKDOWN);

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
        let mut github = GithubSource::with_markdown(PAGE_MARKDOWN);

        assert_eq!(github.rates_for_model("nonexistent-model-9000"), None);
    }

    #[test]
    fn load_pricing_falls_back_to_generic_catalog() {
        let keys = [
            ("openai".to_owned(), "gpt-4o".to_owned()),
            ("openai".to_owned(), "unknown-model-9000".to_owned()),
        ];

        let pricing = load_pricing(None, keys.iter()).unwrap();

        assert_eq!(pricing.len(), 1);
        assert_eq!(
            pricing[&("openai".to_owned(), "gpt-4o".to_owned())],
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

        let keys = [("openai".to_owned(), "gpt-4o".to_owned())];

        let pricing = load_pricing(Some(&models_file), keys.iter()).unwrap();

        assert_eq!(
            pricing[&("openai".to_owned(), "gpt-4o".to_owned())],
            ModelRates {
                input: Some(1.25),
                cache_read: Some(0.125),
                cache_write: None,
                output: Some(5.00),
            }
        );
    }
}
