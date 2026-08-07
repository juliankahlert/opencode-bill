//! OpenCode session bill generation.
//!
//! The crate is split into data-oriented modules:
//!
//! * `data`: plain records (`UsageRecord`, `Totals`, `ModelRates`, ...)
//! * `store`: the `SessionStore` trait and session model
//! * `pricing`: the `PricingSource` trait and its catalog / GitHub impls
//! * `database` / `legacy`: the two concrete session stores
//! * `parse` / `timing` / `aggregate` / `report` / `render`: pipelines
//! * `bill`: report loading, the public entry point

mod aggregate;
mod bill;
mod context;
mod data;
mod database;
mod error;
mod legacy;
mod parse;
mod pricing;
mod render;
mod report;
mod store;
mod timing;

pub use bill::{generate_bill, load_report_with_progress};
pub use context::{DataDirectorySet, MissingDataDirectory, StorageContext, StorageContextBuilder};
pub use error::BillError;
pub use report::BillReport;
