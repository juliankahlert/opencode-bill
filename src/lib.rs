//! OpenCode session and calendar-month bill generation.
//!
//! The crate is split into data-oriented modules:
//!
//! * `data`: plain records (`UsageRecord`, `Totals`, `ModelRates`, ...)
//! * `store`: the `SessionStore` trait, session and month models
//! * `pricing`: the `PricingSource` trait and its catalog / GitHub impls
//! * `database` / `legacy`: the two concrete session stores
//! * `parse` / `timing` / `aggregate` / `report` / `render`: pipelines
//! * `bill`: report loading, the public entry points

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

pub use bill::{
    generate_bill, generate_monthly_bill, load_month_report_with_progress,
    load_report_with_progress,
};
pub use context::{DataDirectorySet, MissingDataDirectory, StorageContext, StorageContextBuilder};
pub use error::BillError;
pub use render::render_bill;
pub use report::{BillHeader, BillReport};
