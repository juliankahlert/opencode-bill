//! OpenCode session, calendar-month, calendar-week, calendar-year, and
//! calendar-day bill generation.
//!
//! The crate is split into data-oriented modules:
//!
//! * `data`: component storage (`UsageStore` SoA rows, `Totals`, `ModelRates`, ...)
//! * `store`: the `SessionStore` trait, session and calendar-window models
//! * `pricing`: the `PricingSource` trait and its catalog / GitHub impls
//! * `models_dev`: generic models.dev-style fallback pricing catalog
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
mod models_dev;
mod parse;
mod pricing;
mod render;
mod report;
mod store;
mod timing;

pub use bill::{
    generate_bill, generate_daily_bill, generate_monthly_bill, generate_weekly_bill,
    generate_yearly_bill, load_day_report_with_progress, load_day_report_with_progress_counted,
    load_month_report_with_progress, load_month_report_with_progress_counted,
    load_report_with_progress, load_report_with_progress_counted, load_week_report_with_progress,
    load_week_report_with_progress_counted, load_year_report_with_progress,
    load_year_report_with_progress_counted, recent_roots,
};
pub use context::{DataDirectorySet, MissingDataDirectory, StorageContext, StorageContextBuilder};
pub use data::BillProgress;
pub use error::BillError;
pub use render::{graph_lines, render_bill};
pub use report::{BarGraph, BillHeader, BillReport, MetricKind};
pub use store::RecentRoot;
pub use timing::format_local_iso;
