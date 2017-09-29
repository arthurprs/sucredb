use std::sync::Arc;
use rust_metrics::metrics::{StdGauge, StdMeter};
pub use rust_metrics::metrics::{Counter, Gauge, Meter, Metric};

// TODO: Expose these metrics
lazy_static!{
    pub static ref CLIENT_CONNECTION: Arc<StdGauge> = {
        StdGauge::new()
    };
    pub static ref REQUEST_GET: Arc<Meter> = {
        StdMeter::new()
    };
    pub static ref REQUEST_PUT: Arc<StdMeter> = {
        StdMeter::new()
    };
    pub static ref REQUEST_DEL: Arc<StdMeter> = {
        StdMeter::new()
    };
    pub static ref SYNC_SEND: Arc<StdMeter> = {
        StdMeter::new()
    };
    pub static ref SYNC_RECV: Arc<StdMeter> = {
        StdMeter::new()
    };
    pub static ref SYNC_RESEND: Arc<StdMeter> = {
        StdMeter::new()
    };
    pub static ref SYNC_OUTGOING: Arc<StdGauge> = {
        StdGauge::new()
    };
    pub static ref SYNC_INCOMING: Arc<StdGauge> = {
        StdGauge::new()
    };
}
