pub trait SnapshotCodec: Send + Sync + 'static {
    fn encode(&self, val: &serde_json::Value) -> Result<Vec<u8>, String>;
    fn decode(&self, data: &[u8]) -> Result<serde_json::Value, String>;
    fn format(&self) -> &'static str;
}
pub struct JsonCodec;
pub struct BincodeCodec;



