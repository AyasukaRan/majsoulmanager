/// Compatibility shim for the file conversion helpers retained from
/// majsoul2mjai. The managed watch path reports through `WatchRegistry`.
pub struct PipelineStatus;

impl PipelineStatus {
    pub fn conversion_result(&self, _success: bool) {}
}
