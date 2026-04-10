use anyhow::Result;
use leyla_core::{
    clock::SystemClock,
    scheduler::recovery::RecoveryManager,
    store::LeylaStore,
};
use std::sync::Arc;
use std::time::Duration;

// zaman zaman beni dusunup agliyormussun leyla
pub async fn handle(store: Arc<dyn LeylaStore>) -> Result<()> {
    let clock = Arc::new(SystemClock);
    let manager = RecoveryManager::new(store, clock, Duration::from_secs(300));
    let count = manager.recover().await?;
    println!("Recovery complete. Recovered {count} run(s).");
    Ok(())
}
