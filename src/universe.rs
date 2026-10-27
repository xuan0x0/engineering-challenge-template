//! Tracks the set of live symbols and load-balances them across connection
//! shards. The discovery loop is exchange-agnostic: it is given a function to
//! fetch the current symbol set and a function to spawn a shard.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// Commands sent from the universe to a connection shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardCmd {
    Add(Vec<String>),
    Remove(Vec<String>),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Diff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

pub fn diff(current: &BTreeSet<String>, latest: &BTreeSet<String>) -> Diff {
    Diff {
        added: latest.difference(current).cloned().collect(),
        removed: current.difference(latest).cloned().collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLimits {
    /// Soft cap on symbols per shard; new shards open once all are full.
    pub max_per_shard: usize,
    /// Hard cap on shards (the exchange limits concurrent connections). Once
    /// reached, symbols overflow onto the least-loaded shard.
    pub max_shards: usize,
}

/// Which shard each symbol lives on.
#[derive(Debug, Clone)]
pub struct Assignment {
    limits: ShardLimits,
    shard_sizes: Vec<usize>,
    symbol_to_shard: HashMap<String, usize>,
}

impl Assignment {
    pub fn new(limits: ShardLimits) -> Self {
        assert!(
            limits.max_per_shard > 0 && limits.max_shards > 0,
            "shard limits must be positive"
        );
        Self {
            limits,
            shard_sizes: Vec::new(),
            symbol_to_shard: HashMap::new(),
        }
    }

    pub fn symbols(&self) -> BTreeSet<String> {
        self.symbol_to_shard.keys().cloned().collect()
    }

    pub fn shard_count(&self) -> usize {
        self.shard_sizes.len()
    }

    /// Places each new symbol on the least-loaded shard with spare capacity,
    /// opening new shards when all are full (up to `max_shards`, after which
    /// the least-loaded shard takes it). Returns symbols grouped by shard.
    pub fn assign(&mut self, symbols: &[String]) -> BTreeMap<usize, Vec<String>> {
        let mut placed: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for symbol in symbols {
            if self.symbol_to_shard.contains_key(symbol) {
                continue;
            }
            let shard = self.pick_shard();
            self.shard_sizes[shard] += 1;
            self.symbol_to_shard.insert(symbol.clone(), shard);
            placed.entry(shard).or_default().push(symbol.clone());
        }
        placed
    }

    /// Removes symbols, returning them grouped by the shard they were on.
    pub fn unassign(&mut self, symbols: &[String]) -> BTreeMap<usize, Vec<String>> {
        let mut removed: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for symbol in symbols {
            if let Some(shard) = self.symbol_to_shard.remove(symbol) {
                self.shard_sizes[shard] -= 1;
                removed.entry(shard).or_default().push(symbol.clone());
            }
        }
        removed
    }

    fn pick_shard(&mut self) -> usize {
        let least_loaded = self
            .shard_sizes
            .iter()
            .enumerate()
            .min_by_key(|&(_, &size)| size)
            .map(|(i, &size)| (i, size));
        match least_loaded {
            Some((i, size)) if size < self.limits.max_per_shard => i,
            Some((i, _)) if self.shard_sizes.len() >= self.limits.max_shards => i,
            _ => {
                self.shard_sizes.push(0);
                self.shard_sizes.len() - 1
            }
        }
    }
}

struct Shard {
    commands: mpsc::Sender<ShardCmd>,
    handle: JoinHandle<()>,
}

/// Periodically refreshes the symbol set and keeps shards subscribed to it.
/// On cancellation, waits for every shard task to finish before returning.
pub async fn run<F, Fut, S>(
    fetch_symbols: F,
    spawn_shard: S,
    limits: ShardLimits,
    refresh_interval: Duration,
    cancel: CancellationToken,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<BTreeSet<String>>>,
    S: Fn(usize, mpsc::Receiver<ShardCmd>) -> JoinHandle<()>,
{
    let mut assignment = Assignment::new(limits);
    let mut shards: Vec<Shard> = Vec::new();
    let mut ticker = tokio::time::interval(refresh_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }
        let latest = tokio::select! {
            _ = cancel.cancelled() => break,
            result = fetch_symbols() => match result {
                Ok(latest) => latest,
                Err(e) => {
                    tracing::warn!(error = %format!("{e:#}"), "symbol discovery failed; keeping current set");
                    continue;
                }
            },
        };
        if latest.is_empty() && assignment.shard_count() > 0 {
            // An empty listing is far more likely to be an exchange glitch than
            // every market closing at once; don't tear everything down.
            tracing::warn!("symbol discovery returned no symbols; ignoring");
            continue;
        }

        let Diff { added, removed } = diff(&assignment.symbols(), &latest);
        if added.is_empty() && removed.is_empty() {
            continue;
        }
        for (shard, symbols) in assignment.unassign(&removed) {
            send(&shards[shard], ShardCmd::Remove(symbols)).await;
        }
        for (shard, symbols) in assignment.assign(&added) {
            while shards.len() <= shard {
                let (tx, rx) = mpsc::channel(64);
                let handle = spawn_shard(shards.len(), rx);
                shards.push(Shard {
                    commands: tx,
                    handle,
                });
            }
            send(&shards[shard], ShardCmd::Add(symbols)).await;
        }
        tracing::info!(
            added = added.len(),
            removed = removed.len(),
            total = latest.len(),
            shards = shards.len(),
            "symbol universe updated"
        );
    }

    tracing::info!(shards = shards.len(), "waiting for shards to shut down");
    for shard in shards {
        drop(shard.commands);
        if let Err(e) = shard.handle.await {
            tracing::error!(error = %e, "shard task panicked");
        }
    }
}

async fn send(shard: &Shard, cmd: ShardCmd) {
    if shard.commands.send(cmd).await.is_err() {
        tracing::error!("shard task exited unexpectedly; command dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn vec(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn limits(max_per_shard: usize, max_shards: usize) -> ShardLimits {
        ShardLimits {
            max_per_shard,
            max_shards,
        }
    }

    #[test]
    fn diff_finds_added_and_removed() {
        let d = diff(&set(&["A", "B", "C"]), &set(&["B", "C", "D"]));
        assert_eq!(
            d,
            Diff {
                added: vec(&["D"]),
                removed: vec(&["A"])
            }
        );
    }

    #[test]
    fn assign_fills_shards_up_to_capacity() {
        let mut a = Assignment::new(limits(2, 10));
        let placed = a.assign(&vec(&["A", "B", "C", "D", "E"]));
        assert_eq!(a.shard_count(), 3);
        let sizes: Vec<usize> = placed.values().map(Vec::len).collect();
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[test]
    fn assign_ignores_already_assigned_symbols() {
        let mut a = Assignment::new(limits(10, 10));
        a.assign(&vec(&["A"]));
        assert!(a.assign(&vec(&["A"])).is_empty());
        assert_eq!(a.symbols(), set(&["A"]));
    }

    #[test]
    fn freed_capacity_is_reused_by_least_loaded_shard() {
        let mut a = Assignment::new(limits(2, 10));
        a.assign(&vec(&["A", "B", "C", "D"]));
        let removed = a.unassign(&vec(&["C", "Z"]));
        assert_eq!(removed, BTreeMap::from([(1, vec(&["C"]))]));
        let placed = a.assign(&vec(&["E"]));
        assert_eq!(placed, BTreeMap::from([(1, vec(&["E"]))]));
        assert_eq!(a.shard_count(), 2);
    }

    #[test]
    fn overflows_onto_least_loaded_shard_at_shard_cap() {
        let mut a = Assignment::new(limits(1, 2));
        a.assign(&vec(&["A", "B"]));
        a.unassign(&vec(&["A"]));
        a.assign(&vec(&["C", "D", "E"]));
        assert_eq!(a.shard_count(), 2);
        assert_eq!(a.shard_sizes, vec![2, 2]);
    }

    #[tokio::test]
    async fn run_spawns_shards_and_routes_commands() {
        use std::sync::{Arc, Mutex};

        let listings = Arc::new(Mutex::new(vec![
            set(&["A", "B", "C"]),
            set(&["B", "C", "D"]),
        ]));
        let received: Arc<Mutex<Vec<(usize, ShardCmd)>>> = Arc::default();
        let cancel = CancellationToken::new();

        let fetch = {
            let listings = listings.clone();
            let cancel = cancel.clone();
            move || {
                let next = {
                    let mut l = listings.lock().unwrap();
                    if l.is_empty() {
                        None
                    } else {
                        Some(l.remove(0))
                    }
                };
                let cancel = cancel.clone();
                async move {
                    match next {
                        Some(s) => Ok(s),
                        None => {
                            cancel.cancel();
                            Ok(BTreeSet::new())
                        }
                    }
                }
            }
        };
        let spawn = {
            let received = received.clone();
            move |id: usize, mut rx: mpsc::Receiver<ShardCmd>| {
                let received = received.clone();
                tokio::spawn(async move {
                    while let Some(cmd) = rx.recv().await {
                        received.lock().unwrap().push((id, cmd));
                    }
                })
            }
        };

        run(
            fetch,
            spawn,
            limits(2, 10),
            Duration::from_millis(1),
            cancel,
        )
        .await;

        let got = received.lock().unwrap().clone();
        assert_eq!(
            got,
            vec![
                (0, ShardCmd::Add(vec(&["A", "B"]))),
                (1, ShardCmd::Add(vec(&["C"]))),
                (0, ShardCmd::Remove(vec(&["A"]))),
                (0, ShardCmd::Add(vec(&["D"]))),
            ]
        );
    }
}
