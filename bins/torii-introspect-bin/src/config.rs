use anyhow::{bail, Result};
use clap::Parser;
use starknet::core::types::Felt;

/// Dojo introspect indexer backed by the PostgreSQL sink.
///
/// This binary targets explicitly configured Dojo contracts and persists the
/// decoded introspect messages into PostgreSQL tables.
#[derive(Parser, Debug)]
#[command(name = "torii-introspect")]
#[command(
    about = "Index Dojo introspect events into PostgreSQL",
    long_about = None
)]
pub struct Config {
    /// Starknet RPC URL.
    #[arg(
        long,
        env = "STARKNET_RPC_URL",
        default_value = "https://api.cartridge.gg/x/starknet/mainnet"
    )]
    pub rpc_url: String,

    /// Dojo contracts to index (comma-separated hex addresses).
    #[arg(long, value_delimiter = ',', required = true)]
    pub contracts: Vec<String>,

    /// Starting block number.
    #[arg(long, default_value = "0")]
    pub from_block: u64,

    /// Ending block number (None = follow chain head).
    #[arg(long)]
    pub to_block: Option<u64>,

    /// PostgreSQL URL used by the introspect sink.
    #[arg(long, env = "STORAGE_DATABASE_URL")]
    pub storage_database_url: String,

    /// Port for the Torii gRPC/HTTP server.
    #[arg(long, default_value = "3000")]
    pub port: u16,

    /// Enable Prometheus metrics collection.
    #[arg(long)]
    pub observability: bool,

    /// Events per `starknet_getEvents` request.
    #[arg(long, default_value = "1000")]
    pub event_chunk_size: u64,

    /// Block range to query per iteration.
    #[arg(long, default_value = "10000")]
    pub event_block_batch_size: u64,

    /// Maximum PostgreSQL connections for the sink.
    #[arg(long)]
    pub max_db_connections: Option<u32>,
}

impl Config {
    pub fn contract_addresses(&self) -> Result<Vec<Felt>> {
        self.contracts
            .iter()
            .map(|addr| {
                Felt::from_hex(addr).map_err(|e| anyhow::anyhow!("Invalid contract {addr}: {e}"))
            })
            .collect()
    }

    pub fn engine_database_url(&self) -> String {
        self.storage_database_url.clone()
    }

    pub fn storage_database_url(&self) -> Result<&str> {
        if self.storage_database_url.starts_with("postgres://")
            || self.storage_database_url.starts_with("postgresql://")
        {
            Ok(&self.storage_database_url)
        } else {
            bail!("--storage-database-url must be a PostgreSQL URL")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn observability_defaults_to_disabled() {
        let cfg = Config::parse_from([
            "torii-introspect",
            "--contracts",
            "0x1",
            "--storage-database-url",
            "postgres://localhost/torii",
        ]);
        assert!(!cfg.observability);
    }

    #[test]
    fn observability_flag_enables_metrics() {
        let cfg = Config::parse_from([
            "torii-introspect",
            "--contracts",
            "0x1",
            "--storage-database-url",
            "postgres://localhost/torii",
            "--observability",
        ]);
        assert!(cfg.observability);
    }

    #[test]
    fn storage_database_url_must_be_postgres() {
        let cfg = Config::parse_from([
            "torii-introspect",
            "--contracts",
            "0x1",
            "--storage-database-url",
            "sqlite://torii.db",
        ]);

        assert!(cfg.storage_database_url().is_err());
    }

    #[test]
    fn contract_addresses_parse_from_hex() {
        let cfg = Config::parse_from([
            "torii-introspect",
            "--contracts",
            "0x1,0x2",
            "--storage-database-url",
            "postgres://localhost/torii",
        ]);

        let contracts = cfg.contract_addresses().unwrap();
        assert_eq!(contracts.len(), 2);
        assert_eq!(contracts[0], Felt::ONE);
        assert_eq!(contracts[1], Felt::TWO);
    }
}
