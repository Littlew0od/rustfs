// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use rustfs_tftp::config::Config;
use rustfs_tftp::server;

#[tokio::main]
async fn main() {
    if let Err(e) = try_main().await {
        error!("Fatal: {}", e);
        std::process::exit(1);
    }
}

async fn try_main() -> Result<()> {
    let config = Config::parse();

    init_tracing(&config)?;

    info!("Starting RustFS TFTP Server v{}", env!("CARGO_PKG_VERSION"));

    if let Err(e) = config.validate() {
        error!("Configuration validation failed: {}", e);
        anyhow::bail!("Configuration validation failed: {}", e);
    }

    config.log_configuration();

    server::run(config).await?;

    info!("RustFS TFTP Server shutdown complete");
    Ok(())
}

fn init_tracing(config: &Config) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.log_level))
        .context("Failed to create log filter")?;

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_writer(std::io::stderr)
        .finish();

    tracing::subscriber::set_global_default(subscriber).context("Failed to set global tracing subscriber")?;

    Ok(())
}
