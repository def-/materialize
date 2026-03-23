// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_tls_util::make_tls;
use tokio_postgres::config::Host;
use tokio_postgres::{Client, Config};
use url::Url;

/// Constructs a URL from PostgreSQL configuration parameters.
///
/// Returns an error if the set of configuration parameters is not representable
/// as a URL, e.g., if there are multiple hosts.
pub fn config_url(config: &Config) -> Result<Url, anyhow::Error> {
    let mut url = Url::parse("postgresql://").unwrap();

    let host = match config.get_hosts() {
        [] => "localhost".into(),
        [Host::Tcp(host)] => host.clone(),
        [Host::Unix(path)] => path.display().to_string(),
        _ => bail!("Materialize URL cannot contain multiple hosts"),
    };
    url.set_host(Some(&host))
        .context("parsing Materialize host")?;

    url.set_port(Some(match config.get_ports() {
        [] => 5432,
        [port] => *port,
        _ => bail!("Materialize URL cannot contain multiple ports"),
    }))
    .expect("known to be valid to set port");

    if let Some(user) = config.get_user() {
        url.set_username(user)
            .expect("known to be valid to set username");
    }

    Ok(url)
}

pub async fn postgres_client(
    url: &str,
    default_timeout: Duration,
) -> Result<(Client, task::JoinHandle<Result<(), tokio_postgres::Error>>), anyhow::Error> {
    let t0 = Instant::now();
    let (client, connection) = Retry::default()
        .max_duration(default_timeout)
        .retry_async_canceling(|retry_state| {
            let t_retry = Instant::now();
            async move {
                let t_config = Instant::now();
                let pgconfig = &mut Config::from_str(url)?;
                pgconfig.connect_timeout(default_timeout);
                let tls = make_tls(pgconfig)?;
                let config_dur = t_config.elapsed();
                let t_connect = Instant::now();
                let result = pgconfig.connect(tls).await.map_err(|e| anyhow!(e));
                eprintln!(
                    "postgres_client: retry={} config={:?} connect={:?} total_retry={:?} ok={}",
                    retry_state.i,
                    config_dur,
                    t_connect.elapsed(),
                    t_retry.elapsed(),
                    result.is_ok(),
                );
                result
            }
        })
        .await?;
    let total = t0.elapsed();

    if url.contains("mzp_") {
        println!("Connecting to PostgreSQL server at [REDACTED]... ({total:?})");
    } else {
        println!("Connecting to PostgreSQL server at {url}... ({total:?})");
    }
    let handle = task::spawn(|| "postgres_client_task", connection);

    Ok((client, handle))
}
