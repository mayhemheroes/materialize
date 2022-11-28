// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod backend;
mod frontend;
mod params;
mod value;

use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use itertools::Itertools;
use tokio::time;

pub use backend::SystemParameterBackend;
pub use frontend::SystemParameterFrontend;
pub use params::SynchronizedParameters;
pub use value::Value;

/// Run a loop that periodically pulls system parameters defined in the
/// LaunchDarkly-backed [SystemParameterFrontend] and pushes modified values to the
/// `ALTER SYSTEM`-backed [SystemParameterBackend].
pub async fn system_parameter_sync(
    frontend: Arc<SystemParameterFrontend>,
    backend: SystemParameterBackend,
    sleep_duration: Duration,
) -> Result<(), anyhow::Error> {
    // Ensure the frontend client is initialize.
    frontend.ensure_initialized().await;

    // Ryn synchronization loop.
    tracing::info!(
        "synchronizing system parameter values every {} seconds",
        sleep_duration.as_secs()
    );

    let mut params = SynchronizedParameters::default();
    loop {
        backend.pull(&mut params).await;
        if frontend.pull(&mut params) {
            backend.push(&mut params).await;
        }
        time::sleep(sleep_duration).await;
    }
}

/// Start configd as a subprocess and ensure that it is restarted on failure
/// until a termination signal is received.
///
/// Strictly speaking this is not needed for the current configuration.
pub async fn ensure_computed(
    internal_sql_listen_addr: SocketAddr,
    launchdarkly_sdk_key: String,
) -> Result<(), anyhow::Error> {
    // Look for computed in the same directory as the
    // running binary. When running via `cargo run`, this
    // means that debug binaries look for other debug
    // binaries and release binaries look for other release
    // binaries.
    let base_dir = env::current_exe()?.parent().unwrap().to_path_buf();
    let computed_path = tokio::fs::canonicalize(base_dir).await?.join("configd");

    if computed_path.exists() {
        let mut cmd = tokio::process::Command::new(&computed_path);
        cmd.args([
            "--internal-sql-listen-addr",
            &internal_sql_listen_addr.to_string(),
            "--launchdarkly-sdk-key",
            &launchdarkly_sdk_key,
        ]);

        // cmd.stdout(std::process::Stdio::null());
        // cmd.stderr(std::process::Stdio::null());

        loop {
            println!(
                "Launching {} {}...",
                computed_path.display(),
                cmd.as_std()
                    .get_args()
                    .map(|arg| arg.to_string_lossy())
                    .join(" ")
            );

            match cmd.spawn() {
                Ok(process) => {
                    let status = KillOnDropChild(process).0.wait().await;
                    eprintln!("configd exited: {:?}; relaunching in 5s", status);
                }
                Err(e) => {
                    eprintln!("configd failed to launch: {}; relaunching in 5s", e);
                }
            };

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    } else {
        eprintln!("computed path {} does not exist", computed_path.display());
    }

    Ok(())
}

struct KillOnDropChild(tokio::process::Child);

impl Drop for KillOnDropChild {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}
