// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process;
use std::sync::Arc;
use std::{net::SocketAddr, time::Duration};

use once_cell::sync::Lazy;
use tracing_subscriber;

use mz_build_info::{build_info, BuildInfo};
use mz_config::{system_parameter_sync, SystemParameterBackend, SystemParameterFrontend};
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::cli::{self, CliConfig};

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
//
// Furthermore, as of Aug. 2022, some engineers are using profiling
// tools, e.g. `heaptrack`, that only work with the system allocator.
#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BUILD_INFO: BuildInfo = build_info!();

pub static VERSION: Lazy<String> = Lazy::new(|| BUILD_INFO.human_version());

/// Independent compute server for Materialize.
#[derive(clap::Parser)]
#[clap(name = "computed", version = VERSION.as_str())]
struct Args {
    /// The address on which to listen for trusted SQL connections.
    ///
    /// Connections to this address are not subject to encryption,
    /// authentication, or access control. Care should be taken to not expose
    /// this address to the public internet or other unauthorized parties.
    #[clap(
        long,
        value_name = "HOST:PORT",
        env = "INTERNAL_SQL_LISTEN_ADDR",
        default_value = "127.0.0.1:6877"
    )]
    internal_sql_listen_addr: SocketAddr,
    /// An SDK key for LaunchDarkly.
    #[clap(long, env = "LAUNCHDARKLY_SDK_KEY")]
    launchdarkly_sdk_key: String,
    /// A user key for LaunchDarkly.
    #[clap(
        long,
        env = "LAUNCHDARKLY_USER_KEY",
        default_value = "anonymous-dev@materialize.com"
    )]
    launchdarkly_user_key: String,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("CONFIGD_"),
        enable_version_flag: true,
    });

    let frontend = match SystemParameterFrontend::new(
        args.launchdarkly_sdk_key.as_str(),
        args.launchdarkly_user_key.as_str(),
    ) {
        Ok(frontend) => frontend,
        Err(err) => {
            eprintln!("computed: fatal: {:#}", err);
            process::exit(1);
        }
    };

    let backend = match SystemParameterBackend::new(args.internal_sql_listen_addr).await {
        Ok(backend) => backend,
        Err(err) => {
            eprintln!("computed: fatal: {:#}", err);
            process::exit(1);
        }
    };

    if let Err(err) =
        system_parameter_sync(Arc::new(frontend), backend, Duration::from_secs(5)).await
    {
        eprintln!("computed: fatal: {:#}", err);
        process::exit(1);
    }
}
