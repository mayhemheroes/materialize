// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use reqwest::Client;
use serde::Deserialize;

use crate::configuration::{Configuration, Endpoint, WEB_DOCS_URL};
use crate::login::{generate_api_token, login_with_browser, login_with_console};
use crate::password::list_passwords;
use crate::region::{
    disable_region_environment, enable_region_environment, get_provider_by_region_name,
    get_provider_region_environment, get_region_environment, list_cloud_providers, list_regions,
    print_environment_status, print_region_enabled, CloudProviderRegion,
};
use crate::shell::{check_environment_health, shell};
use crate::utils::run_loading_spinner;

mod configuration;
mod login;
mod password;
mod region;
mod shell;
mod utils;

/// Command-line interface for Materialize.
#[derive(Debug, Parser)]
#[clap(name = "Materialize CLI")]
#[clap(about = "Command-line interface for Materialize.", long_about = None)]
struct Cli {
    /// The configuration profile to use.
    #[clap(long)]
    profile: Option<String>,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Show commands to interact with passwords
    AppPassword(AppPasswordCommand),
    /// Open the docs
    Docs,
    /// Login to a profile and make it the active profile
    Login {
        /// Login by typing your email and password
        #[clap(short, long)]
        interactive: bool,

        /// Force reauthentication for the profile
        #[clap(short, long)]
        force: bool,

        /// Override the default API endpoint.
        #[clap(long, hide = true, default_value_t)]
        endpoint: Endpoint,
    },
    /// Show commands to interact with regions
    Region {
        #[clap(subcommand)]
        command: RegionCommand,
    },
    /// Connect to a region using a SQL shell
    Shell {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: Option<String>,
    },
}

#[derive(Debug, Args)]
struct AppPasswordCommand {
    #[clap(subcommand)]
    command: AppPasswordSubommand,
}

#[derive(Debug, Subcommand)]
enum AppPasswordSubommand {
    /// Create a password.
    Create {
        /// Name for the password.
        name: String,
    },
    /// List all enabled passwords.
    List,
}

#[derive(Debug, Subcommand)]
enum RegionCommand {
    /// Enable a region.
    Enable {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
        #[clap(long, hide = true)]
        version: Option<String>,
        #[clap(long, hide = true)]
        environmentd_extra_arg: Vec<String>,
    },
    /// Disable a region.
    #[clap(hide = true)]
    Disable {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
    /// List all enabled regions.
    List,
    /// Display a region's status.
    Status {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

/// Internal types, struct and enums
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Region {
    environment_controller_url: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Environment {
    environmentd_pgwire_address: String,
    environmentd_https_address: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct CloudProvider {
    region: String,
    region_controller_url: String,
    provider: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FronteggAppPassword {
    description: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    email: String,
    client_id: String,
    secret: String,
}

struct CloudProviderAndRegion {
    cloud_provider: CloudProvider,
    region: Option<Region>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let mut config = Configuration::load(args.profile.as_deref())?;

    match args.command {
        Commands::AppPassword(password_cmd) => {
            let profile = config.get_profile()?;

            let client = Client::new();
            let valid_profile = profile.validate(&client).await?;

            match password_cmd.command {
                AppPasswordSubommand::Create { name } => {
                    let api_token = generate_api_token(
                        profile.endpoint(),
                        &client,
                        valid_profile.frontegg_auth,
                        &name,
                    )
                    .await
                    .with_context(|| "failed to create a new app password")?;

                    println!("{}", api_token)
                }
                AppPasswordSubommand::List => {
                    let app_passwords = list_passwords(&client, &valid_profile)
                        .await
                        .with_context(|| "failed to retrieve app passwords")?;

                    println!("{0: <24} | {1: <24} ", "Name", "Created At");
                    println!("----------------------------------------------------");

                    app_passwords.iter().for_each(|app_password| {
                        let mut name = app_password.description.clone();

                        if name.len() > 20 {
                            let short_name = name[..20].to_string();
                            name = format!("{:}...", short_name);
                        }

                        println!("{0: <24} | {1: <24}", name, app_password.created_at);
                    })
                }
            }
        }

        Commands::Docs => {
            // Open the browser docs
            open::that(WEB_DOCS_URL).with_context(|| "Opening the browser.")?
        }

        Commands::Login {
            interactive,
            force,
            endpoint,
        } => {
            let profile = args.profile.unwrap_or_else(|| "default".into());
            config.update_current_profile(profile.clone());
            if config.get_profile().is_err() || force {
                if interactive {
                    login_with_console(endpoint, &profile, &mut config).await?
                } else {
                    login_with_browser(endpoint, &profile, &mut config).await?
                }
            }
        }

        Commands::Region { command } => {
            let client = Client::new();

            match command {
                RegionCommand::Enable {
                    cloud_provider_region,
                    version,
                    environmentd_extra_arg,
                } => {
                    let cloud_provider_region =
                        CloudProviderRegion::from_str(&cloud_provider_region)?;
                    let mut profile = config.get_profile()?;

                    let valid_profile = profile.validate(&client).await?;

                    let loading_spinner = run_loading_spinner("Enabling region...".to_string());
                    let cloud_provider = get_provider_by_region_name(
                        &client,
                        &valid_profile,
                        &cloud_provider_region,
                    )
                    .await
                    .with_context(|| "Retrieving cloud provider.")?;

                    let region = enable_region_environment(
                        &client,
                        &cloud_provider,
                        version,
                        environmentd_extra_arg,
                        &valid_profile,
                    )
                    .await
                    .with_context(|| "Enabling region.")?;

                    let environment = get_region_environment(&client, &valid_profile, &region)
                        .await
                        .with_context(|| "Retrieving environment data.")?;

                    loop {
                        if check_environment_health(&valid_profile, &environment)? {
                            break;
                        }
                    }

                    loading_spinner.finish_with_message(format!("{cloud_provider_region} enabled"));
                    profile.set_default_region(cloud_provider_region);
                }

                RegionCommand::Disable {
                    cloud_provider_region,
                } => {
                    let cloud_provider_region =
                        CloudProviderRegion::from_str(&cloud_provider_region)?;
                    let profile = config.get_profile()?;

                    let valid_profile = profile.validate(&client).await?;

                    let loading_spinner = run_loading_spinner("Disabling region...".to_string());
                    let cloud_provider = get_provider_by_region_name(
                        &client,
                        &valid_profile,
                        &cloud_provider_region,
                    )
                    .await
                    .with_context(|| "Retrieving cloud provider.")?;

                    disable_region_environment(&client, &cloud_provider, &valid_profile)
                        .await
                        .with_context(|| "Disabling region.")?;

                    loading_spinner
                        .finish_with_message(format!("{cloud_provider_region} disabled"));
                }

                RegionCommand::List => {
                    let profile = config.get_profile()?;

                    let valid_profile = profile.validate(&client).await?;

                    let cloud_providers = list_cloud_providers(&client, &valid_profile)
                        .await
                        .with_context(|| "Retrieving cloud providers.")?;
                    let cloud_providers_regions =
                        list_regions(&cloud_providers, &client, &valid_profile)
                            .await
                            .with_context(|| "Listing regions.")?;
                    cloud_providers_regions
                        .iter()
                        .for_each(|cloud_provider_region| {
                            print_region_enabled(cloud_provider_region);
                        });
                }

                RegionCommand::Status {
                    cloud_provider_region,
                } => {
                    let cloud_provider_region =
                        CloudProviderRegion::from_str(&cloud_provider_region)?;

                    let profile = config.get_profile()?;

                    let valid_profile = profile.validate(&client).await?;

                    let environment = get_provider_region_environment(
                        &client,
                        &valid_profile,
                        &cloud_provider_region,
                    )
                    .await
                    .with_context(|| "Retrieving cloud provider region.")?;
                    let health = check_environment_health(&valid_profile, &environment)?;

                    print_environment_status(environment, health);
                }
            }
        }

        Commands::Shell {
            cloud_provider_region,
        } => {
            let profile = config.get_profile()?;

            let cloud_provider_region = match cloud_provider_region {
                Some(ref cloud_provider_region) => {
                    CloudProviderRegion::from_str(cloud_provider_region)?
                }
                None => profile
                    .get_default_region()
                    .context("no region specified and no default region set")?,
            };

            let client = Client::new();
            let valid_profile = profile.validate(&client).await?;

            shell(client, valid_profile, cloud_provider_region)
                .await
                .with_context(|| "Running shell")?;
        }
    }

    config.close()
}
