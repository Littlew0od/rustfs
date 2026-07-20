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

use anyhow::Result;
use clap::Parser;
use tracing::info;

/// Configuration for RustFS TFTP Server
#[derive(Parser, Debug, Clone)]
#[command(
    name = "rustfs-tftp-server",
    about = "RustFS TFTP (Trivial File Transfer Protocol) Server for S3 operations",
    version,
    long_about = r#"
RustFS TFTP (Trivial File Transfer Protocol) Server for S3 operations

This server provides get_object and put_object operations through the Trivial File Transfer Protocol(TFTP).

ENVIRONMENT VARIABLES:
  All command-line options can also be set via environment variables.
  Command-line arguments take precedence over environment variables.

EXAMPLES:
  # Using command-line arguments
  rustfs-tftp-server --access-key-id your_key --secret-access-key your_secret

  # Using environment variables
  export AWS_ACCESS_KEY_ID=your_key
  export AWS_SECRET_ACCESS_KEY=your_secret
  rustfs-tftp-server

  # Mixed usage (command-line overrides environment)
  export AWS_REGION=us-east-1
  rustfs-tftp-server --access-key-id mykey --secret-access-key mysecret --endpoint-url http://localhost:9000
"#
)]
pub struct Config {
    /// AWS Access Key ID
    #[arg(
        long = "access-key-id",
        env = "AWS_ACCESS_KEY_ID",
        help = "AWS Access Key ID for S3 authentication"
    )]
    pub access_key_id: Option<String>,

    /// AWS Secret Access Key
    #[arg(
        long = "secret-access-key",
        env = "AWS_SECRET_ACCESS_KEY",
        help = "AWS Secret Access Key for S3 authentication"
    )]
    pub secret_access_key: Option<String>,

    /// AWS Region
    #[arg(
        long = "region",
        env = "AWS_REGION",
        default_value = "us-east-1",
        help = "AWS region to use for S3 operations"
    )]
    pub region: String,

    /// Custom S3 endpoint URL
    /// 这个不能为空吧
    #[arg(
        long = "endpoint-url",
        env = "AWS_ENDPOINT_URL",
        help = "Custom S3 endpoint URL (for MinIO, LocalStack, etc.)"
    )]
    pub endpoint_url: Option<String>,

    /// Log level
    #[arg(
        long = "log-level",
        env = "RUST_LOG",
        default_value = "rustfs_tftp_server=info",
        help = "Log level configuration"
    )]
    pub log_level: String,

    /// Force path-style addressing
    #[arg(
        long = "force-path-style",
        help = "Force path-style S3 addressing (automatically enabled for custom endpoints)"
    )]
    pub force_path_style: bool,

    /// Default S3 bucket for TFTP operations.
    /// When set, all TFTP paths without an explicit bucket prefix use this bucket.
    /// When not set, every TFTP path must include the bucket as its first component
    /// (e.g. `/mybucket/object/key`).
    #[arg(
        long = "bucket",
        env = "RUSTFS_TFTP_BUCKET",
        help = "Default S3 bucket; when omitted the bucket is taken from the first path component"
    )]
    pub bucket: Option<String>,

    /// Temporary directory for staging downloads/uploads
    #[arg(
        long = "temp-dir",
        env = "RUSTFS_TFTP_TEMP_DIR",
        help = "Temporary directory for staging S3 objects"
    )]
    pub temp_dir: Option<String>,

    // Port of the TFTP server to listen on (default: 69)
    #[arg(
        long = "tftp-port",
        env = "RUSTFS_TFTP_PORT",
        default_value = "69",
        help = "Port for the TFTP server to listen on (default: 69)"
    )]
    pub tftp_port: u16,
}

impl Config {
    pub fn new() -> Self {
        Config::parse()
    }

    pub fn validate(&self) -> Result<()> {
        if self.access_key_id.is_none() {
            anyhow::bail!("AWS Access Key ID is required. Set via --access-key-id or AWS_ACCESS_KEY_ID environment variable");
        }

        if self.secret_access_key.is_none() {
            anyhow::bail!(
                "AWS Secret Access Key is required. Set via --secret-access-key or AWS_SECRET_ACCESS_KEY environment variable"
            );
        }

        Ok(())
    }

    pub fn access_key_id(&self) -> &str {
        self.access_key_id.as_ref().expect("Access key ID should be validated")
    }

    pub fn secret_access_key(&self) -> &str {
        self.secret_access_key
            .as_ref()
            .expect("Secret access key should be validated")
    }

    pub fn log_configuration(&self) {
        let access_key_display = self
            .access_key_id
            .as_ref()
            .map(|key| {
                if key.len() > 8 {
                    format!("{}...{}", &key[..4], &key[key.len() - 4..])
                } else {
                    "*".repeat(key.len())
                }
            })
            .unwrap_or_else(|| "Not set".to_string());

        let endpoint_display = self
            .endpoint_url
            .as_ref()
            .map(|url| format!("Custom endpoint: {url}"))
            .unwrap_or_else(|| "Default AWS endpoints".to_string());

        info!("Configuration:");
        info!("  AWS Region: {}", self.region);
        info!("  AWS Access Key ID: {}", access_key_display);
        info!("  AWS Secret Access Key: [HIDDEN]");
        info!("  S3 Endpoint: {}", endpoint_display);
        info!("  Force Path Style: {}", self.force_path_style);
        info!("  Log Level: {}", self.log_level);
        info!("  TFTP Port: {}", self.tftp_port);
        info!("  Default Bucket: {}", self.bucket.as_deref().unwrap_or("(from path)"));
        info!("  Temp Directory: {}", self.temp_dir.as_deref().unwrap_or("(system temp)"));
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            access_key_id: None,
            secret_access_key: None,
            region: "us-east-1".to_string(),
            endpoint_url: None,
            log_level: "rustfs_tftp_server=info".to_string(),
            force_path_style: false,
            bucket: None,
            temp_dir: None,
            tftp_port: 69,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation_success() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            ..Config::default()
        };

        assert!(config.validate().is_ok());
        assert_eq!(config.access_key_id(), "test_key");
        assert_eq!(config.secret_access_key(), "test_secret");
    }

    #[test]
    fn test_config_validation_missing_access_key() {
        let config = Config {
            access_key_id: None,
            secret_access_key: Some("test_secret".to_string()),
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Access Key ID"));
    }

    #[test]
    fn test_config_validation_missing_secret_key() {
        let config = Config {
            access_key_id: Some("test_key".to_string()),
            secret_access_key: None,
            ..Config::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Secret Access Key"));
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.log_level, "rustfs_tftp_server=info");
        assert_eq!(config.tftp_port, 69);
        assert!(!config.force_path_style);
        assert!(config.access_key_id.is_none());
        assert!(config.secret_access_key.is_none());
        assert!(config.endpoint_url.is_none());
    }
}
