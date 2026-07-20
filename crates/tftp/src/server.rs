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
use async_tftp::packet;
use async_tftp::server::{Handler, TftpServerBuilder};
use blocking::Unblock;
use futures_lite::{AsyncRead, AsyncWrite};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::s3_client::S3Client;

// ---------------------------------------------------------------------------
// S3TempFileReader - wraps Unblock<File>, cleans up the temp file on drop
// ---------------------------------------------------------------------------

/// A reader that serves a file downloaded from S3 to a local temp file.
/// On drop, the temp file is removed asynchronously.
pub struct S3TempFileReader {
    inner: Unblock<std::fs::File>,
    temp_path: PathBuf,
}

impl S3TempFileReader {
    fn new(file: std::fs::File, temp_path: PathBuf) -> Self {
        S3TempFileReader {
            inner: Unblock::new(file),
            temp_path,
        }
    }
}

impl AsyncRead for S3TempFileReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl Drop for S3TempFileReader {
    fn drop(&mut self) {
        let temp_path = self.temp_path.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::fs::remove_file(&temp_path).await {
                warn!(path = %temp_path.display(), error = %e, "Failed to remove TFTP temp file");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// S3UploadWriter - wraps Unblock<File>, uploads to S3 on drop
// ---------------------------------------------------------------------------

/// A writer that stages TFTP uploads to a local temp file.
/// On drop (after the TFTP transfer completes), the file is uploaded to S3
/// and the temp file is removed.
pub struct S3UploadWriter {
    inner: Unblock<std::fs::File>,
    temp_path: PathBuf,
    s3_client: S3Client,
    bucket: String,
    key: String,
}

impl S3UploadWriter {
    fn new(file: std::fs::File, temp_path: PathBuf, s3_client: S3Client, bucket: String, key: String) -> Self {
        S3UploadWriter {
            inner: Unblock::new(file),
            temp_path,
            s3_client,
            bucket,
            key,
        }
    }
}

impl AsyncWrite for S3UploadWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

impl Drop for S3UploadWriter {
    fn drop(&mut self) {
        let temp_path = self.temp_path.clone();
        let s3_client = self.s3_client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        tokio::spawn(async move {
            match s3_client
                .upload_file(temp_path.to_string_lossy().as_ref(), &bucket, &key)
                .await
            {
                Ok(result) => {
                    info!(
                        bucket = %bucket, key = %key, size = result.file_size,
                        "TFTP upload to S3 completed"
                    );
                }
                Err(e) => {
                    error!(
                        bucket = %bucket, key = %key, error = %e,
                        "Failed to upload TFTP file to S3"
                    );
                }
            }

            if let Err(e) = tokio::fs::remove_file(&temp_path).await {
                warn!(path = %temp_path.display(), error = %e, "Failed to remove TFTP temp file");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// TftpdHandler
// ---------------------------------------------------------------------------

pub struct TftpdHandler {
    s3_client: S3Client,
    default_bucket: Option<String>,
    temp_dir: Option<PathBuf>,
}

impl TftpdHandler {
    pub async fn from_config(config: &Config) -> Result<Self> {
        let s3_client = S3Client::new(config).await?;

        // Verify default bucket exists when configured (best-effort)
        if let Some(ref bucket) = config.bucket {
            match s3_client.head_bucket(bucket).await {
                Ok(_) => info!(bucket = %bucket, "Default S3 bucket verified"),
                Err(e) => error!(bucket = %bucket, error = %e, "Could not verify default bucket"),
            }
        } else {
            match s3_client.health_check().await {
                Ok(_) => info!("S3 health check passed"),
                Err(e) => error!(error = %e, "S3 health check failed"),
            }
        }

        let temp_dir = config.temp_dir.as_ref().map(PathBuf::from);

        Ok(TftpdHandler {
            s3_client,
            default_bucket: config.bucket.clone(),
            temp_dir,
        })
    }

    /// Resolve a TFTP path into an S3 (bucket, key) pair.
    ///
    /// When `default_bucket` is set, the entire path is the S3 key:
    ///   `/any/path`  → bucket=<default>, key="any/path"
    ///   `relative`   → bucket=<default>, key="relative"
    ///
    /// When `default_bucket` is NOT set, the first path component is the bucket:
    ///   `/bucket/obj/key` → bucket="bucket", key="obj/key"
    ///   `bucket/obj/key`  → bucket="bucket", key="obj/key"
    ///   `/just-key`       → ERROR (no key after bucket prefix)
    ///   `error-path`      → ERROR (no bucket prefix)
    fn resolve_path(&self, path: &Path) -> Result<(String, String), packet::Error> {
        let path_str = path.to_string_lossy();
        let key = path_str.trim_start_matches('/');

        if let Some(ref bucket) = self.default_bucket {
            // Default bucket configured: entire path is the key
            Ok((bucket.clone(), key.to_string()))
        } else if let Some((first, rest)) = key.split_once('/') {
            // No default bucket: first component is bucket, rest is key
            if rest.is_empty() {
                return Err(packet::Error::Msg(format!(
                    "path '{}' has no key after bucket prefix; use /<bucket>/<key>, or set default bucket with `--bucket` or `RUSTFS_TFTP_BUCKET`",
                    path.display()
                )));
            }
            Ok((first.to_string(), rest.to_string()))
        } else {
            // No default bucket and no slash → cannot determine bucket
            Err(packet::Error::Msg(format!(
                "no default bucket configured and path '{}' has no bucket prefix; use /<bucket>/<key>",
                path.display()
            )))
        }
    }

    /// Create a temp file path for staging transfers.
    fn temp_file_path(&self) -> PathBuf {
        let dir = self.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
        let filename = format!("rustfs-tftp-{}", Uuid::new_v4());
        dir.join(filename)
    }
}

impl Handler for TftpdHandler {
    type Reader = S3TempFileReader;
    type Writer = S3UploadWriter;

    async fn read_req_open(&mut self, _client: &SocketAddr, path: &Path) -> Result<(Self::Reader, Option<u64>), packet::Error> {
        let (bucket, key) = self.resolve_path(path)?;
        debug!(bucket = %bucket, key = %key, "TFTP RRQ");

        let temp_path = self.temp_file_path();

        // Download from S3 to a temp file
        let (file_size, abs_path) = self
            .s3_client
            .download_object_to_file(&bucket, &key, temp_path.to_string_lossy().as_ref())
            .await
            .map_err(|e| {
                error!(bucket = %bucket, key = %key, error = %e, "S3 download failed for TFTP RRQ");
                packet::Error::FileNotFound
            })?;

        info!(
            bucket = %bucket, key = %key, size = file_size,
            temp = %abs_path,
            "TFTP RRQ: downloaded from S3"
        );

        let file = std::fs::File::open(&temp_path).map_err(|e| {
            error!(path = %temp_path.display(), error = %e, "Failed to open temp file for TFTP read");
            packet::Error::from(e)
        })?;

        let reader = S3TempFileReader::new(file, temp_path);

        Ok((reader, Some(file_size)))
    }

    async fn write_req_open(
        &mut self,
        _client: &SocketAddr,
        path: &Path,
        _size: Option<u64>,
    ) -> Result<Self::Writer, packet::Error> {
        let (bucket, key) = self.resolve_path(path)?;
        debug!(bucket = %bucket, key = %key, "TFTP WRQ");

        let temp_path = self.temp_file_path();

        // Create the temp file for staging the upload
        if let Some(parent) = temp_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error!(dir = %parent.display(), error = %e, "Failed to create temp dir for TFTP write");
                packet::Error::from(e)
            })?;
        }

        let file = std::fs::File::create(&temp_path).map_err(|e| {
            error!(path = %temp_path.display(), error = %e, "Failed to create temp file for TFTP write");
            packet::Error::from(e)
        })?;

        info!(
            bucket = %bucket, key = %key,
            temp = %temp_path.display(),
            "TFTP WRQ: staging upload to temp file"
        );

        let writer = S3UploadWriter::new(file, temp_path, self.s3_client.clone(), bucket, key);

        Ok(writer)
    }
}

// ---------------------------------------------------------------------------
// Server bootstrap
// ---------------------------------------------------------------------------

/// Build and start the TFTP server.
pub async fn run(config: Config) -> Result<()> {
    let handler = TftpdHandler::from_config(&config).await?;

    let bind_addr: SocketAddr = format!("0.0.0.0:{}", config.tftp_port)
        .parse()
        .expect("Invalid TFTP bind address");

    let tftpd = TftpServerBuilder::with_handler(handler).bind(bind_addr).build().await?;

    info!("TFTP server listening on {}", tftpd.listen_addr()?);
    tftpd.serve().await?;

    Ok(())
}
