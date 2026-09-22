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

//! Read-side helper for the TFTP server.

use crate::common::client::s3::StorageBackend;
use crate::common::session::SessionContext;
use async_tftp::packet;
use futures_util::stream::{IntoAsyncRead, MapErr};
use futures_util::TryStreamExt;
use s3s::dto::StreamingBlob;
use tokio::sync::OwnedSemaphorePermit;
use std::io;
use tracing::{error, info};
use futures_lite::AsyncRead;
use std::pin::Pin;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_SERVER: &str = "tftp_server";
const EVENT_TFTP_RRQ_STATE: &str = "tftp_rrq_state";

pub(super) type TftpReader = IntoAsyncRead<MapErr<StreamingBlob, fn(Box<dyn std::error::Error + Send + Sync + 'static>) -> std::io::Error>>;

pub struct ReadHandler {
    reader: Pin<Box<TftpReader>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl AsyncRead for ReadHandler {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<io::Result<usize>>
    {
        self.reader.as_mut().poll_read(cx, buf)
    }
}

pub(super) async fn open_reader<S: StorageBackend + Send + Sync + 'static>(
	storage: &S,
	bucket: &str,
	key: &str,
	session_ctx: &SessionContext,
    _permit: OwnedSemaphorePermit,
) -> Result<(ReadHandler, Option<u64>), packet::Error> {
	let output = storage
		.get_object(bucket, key, session_ctx.credentials(), None)
		.await
		.map_err(|e| {
			error!(
				event = EVENT_TFTP_RRQ_STATE,
				component = LOG_COMPONENT_PROTOCOLS,
				subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
				bucket = %bucket, key = %key, error = %e,
				"S3 get_object failed for TFTP RRQ"
			);
			packet::Error::FileNotFound
		})?;

	let content_length = output.content_length.unwrap_or(0).max(0) as u64;

	let Some(body) = output.body else {
		error!(
			event = EVENT_TFTP_RRQ_STATE,
			component = LOG_COMPONENT_PROTOCOLS,
			subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
			bucket = %bucket, key = %key,
			"S3 get_object returned no body for TFTP RRQ"
		);
		return Err(packet::Error::FileNotFound);
	};

	fn boxed_error_to_io(e: Box<dyn std::error::Error + Send + Sync>) -> io::Error {
		io::Error::new(io::ErrorKind::Other, e)
	}

	let reader = body
		.map_err(boxed_error_to_io as fn(Box<dyn std::error::Error + Send + Sync>) -> io::Error)
		.into_async_read();

	info!(
		event = EVENT_TFTP_RRQ_STATE,
		component = LOG_COMPONENT_PROTOCOLS,
		subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
		bucket = %bucket, key = %key,
		"TFTP RRQ: loaded from S3"
	);

	Ok((ReadHandler {
        reader: Box::pin(reader),
        _permit,
    }, Some(content_length)))
}