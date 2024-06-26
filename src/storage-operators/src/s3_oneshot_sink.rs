// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A sink operator that writes to s3.

use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::anyhow;
use aws_types::sdk_config::SdkConfig;
use bytesize::ByteSize;
use differential_dataflow::{Collection, Hashable};
use http::Uri;
use mz_aws_util::s3_uploader::{
    CompletedUpload, S3MultiPartUploadError, S3MultiPartUploader, S3MultiPartUploaderConfig,
};
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_ore::task::JoinHandleExt;
use mz_pgcopy::{encode_copy_format, CopyFormatParams};
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::S3UploadInfo;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Broadcast, ConnectLoop, Feedback};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::{debug, info};

/// Copy the rows from the input collection to s3.
/// `onetime_callback` is used to send the final count of rows uploaded to s3,
/// or an error message if the operator failed.
/// `sink_id` is used to identify the sink for logging purposes and as a
/// unique prefix for files created by the sink.
pub fn copy_to<G, F>(
    input_collection: Collection<G, ((Row, u64), ()), Diff>,
    err_collection: Collection<G, ((DataflowError, u64), ()), Diff>,
    up_to: Antichain<G::Timestamp>,
    connection_details: S3UploadInfo,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    sink_id: GlobalId,
    connection_id: GlobalId,
    onetime_callback: F,
) where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<u64, String>) -> () + 'static,
{
    let mut scope = input_collection.scope();
    let worker_id = scope.index();
    let is_leader = usize::cast_from((sink_id, "leader").hashed()) % scope.peers() == worker_id;

    let mut builder = AsyncOperatorBuilder::new("CopyToS3".to_string(), scope.clone());

    let mut input_handle = builder.new_disconnected_input(&input_collection.inner, Pipeline);
    let mut error_handle = builder.new_disconnected_input(&err_collection.inner, Pipeline);

    // Signal mechanism from leader to all workers to start the upload after it has verified
    // the integrity of the S3 bucket path.
    // A `()` datum along this edge indicates success. A `[]` frontier indicates failure. Note that
    // this is disconnected and has a `0` summary, as its unrelated to the frontier of the output.
    let (mut start_handle, start_stream) = builder.new_output();
    let (start_feedback_handle, start_feedback_stream) = scope.feedback(Default::default());
    let mut start_input = builder.new_disconnected_input(&start_feedback_stream, Pipeline);
    start_stream.broadcast().connect_loop(start_feedback_handle);

    builder.build(move |caps| async move {
        let [start_signal_cap] = caps.try_into().unwrap();

        while let Some(event) = error_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    for (((error, _), _), ts, _) in data {
                        if !up_to.less_equal(&ts) {
                            onetime_callback(Err(error.to_string()));
                            return;
                        }
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if PartialOrder::less_equal(&up_to, &frontier) {
                        // No error, break from loop and proceed
                        break;
                    }
                }
            }
        }

        let sdk_config = match aws_connection
            .load_sdk_config(&connection_context, connection_id, InTask::Yes)
            .await
        {
            Ok(sdk_config) => sdk_config,
            Err(e) => {
                onetime_callback(Err(e.to_string()));
                return;
            }
        };

        // Check that the S3 bucket path is empty before beginning the upload.
        // We check the S3 bucket path from a single worker to avoid a race
        // between checking the path vs workers uploading objects to the path.
        // We also race against other replicas running the same sink, so we allow
        // for objects to exist in the path if they were created by this sink
        // (identified by the sink_id prefix).
        if is_leader {
            info!(%worker_id, "leader worker verifying S3 bucket path is empty");
            let (bucket, path_prefix) =
                CopyToS3Uploader::extract_s3_bucket_path(&connection_details.prefix);
            let client = mz_aws_util::s3::new_client(&sdk_config);
            match mz_aws_util::s3::list_bucket_path(&client, &bucket, &path_prefix).await {
                Ok(Some(files)) => {
                    let sink_id_prefix = format!("batch-{}-", sink_id);
                    let files = files
                        .iter()
                        .filter(|file| !file.starts_with(&sink_id_prefix))
                        .collect::<Vec<_>>();
                    if !files.is_empty() {
                        onetime_callback(Err(format!(
                            "S3 bucket path is not empty, contains: {:?}",
                            files
                        )));
                        return;
                    }
                }
                Err(e) => {
                    onetime_callback(Err(e.to_string()));
                    return;
                }
                _ => {}
            }
            // Send the signal to all workers to start the upload.
            start_handle.give(&start_signal_cap, ()).await;
            drop(start_signal_cap);
        } else {
            info!(%worker_id, "non-leader worker waiting for start signal from leader");
            // Workers wait for the signal from the leader to start the upload.
            drop(start_signal_cap);
            loop {
                match start_input.next().await {
                    Some(AsyncEvent::Data(_ts, _data)) => {
                        // received the signal from the leader, break from loop and proceed
                        break;
                    }
                    Some(AsyncEvent::Progress(_)) => continue,
                    None => {
                        onetime_callback(Err(
                            "Failed to receive start signal from leader".to_string()
                        ));
                        return;
                    }
                }
            }
        }

        // Map of an uploader per batch.
        let mut s3_uploaders: BTreeMap<u64, CopyToS3Uploader> = BTreeMap::new();
        let mut row_count = 0;
        while let Some(event) = input_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    for (((row, batch), ()), ts, diff) in data {
                        if !up_to.less_equal(&ts) {
                            if diff < 0 {
                                onetime_callback(Err(format!(
                                    "Invalid data in source errors, saw retractions ({}) for row that does not exist", diff * -1,
                                )));
                                return;
                            }
                            row_count += u64::try_from(diff).unwrap();
                            let uploader = s3_uploaders
                                .entry(batch)
                                .or_insert_with(|| {
                                    debug!("worker_id: {} will be handling batch: {}", worker_id, batch);
                                    let file_name_prefix = format!("batch-{}-{:04}", &sink_id, batch);
                                    CopyToS3Uploader::new(sdk_config.clone(), connection_details.clone(), file_name_prefix)
                                });
                            for _ in 0..diff {
                                match uploader.append_row(&row).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        onetime_callback(Err(e.to_string()));
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if PartialOrder::less_equal(&up_to, &frontier) {
                        for uploader in s3_uploaders.values_mut() {
                            match uploader.flush().await {
                                Ok(()) => {}
                                Err(e) => {
                                    onetime_callback(Err(e.to_string()));
                                    return;
                                }
                            }
                        }
                        // We are done, send the final count.
                        onetime_callback(Ok(row_count));
                        return;
                    }
                }
            }
        }
    });
}

/// Required state to upload batches to S3
struct CopyToS3Uploader {
    /// The output description.
    desc: RelationDesc,
    /// Params to format the data.
    format: CopyFormatParams<'static>,
    /// The index of the current file.
    file_index: usize,
    /// The prefix for the file names.
    file_name_prefix: String,
    /// The s3 bucket.
    bucket: String,
    ///The path prefix where the files should be uploaded to.
    path_prefix: String,
    /// The desired file size. A new file upload will be started
    /// when the size exceeds this amount.
    max_file_size: u64,
    /// The aws sdk config.
    /// This is an option so that we can get an owned value later to move to a
    /// spawned tokio task.
    sdk_config: Option<SdkConfig>,
    /// Multi-part uploader for the current file.
    /// Keeping the uploader in an `Option` to later take owned value.
    current_file_uploader: Option<S3MultiPartUploader>,
    /// Temporary buffer to store the encoded bytes.
    /// Currently at a time this will only store one single encoded row
    /// before getting added to the `current_file_uploader`'s buffer.
    buf: Vec<u8>,
}

impl CopyToS3Uploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        file_name_prefix: String,
    ) -> CopyToS3Uploader {
        let (bucket, path_prefix) = Self::extract_s3_bucket_path(&connection_details.prefix);
        CopyToS3Uploader {
            desc: connection_details.desc,
            sdk_config: Some(sdk_config),
            format: connection_details.format,
            file_name_prefix,
            bucket,
            path_prefix,
            max_file_size: connection_details.max_file_size,
            file_index: 0,
            current_file_uploader: None,
            buf: Vec::new(),
        }
    }

    /// Creates the uploader for the next file and starts the multi part upload.
    async fn start_new_file_upload(&mut self) -> Result<(), anyhow::Error> {
        self.flush().await?;
        assert!(self.current_file_uploader.is_none());

        self.file_index += 1;
        let file_path = self.current_file_path();

        let bucket = self.bucket.clone();
        info!("starting upload: bucket {}, file {}", &bucket, &file_path);
        let sdk_config = self
            .sdk_config
            .take()
            .expect("sdk_config should always be present");
        let max_file_size = self.max_file_size;
        // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
        let handle = mz_ore::task::spawn(|| "s3_uploader::try_new", async move {
            let uploader = S3MultiPartUploader::try_new(
                &sdk_config,
                bucket,
                file_path,
                S3MultiPartUploaderConfig {
                    part_size_limit: ByteSize::mib(10).as_u64(),
                    file_size_limit: max_file_size,
                },
            )
            .await;
            (uploader, sdk_config)
        });
        let (uploader, sdk_config) = handle.wait_and_assert_finished().await;
        self.sdk_config = Some(sdk_config);
        self.current_file_uploader = Some(uploader?);
        Ok(())
    }

    fn current_file_path(&self) -> String {
        // TODO: remove hard-coded file extension .csv
        format!(
            "{}/{}-{:04}.csv",
            self.path_prefix, self.file_name_prefix, self.file_index
        )
    }

    fn extract_s3_bucket_path(prefix: &str) -> (String, String) {
        // This url is already validated to be a valid s3 url in sequencer.
        let uri = Uri::from_str(prefix).expect("valid s3 url");
        let bucket = uri.host().expect("s3 bucket");
        let path = uri.path().trim_start_matches('/').trim_end_matches('/');
        (bucket.to_string(), path.to_string())
    }

    /// Finishes any remaining in-progress upload.
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Some(uploader) = self.current_file_uploader.take() {
            let current_file = self.current_file_path();
            // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
            let handle =
                mz_ore::task::spawn(|| "s3_uploader::finish", async { uploader.finish().await });
            let CompletedUpload {
                part_count,
                total_bytes_uploaded,
            } = handle.wait_and_assert_finished().await?;
            info!(
                "finished upload: bucket {}, file {}, bytes_uploaded {}, parts_uploaded {}",
                &self.bucket, current_file, total_bytes_uploaded, part_count
            );
        }
        Ok(())
    }

    async fn upload_buffer(&mut self) -> Result<(), S3MultiPartUploadError> {
        assert!(!self.buf.is_empty());
        assert!(self.current_file_uploader.is_some());

        let mut uploader = self.current_file_uploader.take().unwrap();
        // TODO: Make buf a Bytes so it can be cheaply cloned.
        let buf = std::mem::take(&mut self.buf);
        // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
        let handle = mz_ore::task::spawn(|| "s3_uploader::buffer_chunk", async move {
            let result = uploader.buffer_chunk(&buf).await;
            (uploader, buf, result)
        });
        let (uploader, buf, result) = handle.wait_and_assert_finished().await;
        self.current_file_uploader = Some(uploader);
        self.buf = buf;

        let _ = result?;
        Ok(())
    }

    /// Appends the row to the in-progress upload where it is buffered till it reaches the configured
    /// `part_size_limit` after which the `S3MultiPartUploader` will upload that part. In case it will
    /// exceed the max file size of the ongoing upload, then a new `S3MultiPartUploader` for a new file will
    /// be created and the row data will be appended there.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        self.buf.clear();
        // encode the row and write to temp buffer.
        encode_copy_format(&self.format, row, self.desc.typ(), &mut self.buf)
            .map_err(|_| anyhow!("error encoding row"))?;

        if self.current_file_uploader.is_none() {
            self.start_new_file_upload().await?;
        }

        match self.upload_buffer().await {
            Ok(_) => Ok(()),
            Err(S3MultiPartUploadError::UploadExceedsMaxFileLimit(_)) => {
                // Start a multi part upload of next file.
                self.start_new_file_upload().await?;
                // Upload data for the new part.
                self.upload_buffer().await?;
                Ok(())
            }
            Err(e) => Err(e),
        }?;

        Ok(())
    }
}

/// On CI, these tests are enabled by adding the scratch-aws-access plugin
/// to the `cargo-test` step in `ci/test/pipeline.template.yml` and setting
/// `MZ_S3_UPLOADER_TEST_S3_BUCKET` in
/// `ci/test/cargo-test/mzcompose.py`.
///
/// For a Materialize developer, to opt in to these tests locally for
/// development, follow the AWS access guide:
///
/// ```text
/// https://www.notion.so/materialize/AWS-access-5fbd9513dcdc4e11a7591e8caa5f63fe
/// ```
///
/// then running `source src/aws-util/src/setup_test_env_mz.sh`. You will also have
/// to run `aws sso login` if you haven't recently.
#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use mz_repr::{ColumnName, ColumnType, Datum, RelationType};
    use uuid::Uuid;

    use super::*;

    fn s3_bucket_path_for_test() -> Option<(String, String)> {
        let bucket = match std::env::var("MZ_S3_UPLOADER_TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return None;
            }
        };

        let prefix = Uuid::new_v4().to_string();
        let path = format!("cargo_test/{}/file", prefix);
        Some((bucket, path))
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/materialize/issues/18898
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    async fn test_multiple_files() -> Result<(), anyhow::Error> {
        let sdk_config = mz_aws_util::defaults().load().await;
        let (bucket, path) = match s3_bucket_path_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };
        let typ: RelationType = RelationType::new(vec![ColumnType {
            scalar_type: mz_repr::ScalarType::String,
            nullable: true,
        }]);
        let column_names = vec![ColumnName::from("col1")];
        let desc = RelationDesc::new(typ, column_names.into_iter());
        let mut uploader = CopyToS3Uploader::new(
            sdk_config.clone(),
            S3UploadInfo {
                prefix: format!("s3://{}/{}", bucket, path),
                // this is only for testing, users will not be able to set value smaller than 16MB.
                max_file_size: ByteSize::b(6).as_u64(),
                desc,
                format: CopyFormatParams::Csv(Default::default()),
            },
            "part".to_string(),
        );
        let mut row = Row::default();
        // Even though this will exceed max_file_size, it should be successfully uploaded in a single file.
        row.packer().push(Datum::from("1234567"));
        uploader.append_row(&row).await?;

        // Since the max_file_size is 6B, this row will be uploaded to a new file.
        row.packer().push(Datum::Null);
        uploader.append_row(&row).await?;

        row.packer().push(Datum::from("5678"));
        uploader.append_row(&row).await?;

        uploader.flush().await?;

        // Based on the max_file_size, the uploader should have uploaded two
        // files, part-0001.csv and part-0002.csv
        let s3_client = mz_aws_util::s3::new_client(&sdk_config);
        let first_file = s3_client
            .get_object()
            .bucket(bucket.clone())
            .key(format!("{}/part-0001.csv", path))
            .send()
            .await
            .unwrap();

        let body = first_file.body.collect().await.unwrap().into_bytes();
        let expected_body: &[u8] = b"1234567\n";
        assert_eq!(body, *expected_body);

        let second_file = s3_client
            .get_object()
            .bucket(bucket)
            .key(format!("{}/part-0002.csv", path))
            .send()
            .await
            .unwrap();

        let body = second_file.body.collect().await.unwrap().into_bytes();
        let expected_body: &[u8] = b"\n5678\n";
        assert_eq!(body, *expected_body);

        Ok(())
    }
}
