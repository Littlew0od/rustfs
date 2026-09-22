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

use rustfs_utils::path;
use std::path::Path;

fn parse_s3_path(path_input: &str) -> std::result::Result<(String, Option<String>), String> {
    if path_input.chars().any(char::is_control) {
        return Err("control characters are not allowed in TFTP paths".to_string());
    }

    let cleaned_path = path::clean(path_input);
    let (bucket, object) = path::path_to_bucket_object(&cleaned_path);

    if object.contains(path::GLOBAL_DIR_SUFFIX) {
        return Err("internal directory marker is not allowed in TFTP paths".to_string());
    }

    let key = if object.is_empty() { None } else { Some(object) };
    Ok((bucket, key))
}

fn parse_key_path(path_input: &str) -> std::result::Result<String, String> {
    // correct
    if path_input.chars().any(char::is_control) {
        return Err("control characters are not allowed in TFTP paths".to_string());
    }

    // correct
    let cleaned_path = path::clean(path_input);
    let object = if cfg!(target_os = "windows") {
        cleaned_path.replace('\\', "/")
    } else {
        cleaned_path
    };

    if object.contains(path::GLOBAL_DIR_SUFFIX) {
        return Err("internal directory marker is not allowed in TFTP paths".to_string());
    }

    Ok(object)
}

/// Resolve a TFTP request path into an S3 (bucket, key) pair.
///
/// When `default_bucket` is set, the entire path is the S3 key:
///   `/any/path`  → (default_bucket, "any/path")
///   `relative`   → (default_bucket, "relative")
///
/// When `default_bucket` is NOT set, the first path component is the bucket:
///   `/bucket/obj/key` → ("bucket", "obj/key")
///   `/just-bucket`    → error (no key after bucket)
pub fn resolve_tftp_path(default_bucket: Option<&str>, path: &Path) -> Result<(String, String), String> {
    let path_str = path.to_string_lossy();
    let trimmed = path_str.trim_start_matches('/');

    if let Some(bucket) = default_bucket {
        let trimmed = parse_key_path(trimmed)?;
        if trimmed.is_empty() {
            return Err(format!("path '{}' is a empty path;", path.display()));
        }
        Ok((bucket.to_string(), trimmed))
    } else {
        let (bucket, key) = parse_s3_path(&path_str).map_err(|e| format!("{}: {}", "Invalid path", e))?;
        let key = key.ok_or_else(|| {
            format!(
                "path '{}' has no key after bucket prefix; use /<bucket>/<key> or set RUSTFS_TFTP_BUCKET",
                path.display()
            )
        })?;
        Ok((bucket, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_with_default_bucket() {
        let bucket = "mybucket";

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/foo/bar.txt")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "foo/bar.txt");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("file.txt")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "file.txt");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/foo/bar/")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "foo/bar/");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/foo//bar")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "foo//bar");

        let (b, k) = resolve_tftp_path(Some(bucket), Path::new("/路径/文件.txt")).unwrap();
        assert_eq!(b, bucket);
        assert_eq!(k, "路径/文件.txt");

        let err = resolve_tftp_path(Some(bucket), Path::new("")).unwrap_err();
        assert!(err.contains("is a empty path"));

        let err = resolve_tftp_path(Some(bucket), Path::new("/")).unwrap_err();
        assert!(err.contains("is a empty path"));
    }

    #[test]
    fn resolve_without_default_bucket() {
        let (b, k) = resolve_tftp_path(None, Path::new("/mybucket/foo/bar.txt")).unwrap();
        assert_eq!(b, "mybucket");
        assert_eq!(k, "foo/bar.txt");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/a/b/c/d/e")).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "a/b/c/d/e");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/k")).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "k");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket__XLDIR__/mykey")).unwrap();
        assert_eq!(b, "bucket__XLDIR__");
        assert_eq!(k, "mykey");

        let (b, k) = resolve_tftp_path(None, Path::new("/存储桶/对象.txt")).unwrap();
        assert_eq!(b, "存储桶");
        assert_eq!(k, "对象.txt");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/../other/key")).unwrap();
        assert_eq!(b, "other");
        assert_eq!(k, "key");

        let (b, k) = resolve_tftp_path(None, Path::new("/bucket/./key")).unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "key");

        let err = resolve_tftp_path(None, Path::new("/just-bucket")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("/just-bucket/")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("nobucket")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("/bucket/..")).unwrap_err();
        assert!(err.contains("no key after bucket prefix"));

        let err = resolve_tftp_path(None, Path::new("/bucket/key\x00hidden")).unwrap_err();
        assert!(err.contains("Invalid path"));
        assert!(err.contains("control characters"));

        let err = resolve_tftp_path(None, Path::new("/bucket/key__XLDIR__")).unwrap_err();
        assert!(err.contains("Invalid path"));
        assert!(err.contains("internal directory marker"));
    }
}
