//! Filesystem-safe, reversible names for buckets and objects.
//!
//! A name is percent-encoded: the bytes `A-Z a-z 0-9 _ -` are kept and every
//! other byte (including `.`, `/` and `%`) becomes `%XX` with upper-case hex.
//! An encoded name therefore never contains `.` or `/`, so it can never be
//! `.` or `..`, and it splits unambiguously from the `.<generation>.data` /
//! `.meta` suffixes.
//!
//! An encoding that is empty or longer than [`MAX_STEM_BYTES`] (filesystems
//! cap a file name at 255 bytes, and percent-encoding can triple the length)
//! is replaced by `~` followed by the hex SHA-256 of the name. `~` is never
//! produced by the encoding, so the two forms cannot collide. A hashed stem
//! is not reversible on its own; it does not need to be, because every
//! `.meta` record stores the real bucket and object name, and recovery takes
//! the names from there (checking that they map back to the file's stem).

use sha2::{Digest, Sha256};

/// Longest encoded stem used verbatim. Leaves room for the longest suffix,
/// `.<i64>.data` (at most 25 bytes), under the 255-byte file name limit.
pub const MAX_STEM_BYTES: usize = 200;

const HASH_PREFIX: char = '~';

fn keep(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// Percent-encode `name` (always reversible with [`decode`]).
pub fn encode(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        if keep(byte) {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

/// Invert [`encode`]. Returns `None` for anything [`encode`] cannot produce.
pub fn decode(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'%' {
            let hex = encoded.get(i + 1..i + 3)?;
            if hex.bytes().any(|b| b.is_ascii_lowercase()) {
                return None;
            }
            let value = u8::from_str_radix(hex, 16).ok()?;
            if keep(value) {
                return None;
            }
            out.push(value);
            i += 3;
        } else if keep(byte) {
            out.push(byte);
            i += 1;
        } else {
            return None;
        }
    }
    String::from_utf8(out).ok()
}

/// The file-name stem for a bucket or object name: its encoding, or a hash
/// when the encoding is empty or too long.
pub fn stem(name: &str) -> String {
    let encoded = encode(name);
    if encoded.is_empty() || encoded.len() > MAX_STEM_BYTES {
        let digest = Sha256::digest(name.as_bytes());
        let mut out = String::with_capacity(65);
        out.push(HASH_PREFIX);
        for byte in digest {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    } else {
        encoded
    }
}

/// Whether `candidate` is something [`stem`] can produce (used to skip
/// foreign entries such as `lost+found` in the data directory).
pub fn is_stem(candidate: &str) -> bool {
    if let Some(hash) = candidate.strip_prefix(HASH_PREFIX) {
        return hash.len() == 64 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    }
    !candidate.is_empty()
        && candidate.len() <= MAX_STEM_BYTES
        && decode(candidate).is_some_and(|name| encode(&name) == candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for name in [
            "plain",
            "wal/seg-000001",
            "a/b/c/",
            "..",
            ".",
            "../../etc/passwd",
            "%41%",
            "~tilde",
            "spaces and\ttabs\n",
            "ünïcödé/文件/🦀",
            "UPPER_lower-123",
        ] {
            let encoded = encode(name);
            assert!(!encoded.contains('/'), "{encoded}");
            assert!(!encoded.contains('.'), "{encoded}");
            assert_eq!(decode(&encoded).as_deref(), Some(name));
            let stem = stem(name);
            assert_eq!(stem, encoded);
            assert!(is_stem(&stem), "{stem}");
        }
    }

    #[test]
    fn long_and_empty_names_hash() {
        for name in ["", &"x".repeat(MAX_STEM_BYTES + 1), &"/".repeat(100)] {
            let stem = stem(name);
            assert!(stem.starts_with('~'), "{stem}");
            assert_eq!(stem.len(), 65);
            assert!(is_stem(&stem));
        }
        // Exactly at the limit stays readable.
        let name = "y".repeat(MAX_STEM_BYTES);
        assert_eq!(stem(&name), name);
        // Distinct long names get distinct stems.
        assert_ne!(stem(&"a".repeat(300)), stem(&"b".repeat(300)));
        // The longest data file name fits in 255 bytes.
        let longest = format!("{}.{}.data", "z".repeat(MAX_STEM_BYTES), i64::MIN);
        assert!(longest.len() <= 255, "{}", longest.len());
    }

    #[test]
    fn rejects_foreign_names() {
        for foreign in [
            "lost+found",
            ".",
            "..",
            "",
            "a.meta",
            "%2f",
            "%41",
            "%4",
            "~abc",
            "%FF",
        ] {
            assert!(!is_stem(foreign), "{foreign}");
        }
    }
}
