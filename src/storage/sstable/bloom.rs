use crc32fast::Hasher;
use thiserror::Error;

pub const DEFAULT_BITS_PER_KEY: usize = 10;
pub const DEFAULT_HASH_FUNCTIONS: u8 = 10;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BloomDecodeError {
    #[error("bloom filter payload is truncated")]
    Truncated,
    #[error("bloom filter has invalid bit length")]
    InvalidBitLength,
    #[error("bloom filter has invalid hash count")]
    InvalidHashCount,
}

#[derive(Debug, Clone)]
pub struct BloomFilterBuilder {
    bits_per_key: usize,
    hash_functions: u8,
    keys: Vec<Vec<u8>>,
}

impl Default for BloomFilterBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_BITS_PER_KEY, DEFAULT_HASH_FUNCTIONS)
    }
}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: usize, hash_functions: u8) -> Self {
        Self {
            bits_per_key: bits_per_key.max(1),
            hash_functions: hash_functions.max(1),
            keys: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }

    pub fn build(self) -> BloomFilter {
        let key_count = self.keys.len();
        let bit_len = (key_count.saturating_mul(self.bits_per_key)).max(64);
        let byte_len = bit_len.div_ceil(8);

        let mut filter = BloomFilter {
            bits: vec![0_u8; byte_len],
            bit_len: bit_len as u32,
            hash_functions: self.hash_functions,
        };

        for key in self.keys {
            filter.insert(&key);
        }

        filter
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_len: u32,
    hash_functions: u8,
}

impl BloomFilter {
    pub fn from_keys(keys: &[Vec<u8>], bits_per_key: usize, hash_functions: u8) -> Self {
        let mut builder = BloomFilterBuilder::new(bits_per_key, hash_functions);
        for key in keys {
            builder.add_key(key);
        }
        builder.build()
    }

    pub fn insert(&mut self, key: &[u8]) {
        if self.bit_len == 0 {
            return;
        }

        let (hash1, hash2) = base_hashes(key);
        for i in 0..self.hash_functions {
            let composite = hash1.wrapping_add((i as u32).wrapping_mul(hash2));
            let bit = (composite as usize) % (self.bit_len as usize);
            set_bit(&mut self.bits, bit);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bit_len == 0 {
            return false;
        }

        let (hash1, hash2) = base_hashes(key);
        for i in 0..self.hash_functions {
            let composite = hash1.wrapping_add((i as u32).wrapping_mul(hash2));
            let bit = (composite as usize) % (self.bit_len as usize);
            if !is_bit_set(&self.bits, bit) {
                return false;
            }
        }

        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 4 + self.bits.len());
        out.push(self.hash_functions);
        out.extend_from_slice(&self.bit_len.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BloomDecodeError> {
        if bytes.len() < 5 {
            return Err(BloomDecodeError::Truncated);
        }

        let hash_functions = bytes[0];
        if hash_functions == 0 {
            return Err(BloomDecodeError::InvalidHashCount);
        }

        let mut bit_len_raw = [0_u8; 4];
        bit_len_raw.copy_from_slice(&bytes[1..5]);
        let bit_len = u32::from_le_bytes(bit_len_raw);
        if bit_len == 0 {
            return Err(BloomDecodeError::InvalidBitLength);
        }

        let expected_len = (bit_len as usize).div_ceil(8);
        let payload = &bytes[5..];
        if payload.len() != expected_len {
            return Err(BloomDecodeError::InvalidBitLength);
        }

        Ok(Self { bits: payload.to_vec(), bit_len, hash_functions })
    }

    pub fn bit_len(&self) -> u32 {
        self.bit_len
    }

    pub fn hash_functions(&self) -> u8 {
        self.hash_functions
    }
}

fn base_hashes(key: &[u8]) -> (u32, u32) {
    let mut primary = Hasher::new();
    primary.update(key);
    let hash1 = primary.finalize();

    let mut secondary = Hasher::new();
    secondary.update(&[0xA5]);
    secondary.update(key);
    let hash2 = secondary.finalize().max(1);

    (hash1, hash2)
}

fn set_bit(bits: &mut [u8], bit_index: usize) {
    let byte = bit_index / 8;
    let mask = 1_u8 << (bit_index % 8);
    bits[byte] |= mask;
}

fn is_bit_set(bits: &[u8], bit_index: usize) -> bool {
    let byte = bit_index / 8;
    let mask = 1_u8 << (bit_index % 8);
    (bits[byte] & mask) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_roundtrip() {
        let mut builder = BloomFilterBuilder::new(10, 10);
        builder.add_key(b"alpha");
        builder.add_key(b"beta");
        builder.add_key(b"gamma");

        let bloom = builder.build();
        let encoded = bloom.encode();
        let decoded = BloomFilter::decode(&encoded).expect("decode bloom");

        assert!(decoded.may_contain(b"alpha"));
        assert!(decoded.may_contain(b"beta"));
        assert!(!decoded.may_contain(b"definitely-not-present"));
    }

    #[test]
    fn false_positive_rate_is_reasonable() {
        let mut builder = BloomFilterBuilder::new(10, 10);
        for i in 0..2000_u32 {
            builder.add_key(format!("key-{i}").as_bytes());
        }
        let bloom = builder.build();

        let mut false_positives = 0_u32;
        let probes = 4000_u32;
        for i in 0..probes {
            let key = format!("missing-{i}");
            if bloom.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let rate = false_positives as f64 / probes as f64;
        assert!(rate <= 0.05, "false positive rate too high: {rate:.4}");
    }
}
