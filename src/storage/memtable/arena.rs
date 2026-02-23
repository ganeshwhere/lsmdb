use std::cmp;

pub const DEFAULT_ARENA_BLOCK_SIZE_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArenaSlice {
    block_index: usize,
    offset: usize,
    len: usize,
}

impl ArenaSlice {
    pub fn len(self) -> usize {
        self.len
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}

#[derive(Debug)]
pub struct Arena {
    block_size: usize,
    blocks: Vec<Vec<u8>>,
    allocated_bytes: usize,
}

impl Default for Arena {
    fn default() -> Self {
        Self::new(DEFAULT_ARENA_BLOCK_SIZE_BYTES)
    }
}

impl Arena {
    pub fn new(block_size: usize) -> Self {
        let block_size = cmp::max(block_size, 1);
        Self { block_size, blocks: Vec::new(), allocated_bytes: 0 }
    }

    pub fn allocate(&mut self, bytes: &[u8]) -> ArenaSlice {
        self.ensure_capacity(bytes.len());
        if self.blocks.is_empty() {
            self.allocate_block(bytes.len());
        }

        let block_index = self.blocks.len() - 1;
        let block = &mut self.blocks[block_index];

        let offset = block.len();
        block.extend_from_slice(bytes);

        self.allocated_bytes += bytes.len();

        ArenaSlice { block_index, offset, len: bytes.len() }
    }

    pub fn get(&self, slice: ArenaSlice) -> &[u8] {
        if slice.len == 0 {
            return &[];
        }

        let block = &self.blocks[slice.block_index];
        &block[slice.offset..slice.offset + slice.len]
    }

    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes
    }

    pub fn reserved_bytes(&self) -> usize {
        self.blocks.iter().map(Vec::capacity).sum()
    }

    fn ensure_capacity(&mut self, len: usize) {
        if self.blocks.is_empty() {
            self.allocate_block(len);
            return;
        }

        let last_index = self.blocks.len() - 1;
        let block = &self.blocks[last_index];
        let remaining = block.capacity() - block.len();

        if remaining < len {
            self.allocate_block(len);
        }
    }

    fn allocate_block(&mut self, min_capacity: usize) {
        let capacity = cmp::max(self.block_size, min_capacity);
        self.blocks.push(Vec::with_capacity(capacity));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_and_reads_slices() {
        let mut arena = Arena::new(8);
        let first = arena.allocate(b"abc");
        let second = arena.allocate(b"xyz");

        assert_eq!(arena.get(first), b"abc");
        assert_eq!(arena.get(second), b"xyz");
        assert_eq!(arena.allocated_bytes(), 6);
    }

    #[test]
    fn allocates_new_block_when_current_is_full() {
        let mut arena = Arena::new(4);
        let _ = arena.allocate(b"1234");
        let second = arena.allocate(b"56");

        assert_eq!(arena.get(second), b"56");
        assert!(arena.reserved_bytes() >= 8);
    }
}
