use std::ops::{Index, RangeFrom};

use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct LogEntry {
    pub data: Vec<u8>,
    pub term: u64,
    pub index: usize,
}

impl LogEntry {
    pub const fn info(&self) -> (u64, usize) {
        (self.term, self.index)
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Logs {
    offset: usize,
    inner: Vec<LogEntry>,
}

impl Logs {
    pub fn new() -> Logs {
        Self {
            offset: 0,
            inner: vec![LogEntry {
                data: Vec::new(),
                term: 0,
                index: 0,
            }],
        }
    }

    pub fn last(&self) -> &LogEntry {
        let ret = self.inner.last().unwrap();
        assert_eq!(ret.info().1, self.offset + self.inner.len() - 1);
        ret
    }

    pub fn len(&self) -> usize {
        self.offset + self.inner.len()
    }

    /// return first entry with same term, assume entry exist
    pub fn find_first(&self, term: u64) -> usize {
        self.inner
            .iter()
            .skip(1)
            .find(|e| e.term == term)
            .map(|e| e.index)
            .unwrap()
    }

    /// return last entry with same term
    pub fn find_last(&self, term: u64) -> Option<usize> {
        self.inner
            .iter()
            .skip(1)
            .rev()
            .find(|e| e.term == term)
            .map(|e| e.index)
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.inner.get(self.offset + index)
    }

    /// calculate matched entries number from index
    pub fn matches(&self, entries: &[LogEntry], index: usize) -> usize {
        self[index..]
            .iter()
            .zip(entries.iter())
            .take_while(|&(l, r)| {
                assert_eq!(l.index, r.index);
                l.term == r.term
            })
            .count()
    }

    pub fn truncate(&mut self, len: usize) {
        self.inner.truncate(1.max(len - self.offset));
    }

    pub fn extend_from_slice(&mut self, entries: &[LogEntry]) {
        self.inner.extend_from_slice(entries);
    }

    pub fn push(&mut self, value: LogEntry) {
        self.inner.push(value);
    }
}

impl Index<usize> for Logs {
    type Output = LogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl Index<RangeFrom<usize>> for Logs {
    type Output = [LogEntry];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        &self.inner[(index.start - self.offset)..]
    }
}
