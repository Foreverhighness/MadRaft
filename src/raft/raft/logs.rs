use serde::{Deserialize, Serialize};
use std::ops::{Index, RangeFrom};

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
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
            inner: vec![LogEntry::default()],
        }
    }

    pub fn last(&self) -> &LogEntry {
        let ret = self.inner.last().unwrap();
        assert_eq!(ret.index, self.offset + self.inner.len() - 1);
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
        if index < self.offset {
            return None;
        }
        self.inner.get(index - self.offset)
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
        assert_eq!(self.len(), len);
    }

    pub fn extend_from_slice(&mut self, entries: &[LogEntry]) {
        self.inner.extend_from_slice(entries);
    }

    pub fn push(&mut self, value: LogEntry) {
        self.inner.push(value);
    }

    pub fn snapshot(&self) -> &LogEntry {
        &self.inner[0]
    }

    pub fn install_snapshot(&mut self, term: u64, index: usize) {
        self.inner[0].term = term;
        self.inner[0].index = index;

        let len = self.len();
        let end = self.inner.len().min(index - self.offset + 1);
        self.inner.drain(1..end);

        self.offset = index;
        assert!(self.len() >= len);
    }

    pub fn size(&self) -> usize {
        self.inner.len()
    }
}

impl Index<usize> for Logs {
    type Output = LogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(
            index < self.len(),
            "{index} < {}, offset: {}",
            self.len(),
            self.offset
        );
        self.get(index).unwrap()
    }
}

impl Index<RangeFrom<usize>> for Logs {
    type Output = [LogEntry];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        let start = index.start - self.offset;
        assert!(start >= 1);
        &self.inner[start..]
    }
}
