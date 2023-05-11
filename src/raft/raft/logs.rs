use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct LogEntry {
    data: Vec<u8>,
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
        assert_eq!(ret.info().1, self.offset + self.inner.len());
        ret
    }
}
