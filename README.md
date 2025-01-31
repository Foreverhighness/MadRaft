# FH-notes

## lab2
The Rust version uses a different way to notify a raft server that it is being killed, in the Golang project we use `rf.Kill()`, but in the Rust version we implicitly use `Drop`, the code is in `src/raft/tester.rs:332` `RaftTester::crash1`.

Using `RaftHandle` as a wrapper of `Arc<Mutex<Raft>>` is a good idea, it is better than `fn foo(raft: &Arc<Mutex<Raft>>)`.

But when using the `Drop` scheme, I cannot use `Arc<Mutex<Raft>>` freely. This is because the `Drop` schema assumes that when `RaftHandle` is dropped, `Raft` is also dropped. But if there is still an `Arc<Mutex<Raft>>`, then the `Raft` is not dropped, which break the assumption. Alternatively, if I want to use `Arc<Mutex<Raft>>`, I have to get it by upgrading a weak pointer.

## lab3
`ClerkCore` just call appropriate RPC handler and guarantee success. Because `Clerk` need sequence number to indicate duplicate request, so clerk need additional `seq` property, but `ClerkCore` does not need.

If `start` returns the same `index`, only the highest `term` will be applied. According to the `Log Matching` property, we can use `(index, term)` tuple to indicate a unique operation, and then send back a response to the corresponding client.

## lab4
My go implementation of the shard server is leader to handle everything, but wry's approach is much better than mine. It allows follower to fetch the latest config and then use an RPC call to notify leader to handle that config change event, which improves system availability but increases RPC traffic.

Every request eventually goes into the raft log, so there is no way to create a live lock. A live lock means that a leader cannot advance its `commit_index` because there is no entry with its current term.

# MadRaft

[![CI](https://github.com/madsys-dev/madraft/workflows/CI/badge.svg?branch=main)](https://github.com/madsys-dev/madraft/actions)

The labs of Raft consensus algorithm based on [MadSim](https://github.com/madsys-dev/madsim).

Some codes are derived from [MIT 6.824](http://nil.csail.mit.edu/6.824/2021/) and [PingCAP Talent Plan: Raft Lab](https://github.com/pingcap/talent-plan/tree/master/courses/dss/raft). Thanks for their excellent work!

## Key Features

* **Deterministic simulation**: Catch a rare bug and then reproduce it at any time you want.
* **Discrete event simulation**: No time wasted on sleep. The full test can be completed in a few seconds.
* **Async**: The code is written in a fully async-style.

## Mission

Read the instructions from MIT 6.824: [Lab2](http://nil.csail.mit.edu/6.824/2021/labs/lab-raft.html), [Lab3](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html), [Lab4](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html).

Complete the code and pass all tests!

```sh
cargo test
```

## Tips

To run a part of the tests or a specific test:

```sh
cargo test 2a
cargo test initial_election_2a
```

If a test fails, you will see a seed in the output:

```
---- raft::tests::initial_election_2a stdout ----
thread 'raft::tests::initial_election_2a' panicked at 'expected one leader, got none', src/raft/tester.rs:91:9
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
MADSIM_TEST_SEED=1629626496
```

Run the test again with the seed, and you will get exactly the same output:

```sh
MADSIM_TEST_SEED=1629626496 cargo test initial_election_2a
```

Enable logs to help debugging:

```sh
export RUST_LOG=madraft::raft=info
```

Run the test multiple times to make sure you solution can stably pass the test:

```sh
MADSIM_TEST_NUM=100 cargo test --release
```

### Ensure Determinism

Sometimes you may find that the test is not deterministic :(

Although the testing framework itself (MadSim) provides determinism, the entire system is not deterministic if your code introduces randomness.

Here are some tips to avoid randomness:

* Use `madsim::rand::rng` instead of `rand::thread_rng` to generate random numbers.
* Use `futures::select_biased` instead of `futures::select` macro.
* Do not **iterate** through a `HashMap`.

To make sure your code is deterministic, run your test with the following environment variable:

```sh
MADSIM_TEST_CHECK_DETERMINISTIC=1 cargo test
```

Your test will be run at least twice with the same seed. If any non-determinism is detected, it will panic as soon as possible.

Happy coding and Good luck!

## License

Apache License 2.0
