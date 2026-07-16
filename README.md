# SymbiosisLib (sisl)

[![Conan Build (stable/v13.x)](https://github.com/eBay/sisl/actions/workflows/merge_build.yml/badge.svg?branch=stable%2Fv13.x)](https://github.com/eBay/sisl/actions/workflows/merge_build.yml)
[![Conan Build (dev/v14.x)](https://github.com/eBay/sisl/actions/workflows/merge_build.yml/badge.svg?branch=dev%2Fv14.x)](https://github.com/eBay/sisl/actions/workflows/merge_build.yml)
[![CodeCov](https://codecov.io/gh/szmyd/sisl/branch/master/graph/badge.svg)](https://codecov.io/gh/szmyd/sisl)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> *Pronounced "sizzle" — because your code should too.*

A C++23 library of high-performance data structures, utilities, and infrastructure components for systems that can't afford to be slow. sisl sits on top of Boost and the STL — filling gaps, wrapping complexity, and outperforming general-purpose approaches where it matters most.

## Table of Contents

- [Quick Start](#quick-start)
- [Components](#components)
  - [Logging](#logging)
  - [Metrics](#metrics)
  - [WISR](#wisr--wait-free-inserts-snoozy-reads)
  - [FDS — Fast Data Structures](#fds--fast-data-structures)
  - [Cache](#cache)
  - [Settings](#settings)
  - [Flip — Fault Injection](#flip--fault-injection)
  - [Async](#async)
  - [gRPC Utilities](#grpc-utilities)
  - [Auth Manager](#auth-manager)
  - [Utility](#utility)
  - [Sobject](#sobject)
  - [File Watcher](#file-watcher)
  - [HTTP Server](#http-server)
- [Platform Support](#platform-support)
- [Contributing](#contributing)
- [License](#license)

---

## Quick Start

### Prerequisites

- Conan 2.0+
- CMake 3.22+
- C++23-capable compiler: GCC 14+ or Clang 17+

The library targets C++23 throughout. Key language features in use:

| Feature | Where used |
|---------|-----------|
| `std::expected<T,E>` | gRPC `GrpcResult<T>` / `GrpcAsyncResult<T>` return types |
| `requires` constraints | Range constructors on FDS containers and WISR types |
| `[[nodiscard]]` | All predicate and factory APIs |
| `std::to_underlying` | Enum helpers |

### Build

```bash
git clone git@github.com:eBay/sisl
cd sisl
./prepare_v2.sh          # export local recipes to the conan cache
conan build -s:h build_type=Debug --build missing .
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `metrics` | `True` | Metrics, WISR, FDS, Cache, and Settings components |
| `grpc` | `True` | gRPC transport and Flip fault injection (requires `metrics`) |
| `http` | `True` | HTTP server component built on cpp-httplib |
| `malloc_impl` | `libc` | Memory allocator: `libc`, `tcmalloc`, or `jemalloc` |
| `sanitize` | `False` | Sanitizer to enable in Debug builds: `address` (ASan+UBSan), `thread` (TSan), or `False` |
| `coverage` | `False` | Enable gcov code coverage (Debug only) |

```bash
# Release build with tcmalloc, skip tests
conan build -s:h build_type=Release \
    -o sisl/*:malloc_impl=tcmalloc \
    -c tools.build:skip_test=True .

# Debug + AddressSanitizer
conan build -s:h build_type=Debug -o sisl/*:sanitize=address .

# Coverage report
conan build -s:h build_type=Debug -o sisl/*:coverage=True .
```

---

## Components

### Logging

Structured logging built on [spdlog](https://github.com/gabime/spdlog) with per-module log levels, signal-based stacktrace capture, and assertion macros that log and abort.

```cpp
SISL_LOGGING_DECL(my_module)          // declare module in header
SISL_LOGGING_DEF(my_module)           // define module in one .cpp

LOGINFO("Starting component {}", name);
LOGDEBUGMOD(my_module, "detail: val={}", val);
LOGWARN("Slow path taken: {}", reason);

// Assert macros log + abort on failure
RELEASE_ASSERT(ptr != nullptr, "Expected non-null pointer");
RELEASE_ASSERT_EQ(count, expected, "Count mismatch after flush");
DEBUG_ASSERT_LT(index, size, "Index out of bounds");
```

Log levels are controlled per-module at runtime via `SetModuleLogLevel`. The `DLOG*` variants compile out in Release builds.

### Metrics

High-performance counters, gauges, and histograms with [Prometheus](https://prometheus.io/) export and JSON reporting. The write path costs **under 5 ns per update** regardless of thread count.

Two collection strategies ship out of the box:

- **ThreadBuffer + Signal** (`group_impl_type_t::thread_buf_signal`): per-thread lock-free accumulation; a signal flushes all threads before a scrape.
- **WISR RCU** (`group_impl_type_t::rcu`): wait-free inserts via the WISR framework (see below).

```cpp
class MyMetrics : public sisl::MetricsGroup {
public:
    explicit MyMetrics(std::string const& inst) :
            sisl::MetricsGroup("MyComponent", inst) {
        REGISTER_COUNTER(requests, "Total requests");
        REGISTER_GAUGE(queue_depth, "Current queue depth");
        REGISTER_HISTOGRAM(latency_us, "Latency in microseconds");
        register_me_to_farm();
    }
};

MyMetrics m{"instance1"};
COUNTER_INCREMENT(m, requests, 1);
GAUGE_UPDATE(m, queue_depth, q.size());
HISTOGRAM_OBSERVE(m, latency_us, elapsed_us);
```

Metrics are keyed by the name passed to `REGISTER_*` in the constructor and referenced by that same name in the update macros — there are no per-metric member declarations. See [src/metrics/README.md](src/metrics/README.md) for internals and histogram bucket configuration.

### WISR — Wait-free Inserts, Snoozy Reads

A concurrency framework for the pattern of *frequent fast writes, infrequent slow reads*. Per-thread buffers allow writers to proceed without any synchronization. A reader acquires a single mutex, rotates all thread-local buffers, and merges them — amortizing the read cost across all writers.

Benchmarks: **10–12× better write throughput vs. `std::mutex`** on 8 threads.

Ships with ready-to-use containers:

```cpp
sisl::wisr_vector< Request > pending{1024};   // ctor takes an initial capacity hint

// Writers (any thread, wait-free):
pending.push_back(req);

// Reader (periodic flush):
auto snapshot = pending.now();   // rotates + merges all thread buffers, returns the combined container
for (auto& r : *snapshot) { process(r); }
```

`wisr_list` and `wisr_deque` follow the same pattern. See [src/wisr/README.md](src/wisr/README.md).

### FDS — Fast Data Structures

#### Bitset

Concurrent, dynamically-resizable bitset with bulk operations. Unlike `std::bitset` (compile-time size) and `boost::dynamic_bitset` (no concurrent access), sisl's `Bitset` supports serialization, shrink/expand, and concurrent `set`/`reset`/`test` with a `std::shared_mutex` protecting the structure.

```cpp
sisl::Bitset bs(1024);
bs.set_bit(42);
auto next = bs.get_next_set_bit(0);        // → 42
sisl::byte_array buf = bs.serialize();     // returns a serialized byte_array (optional alignment arg)
```

#### StreamTracker

Tracks a stream of in-flight integer-keyed operations and computes the contiguous completed prefix without exclusive locking. Essential for ordered-completion protocols (e.g., log sequencers, replication pipelines).

```cpp
sisl::StreamTracker< MyState > tracker;
tracker.create(seq, state);            // register in-flight op
tracker.complete(seq, seq);            // mark the [start, end] range done
auto upto = tracker.completed_upto();  // highest contiguous completion
```

#### ThreadBuffer

Per-thread object storage that survives thread exit. The backbone of the metrics subsystem and WISR framework — allocated objects are not lost when a thread terminates.

#### ObjectAllocator (FreelistAllocator)

Thread-local free-list allocator for fixed-size objects. Pre-allocates a slab per thread and recycles in O(1) without size search — measurably faster than tcmalloc/jemalloc for single-size hot-path allocations.

```cpp
auto* obj = sisl::ObjectAllocator< MyObj >::make_object(args...);
sisl::ObjectAllocator< MyObj >::deallocate(obj);
```

#### ConcurrentInsertVector

Lock-free, append-only vector for concurrent producers. Readers take a snapshot. Range constructors on `ConcurrentInsertVector`, `ThreadVector`, `Bitset`, and WISR containers use C++20 `requires` constraints instead of SFINAE, so template errors are human-readable.

#### io_blob / io_blob_list_t

Byte buffer type with optional alignment and owned/borrowed semantics. The standard currency for I/O in the eBay storage stack.

```cpp
sisl::io_blob buf(4096, 512);           // 4 KiB, 512-byte aligned
std::memcpy(buf.bytes(), src, 4096);

sisl::io_blob_list_t scatter;           // small_vector<io_blob, 4>
scatter.emplace_back(buf);
```

### Cache

An LRU evictor and a **range-aware** concurrent hashmap built on top of FDS primitives. `RangeHashMap< K >` keys a base key together with a `[nth, count)` sub-range, stores `io_blob` values, and carves out the requested sub-range on lookup via a caller-supplied *value extractor*.

```cpp
// Extractor slices a stored blob down to the requested [nth, count) sub-range:
auto extractor = [](const sisl::byte_view& v, uint32_t nth, uint32_t count) {
    return sisl::byte_view{v, nth * kValSize, count * kValSize};
};
sisl::RangeHashMap< uint32_t > cache{num_buckets, extractor};

// Insert base key 1 covering offsets [0, 10):
cache.insert(sisl::RangeKey< uint32_t >{1u, 0, 10}, blob);

// Look up a sub-range; returns the covering (RangeKey, byte_view) pairs:
auto entries = cache.get(sisl::RangeKey< uint32_t >{1u, 2, 4});

cache.erase(sisl::RangeKey< uint32_t >{1u, 0, 10});
```

### Settings

Flatbuffers-backed runtime configuration with hot-swap support. Define a schema, generate C++ accessors at build time, and get thread-safe near-zero-cost config reads at runtime — without recompiling to change a tuning knob.

```cpp
// One-time wiring binding the generated schema to a factory singleton:
// SETTINGS_INIT(<namespace>::<SettingsName>, <SchemaName>)
SETTINGS_INIT(myapp::MyAppSettings, myapp_config)

// Read a single value (thread-safe, ~local-variable cost):
auto timeout = SETTINGS_VALUE(myapp_config, config->network->connect_timeout_ms);

// Read several fields atomically under one lock:
SETTINGS(myapp_config, s, {
    connect(s.config.network.host, s.config.network.connect_timeout_ms);
});

// Hot-reload from a JSON file or string; returns true if a restart is required:
SETTINGS_FACTORY(myapp_config).reload_file("/etc/myapp/config.json");
```

Supports hot-swappable (live reload) and cold (restart-required) settings in the same schema. See [src/settings/README.md](src/settings/README.md).

### Flip — Fault Injection

A gRPC-backed fault injection framework for testing failure scenarios without recompilation. Instrument code with named flip points; trigger faults externally via gRPC, Python client, or a local `FlipClient` in unit tests.

```cpp
// In production code — a named injection point (returns true when the fault fires):
if (flip::Flip::instance().test_flip("write_io_error")) { return -EIO; }

// ...or inject a value to return at the point:
if (auto v = flip::Flip::instance().get_test_flip< int >("write_delay_ms")) {
    std::this_thread::sleep_for(std::chrono::milliseconds(*v));
}

// In tests — arm the flip via a FlipClient bound to the Flip instance:
flip::FlipClient fc(&flip::Flip::instance());
flip::FlipFrequency freq;
freq.set_count(2);        // fire at most twice
freq.set_percent(100);    // on every matching call
fc.inject_retval_flip< int >("write_delay_ms", /*conditions=*/{}, freq, 500);
```

Supports boolean flips, return-value injection, async delay injection, callback flips, parameterized conditions, and frequency control (N times, every Nth, X% probability). See [src/flip/README.md](src/flip/README.md).

### Async

C++23 coroutine primitives for structured async code without callback chains. Organized in two dependency tiers: a **stdexec-free** baseline with no dependency beyond the standard library, and a **stdexec-aware** tier that integrates with P2300 execution contexts (`exec::task`, stop tokens, schedulers).

**Coroutine task types:**

| Type | Tier | Description |
|---|---|---|
| `light_task<T>` | Stdexec-free | Lazy stackless coroutine. Runs inline on whichever thread resumes it. Awaitable from any coroutine; supports `.detach()` for fire-and-forget. |
| `disk_task<T>` + `hot_task<T>` | Stdexec-free | io_uring-oriented task. `.start()` submits the SQE and returns a `hot_task` to `co_await` later, enabling SQE batch fan-out. |
| `task<T>` | Stdexec | Alias for `exec::task<T>`. Scheduler-affine, also a P2300 sender; composes with `when_all`, `when_any`, stop tokens. |

**Awaitable bridges** convert callback-style completions into `co_await`-able expressions:

| Type | Consumers | Transfer |
|---|---|---|
| `value_awaitable<T>` | Single | Move (move-only `T` supported) |
| `shared_awaitable<T>` | Many (broadcast) | Copy (all waiters get a copy) |
| `cqe_awaitable` | Single | `int` (io_uring `cqe->res`) |

**Fan-out combinators** work on both `light_task` and `task` vectors:

- `when_all(vector<Ts>)` - resolves when all tasks complete; results preserved in input order.
- `when_quorum(tasks, k)` - resolves when `k` tasks succeed, or all tasks finish. Stragglers continue running detached.

```cpp
// light_task: runs inline on whichever thread resumes it; no scheduler required
sisl::async::light_task<int> fetch_value(Source& src) {
    co_return co_await src.next();
}

// Blocking bridge from synchronous context:
int val = sisl::async::sync_get(fetch_value(src));

// Fire-and-forget (exceptions are logged and swallowed):
std::move(my_task).detach();

// value_awaitable: bridge a callback into co_await (single consumer, move semantics)
auto av = std::make_shared<sisl::async::value_awaitable<Result>>();
some_async_op([av](Result r) { av->complete(std::move(r)); });
auto result = co_await *av;

// shared_awaitable: broadcast one result to N waiting coroutines (copy semantics)
auto shared = std::make_shared<sisl::async::shared_awaitable<Store*>>();
shared->complete(opened_store);           // producer (any thread)
auto* store = co_await *shared;          // each consumer gets a copy

// when_quorum: k-of-n fan-out (resolves when 3 of 5 writes succeed)
std::vector<sisl::async::light_task<sisl::result<void>>> writes;
for (auto& peer : peers) writes.push_back(write_to(peer, data));
auto qr = co_await sisl::async::when_quorum(std::move(writes), /*quorum=*/3);
if (qr.acks < 3) { /* quorum not met */ }
```

`sisl::result<T>` (`std::expected<T, std::error_condition>`) is the synchronous error vocabulary. Convenience aliases for coroutine contexts:

```cpp
// light_result<T>  = light_task<sisl::result<T>>
// light_status     = light_task<sisl::result<std::monostate>>
sisl::async::light_result<Page> load_page(uint64_t id) {
    // co_return a sisl::result<Page>
}
```

### gRPC Utilities

Async and sync client/server helpers on top of sisl's buffer and metrics infrastructure.

```cpp
// Async client — future-based
// GrpcResult<T>      = std::expected<T, grpc::Status>   (C++23)
// GrpcAsyncResult<T> = std::future<GrpcResult<T>>
auto stub = client->make_stub< EchoService >("worker-1");
GrpcAsyncResult< EchoReply > fut =
    stub->call_unary< EchoRequest, EchoReply >(req,
        &EchoService::StubInterface::AsyncEcho, /*deadline_s=*/5);
auto result = fut.get();
if (!result) { LOGERROR("RPC failed: {}", result.error().error_message()); }

// Async client — callback-based
stub->call_unary< EchoRequest, EchoReply >(req,
    &EchoService::StubInterface::AsyncEcho,
    [](EchoReply& reply, grpc::Status& s) { /* handle */ }, 5);

// Async client — coroutine (generic stub): co_await a reply instead of blocking on a future
auto gstub = client->make_generic_stub("worker-1");
sisl::async::light_task< void > send(sisl::io_blob_list_t req) {
    GrpcResult< GenericClientResponse > result =
        co_await gstub->call_unary_co(req, "/EchoService/Echo", /*deadline_s=*/5);
    if (!result) { LOGERROR("RPC failed: {}", result.error().error_message()); }
    // On success, result->response_blob() yields the reply bytes as a sisl::io_blob.
}
```

`GrpcAsyncClientWorker` manages the completion queue and worker threads. `GrpcServer` handles registration of async services and RPC handlers.

> **Coroutine migration (in progress):** the generic (`io_blob_list_t`) stub exposes `call_unary_co()`, which bridges the gRPC completion into a `co_await`-able `sisl::async::value_awaitable` (see [Async](#async)) instead of a `std::future`. The typed `AsyncStub< ServiceT >` still offers only the future and callback overloads above; the future overloads are retained alongside the coroutine path during the migration.

### Auth Manager

`GrpcTokenClient` is the interface for supplying a bearer token to gRPC channels. Subclass it and implement `get_token()`; the async client attaches the `(auth_header_key, get_token())` pair as request metadata on every call. Token fetching, caching, and refresh-before-expiry are the implementation's responsibility.

```cpp
class MyTokenClient : public sisl::GrpcTokenClient {
public:
    MyTokenClient() : sisl::GrpcTokenClient("authorization") {}
    std::string get_token() override { return fetch_or_refresh_bearer_token(); }
};

auto token_client = std::make_shared< MyTokenClient >();
auto grpc_client = std::make_unique< sisl::GrpcAsyncClient >(
    addr, token_client, domain, ssl_cert);
```

### Utility

#### atomic_counter

Compound atomic operations with correct acquire/release fencing — patterns that are verbose and error-prone to write with raw `std::atomic`.

```cpp
sisl::atomic_counter< int32_t > ref{0};

ref.increment();
if (ref.decrement_testz()) { /* last reference — safe to delete */ }

// Increment and check if we hit the threshold exactly:
if (ref.increment_test_eq(max_outstanding)) { /* trigger backpressure */ }
```

`decrement_testz`, `increment_test_ge`, `decrement_test_le`, and — where the post-op value is useful — `_with_count` variants such as `increment_test_ge_with_count` are provided with proper fencing. The bare `bool`-returning predicates are `[[nodiscard]]` — silently discarding a test result is a compile error.

#### enum

Bidirectional name ↔ value mapping for enums, including support for bit-shifted values. C++23's `std::to_underlying` converts enum → integer; this adds string lookup in both directions.

```cpp
ENUM(MyState, uint8_t, INIT, RUNNING, STOPPED)

MyState s = MyState::RUNNING;
std::string name = enum_name(s);                                 // enum → name: "RUNNING"
auto val = enum_value(s);                                        // enum → underlying integer
MyState parsed = MyStateSupport::instance().get_enum("STOPPED"); // name → enum
```

#### thread_factory / name_thread

Thread creation helpers that set the OS-level thread name (visible in `htop`, `gdb`, `perf`). Standard `std::thread` and `std::jthread` have no portable equivalent.

```cpp
auto t = sisl::named_thread("io-worker", [&]{ run_loop(); });

// Or name an existing thread/jthread:
sisl::name_thread(t, "compaction");
```

#### obj_life_counter

CRTP mixin that tracks the count of live instances and total allocations of a class, exposed as sisl metrics. Useful for detecting object leaks in production without a debugger.

```cpp
class AsyncRpc : public sisl::ObjLifeCounter< AsyncRpc > { ... };

// In a metrics scrape:
// sisl_obj_life_counter{class="AsyncRpc",type="alive"} 42
// sisl_obj_life_counter{class="AsyncRpc",type="total"} 10000
```

#### non_null_ptr

`non_null_unique_ptr< T >` — a `std::unique_ptr< T >` subclass that guarantees the pointer is never null by default-constructing `T` when given a null or default-initialized pointer.

#### status_factory

RCU-protected status snapshot: readers get lock-free access, writers do copy-on-write under a mutex. Safe for high-read, occasional-write status objects.

```cpp
sisl::StatusFactory< ComponentStatus > status{default_args...};

status.readable([](const ComponentStatus* s) {
    // lock-free read
});
status.updateable([](ComponentStatus* s) {
    s->bytes_written += delta;   // copy-on-write under mutex
});
```

#### urcu_helper

RAII wrappers around [userspace-rcu](https://liburcu.org/) primitives (`rcu_read_lock`/`unlock`, `synchronize_rcu`, `rcu_dereference`).

### Sobject

Introspectable managed objects with JSON status reporting. Register a callback that produces a JSON blob describing your component; the `sobject_manager` aggregates them into a tree queryable by type, name, or path.

```cpp
auto obj = mgr.create_object("volume", vol_name, [&](const status_request& req) {
    status_response r;
    r.json["size_gb"] = size / Gi;
    r.json["state"] = to_string(state);
    return r;
});
obj->add_child(child_obj);

// Query all volumes:
// designated initializers must follow declaration order (do_recurse precedes obj_type):
auto resp = mgr.get_status({.do_recurse = true, .obj_type = "volume"});
```

### File Watcher

Inotify-based file change notifications with callback registration.

```cpp
sisl::FileWatcher watcher;
watcher.start();   // spin up the inotify thread before registering listeners
watcher.register_listener("/etc/myapp/config.json", "cfg-reload",
    [](const std::string& path, bool deleted) {
        if (!deleted) reload_config(path);
    });
```

### HTTP Server

An HTTP server with pre-routing auth middleware, SSL support, and per-route access control. Routes are classified as `localhost` (local callers only), `safe` (no auth), or `regular` (subject to token verification).

```cpp
sisl::HttpServer server{5000, /*threads=*/4, /*max_request_size=*/4000000, token_verifier};

server.setup_routes({
    {sisl::http_method::Get,  "/status", handle_status, sisl::url_type::safe},
    {sisl::http_method::Post, "/config", handle_config, sisl::url_type::regular},
});

// When compiled with metrics=True, wire up /metrics scrape automatically:
server.register_metrics_endpoint();

server.start();
// ...
server.stop();
```

SSL can be enabled at construction or hot-swapped at runtime:

```cpp
// SSL from the start:
sisl::HttpServer secure{ssl_cert, ssl_key, 5443, 4, 4000000, token_verifier};

// Hot-swap certs without dropping the port:
server.restart(new_cert, new_key);
```

---

## Platform Support

| Platform | Status |
|----------|--------|
| Linux x86_64 (GCC + libstdc++) | Fully supported |
| Linux x86_64 (Clang + libstdc++) | Fully supported |
| Linux x86_64 (Clang + libc++) | Supported — crash dumps (breakpad) unavailable (libc++ incompatibility) |
| Linux ARM64 | Supported |
| macOS (AppleClang) | Supported — crash dumps (breakpad) and file_watcher not available |
| Windows | Not supported |

---

## Contributing

Issues, bug reports, and pull requests are welcome. Please:

1. Follow the code style — run `clang-format -style=file -i` on every modified file
2. Add tests for new functionality (`src/<component>/tests/`)
3. Ensure tests pass with AddressSanitizer (`-o sisl/*:sanitize=address`) and ThreadSanitizer (`-o sisl/*:sanitize=thread`)
4. Submit pull requests against `dev/v14.x` (active development branch)

---

## License

Copyright 2021 eBay Inc.

Primary Author: Harihara Kadayam

Developers: Harihara Kadayam, Rishabh Mittal, Bryan Zimmerman, Brian Szmyd

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](LICENSE) for full terms.
