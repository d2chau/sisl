// Unit tests for sisl::async::light_task<T> and its launch modes, plus the light_task overloads of
// when_all / when_quorum (their first direct coverage -- the exec::task forms are exercised only through
// downstream consumers).
//
// Tests cover: laziness, symmetric transfer, value and exception propagation through co_await, detach()
// (inline start, self-owning frame destruction, throw-swallow), sync_get (same-thread and cross-thread,
// exception rethrow), co_await interop from a disk_task frame, and the when_all / when_quorum semantics
// (input-order results, k-of-n early fire with live stragglers, the acks<quorum results contract, child
// throw handling). No io_uring or stdexec required -- completion is driven manually via value_awaitable.

#include <coroutine>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <sisl/async/disk_task.hpp>
#include <sisl/async/light_task.hpp>
#include <sisl/async/value_awaitable.hpp>
#include <sisl/async/when_all.hpp>
#include <sisl/async/when_quorum.hpp>

namespace {

using sisl::async::light_task;
using sisl::async::value_awaitable;

// Eager, detached coroutine for test drivers (same helper as test_disk_task).
struct eager_task {
    struct promise_type {
        eager_task get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };
};

// Drive an awaitable from an eager frame, publishing the result through an explicit POINTER PARAMETER.
// Parameters are copied into the coroutine frame; lambda captures are NOT (the closure is the implicit
// object parameter, which dies with the temporary) -- a driver that suspends past its launch expression
// must therefore never be a capturing lambda. This is the same rule run_craft_io documents in ublkpp.
template < typename T, typename Awaitable >
eager_task drive(std::optional< T >* out, Awaitable aw) {
    out->emplace(co_await std::move(aw));
}

// Sets *flag when destroyed -- placed in a coroutine frame to observe the frame's destruction.
struct dtor_flag {
    bool* f;
    ~dtor_flag() { *f = true; }
};

// ============================================================================
// Laziness + value propagation
// ============================================================================

TEST(light_task, LazyDoesNotRunUntilAwaited) {
    bool ran = false;
    auto t = [](bool* r) -> light_task< int > {
        *r = true;
        co_return 5;
    }(&ran);
    EXPECT_FALSE(ran); // initial_suspend stops it before the body runs

    std::optional< int > result{};
    drive(&result, std::move(t));
    EXPECT_TRUE(ran);
    EXPECT_EQ(result, std::optional< int >{5});
}

TEST(light_task, ResumesAfterManualCompletion) {
    value_awaitable< int > ev{};
    std::optional< int > result{};
    auto t = [](value_awaitable< int >& e) -> light_task< int > { co_return co_await e; }(ev);
    drive(&result, std::move(t));
    EXPECT_FALSE(result.has_value()); // suspended on ev
    ev.complete(77);                  // producer side: resumes the chain inline
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 77);
}

// ============================================================================
// Exception propagation: captured in the promise, rethrown at the co_await
// ============================================================================

TEST(light_task, ExceptionRethrownInAwaitingFrame) {
    bool caught = false;
    auto thrower = []() -> light_task< int > {
        throw std::runtime_error("boom");
        co_return 0;
    };
    auto catcher = [](bool* c, light_task< int > t) -> eager_task {
        try {
            (void)co_await std::move(t);
        } catch (const std::runtime_error&) { *c = true; }
    };
    catcher(&caught, thrower());
    EXPECT_TRUE(caught);
}

// ============================================================================
// detach(): inline start, self-owning frame, throw-swallow
// ============================================================================

TEST(light_task, DetachRunsInlineToFirstSuspension) {
    bool started = false;
    value_awaitable< int > ev{};
    auto t = [](bool* s, value_awaitable< int >& e) -> light_task< void > {
        *s = true;
        co_await e;
    }(&started, ev);
    std::move(t).detach();
    EXPECT_TRUE(started); // ran to the co_await on THIS thread before detach() returned
    ev.complete(1);       // let the frame finish (and destroy itself)
}

TEST(light_task, DetachedFrameDestroysItselfOnCompletion) {
    bool destroyed = false;
    value_awaitable< int > ev{};
    auto t = [](bool* d, value_awaitable< int >& e) -> light_task< void > {
        dtor_flag guard{d};
        co_await e;
    }(&destroyed, ev);
    std::move(t).detach();
    EXPECT_FALSE(destroyed); // suspended: frame alive, owned by itself
    ev.complete(1);
    EXPECT_TRUE(destroyed); // final_suspend destroyed the frame
}

TEST(light_task, DetachedThrowIsSwallowed) {
    value_awaitable< int > ev{};
    auto t = [](value_awaitable< int >& e) -> light_task< void > {
        co_await e;
        throw std::runtime_error("detached boom");
    }(ev);
    std::move(t).detach();
    ev.complete(1); // resume -> throw -> captured -> logged+swallowed; must not escape or terminate
    SUCCEED();
}

// ============================================================================
// sync_get: blocking bridge, same-thread and cross-thread
// ============================================================================

TEST(light_task, SyncGetReturnsSynchronousValue) {
    auto v = sisl::async::sync_get([]() -> light_task< int > { co_return 11; }());
    EXPECT_EQ(v, 11);
}

TEST(light_task, SyncGetRethrows) {
    auto thrower = []() -> light_task< int > {
        throw std::runtime_error("sync boom");
        co_return 0;
    };
    EXPECT_THROW((void)sisl::async::sync_get(thrower()), std::runtime_error);
}

TEST(light_task, SyncGetCrossThreadCompletion) {
    value_awaitable< int > ev{};
    std::thread producer{[&ev] { ev.complete(123); }};
    auto v = sisl::async::sync_get([](value_awaitable< int >& e) -> light_task< int > { co_return co_await e; }(ev));
    producer.join();
    EXPECT_EQ(v, 123);
}

// ============================================================================
// Interop: a disk_task frame co_awaits a light_task (the ublkpp driver shape)
// ============================================================================

TEST(light_task, AwaitableFromDiskTaskFrame) {
    value_awaitable< int > ev{};
    // Captureless with an explicit parameter: the driver suspends past its launch expression, so it must not
    // read lambda captures after resuming (same rule as drive() above).
    auto driver = [](value_awaitable< int >& e) -> sisl::async::disk_task< int > {
        auto verb = [](value_awaitable< int >& ve) -> sisl::async::light_result< int > {
            co_return sisl::result< int >{co_await ve};
        };
        auto const res = co_await verb(e);
        co_return res.has_value() ? static_cast< int >(*res) : -1;
    };
    auto hot = driver(ev).start();
    EXPECT_FALSE(hot.done()); // suspended inside the light_task on ev
    ev.complete(9);
    EXPECT_TRUE(hot.done());
    EXPECT_EQ(hot.result(), 9);
}

// ============================================================================
// when_all (light overload)
// ============================================================================

TEST(light_when_all, CollectsInInputOrder) {
    value_awaitable< int > e1{}, e2{}, e3{};
    auto child = [](value_awaitable< int >& e) -> light_task< int > { co_return co_await e; };

    std::vector< light_task< int > > kids;
    kids.push_back(child(e1));
    kids.push_back(child(e2));
    kids.push_back(child(e3));

    std::optional< std::vector< int > > out{};
    drive(&out, sisl::async::when_all(std::move(kids)));
    EXPECT_FALSE(out.has_value()); // all three suspended

    // Complete out of order; results must land by input index.
    e2.complete(20);
    e3.complete(30);
    EXPECT_FALSE(out.has_value()); // still one outstanding
    e1.complete(10);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(*out, (std::vector< int >{10, 20, 30}));
}

TEST(light_when_all, EmptyVectorCompletesImmediately) {
    std::optional< std::vector< int > > out{};
    drive(&out, sisl::async::when_all(std::vector< light_task< int > >{}));
    ASSERT_TRUE(out.has_value());
    EXPECT_TRUE(out->empty());
}

TEST(light_when_all, ThrowingChildLeavesSlotDefaultAndStillCompletes) {
    value_awaitable< int > e1{};
    std::vector< light_task< int > > kids;
    kids.push_back([](value_awaitable< int >& e) -> light_task< int > { co_return co_await e; }(e1));
    kids.push_back([]() -> light_task< int > {
        throw std::runtime_error("child boom");
        co_return 0;
    }());

    std::optional< std::vector< int > > out{};
    drive(&out, sisl::async::when_all(std::move(kids)));
    EXPECT_FALSE(out.has_value()); // e1 still pending; the thrower already counted down
    e1.complete(4);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ((*out)[0], 4);
    EXPECT_EQ((*out)[1], 0); // default-constructed slot
}

// ============================================================================
// when_quorum (light overload) -- children complete with std::optional<int>
// (has_value() == acked), mirroring the errors-as-values contract
// ============================================================================

using opt_int = std::optional< int >;

TEST(light_when_quorum, FiresAtQuorumWithStragglerStillRunning) {
    value_awaitable< opt_int > e1{}, e2{}, e3{};
    auto child = [](value_awaitable< opt_int >& e) -> light_task< opt_int > { co_return co_await e; };

    std::vector< light_task< opt_int > > kids;
    kids.push_back(child(e1));
    kids.push_back(child(e2));
    kids.push_back(child(e3));

    std::optional< sisl::async::quorum_result< opt_int > > out{};
    drive(&out, sisl::async::when_quorum(std::move(kids), 2));
    EXPECT_FALSE(out.has_value());

    e1.complete(opt_int{1});
    EXPECT_FALSE(out.has_value()); // 1 of 2
    e2.complete(opt_int{2});
    ASSERT_TRUE(out.has_value()); // quorum fired; e3 is a live straggler
    EXPECT_EQ(out->acks, 2u);

    e3.complete(opt_int{3}); // straggler finishes detached; keeps `results` alive via shared_ptr
}

TEST(light_when_quorum, BelowQuorumCompletesWhenAllFinishAndResultsReadable) {
    value_awaitable< opt_int > e1{}, e2{}, e3{};
    auto child = [](value_awaitable< opt_int >& e) -> light_task< opt_int > { co_return co_await e; };

    std::vector< light_task< opt_int > > kids;
    kids.push_back(child(e1));
    kids.push_back(child(e2));
    kids.push_back(child(e3));

    std::optional< sisl::async::quorum_result< opt_int > > out{};
    drive(&out, sisl::async::when_quorum(std::move(kids), 2));

    e1.complete(opt_int{7}); // 1 ack
    e2.complete(std::nullopt);
    EXPECT_FALSE(out.has_value());
    e3.complete(std::nullopt); // all finished, acks(1) < quorum(2) -> latch fires on trigger (b)
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->acks, 1u);
    // acks < quorum: every child finished, so results are safe to read (the contract)
    EXPECT_EQ((*out->results)[0], opt_int{7});
    EXPECT_EQ((*out->results)[1], std::nullopt);
    EXPECT_EQ((*out->results)[2], std::nullopt);
}

TEST(light_when_quorum, CompletionHookSeesEveryChildWithAckFlag) {
    value_awaitable< opt_int > e1{}, e2{};
    auto child = [](value_awaitable< opt_int >& e) -> light_task< opt_int > { co_return co_await e; };

    std::vector< light_task< opt_int > > kids;
    kids.push_back(child(e1));
    kids.push_back(child(e2));

    // The hook closure is COPIED into the completion_hook std::function (and on into runner frames), so its
    // by-reference capture of `seen` is safe -- unlike a driver closure, which is never copied anywhere.
    std::vector< std::pair< std::size_t, bool > > seen;
    std::optional< sisl::async::quorum_result< opt_int > > out{};
    drive(&out, sisl::async::when_quorum(std::move(kids), 2, [&seen](std::size_t i, bool acked) {
        seen.emplace_back(i, acked);
    }));

    e2.complete(std::nullopt);
    e1.complete(opt_int{1});
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0], (std::pair< std::size_t, bool >{1, false}));
    EXPECT_EQ(seen[1], (std::pair< std::size_t, bool >{0, true}));
}

} // namespace
