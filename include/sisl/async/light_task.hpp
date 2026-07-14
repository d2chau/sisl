#pragma once

// light_task<T>: the freestanding (stdexec-free) lazy coroutine task. Where sisl::async::task<T> is
// exec::task<T> -- a scheduler-affine stdexec sender for consumers composing with when_all/sync_wait on a
// scheduler -- light_task is a plain awaitable with NO scheduler anywhere: the coroutine starts on the thread
// that launches it and every resume happens inline on the thread that completes the awaited work. That IS the
// contract, not an accident (on-ring designs want exactly this); a consumer that needs resumption on its own
// thread must arrange the hop itself (bind the work to its ring, or block via sync_get below).
//
// Three launch modes for a lazy coroutine, and the name tells you who owns the result:
//   co_await t     -- start it, suspend until it finishes, receive the result (structured; the awaiting frame
//                     owns the callee for its whole life; exceptions rethrow here).
//   t.detach()     -- start it and NOBODY is coming back: no handle, result discarded, an escaped exception is
//                     logged and swallowed, and the frame owns ITSELF (destroyed at final_suspend). This is
//                     std::thread::detach()'s semantic; it exists because some coroutines genuinely have no
//                     collector (quorum stragglers, fire-and-forget keepalive legs).
//   sync_get(t)    -- block the calling thread until the task completes and return its value (or rethrow).
//                     Safe only OFF whatever loop completes the task's work: blocking the completing thread
//                     deadlocks, same caveat as the stdexec sync_get in coro.hpp.
// (disk_task<T>::start() is the fourth mode -- start-now-collect-later for SQE batching -- deliberately NOT
// offered here until a fan-out caller needs it; when_all/when_quorum cover the latch-style gathering.)
//
// Composability: co_await light_task<U> from ANY coroutine -- another light_task, a disk_task, or an
// exec::task (plain awaitables pass through exec::task's await_transform). The reverse does NOT hold:
// exec::task cannot be co_awaited from a light_task frame (its awaiter demands a scheduler-bearing promise
// environment), which is the entire reason this type exists.
//
// Exception discipline: the promise CAPTURES an escaped exception (unlike disk_task, which rethrows out of
// resume() so a driving loop can catch it -- that contract is load-bearing elsewhere; do not conflate the two
// types). co_await rethrows it in the awaiting frame; sync_get rethrows on the blocked thread; detach()
// logs-and-swallows. resume() on a light_task frame therefore never throws.

#include <condition_variable>
#include <coroutine>
#include <cstdio>
#include <exception>
#include <mutex>
#include <optional>
#include <utility>
#include <variant>

#include <sisl/result.hpp>

namespace sisl::async {

template < typename T >
struct light_task;

namespace detail {

// State shared by the value and void promises: continuation for symmetric transfer, captured exception, and
// the detached flag that flips frame ownership to the frame itself.
struct light_promise_base {
    std::coroutine_handle<> _continuation{};
    std::exception_ptr _error{};
    bool _detached{false};

    std::suspend_always initial_suspend() noexcept { return {}; }
    void unhandled_exception() noexcept { _error = std::current_exception(); }
};

// stderr, not a sisl logging macro: this header stays dependency-free (like disk_task.hpp) so consumers and
// unit tests never inherit a logging-library link for a backstop that should never fire -- detached tasks
// carry their errors as values; an escaped exception here is a bug being made visible, not a log stream.
inline void log_swallowed_exception(std::exception_ptr ep) noexcept {
    try {
        std::rethrow_exception(ep);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[light_task] detached task threw, swallowing: %s\n", e.what());
    } catch (...) { std::fprintf(stderr, "[light_task] detached task threw an unknown exception, swallowing\n"); }
}

// Final awaiter: a detached frame destroys itself (logging any captured throw); an awaited frame
// symmetric-transfers to its continuation. A frame that was never awaited nor detached parks on
// noop_coroutine and is destroyed by the owning light_task's destructor.
template < typename P >
struct light_final_awaiter {
    bool await_ready() noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle< P > h) noexcept {
        auto& p = h.promise();
        if (p._detached) {
            if (p._error) log_swallowed_exception(p._error);
            h.destroy();
            return std::noop_coroutine();
        }
        return p._continuation ? p._continuation : std::noop_coroutine();
    }
    void await_resume() noexcept {}
};

} // namespace detail

template < typename T >
struct light_task {
    struct promise_type : detail::light_promise_base {
        std::optional< T > _value{};

        light_task get_return_object() noexcept {
            return light_task{std::coroutine_handle< promise_type >::from_promise(*this)};
        }
        detail::light_final_awaiter< promise_type > final_suspend() noexcept { return {}; }
        void return_value(T v) { _value.emplace(std::move(v)); }
    };

    std::coroutine_handle< promise_type > _coro;

    explicit light_task(std::coroutine_handle< promise_type > h) noexcept : _coro(h) {}
    light_task(light_task&& o) noexcept : _coro(std::exchange(o._coro, {})) {}
    light_task(light_task const&) = delete;
    ~light_task() {
        if (_coro) _coro.destroy();
    }

    // Awaitable interface (plain -- awaitable from any coroutine). Symmetric transfer starts the callee.
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
        _coro.promise()._continuation = cont;
        return _coro;
    }
    T await_resume() {
        auto& p = _coro.promise();
        if (p._error) std::rethrow_exception(p._error);
        return std::move(*p._value);
    }

    // Fire-and-forget: start now (runs to its first suspension on THIS thread -- callers rely on that for
    // payload-lifetime contracts, see when_quorum), relinquish ownership to the frame. After this call the
    // task object is empty; the frame destroys itself when the coroutine finishes, on whatever thread that
    // happens. An escaped exception is logged and swallowed.
    void detach() && noexcept {
        auto h = std::exchange(_coro, {});
        h.promise()._detached = true;
        h.resume();
    }
};

// void specialization: same shell, return_void, no value slot.
template <>
struct light_task< void > {
    struct promise_type : detail::light_promise_base {
        light_task get_return_object() noexcept {
            return light_task{std::coroutine_handle< promise_type >::from_promise(*this)};
        }
        detail::light_final_awaiter< promise_type > final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
    };

    std::coroutine_handle< promise_type > _coro;

    explicit light_task(std::coroutine_handle< promise_type > h) noexcept : _coro(h) {}
    light_task(light_task&& o) noexcept : _coro(std::exchange(o._coro, {})) {}
    light_task(light_task const&) = delete;
    ~light_task() {
        if (_coro) _coro.destroy();
    }

    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
        _coro.promise()._continuation = cont;
        return _coro;
    }
    void await_resume() {
        if (auto& p = _coro.promise(); p._error) std::rethrow_exception(p._error);
    }

    void detach() && noexcept {
        auto h = std::exchange(_coro, {});
        h.promise()._detached = true;
        h.resume();
    }
};

// The asynchronous-result forms (mirror result.hpp's task-based aliases): a coroutine you co_await that
// resolves to a sisl::result<T>. Errors travel as values; the exception channel is a backstop only.
template < typename T >
using light_result = light_task< sisl::result< T > >;

using light_status = light_result< std::monostate >;

namespace detail {

template < typename T >
struct sync_state {
    std::mutex m;
    std::condition_variable cv;
    bool done{false};
    std::optional< T > value{};
    std::exception_ptr error{};
};

// Detached runner: awaits the task on behalf of the blocked thread and publishes value-or-exception under
// the lock. The runner (not the blocked caller) is what the completing thread resumes.
template < typename T >
light_task< void > sync_run(light_task< T > t, std::shared_ptr< sync_state< T > > st) {
    std::optional< T > val{};
    std::exception_ptr err{};
    try {
        val.emplace(co_await std::move(t));
    } catch (...) { err = std::current_exception(); }
    {
        std::lock_guard< std::mutex > lg(st->m);
        st->value = std::move(val);
        st->error = err;
        st->done = true;
    }
    st->cv.notify_one();
}

} // namespace detail

// Block the calling thread until the task completes; return its value or rethrow its exception. The task is
// fulfilled by whatever thread completes its awaited work (a transport pool, a ring's reap loop) -- do NOT
// call this ON that thread. Overloads the stdexec sync_get in coro.hpp by type; the two never collide.
template < typename T >
T sync_get(light_task< T > t) {
    auto st = std::make_shared< detail::sync_state< T > >();
    detail::sync_run(std::move(t), st).detach();
    std::unique_lock< std::mutex > ul(st->m);
    st->cv.wait(ul, [&st] { return st->done; });
    if (st->error) std::rethrow_exception(st->error);
    return std::move(*st->value);
}

} // namespace sisl::async
