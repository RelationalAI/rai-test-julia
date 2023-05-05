import Test: Test, finish, record

using Test
using Test: AbstractTestSet

# Wraps a DefaultTestSet and adds a list of Tasks for concurrent tests
mutable struct ConcurrentTestSet <: Test.AbstractTestSet
    dts::Test.DefaultTestSet
    tests::Vector{Task}

    ConcurrentTestSet(desc) = new(Test.DefaultTestSet(desc), [])
end

function record(ts::ConcurrentTestSet, child::AbstractTestSet)
    return record(ts.dts, child)
end

function record(ts::ConcurrentTestSet, res::Test.Result)
    return record(ts.dts, res)
end

# Record any results directly stored and fetch results from any listed concurrent tests
# If this is the parent then show results
function finish(ts::ConcurrentTestSet)
    for t in ts.tests
        record(ts.dts, fetch(t))
    end
    if Test.get_testset_depth() > 0
        # Attach this test set to the parent test set
        parent_ts = Test.get_testset()
        record(parent_ts, ts.dts)
        return ts
    end
    finish(ts.dts)
    return ts.dts
end

function add_test_ref(testset::ConcurrentTestSet, test_ref)
    return push!(testset.tests, test_ref)
end

# Handle attempted use outside of a ConcurrentTestSEt
function add_test_ref(testset::AbstractTestSet, test_ref)
    return fetch(test_ref)
end

# Wrap a DefaultTestSet. Results are recorded, but not printed.
# This is helpful when used with a separate environment
mutable struct QuietTestSet <: AbstractTestSet
    dts::Test.DefaultTestSet

    QuietTestSet(desc) = new(Test.DefaultTestSet(desc))
end

record(ts::QuietTestSet, child::AbstractTestSet) = record(ts.dts, child)
record(ts::QuietTestSet, res::Test.Result) = record(ts.dts, res)

function record(ts::QuietTestSet, t::Union{Test.Fail, Test.Error})
    io = IOBuffer()
    ctx = IOContext(io, :color => get(stderr, :color, false))
    print(ctx, ts.description, ": ")
    print(ctx, t)
    if !isa(t, Error) # if not gets printed in the show method
        Base.show_backtrace(ctx, scrub_backtrace(backtrace()))
    end
    println(ctx)
    msg = String(take!(io))

    if t isa Test.Fail 
        @warn msg
    else
        @error msg
    end

    push!(ts.dts.results, t)
    return t
end

function finish(ts::QuietTestSet)
    if Test.get_testset_depth() > 0
        # Attach this test set to the parent test set
        parent_ts = Test.get_testset()
        record(parent_ts, ts.dts)
    end
    return ts.dts
end

anyerror(ts::QuietTestSet) = anyerror(ts.dts)
function anyerror(ts::Test.DefaultTestSet)
    stats = Test.get_test_counts(ts)
    return stats[3] + stats[7] > 0
end

anyfail(ts::QuietTestSet) = anyfail(ts.dts)
function anyfail(ts::Test.DefaultTestSet)
    stats = Test.get_test_counts(ts)
    return stats[2] + stats[6] > 0
end

# TestSet that can be marked as broken.
# This allows the broken status to be applied to a group of tests.
mutable struct BreakableTestSet <: Test.AbstractTestSet
    broken::Bool
    broken_found::Bool
    quiet::Bool
    dts::Test.DefaultTestSet

    BreakableTestSet(desc; broken = false, quiet = false) =
        new(broken, false, quiet, Test.DefaultTestSet(desc))
end

record(ts::BreakableTestSet, child::AbstractTestSet) = record(ts.dts, child)
record(ts::BreakableTestSet, res::Test.Result) = record(ts.dts, res)

function record(ts::BreakableTestSet, t::Union{Test.Fail, Test.Error})
    if ts.broken
        ts.broken_found = true
        push!(ts.dts.results, Test.Broken(t.test_type, t.orig_expr))
    else
        record(ts.dts, t)
    end
end

function finish(ts::BreakableTestSet)
    if ts.broken && !ts.broken_found
        # If we expect broken tests and everything passes, drop the results and replace with an unbroken Error

        ts.dts.n_passed = 0
        empty!(ts.dts.results)

        push!(
            ts.dts.results,
            Test.Error(:test_unbroken, ts.dts.description, "", "", LineNumberNode(0)),
        )
    end
    if Test.get_testset_depth() > 0
        # Attach this test set to the parent test set
        parent_ts = Test.get_testset()
        record(parent_ts, ts.dts)
    end
end
