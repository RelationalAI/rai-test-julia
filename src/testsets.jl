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
    return ts
end

function add_test_ref(testset::ConcurrentTestSet, test_ref)
    return push!(testset.tests, test_ref)
end

# Handle attempted use outside of a ConcurrentTestSet
function add_test_ref(testset::AbstractTestSet, test_ref)
    return fetch(test_ref)
end

# Wrap a DefaultTestSet with some behavior specific to @test_rel.
# 
# Results are recorded, but not printed if nested=true.
# This is helpful when used in a ConcurrentTestSet where the parent
# linkage is lost due to the concurrency.
#
# Additionally the whole test set can be checked for broken-ness, this is
# needed as a test rel desugars into multiple checks and we don't know which
# is expected not to work.
mutable struct TestRelTestSet <: AbstractTestSet
    dts::Test.DefaultTestSet
    nested::Bool
    broken::Bool
    broken_found::Bool

    TestRelTestSet(desc; nested=false, broken=false) = 
        new(Test.DefaultTestSet(desc), nested, broken, false)
end

record(ts::TestRelTestSet, child::AbstractTestSet) = record(ts.dts, child)
record(ts::TestRelTestSet, res::Test.Result) = record(ts.dts, res)

# Flip to broken if expected, if not, log them (recording to dts goes to stdout)
function record(ts::TestRelTestSet, t::Union{Test.Fail, Test.Error})
    if ts.broken
        ts.broken_found = true
        push!(ts.dts.results, Test.Broken(t.test_type, t.orig_expr))
    else
        log_test_error(ts, t)
        push!(ts.dts.results, t)
    end
    return t
end

function finish(ts::TestRelTestSet)
    if ts.broken && !ts.broken_found
        # If we expect broken tests and everything passes, drop the results and 
        # replace with an unbroken Error

        ts.dts.n_passed = 0
        empty!(ts.dts.results)

        t = Test.Error(:test_unbroken, ts.dts.description, "", "", LineNumberNode(0))
        push!(ts.dts.results, t)
        log_test_error(ts, t)
        @info "I'm unbroken" ts
    end

    if Test.get_testset_depth() > 0
        # Attach this test set to the parent test set
        parent_ts = Test.get_testset()
        record(parent_ts, ts.dts)
        return ts
    end
    !ts.nested && finish(ts.dts)
    return ts
end

anyerror(ts::TestRelTestSet) = anyerror(ts.dts)
function anyerror(ts::Test.DefaultTestSet)
    stats = Test.get_test_counts(ts)
    return stats[3] + stats[7] > 0
end

anyfail(ts::TestRelTestSet) = anyfail(ts.dts)
function anyfail(ts::Test.DefaultTestSet)
    stats = Test.get_test_counts(ts)
    return stats[2] + stats[6] > 0
end

function log_test_error(ts::TestRelTestSet, t::Union{Test.Fail, Test.Error})
    io, ctx = get_logging_io()
    print(ctx, ts.dts.description, ": ")
    print(ctx, t)
    msg = String(take!(io))
    @error msg
end