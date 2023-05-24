import Test: Test, finish, record

using Test
using Test: AbstractTestSet
using ReTestItems: JUnitTestSuites, JUnitTestSuite, JUnitTestCase, write_junit_file, junit_record!
import ReTestItems

mutable struct RAITestSet <: Test.AbstractTestSet
    dts::Test.DefaultTestSet
    report::Bool
    distributed::Bool
    distributed_tests::Vector{Task}
    junit::Union{JUnitTestSuites, JUnitTestSuite}

    function RAITestSet(dts, report, distributed)
        desc = dts.description
        if Test.get_testset_depth() == 0
            junit = JUnitTestSuites(desc)
        else
            junit = JUnitTestSuite(desc)
        end
        new(dts, report, distributed, [], junit)
    end
end

function RAITestSet(desc; report::Option{Bool}=nothing, distributed::Option{Bool}=nothing)
    dts = Test.DefaultTestSet(desc)
    is_nested = Test.get_testset_depth() > 0
    default_report = false
    default_distributed = true

    # Pass on the parent RAITestSet's options if nested
    if is_nested
        parent = Test.get_testset()
        if parent isa RAITestSet
            default_report = parent.report
            default_distributed = parent.distributed
        end
    end

    return RAITestSet(dts, something(report, default_report), something(distributed, default_distributed))
end

is_distributed(ts::RAITestSet) = ts.distributed
is_distributed(ts::Test.AbstractTestSet) = false

function distribute_test(f, ts::RAITestSet)
    ref = Threads.@spawn f()
    push!(ts.distributed_tests, ref)
end

function record(ts::RAITestSet, child::RAITestSet)
    junit_record!(ts.junit, child.junit)
    record(ts.dts, child.dts)
end
record(ts::RAITestSet, child::AbstractTestSet) = record(ts.dts, child)
record(ts::Test.DefaultTestSet, child::RAITestSet) = record(ts, child.dts)
record(ts::RAITestSet, res::Test.Result) = record(ts.dts, res)

# Record any results directly stored and fetch results from any listed concurrent tests
# If this is the parent then show results
function finish(ts::RAITestSet)
    for t in ts.distributed_tests
        record(ts, fetch(t))
    end
    if Test.get_testset_depth() > 0
        # Attach this test set to the parent test set
        parent_ts = Test.get_testset()
        record(parent_ts, ts)
        return ts
    end
    finish(ts.dts)

    # We are the root testet, Write JUnit XML
    if ts.report
        projectfile = Base.active_project()
        proj_name = something(Pkg.Types.read_project(projectfile).name, "")
        ReTestItems.write_junit_file(proj_name, dirname(projectfile), ts.junit)
    end
    
    return ts
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
    broken_expected::Bool
    broken_found::Bool

    # Added after running, before recording
    logs::Vector{LogRecord}
    error_message::Option{String}

    TestRelTestSet(desc; nested=false, broken=false) = 
        new(Test.DefaultTestSet(desc), nested, broken, false, [], nothing)
end

function record(ts::RAITestSet, child::TestRelTestSet)
    tc = JUnitTestCase(child.dts)
    # Populate logs if error message is set
    tc.error_message = child.error_message
    if !isnothing(tc.error_message)
        io = IOBuffer()
        playback_log.(io, tc.logs)
        tc.logs = take!(io)
    end
    junit_record!(ts.junit, tc)
    record(ts.dts, child.dts)
end
record(ts::TestRelTestSet, child::AbstractTestSet) = record(ts.dts, child)
record(ts::Test.DefaultTestSet, child::TestRelTestSet) = record(ts, child.dts)
record(ts::TestRelTestSet, res::Test.Result) = record(ts.dts, res)

# Change error/fail to broken if expected and record as such. If not expected, 
# log the failure and record the result.
function record(ts::TestRelTestSet, t::Union{Test.Fail, Test.Error})
    if ts.broken_expected
        ts.broken_found = true
        push!(ts.dts.results, Test.Broken(t.test_type, t.orig_expr))
    else
        log_test_error(ts, t)
        push!(ts.dts.results, t)
    end
    return t
end

function finish(ts::TestRelTestSet)
    if ts.broken_expected && !ts.broken_found
        # If we expect broken tests and everything passes, drop the results and 
        # replace with an unbroken Error
        ts.dts.n_passed = 0
        empty!(ts.dts.results)

        # Default unbroken message doesn't make sense for @test_rel
        @error """Unexpected pass
        Got correct result: $(ts.dts.description) 
        Please remove `broken` flag if no longer broken.
        """
        t = Test.Error(:test_unbroken, ts.dts.description, "", "", LineNumberNode(0))
        push!(ts.dts.results, t)
    end

    if Test.get_testset_depth() > 0
        # Attach this test set to the parent test set
        parent_ts = Test.get_testset()
        record(parent_ts, ts)
        return ts
    end
    !ts.nested && finish(ts.dts)
    return ts
end

function get_log_header(ts::TestRelTestSet, duration, database, engine_name)
    io, ctx = get_logging_io()

    # status
    anyerror(ts) && write(ctx, "[ERROR]")
    anyfail(ts) && write(ctx, "[FAIL]")
    all_pass = !anyerror(ts) && !anyfail(ts)
    all_pass && write(ctx, "[PASS]")
    
    # core info
    name = ts.dts.description
    write(ctx, " $name, duration=$duration")

    # tail
    if all_pass
        txnids = Set()
        for log in ts.logs
            if haskey(log.kwargs, :transaction_id)
                push!(txnids, log.kwargs[:transaction_id])
            end
        end
        write(ctx, """ TxIDs=[$(join(txnids, ", "))]""")
    else
        write(ctx, "\n\ndatabase=$database\nengine_name=$engine_name")
    end
    
    return String(take!(io))
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