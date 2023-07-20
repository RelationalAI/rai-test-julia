using RAI
using RAI: TransactionResponse
using Arrow

using Random: MersenneTwister
using Test
using UUIDs

# Generates a name for the given base name that makes it unique between multiple
# processing units
# Generated names are truncated at 63 characters. This limit is reached when the
# base name is 28 characters long. Longer base names can be used but uniqueness
# is not guaranteed
function gen_safe_name(basename)
    name = "$(basename)-$(UUIDs.uuid4(MersenneTwister()))"
    return name[1:min(sizeof(name), 63)]
end

const TEST_CONTEXT = Ref{Option{Context}}(nothing)

function get_context()
    return TEST_CONTEXT[]
end

function set_context(new_context::Context)
    return TEST_CONTEXT[] = new_context
end

function create_test_database_name(; default_basename="test_rel")::String
    basename = get(ENV, "TEST_REL_DB_BASENAME", default_basename)
    return gen_safe_name(basename)
end

function create_test_database(name::String, clone_db::Option{String}=nothing)
    return create_database(get_context(), name; source=clone_db).database
end

function delete_test_database(name::String)
    return delete_database(get_context(), name)
end

"""
    test_expected(expected::AbstractDict, results, testname)

Given a Dict of expected relations, test if the actual results contain those relations.
Types and contents of the relations must match.
"""
function test_expected(expected::AbstractDict, results, testname::String)
    # No testing to do, return immediaely
    isempty(expected) && return true
    if isnothing(results)
        @info("$testname: No results")
        return false
    end

    for e in expected
        name = relation_id(e.first, e.second)
        @debug("$testname: looking for expected result for relation " * string(e.first))

        # Expected results can be a tuple, or a vector of tuples
        # Actual results are an arrow table that can be iterated over
        expected_result_tuple_vector = sort(to_vector_of_tuples(e.second))

        # Empty results will not be in the output so check for non-presence
        if isempty(expected_result_tuple_vector) && !haskey(results, name)
            @debug("#testname: Expected empty $name was successfully not found")
            continue
        end

        if !haskey(results, name)
            @info("$testname: Expected relation $name not found")
            @debug("$testname: Results", results)
            return false
        end

        # Existence check only
        expected_result_tuple_vector == [()] && continue

        # convert actual results to a vector for comparison
        actual_result = results[name]
        if isempty(actual_result)
            actual_result_vector = [()]
        else
            actual_result_vector = sort(collect(zip(actual_result...)))
        end

        if !isequal(expected_result_tuple_vector, actual_result_vector)
            @warn(
                "$testname: Expected result vs. actual for $name",
                expected_result_tuple_vector,
                actual_result_vector
            )
            return false
        else
            @debug(
                "$testname: Expected result vs. actual for $name",
                expected_result_tuple_vector,
                actual_result_vector
            )
        end
    end

    return true
end

const AcceptedSourceTypes =
    Union{String, Pair{String, String}, Vector{String}, Dict{String, String}}

convert_to_install_kv(install_dict::Dict{String, String}) = install_dict
convert_to_install_kv(install_pair::Pair{String, String}) = Dict(install_pair)
convert_to_install_kv(install_string::String) = convert_to_install_kv([install_string])
function convert_to_install_kv(install_vector::Vector{String})
    models = Dict{String, String}()
    for i in enumerate(install_vector)
        models["test_install" * string(i[1])] = i[2]
    end
    return models
end

"""
    Transaction Step used for `test_rel`

    - `install::Dict{String, String}`:
        sources to install in the database.

    - `broken::Bool`: if the computed values are not currently correct (wrt the `expected`
    results), then `broken` can be used to mark the tests as broken and prevent the test
    from failing.

    - `expected_problems::Vector}`: expected problems. The semantics of
      `expected_problems` is that the program must contain a super set of the specified
      errors. When `expected_problems` is `[]`, this means that errors are allowed.
      Expected problems are defined by a code and an optional starting line number
      e.g. `Dict(:code => "name" [, :line => <number>])`
"""
struct Step
    query::Option{String}
    install::Dict{String, String}
    broken::Bool
    schema_inputs::AbstractDict
    inputs::AbstractDict
    expected::AbstractDict
    expected_problems::Vector
    allow_unexpected::Symbol
    expect_abort::Bool
    timeout_sec::Int64
    readonly::Bool
end

function Step(;
    query::Option{String}=nothing,
    install::AcceptedSourceTypes=Dict{String, String}(),
    broken::Bool=false,
    schema_inputs::AbstractDict=Dict(),
    inputs::AbstractDict=Dict(),
    expected::AbstractDict=Dict(),
    expected_problems::Vector=[],
    allow_unexpected::Symbol=:warning,
    expect_abort::Bool=false,
    timeout_sec::Int64=1800,
    readonly::Bool=false,
)
    return Step(
        query,
        convert_to_install_kv(install),
        broken,
        schema_inputs,
        inputs,
        expected,
        expected_problems,
        allow_unexpected,
        expect_abort,
        timeout_sec,
        readonly,
    )
end

# Helpers for commonly used Steps
ReadQuery(s::String; kwargs...) = Step(; query=s, readonly=true, kwargs...)
WriteQuery(s::String; kwargs...) = Step(; query=s, readonly=false, kwargs...)
Install(x; kwargs...) = Step(; install=x, kwargs...)

"""
The macro `@test_rel` takes a named tuple as an argument and calls the
`test_rel` function, augmenting the parameters with the location of the macro
call.
"""
macro test_rel(args...)
    # Arguments need to be escaped individually, not all at the same time.
    # `setup` keyword is silently ignored, for compatibility with our old internal @test_rel.
    kwargs = [esc(a) for a in args if !(a isa Expr && a.args[1] == :setup)]

    # QuoteNode is needed around __source__ because it is a LineNumberNode, and
    # in quoted code these already have a meaning.
    if args isa Tuple{String}
        quote
            test_rel(; query=$(kwargs[1]), location=$(QuoteNode(__source__)))
        end
    else
        quote
            test_rel(; location=$(QuoteNode(__source__)), $(kwargs...))
        end
    end
end

"""
    test_rel(query; kwargs...)

Run a single step Rel testcase.

If `expected_problems` is not set, then no errors are
allowed. The test fails if there are any errors in the program.

It is preferred to use integrity constraints to set test conditions. If the integrity
constraints have any compilation errors, then the test will still fail (unless
`expected_problems` is set).

Note that `test_rel` creates a new schema for each test.

  - `query::String`: The query to use for the test
  - `name::String`: name of the testcase
  - `location::LineNumberNode`: Sourcecode location
  - `expected::AbstractDict`: Expected values in the form
    `Dict("/:output/:a/Int64" => [1, 2])` or
    `Dict(:a => p1, 2])`
    Keys can be symbols, which are mapped to /:output/:[symbol] and type derived from the
    values, or a type that can be converted to string and used as relation path.
  - `expected_problems::Vector`: expected problems. The semantics of
    `expected_problems` is that the program must contain a super set of the specified
    error codes.
  - `allow_unexpected::Symbol`: ignore problems with severity equal or lower than
    specified. Accepted values are `:none`, `:warning`, `:error`.
  - `include_stdlib::Bool`: boolean that specifies whether to include the stdlib
  - `install::Dict{String, String}`: source files to install in the database.
  - `schema_inputs::AbstractDict`: input schema for the transaction
  - `inputs::AbstractDict`: input data to the transaction
  - `abort_on_error::Bool`: boolean that specifies whether to abort on any
    triggered error.
  - `debug::Bool`: boolean that specifies debugging mode.
  - `debug_trace::Bool`: boolean that specifies printing out the debug_trace
  - `expect_abort::Bool`: boolean indicating if the transaction is expected to abort. If it
    is expected to abort, but it does not, then the test fails.
  - `timeout_sec`: an upper bound on test execution time.
  - `broken::Bool`: if the test is not currently correct (wrt the `expected`
    results), then `broken` can be used to mark the tests as broken and prevent the test
    from failing.
  - `engine::String` (optional): the name of an existing engine where tests will be executed
"""
function test_rel(;
    query::Option{String}=nothing,
    steps::Vector{Step}=Step[],
    name::Option{String}=nothing,
    location::Option{LineNumberNode}=nothing,
    include_stdlib::Bool=true,
    install::AcceptedSourceTypes=Dict{String, String}(),
    abort_on_error::Bool=false,
    debug::Bool=false,
    debug_trace::Bool=false,
    schema_inputs::AbstractDict=Dict(),
    inputs::AbstractDict=Dict(),
    expected::AbstractDict=Dict(),
    expected_problems::Vector=[],
    allow_unexpected::Symbol=:warning,
    expect_abort::Bool=false,
    timeout_sec::Int64=1800,
    broken::Bool=false,
    clone_db::Option{String}=nothing,
    engine::Option{String}=nothing,
)
    query !== nothing && insert!(
        steps,
        1,
        Step(;
            query=query,
            expected=expected,
            expected_problems=expected_problems,
            allow_unexpected=allow_unexpected,
            expect_abort=expect_abort,
            timeout_sec=timeout_sec,
            broken=broken,
        ),
    )

    # Perform all inserts before other tests
    if !isempty(install)
        insert!(steps, 1, Step(; install=convert_to_install_kv(install)))
    end
    if !isempty(schema_inputs)
        insert!(steps, 1, Step(; schema_inputs=schema_inputs))
    end
    if !isempty(inputs)
        insert!(steps, 1, Step(; inputs=inputs))
    end

    debug_env = get(ENV, "JULIA_DEBUG", "")
    if debug
        debug_env = debug_env * ",RAITest"
    end

    return withenv("JULIA_DEBUG" => debug_env) do
        return test_rel_steps(;
            steps=steps,
            name=name,
            location=location,
            include_stdlib=include_stdlib,
            abort_on_error=abort_on_error,
            debug=debug,
            debug_trace=debug_trace,
            clone_db=clone_db,
            engine=engine,
        )
    end
end

"""
test_rel_steps(query; kwargs...)

Run a Rel testcase composed of a series of steps.

If `expected_problems` is not set, then no errors are
allowed. The test fails if there are any errors in the program.

It is preferred to use integrity constraints to set test conditions. If the integrity
constraints have any compilation errors, then the test will still fail (unless
`expected_problems` is set).

Note that `test_rel` creates a new schema for each test.

  - `steps`::Vector{Step}: a vector of Steps that represent a series of transactions in the
    test
  - `name::String`: name of the testcase
  - `location::LineNumberNode`: Sourcecode location
  - `include_stdlib::Bool`: boolean that specifies whether to include the stdlib
  - `abort_on_error::Bool`: boolean that specifies whether to abort on any
    triggered error.
  - `debug::Bool`: boolean that specifies debugging mode.
  - `debug_trace::Bool`: boolean that specifies printing out the debug_trace
  - `engine::String` (optional): the name of an existing engine where tests will be executed
"""
function test_rel_steps(;
    steps::Vector{Step},
    name::Option{String}=nothing,
    location::Option{LineNumberNode}=nothing,
    include_stdlib::Bool=true,
    abort_on_error::Bool=false,
    debug::Bool=false,
    debug_trace::Bool=false,
    clone_db::Option{String}=nothing,
    engine::Option{String}=nothing,
)
    # Setup steps that run before the first testing Step
    config_query = ""
    if !include_stdlib
        config_query *= """def delete:rel:catalog:model = rel:catalog:model\n"""
    end

    if debug && !debug_trace
        config_query *= """def insert:rel:config:debug = "basic"\n"""
    end

    if debug_trace
        # Also set debug for its use in tracing test_rel execution
        debug = true
        config_query *= """def insert:rel:config:debug = "trace"\n"""
    end

    if abort_on_error
        config_query *= """def insert:rel:config:abort_on_error = true\n"""
    end

    if config_query != ""
        insert!(steps, 1, Step(; query=config_query))
    end

    parent = Test.get_testset()

    if isnothing(name)
        if isnothing(location)
            name = ""
        else
            name = string(basename(string(location.file)), ":", location.line)
        end
    end

    if !isnothing(location)
        path = joinpath(splitpath(string(location.file))[max(1, end - 2):end])
        resolved_location = string(path, ":", location.line)
        # Use `@` so it's easier to strip this info later.
        name = string(name, " @ ", resolved_location)
    end

    distribute_test(parent) do
        return _test_rel_steps(;
            steps,
            name,
            nested=is_distributed(parent),
            debug,
            clone_db,
            user_engine=engine,
        )
    end
end

# This internal function executes `test_rel`
function _test_rel_steps(;
    steps::Vector{Step},
    name::Option{String},
    nested::Bool=false,
    debug::Bool=false,
    clone_db::Option{String}=nothing,
    user_engine::Option{String}=nothing,
)
    # Generate a name for the test database
    schema = create_test_database_name()
    @debug("$name: Using database name $schema")

    test_engine = user_engine === nothing ? get_test_engine() : user_engine
    @debug("$name: using test engine: $test_engine")

    logger = TestLogger(; catch_exceptions=true)

    try
        stats = @timed Logging.with_logger(logger) do
            @testset TestRelTestSet nested = nested "$name" begin
                create_test_database(schema, clone_db)
                for (index, step) in enumerate(steps)
                    inner_ts = _test_rel_step(
                        index,
                        step,
                        schema,
                        test_engine,
                        name,
                        length(steps),
                    )
                    # short circuit if something errored
                    if anyerror(inner_ts) && index < length(steps)
                        @error "Terminal error running $name - not executing further steps"
                        break
                    end
                end
            end
        end
        duration = sprint(show, stats.time; context=:compact => true)
        ts = stats.value
        ts.logs = logger.logs

        check_flaky(name, logger.logs)

        log_header = get_log_header(ts, duration, schema, test_engine)
        if anyerror(ts) || anyfail(ts)
            ts.error_message = log_header
            io, ctx = get_logging_io()
            write(ctx, log_header)
            write(ctx, "\n\nCAPTURED LOGS:\n")
            playback_log.(ctx, logger.logs)
            msg = String(take!(io))
            @error msg database = schema engine_name = test_engine
        elseif debug || !nested
            io, ctx = get_logging_io()
            playback_log.(ctx, logger.logs)
            msg = String(take!(io))
            println(msg)
        else
            @info log_header
        end

        ts
    catch err
        io, ctx = get_logging_io()
        write(ctx, "[ERROR] Something went wrong running test $name \n\n CAPTURED LOGS:\n")

        # dump all of the captured logs
        playback_log.(ctx, logger.logs)
        Base.show(ctx, err)
        msg = String(take!(io))

        @error msg database = schema engine_name = test_engine test_name = name
    finally
        try
            delete_test_database(schema)
        catch
            @warn("Could not delete test database: ", schema)
        end
        user_engine === nothing && release_test_engine(test_engine)
    end
end

function check_flaky(name::String, logs::Vector{LogRecord})
    retries = 0
    request_ids = []
    for log in logs
        if haskey(log.kwargs, :submit_failed)
            if log.kwargs[:retry_number] > retries
                retries = log.kwargs[:retry_number]
            end
            push!(request_ids, log.kwargs[:request_id])
        end
    end

    if retries > 0
        req_ids = """ReqIds=[$(join(request_ids, ", "))]"""
        @warn """[FLAKY] $name: transaction submission had to be retried $retries times $req_ids"""
    end
end

function wait_until_done(ctx::Context, id::AbstractString, timeout_sec::Int64)
    start_time_ns = time_ns()
    delta_sec = 1

    txn = get_transaction(ctx, id; readtimeout=timeout_sec)
    while !RAI.transaction_is_done(txn)
        duration = time_ns() - start_time_ns
        if duration > timeout_sec * 1e9
            error("Transaction $id timed out after $timeout_sec seconds")
        end

        sleep(delta_sec)

        remaining = timeout_sec - floor(Int64, duration / 1e9)
        txn = get_transaction(ctx, id; readtimeout=remaining)
    end

    # The server has finished processing the transaction so we assume that worst-case
    # timeouts can be much shorter
    m = Threads.@spawn get_transaction_metadata(ctx, id; readtimeout=120)
    p = Threads.@spawn get_transaction_problems(ctx, id; readtimeout=120)
    r = Threads.@spawn get_transaction_results(ctx, id; readtimeout=120)
    try
        return TransactionResponse(txn, fetch(m), fetch(p), fetch(r))
    catch e
        @info("Transaction response error", e)
        # (We use has_wrapped_exception to unwrap the TaskFailedException.)
        if RAI.has_wrapped_exception(e, HTTPError) &&
           RAI.unwrap_exception_to_root(e).status_code == 404
            # This is an (unfortunately) expected case if the engine crashes during a
            # transaction, or the transaction is cancelled. The transaction is marked
            # as ABORTED, but it has no results.
            return TransactionResponse(txn, nothing, nothing, nothing)
        else
            rethrow()
        end
    end
end

# Execute the test query. Outputs the transaction id and returns the response when done.
function _execute_test(
    name::String,
    context::Context,
    schema::String,
    engine::String,
    program::String,
    timeout_sec::Int64,
    readonly::Bool;
    retry_number=1,
)
    request_id = string(UUIDs.uuid4())
    @debug("$name: Starting execution ReqId=$request_id")
    rsp = try
        # Exec async really should return after 2-3 seconds
        headers = ["X-Request-ID" => request_id]
        exec_async(context, schema, engine, program; readtimeout=30, readonly, headers)
    catch e
        @error "$name: Failed to submit transaction\n\n$e" retry_number submit_failed = true request_id
        if retry_number < 3
            # Transactions may have been successfully submitted, even though the connection
            # failed. Sleep for 30s to give the engine a chance to finish.
            # This will help simple tests with a briefly flaky connection. Long tests will
            # not benefit.
            sleep(30)
            # Try again
            return _execute_test(
                name,
                context,
                schema,
                engine,
                program,
                timeout_sec,
                readonly;
                retry_number=retry_number + 1,
            )
        end
        rethrow()
    end

    txn_id = rsp.transaction.id
    @info "$name: Executing with txn $txn_id" transaction_id = txn_id

    # The transaction was not immediately completed.
    # Poll until the transaction is done, or times out, then return the results.
    try
        return RAITest.wait_until_done(context, txn_id, timeout_sec)
    catch e
        # The transaction errored (not necessarily due to the timeout). Cancel the
        # transaction and rethrow.
        @info "$name: Cancelling failed transaction ($txn_id)" e txn_id name
        RAI.cancel_transaction(context, txn_id; readtimeout=timeout_sec)
        rethrow()
    end
end

# This internal function executes a single step of a `test_rel`
function _test_rel_step(
    index::Int,
    step::Step,
    schema::String,
    engine::String,
    name::String,
    steps_length::Int,
)
    program = something(step.query, "")

    #Append inputs to program
    program *= convert_input_dict_to_string(step.inputs)

    #Append schema inputs to program
    program *= convert_input_dict_to_string(step.schema_inputs)

    #TODO: Remove this when the incoming tests are appropriately rewritten
    program *= generate_output_string_from_expected(step.expected)

    @debug("$name: generated program", program)
    step_postfix = steps_length > 1 ? " - step$index" : ""
    name = "$(string(name))$step_postfix"

    @testset TestRelTestSet "$name" broken = step.broken begin
        if !isempty(step.install)
            load_models(get_context(), schema, engine, step.install)
        end

        # Don't test empty strings
        if program == ""
            @goto end_of_test
        end

        response = _execute_test(
            name,
            get_context(),
            schema,
            engine,
            program,
            step.timeout_sec,
            step.readonly,
        )

        state = response.transaction.state
        @debug("Response state:", state)
        results = response.results

        results_dict = result_table_to_dict(results)
        problems = extract_problems(results_dict)

        # Check that expected problems were found
        for expected_problem in step.expected_problems
            expected_problem_found = contains_problem(problems, expected_problem)
            @test expected_problem_found
        end

        # PASS:
        #   If an abort is expected it is encountered
        #   If no abort is expected it is not encountered
        #   If results are expected, they are found (other results are ignored)
        #   If problems are expected, they are found
        #   Unexpected problems with severity worse than allowable are not found

        unexpected_errors_found = false
        error_levels = Set()
        # Exceptions are always unexpected
        if step.allow_unexpected == :error
            push!(error_levels, "exception")
        elseif step.allow_unexpected == :warning
            push!(error_levels, "error")
            push!(error_levels, "exception")
        elseif step.allow_unexpected == :none
            push!(error_levels, "warning")
            push!(error_levels, "error")
            push!(error_levels, "exception")
        end

        # Check if there were any unexpected errors/exceptions
        for problem in problems
            if contains_problem(step.expected_problems, problem)
                @debug("$name: Expected problem", problem)
            else
                unexpected_errors_found |= in(problem[:severity], error_levels)
                @info("$name: Unexpected problem: $(problem)")
                @debug("$name: Unexpected problem", problem)
            end
        end

        @test test_expected(step.expected, results_dict, name)

        if !step.expect_abort
            @test state == "COMPLETED"
            @test !unexpected_errors_found
            if state == "ABORTED"
                @info(
                    "$name: Transaction $(response.transaction.id) aborted due to \"$(response.transaction.abort_reason)\""
                )
            end
        else
            @test state == "ABORTED"
        end

        @label end_of_test
    end
end
