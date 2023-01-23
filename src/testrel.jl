using RAI
using RAI: TransactionResponse
using Arrow

using Random: MersenneTwister
using Test
using UUIDs

mutable struct ContextWrapper
    context::Context
end

# Generates a name for the given base name that makes it unique between multiple
# processing units
function gen_safe_name(basename)
    return "$(basename)-p$(getpid())-t$(Base.Threads.threadid())-$(UUIDs.uuid4(MersenneTwister()))"
end

TEST_CONTEXT_WRAPPER::ContextWrapper = ContextWrapper(Context(load_config()))

function get_context()::Context
    return TEST_CONTEXT_WRAPPER.context
end

function set_context(new_context::Context)
    return TEST_CONTEXT_WRAPPER.context = new_context
end

function create_test_database(clone_db::Union{Nothing, String} = nothing)::String
    # TODO: Change to 'test-' when the account is changed
    schema = gen_safe_name("julia-sdk-test")

    return create_database(get_context(), schema; source = clone_db).database.name
end

function delete_test_database(name::String)
    return delete_database(get_context(), name)
end

"""
    test_expected(expected::AbstractDict, results})

Given a Dict of expected relations, test if the actual results contain those relations.
Types and contents of the relations must match.
"""
function test_expected(expected::AbstractDict, results, debug::Bool = false)
    # No testing to do, return immediaely
    isempty(expected) && return
    if isnothing(results)
        @info("No results")
        return false
    end

    for e in expected
        name = string(e.first)
        debug && println("looking for expected result for " * name)
        if e.first isa Symbol
            name = "/:"
            if !is_special_symbol(e.first)
                name = "/:output/:"
            end

            name *= string(e.first)

            # Now determine types
            name *= type_string(e.second)
        end

        # Expected results can be a tuple, or a vector of tuples
        # Actual results are an arrow table that can be iterated over
        expected_result_tuple_vector = sort(to_vector_of_tuples(e.second))

        # Empty results will not be in the output so check for non-presence
        if isempty(expected_result_tuple_vector)
            if haskey(results, name)
                println("Expected empty " * name * " not empty")
                return false
            end
            continue
        end
        if !haskey(results, name)
            println("Expected relation ", name, " not found")
            debug && @info("results", results)
            return false
        end

        # Existence check only
        expected_result_tuple_vector == [()] && continue

        # convert actual results to a vector for comparison
        actual_result = results[name]
        actual_result_vector = sort(collect(zip(actual_result...)))

        if debug
            @info("expected", expected_result_tuple_vector)
            @info("actual", actual_result_vector)
        end
        !isequal(expected_result_tuple_vector, actual_result_vector) && return false
    end

    return true
end

"""
Expected problems are defined by a code and an optional starting line number
Dict(:code => "name" [, :line => <number>])
"""
const Problem = Dict{Symbol, Any}

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
    results), then `broken` can be used to mark the tests as broken and prevent the test from
    failing.

    - `expected_problems::Vector}`: expected problems. The semantics of
      `expected_problems` is that the program must contain a super set of the specified
      errors. When `expected_problems` is `[]`, this means that errors are allowed.
"""
struct Step
    query::Union{String, Nothing}
    install::Dict{String, String}
    broken::Bool
    schema_inputs::AbstractDict
    inputs::AbstractDict
    expected::AbstractDict
    expected_problems::Vector
    expect_abort::Bool
    timeout_sec::Int64
end

function Step(;
    query::Union{String, Nothing} = nothing,
    install::AcceptedSourceTypes = Dict{String, String}(),
    broken::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict(),
    expected_problems::Vector = Problem[],
    expect_abort::Bool = false,
    timeout_sec::Int64 = 1800,
)
    return Step(
        query,
        convert_to_install_kv(install),
        broken,
        schema_inputs,
        inputs,
        expected,
        expected_problems,
        expect_abort,
        timeout_sec,
    )
end

"""
The macro `@test_rel` takes a named tuple as an argument and calls the
`test_rel` function, augmenting the parameters with the location of the macro
call.
"""
macro test_rel(args...)
    # Arguments need to be escaped individually, not all at the same time.
    kwargs = [esc(a) for a in args]

    # QuoteNode is needed around __source__ because it is a LineNumberNode, and
    # in quoted code these already have a meaning.
    if args isa Tuple{String}
        quote
            test_rel(; query = $(kwargs[1]), location = $(QuoteNode(__source__)))
        end
    else
        quote
            test_rel(; location = $(QuoteNode(__source__)), $(kwargs...))
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
  - `expected::AbstractDict`: Expected values in the form `Dict("/:output/:a/Int64" => [1, 2])`.
    Keys can be symbols, which are mapped to /:output/:[symbol] and type derived from the values.
    or a type that can be converted to string and used as relation path.
  - `expected_problems::Vector`: expected problems. The semantics of
    `expected_problems` is that the program must contain a super set of the specified
    error codes.
  - `include_stdlib::Bool`: boolean that specifies whether to include the stdlib
  - `install::Dict{String, String}`: source files to install in the database.
  - `schema_inputs::AbstractDict`: input schema for the transaction
  - `inputs::AbstractDict`: input data to the transaction
  - `abort_on_error::Bool`: boolean that specifies whether to abort on any
    triggered error.
  - `debug::Bool`: boolean that specifies debugging mode.
  - `debug_trace::Bool`: boolean that specifies printing out the debug_trace
  - `expect_abort::Bool`: boolean indicating if the transaction is expected to abort. If it is
    expected to abort, but it does not, then the test fails.
  - `timeout_sec`: an upper bound on test execution time.
  - `broken::Bool`: if the test is not currently correct (wrt the `expected`
    results), then `broken` can be used to mark the tests as broken and prevent the test from
    failing.
  - `engine::String` (optional): the name of an existing engine where tests will be executed
"""
function test_rel(;
    query::Union{String, Nothing} = nothing,
    steps::Vector{Step} = Step[],
    name::Union{String, Nothing} = nothing,
    location::Union{LineNumberNode, Nothing} = nothing,
    include_stdlib::Bool = true,
    install::AcceptedSourceTypes = Dict{String, String}(),
    abort_on_error::Bool = false,
    debug::Bool = false,
    debug_trace::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict(),
    expected_problems::Vector = Problem[],
    expect_abort::Bool = false,
    timeout_sec::Int64 = 1800,
    broken::Bool = false,
    clone_db::Union{String, Nothing} = nothing,
    engine::Union{String, Nothing} = nothing,
)
    query !== nothing && insert!(
        steps,
        1,
        Step(;
            query = query,
            expected = expected,
            expected_problems = expected_problems,
            expect_abort = expect_abort,
            timeout_sec = timeout_sec,
            broken = broken,
        ),
    )

    # Perform all inserts before other tests
    if !isempty(install)
        insert!(steps, 1, Step(; install = convert_to_install_kv(install)))
    end
    if !isempty(schema_inputs)
        insert!(steps, 1, Step(; schema_inputs = schema_inputs))
    end
    if !isempty(inputs)
        insert!(steps, 1, Step(; inputs = inputs))
    end

    return test_rel_steps(;
        steps = steps,
        name = name,
        location = location,
        include_stdlib = include_stdlib,
        abort_on_error = abort_on_error,
        debug = debug,
        debug_trace = debug_trace,
        clone_db = clone_db,
        engine = engine,
    )
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
    name::Union{String, Nothing} = nothing,
    location::Union{LineNumberNode, Nothing} = nothing,
    include_stdlib::Bool = true,
    abort_on_error::Bool = false,
    debug::Bool = false,
    debug_trace::Bool = false,
    clone_db::Union{String, Nothing} = nothing,
    engine::Union{String, Nothing} = nothing,
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
        insert!(steps, 1, Step(; query = config_query))
    end

    parent = Test.get_testset()
    if parent isa ConcurrentTestSet
        ref = Threads.@spawn _test_rel_steps(;
            steps = steps,
            name = name,
            location = location,
            debug = debug,
            quiet = true,
            clone_db = clone_db,
            user_engine = engine,
        )
        add_test_ref(parent, ref)
    else
        _test_rel_steps(;
            steps = steps,
            name = name,
            location = location,
            debug = debug,
            clone_db = clone_db,
            user_engine = engine,
        )
    end
end

# This internal function executes `test_rel`
function _test_rel_steps(;
    steps::Vector{Step},
    name::Union{String, Nothing},
    location::Union{LineNumberNode, Nothing},
    debug::Bool = false,
    quiet::Bool = false,
    clone_db::Union{String, Nothing} = nothing,
    user_engine::Union{String, Nothing} = nothing,
)
    if isnothing(name)
        name = ""
    else
        name = name * " at "
    end

    if !isnothing(location)
        path = joinpath(splitpath(string(location.file))[max(1, end - 2):end])
        resolved_location = string(path, ":", location.line)

        name *= resolved_location
    end

    test_engine = user_engine === nothing ? get_test_engine() : user_engine
    debug && println(name, " using test engine: ", test_engine)
    schema = create_test_database(clone_db)

    try
        type = quiet ? QuietTestSet : Test.DefaultTestSet
        @testset type "$(string(name))" begin
            elapsed_time = @timed begin
                for (index, step) in enumerate(steps)
                    _test_rel_step(index, step, schema, test_engine, name, length(steps), debug)
                end
            end
            println(name, ": time", elapsed_time)
        end
    finally
        # If database deletion fails
        try
            delete_test_database(schema)
        catch
            println("Could not delete test database: ", schema)
        end
        user_engine === nothing && release_test_engine(test_engine)
    end
end

function wait_until_done(ctx::Context, id::AbstractString, timeout_sec::Int64)
    start_time_ns = time_ns()
    delta_sec = 1

    txn = get_transaction(ctx, id)
    while !RAI.transaction_is_done(txn)
        duration = time_ns() - start_time_ns
        if duration > timeout_sec * 1e9
            error("Transaction $id timed out after $timeout_sec seconds")
        end

        sleep(delta_sec)

        txn = get_transaction(ctx, id)
    end

    m = Threads.@spawn get_transaction_metadata(ctx, id)
    p = Threads.@spawn get_transaction_problems(ctx, id)
    r = Threads.@spawn get_transaction_results(ctx, id)
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
    timeout_sec::Int64)
    transactionResponse = exec_async(context, schema, engine, program)
    txn_id = transactionResponse.transaction.id
    @info("Executing $name with txn $txn_id")

    # The response may already contain the result. If so, we can return it immediately
    if !isnothing(transactionResponse.results)
        return transactionResponse
    end
    # The transaction was not immediately completed.
    # Poll until the transaction is done, or times out, then return the results.
    try
        return RAITest.wait_until_done(context, txn_id, timeout_sec)
    catch
        # The transaction errored (not necessarily due to the timeout). Cancel the transaction and rethrow.
        @info("Cancelling failed transaction")
        RAI.cancel_transaction(context, txn_id)
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
    debug::Bool,
)
    if !isnothing(step.query)
        program = step.query
    else
        program = ""
    end

    #Append inputs to program
    program *= convert_input_dict_to_string(step.inputs)

    #Append schema inputs to program
    program *= convert_input_dict_to_string(step.schema_inputs)

    #TODO: Remove this when the incoming tests are appropriately rewritten
    program *= generate_output_string_from_expected(step.expected)

    debug && println(">>>>\n", program, "\n<<<<")
    step_postfix = steps_length > 1 ? " - step$index" : ""

    @testset BreakableTestSet "$(string(name))$step_postfix" broken = step.broken begin
        try
            if !isempty(step.install)
                load_models(get_context(), schema, engine, step.install)
            end

            # Don't test empty strings
            if program == ""
                return nothing
            end

            response = _execute_test(name, get_context(), schema, engine, program, step.timeout_sec)

            state = response.transaction.state

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
            #   If problems are expected, they are found (other problems are ignored)
            #   If no problems are expected, warning level problems are ignored

            unexpected_errors_found = false
            # Check if there were any unexpected errors/exceptions
            for problem in problems
                if contains_problem(step.expected_problems, problem)
                    debug && @info("Expected problem", problem)
                else
                    unexpected_errors_found |= problem[:severity] == "error"
                    unexpected_errors_found |= problem[:severity] == "exception"
                    println(name, " - Unexpected: ", problem[:code])
                    debug && @info("Unexpected problem", problem)
                end
            end

            if !isempty(step.expected)
                @test test_expected(step.expected, results_dict, debug)
            end

            # Allow all errors if any problems were expected
            if !isempty(step.expected_problems)
                unexpected_errors_found = false
            end

            if !step.expect_abort
                @test state == "COMPLETED" && !unexpected_errors_found
            else
                @test state == "ABORTED"
            end
        catch e
            Base.display_error(stderr, current_exceptions())
            rethrow()
        end
        return nothing
    end
end
