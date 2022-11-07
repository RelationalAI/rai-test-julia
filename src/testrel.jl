using RAI
using RAI: TransactionResponse
using Arrow

using Random: MersenneTwister
using Test
using UUIDs


# Generates a name for the given base name that makes it unique between multiple
# processing units
function gen_safe_name(basename)
    return "$(basename)-p$(getpid())-t$(Base.Threads.threadid())-$(UUIDs.uuid4(MersenneTwister()))"
end

context::Context = Context(load_config())

function get_context()::Context
    return context
end

function create_test_database()::String
    # TODO: Change to 'test-' when the account is changed
    schema = gen_safe_name("julia-sdk-test")

    return create_database(get_context(), schema).database.name
end

function delete_test_database(name::String)
   return delete_database(get_context(), name)
end

"""
    test_expected(expected::AbstractDict, results})

Given a Dict of expected relations, test if the actual results contain those relations.
Types and contents of the relations must match.

"""
function test_expected(
        expected::AbstractDict,
        results,
        debug::Bool = false)
    # No testing to do, return immediaely
    isempty(expected) && return
    if results === nothing
        println("No results")
        return false
    end

    for e in expected
        name = string(e.first)
        if e.first isa Symbol
            name = "/:"
            if e.first != :output
                name = "/:output/:"
            end

            name *= string(e.first)

            # Now determine types
            name *= type_string(e.second)
        end

        # Check result key exists
        if !haskey(results, name)
            println("Expected relation ", name, " not found")
            return false
        end

        actual_result = results[name]

        # Existence check
        e.second == [()] && return true
        e.second == () && return true

        # Expected results can be a tuple, or a vector of tuples
        # Actual results are an arrow table that can be iterated over

        expected_result_tuple_vector = sort(to_vector_of_tuples(e.second))

        # convert actual results to a vector for comparison
        actual_result_vector = sort(collect(zip(actual_result...)))

        if debug
            @info("expected", expected_result_tuple_vector)
            @info("actual", actual_result_vector)
        end
        return isequal(expected_result_tuple_vector, actual_result_vector)
    end

    return true
end

struct Problem
    code::String
    severity::Union{String, Nothing}
    line::Union{Int64, Nothing}
end

Problem(problem::Problem) = problem
Problem(problem::Pair) = Problem((problem,))

function Problem(;code::String, severity::Union{String, Nothing} = nothing, line::Union{Int64, Nothing} = nothing)
    return Problem(code, severity, line)
end

function Problem(problem::Tuple)
    code = nothing
    severity = nothing
    line = nothing
    for (k, v) in problem
        if k === :code
            code = string(v)
        elseif k === string(:severity)
            severity = v
        elseif k === :line
            line = v
        end
    end

    return Problem(code = code, severity = severity, line = line)
end

"""
    Transaction Step used for `test_rel`

    - `install::Dict{String, String}`:
        sources to install in the database.

    - `broken::Bool`: if the computed values are not currently correct (wrt the `expected`
    results), then `broken` can be used to mark the tests as broken and prevent the test from
    failing.

    - `expected_problems::Option{Vector{<:Any}}`: expected problems. The semantics of
      `expected_problems` is that the program must contain a super set of the specified
      errors. When `expected_problems` is `[]` instead of `nothing`, then this means that errors
      are allowed.

"""
struct Step
    query::Union{String, Nothing}
    install::Dict{String, String}
    broken::Bool
    schema_inputs::AbstractDict
    inputs::AbstractDict
    expected::AbstractDict
    expected_output::AbstractDict
    expected_problems::Vector
    expect_abort::Bool
end

function Step(;
    query::Union{String, Nothing} = nothing,
    install::Dict{String, String} = Dict{String, String}(),
    broken::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict(),
    expected_output::AbstractDict = Dict(),
    expected_problems::Vector = Problem[],
    expect_abort::Bool = false,
)
    return Step(
        query,
        install,
        broken,
        schema_inputs,
        inputs,
        expected,
        expected_output,
        expected_problems,
        expect_abort,
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
            test_rel(;query = $(kwargs[1]), location = $(QuoteNode(__source__)))
        end
    else
        quote
            test_rel(;location = $(QuoteNode(__source__)), $(kwargs...))
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

!!! warning

    `test_rel` creates a new schema for each test.

- `query::String`: The query to use for the test

- `name::String`: name of the testcase

- `location::LineNumberNode`: Sourcecode location

- `expected::AbstractDict`: Expected values in the form `Dict("/:output/:a/Int64" => [1, 2])`

- `expected_output::AbstractDict`: Expected output values in the form `Dict(:a => [1, 2])`

- `expected_problems::Vector{String}`: expected problems. The semantics of
  `expected_problems` is that the program must contain a super set of the specified
  error codes.

- `engine::String`: The name of an existing compute engine

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

- `broken::Bool`: if the test is not currently correct (wrt the `expected`
  results), then `broken` can be used to mark the tests as broken and prevent the test from
  failing.

"""
function test_rel(;
    query::Union{String, Nothing} = nothing,
    steps::Vector{Step} = Step[],
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
    location::Union{LineNumberNode,Nothing} = nothing,
    include_stdlib::Bool = true,
    install::Dict{String, String} = Dict{String, String}(),
    abort_on_error::Bool = false,
    debug::Bool = false,
    debug_trace::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict(),
    expected_output::AbstractDict = Dict(),
    expected_problems::Vector = Problem[],
    expect_abort::Bool = false,
    broken::Bool = false,
)
    query !== nothing && insert!(steps, 1, Step(
        query = query,
        expected = expected,
        expected_output = expected_output,
        expected_problems = expected_problems,
        expect_abort = expect_abort,
        broken = broken,
        ))

    # Perform all inserts before other tests
    if !isempty(install)
        insert!(steps, 1, Step(
            install = install,
            ))
    end
    if !isempty(schema_inputs)
        insert!(steps, 1, Step(
            schema_inputs = schema_inputs,
            ))
    end
    if !isempty(inputs)
        insert!(steps, 1, Step(
            inputs = inputs,
            ))
    end

    test_rel_steps(;
        steps = steps,
        name = name,
        engine = engine,
        location = location,
        include_stdlib = include_stdlib,
        abort_on_error = abort_on_error,
        debug = debug,
        debug_trace = debug_trace,
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

!!! warning

    `test_rel` creates a new schema for each test.

- `steps`::Vector{Step}: a vector of Steps that represent a series of transactions in the
  test

- `name::String`: name of the testcase

- `location::LineNumberNode`: Sourcecode location

- `engine::String`: The name of an existing compute engine

- `include_stdlib::Bool`: boolean that specifies whether to include the stdlib

- `abort_on_error::Bool`: boolean that specifies whether to abort on any
    triggered error.

- `debug::Bool`: boolean that specifies debugging mode.

- `debug_trace::Bool`: boolean that specifies printing out the debug_trace

"""
function test_rel_steps(;
    steps::Vector{Step},
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
    location::Union{LineNumberNode,Nothing} = nothing,
    include_stdlib::Bool = true,
    abort_on_error::Bool = false,
    debug::Bool = false,
    debug_trace::Bool = false,
)
    if isnothing(name)
        name = "Unnamed test"
    end

    # Setup steps that run before the first testing Step
    if !include_stdlib
        insert!(steps, 1, Step(query="""def delete:rel:catalog:model = rel:catalog:model"""))
    end

    if debug
        insert!(steps, 1, Step(query="""def insert:rel:config:debug = "basic" """))
    end

    if debug_trace
        insert!(steps, 1, Step(query="""def insert:rel:config:debug = "trace" """))
    end

    if abort_on_error
        insert!(steps, 1, Step(query="""def insert:rel:config:abort_on_error = true """))
    end

    parent = Test.get_testset()
    if parent isa ConcurrentTestSet
        ref = Threads.@spawn _test_rel_steps(;
            steps = steps,
            name = name,
            engine = engine,
            location = location,
            debug = debug,
            quiet = true
        )
        add_test_ref(parent, ref)
    else
        _test_rel_steps(;
        steps = steps,
        name = name,
        engine = engine,
        location = location,
        debug = debug,
    )
    end
end

# This internal function executes `test_rel`
function _test_rel_steps(;
    steps::Vector{Step},
    name::String,
    engine::Union{String,Nothing},
    location::Union{LineNumberNode,Nothing},
    debug::Bool = false,
    quiet::Bool = false
)
    test_engine = get_or_create_test_engine(engine)
    println(name, " using test engine: ", test_engine)

    schema = create_test_database()

    if !isnothing(location)
        path = joinpath(splitpath(string(location.file))[max(1,end-2):end])
        resolved_location = string(path, ":", location.line)

        name = name * " at " * resolved_location
    end

    try
        type = quiet ? QuietTestSet : Test.DefaultTestSet
        @testset type "$(string(name))" begin
            elapsed_time = @timed begin
                for (index, step) in enumerate(steps)
                    _test_rel_step(
                        index,
                        step,
                        schema,
                        test_engine,
                        name,
                        length(steps),
                        debug,
                    )
                end
            end
            println(name, ": ", elapsed_time)
        end
    finally
        delete_test_database(schema)
        release_test_engine(test_engine)
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

    program *= generate_output_string_from_expected(step.expected_output)

    #TODO: Remove this when the incoming tests are appropriately rewritten
    program *= generate_output_string_from_expected(step.expected)

    debug && println(">>>>\n", program, "\n<<<<")
    step_postfix = steps_length > 1 ? " - step$index" : ""

    @testset BreakableTestSet "$(string(name))$step_postfix" broken = step.broken begin
        try
            if !isempty(step.install)
                load_model(get_context(), schema, engine,
                        Dict("test_install" => step.install))
            end

            # Don't test empty strings
            if program == ""
                return nothing
            end

            response = exec(get_context(), schema, engine, program)

            state = response.transaction.state

            results = response.results

            results_dict = result_table_to_dict(results)
            problems = extract_problems(results_dict)

            eps = Problem[]
            # Check that expected problems were found
            for expected_problem in step.expected_problems
                ep = Problem(expected_problem)
                push!(eps, ep)

                expected_problem_found = any(p->(p.code == ep.code), problems)
                @test expected_problem_found
            end

            # If there are no expected problems then we expect the transaction to complete
            if !step.expect_abort
                is_error = false
                for problem in problems
                    any(p->(p.code == problem.code), eps) && continue
                    is_error |= problem.severity == "error"
                    println("Unexpected problem type: ", problem.code)
                end

                @test state == "COMPLETED" && is_error == !isempty(step.expected_problems)

                if !isempty(step.expected)
                    @test test_expected(step.expected, results_dict, debug)
                end
            else
                @test state == "ABORTED"
            end
        catch e
            Base.display_error(stderr, current_exceptions())
        end
        return nothing
    end
end
