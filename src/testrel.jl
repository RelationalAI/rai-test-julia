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

function get_context()::Context
    conf = load_config()
    return Context(conf)
end

function create_test_database()::String
    # TODO: Change to 'test-' when the account is changed
    schema = gen_safe_name("mm-test")

    return create_database(get_context(), schema).database.name
end

function delete_test_database(name::String)
   return delete_database(get_context(), name)
end

# Extract relation names from the inputs and adds them to the program
# Turns a dict of name=>vector, with names of form :othername/Type,
# into a series of def name = list of tuples
function convert_input_dict_to_string(inputs::AbstractDict)
    program = ""
    for input in inputs
        name = ""
        tokens = split(input.first, "/")

        for token in tokens
            if startswith(token, ":")
                name *= token
            else
                break
            end
        end

        name = SubString(name, 2)


        program *= "\ndef insert:" * name * " = "

        first = true

        for i in input.second
            if first
                first = false
            else
                program *= "; "
            end
            program *= string(i)
        end
    end
    return program
end

# Extract relation names from the expected output and append them to output
# Turns a dict of name=>vector, with names of form :othername/Type
# into a series of def output:othername = othername
function generate_output_string_from_expected(expected::AbstractDict)
    program = ""

    for e in expected
        # If we are already explicitly testing an output relation, we don't need to add it
        if startswith(string(e.first), "/:output")
            continue
        end
        name = ""

        if e.first isa Symbol
            name = string(e.first)
        else
            # rel path, e.g. ":a/:b/Int64"
            tokens = split(string(e.first), "/")
            for token in tokens
                if startswith(token, ":")
                    name *= token
                else
                    break
                end
            end

            name = SubString(name, 2)
        end
        program *= "\ndef output:" * name * " = " * name
    end
    return program
end

"""
    test_expected(expected::AbstractDict, rsp:TransactionResponse})

Given a Dict of expected relations, test if the actual results contain those relations.
Types and contents of the relations must match.

"""
function test_expected(
        expected::AbstractDict,
        metadata,
        results)
    # No testing to do, return immediaely
    isempty(expected) && return
    if metadata === nothing
        println("Invalid response")
        return false
    end
    if results === nothing
        println("No results")
        return false
    end

    for e in expected
        # prepend `/:output/` if it's not present
        name = string(e.first)
        name = startswith(name, "/:output") ? name : "/:output/" * name

        # Check result key exists
        if !haskey(results, name)
            println("Expected relation not found")
            return false
        end

        result = results[name]

        # We've found a matching result, now test the contents
        tuples = isempty(result) ? [()] : zip(result...)
        tuples_as_vector = sort(collect(tuples))
        sort!(e.second)

        # Special case for single value tuples
        if length(tuples_as_vector[1]) == 1
            tuples_as_single = typeof(e.second[1])[]
            for tuple in tuples_as_vector
                push!(tuples_as_single, tuple[1])
            end
            tuples_as_vector = tuples_as_single
        end

        if tuples_as_vector != e.second
            println("Results do not match")
            @info(tuples_as_vector)
            @info(e.second)
            return false
        end

    end
    return true
end

struct Problem
    code::String
    line::Union{Int64, Nothing}
end

function Problem(code::String)
    return Problem(code, nothing)
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
    query::String
    install::Dict{String, String}
    broken::Bool
    schema_inputs::AbstractDict
    inputs::AbstractDict
    expected::AbstractDict
    expected_problems::Vector{Problem}
    expect_abort::Bool
end

function Step(;
    query::String = nothing,
    install::Dict{String, String} = Dict{String, String}(),
    broken::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict(),
    expected_problems::Vector{Problem} = Problem[],
    expect_abort::Bool = false,
)
    return Step(
        query,
        install,
        broken,
        schema_inputs,
        inputs,
        expected,
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

- `expected::AbstractDict`: Expected values in the form `Dict("/:output/:a/Int64 => [1, 2]")`

- `expected_problems::Vector{String}`: expected problems. The semantics of
  `expected_problems` is that the program must contain a super set of the specified
  error codes. When `expected_problems` is `[]` instead of `nothing`, then this means that errors
  are allowed.

- `engine::String`: The name of an existing compute engine
"""
function test_rel(;
    query::String,
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
    expected_problems::Vector{Problem} = Problem[],
    expect_abort::Bool = false,
)
    steps = Step[]
    push!(steps, Step(
        query = query,
        schema_inputs = schema_inputs,
        inputs = inputs,
        install = install,
        expected = expected,
        expected_problems = expected_problems,
        expect_abort = expect_abort,
        ))

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
    test_engine = get_or_create_test_engine(engine)
    println("Using test engine: ", test_engine)

    try
        if isnothing(name)
            name = "Unnamed test"
        end

        # Setup steps that run before the first testing Step
        if !include_stdlib
            insert!(steps, 1, Step(query="""def delete:rel:catalog:model = rel:catalog:model"""))
        end

        if debug
            insert!(steps, 1, Step(query="""def insert:debug = "basic" """))
        end

        if debug_trace
            insert!(steps, 1, Step(query="""def insert:debug = "trace" """))
        end

        if abort_on_error
            insert!(steps, 1, Step(query="""def insert:relconfig:abort_on_error = true """))
        end

        _test_rel_steps(;
            steps = steps,
            name = name,
            engine = test_engine,
            location = location,
        )

    finally
        release_test_engine(test_engine)
    end
    return nothing
end

# This internal function executes `test_rel`
function _test_rel_steps(;
    steps::Vector{Step},
    name::String,
    engine::String,
    location::Union{LineNumberNode,Nothing},
)
    schema = create_test_database()

    if !isnothing(location)
        path = joinpath(splitpath(string(location.file))[max(1,end-2):end])
        resolved_location = string(path, ":", location.line)

        name = name * " at " * resolved_location
    end

    try
        @testset verbose = true "$(string(name))" begin
            elapsed_time = @timed begin
                for (index, step) in enumerate(steps)
                    _test_rel_step(
                        index,
                        step,
                        schema,
                        engine,
                        name,
                        length(steps),
                    )
                end
            end
            println("Timing: ", elapsed_time)
        end
    catch e
    finally
        delete_test_database(schema)
    end

    return nothing
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
    if !isnothing(step.query)
        program = step.query
    else
        program = ""
    end

    #Append inputs to program
    program *= convert_input_dict_to_string(step.inputs)

    #Append schema inputs to program
    program *= convert_input_dict_to_string(step.schema_inputs)

    # If there are expected results, make sure they are in the output
    program *= generate_output_string_from_expected(step.expected)

    step_postfix = steps_length > 1 ? " - step$index" : ""

    @testset "$(string(name))$step_postfix" begin
        try
            if !isempty(step.install)
                load_model(get_context(), schema, engine,
                        Dict("test_install" => step.install))
            end

            #TODO: Currently this fails on the first run of a fresh engine
            #response = exec_async(get_context(), schema, engine, program)
            response = exec(get_context(), schema, engine, program)
            transaction_id = response.transaction.id

            try
                wait_until_done(get_context(), response)
            catch
                # Errors thrown may be due to both ICs or system, so keep going
            end

            response = get_transaction(get_context(), transaction_id)
            state = response.state

            # problems is deprecated, replaced by the results
            # /:rel/:catalog/:diagnostic/*
            #problems = get_transaction_problems(get_context(), transaction_id)
            metadata = get_transaction_metadata(get_context(), transaction_id)
            results = get_transaction_results(get_context(), transaction_id)

            results_dict = result_table_to_dict(results)
            problems = extract_problems(results_dict)

            # If there are no expected problems then we expect the transaction to complete
            if isempty(step.expected_problems) && !step.expect_abort
                problems_found = !isempty(problems)
                problems_found |= state !== "COMPLETED"
                for problem in problems
                    println("Unexpected problem type: ", problem.code)
                end
                @test !problems_found broken = step.broken

                if state == "ABORTED"
                    for problem in problems
                        println("Aborted with problem type: ", problem.code)
                    end
                else
                    if !isempty(step.expected)
                        @test test_expected(step.expected, metadata, results_dict) broken = step.broken
                    end
                end
            else
                @test step.expect_abort && state == "ABORTED" broken = step.broken
                # Check that expected problems were found
                for expected_problem in step.expected_problems
                    expected_problem_found = any(p->(p.code == expected_problem.code), problems)
                    @test expected_problem_found broken = step.broken
                end
            end
        catch e
            Base.display_error(stderr, current_exceptions())
        end
        return nothing
    end
end


function extract_problems(results)
    problems = Problem[]

    if !haskey(results, "/:rel/:catalog/:diagnostic/:code/Int64/String") ||
        !haskey(results, "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64")
        return problems
    end

    # [index, code]
    problem_codes = results["/:rel/:catalog/:diagnostic/:code/Int64/String"]
    # [index, ?, line]
    problem_lines = results["/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64"]

    if length(problem_codes) > 0
        for i = 1:1:length(problem_codes[1])
            push!(problems, Problem(problem_codes[2][i], problem_lines[3][i]))
        end
    end

    return problems
end

function result_table_to_dict(results)
    dict_results = Dict{String, Arrow.Table}()
    for result in results
        dict_results[result[1]] = result[2]
    end
    return dict_results
end
