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
        if startswith(e.first, "/:output")
            continue
        end
        name = ""

        tokens = split(e.first, "/")

        for token in tokens
            if startswith(token, ":")
                name *= token
            else
                break
            end
        end

        name = SubString(name, 2)

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
        rsp::TransactionResponse)
    # No testing to do, return immediaely
    isempty(expected) && return
    if rsp.metadata === nothing
        println("Invalid response")
        return false
    end
    if rsp.results === nothing
        println("No results")
        return false
    end

    for e in expected
        found = false

        # prepend `/:output/` if it's not present
        name = startswith(e.first, "/:output") ? e.first : "/:output/" * e.first

        # Find a matching result
        for result in rsp.results
            id = result.first
            if id != name
                continue
            end
            found = true

            # We've found a matching result, now test the contents
            data = result.second
            tuples = isempty(data) ? [()] : zip(data...)
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
                return false
            end

            # We've found the matching result so stop searching
            break
        end
        if !found
            println("Expected relation not found")
            return false
        end
    end
    return true
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
    expected_problems::Vector{String}
end

function Step(;
    query::String = nothing,
    install::Dict{String, String} = Dict{String, String}(),
    broken::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict(),
    expected_problems::Vector{String} = String[]
)
    return Step(
        query,
        install,
        broken,
        schema_inputs,
        inputs,
        expected,
        expected_problems,
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

- `engine::String`: The name of an existing compute engine
"""
function test_rel(;
    query::String,
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
    location::Union{LineNumberNode,Nothing} = nothing,
    include_stdlib::Bool = true,
    abort_on_error::Bool = false,
    debug::Bool = false,
    debug_trace::Bool = false,
    schema_inputs::AbstractDict = Dict(),
    inputs::AbstractDict = Dict(),
    expected::AbstractDict = Dict()
)
    steps = Step[]
    push!(steps, Step(query = query, schema_inputs = schema_inputs, inputs = inputs, expected = expected))

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

    # If there are expected results, make sure they are in the output
    program *= generate_output_string_from_expected(step.expected)

    step_postfix = steps_length > 1 ? " - step$index" : ""

    @testset "$(string(name))$step_postfix" begin
        try
            if !isempty(step.install)
                response = load_model(get_context(), schema, engine,
                        Dict("test_install" => step.install))
            end

            if !isempty(step.schema_inputs)
                response = load_model(get_context(), schema, engine,
                        Dict("test_inputs" => convert_input_dict_to_string(step.schema_inputs)))
            end

            response = exec_async(get_context(), schema, engine, program)

            while response.transaction.state !== "COMPLETED" && response.transaction.state !== "ABORTED"
                response = get_transaction_results(ctx, rsp_async.transaction["id"])
            end

            # If there are no expected problems then we expect the transaction to complete
            if isempty(step.expected_problems)
                problems_found = !isempty(response.problems)
                problems_found |= response.transaction.state !== "COMPLETED"
                for problem in response.problems
                    println("Unexpected problem type: ", problem.type)
                end
                @test !problems_found broken = step.broken

                if response.transaction.state == "ABORTED"
                    for problem in response.problems
                        println("Aborted with problem type: ", problem.type)
                    end
                else
                    if !isempty(step.expected)
                        @test test_expected(step.expected, response) broken = step.broken
                    end
                end
            else
                # Check that expected problems were found
                for problem in step.expected_problems
                    expected_problem_found = any(i->(i.type == problem), response.problems)
                    @test expected_problem_found broken = step.broken
                end
            end
        catch e
            Base.display_error(stderr, current_exceptions())
        end
        return nothing
    end
end
