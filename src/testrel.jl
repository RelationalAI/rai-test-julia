using RAI

using Random: MersenneTwister
using Test
using UUIDs


# Generates a database name for the given base name that makes it uniques between multiple
# processing units
function gen_safe_dbname(basename)
    return "$(basename)-p$(getpid())-t$(Base.Threads.threadid())-$(UUIDs.uuid4(MersenneTwister()))"
end

function get_context()::Context
    conf = load_config()
    return Context(conf)
end

function create_test_database()::String
    # TODO: Change to 'test-' when the account is changed
    schema = gen_safe_dbname("mm-test")
   
    return create_database(get_context(), schema).database.name
end

function delete_test_database(name::String)
   return delete_database(get_context(), name)
end

function create_test_engine()::String
    name = "mm-test-engine"
    size = "XS"
    try
        get_engine(get_context(), name)
        # The engine already exists so return it immediately
        return name
    catch
        # There's no engine yet, so proceed to creating it
    end
    response = create_engine(get_context(), name, size = size)

    println("Created test engine: ", response.compute.name)
    return response.compute.name
end

function destroy_test_engine(name::String)
    try
        delete_engine(get_context(), name)
    catch e
        Base.error(current_exceptions())
        return
    end
    println("Destroyed test engine: ", name)
end

function test_test_engine_is_valid(name::String)::Bool
    response = ""
    try 
        response = get_engine(get_context(), name)
    catch
        # The engine could not be found
        return false
    end

    # The engine exists - now we wait until it is not in the provisioning stage
    while (response.state == "PROVISIONING" || response.state == "REQUESTED")
        println("Waiting for test engine to be provisioned...")
        sleep(1)
        response = get_engine(get_context(), name)
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
    expected_problems::Vector{String}
end

function Step(;
    query::String = nothing,
    install::Dict{String, String} = Dict{String, String}(),
    broken::Bool = false,
    expected_problems::Vector{String} = String[]
)
    return Step(
        query,
        install,
        broken,
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
            test_rel($(kwargs[1]); location = $(QuoteNode(__source__)))
        end
    elseif args isa Tuple{Vector{String}}
        quote
            test_rel($(kwargs[1]); location = $(QuoteNode(__source__)))
        end
    elseif args isa Tuple{Vector{Step}}
        quote
            test_rel($(kwargs[1]); location = $(QuoteNode(__source__)))
        end
    else
        quote
            test_rel(; location = $(QuoteNode(__source__)), $(kwargs...))
        end
    end
end

"""
    test_rel(query; kwargs...)

Run a Rel testcase.


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

- `engine::String`: The name of an existing compute engine
"""
function test_rel(
    query::String;
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
    location::Union{LineNumberNode,Nothing} = nothing,
)
    steps = Step[]
    push!(steps, Step(query = query))

    test_rel(
        steps;
        name = name,
        engine = engine,
        location = location,
    )
end

"""
test_rel(queries; kwargs...)

Run a Rel testcase.


If `expected_problems` is not set, then no errors are
allowed. The test fails if there are any errors in the program.

It is preferred to use integrity constraints to set test conditions. If the integrity
constraints have any compilation errors, then the test will still fail (unless
`expected_problems` is set).

!!! warning

    `test_rel` creates a new schema for each test.

- `queries::Vector{String}`: A series of queries to use for the test

- `name::String`: name of the testcase

- `location::LineNumberNode`: Sourcecode location

- `engine::String`: The name of an existing compute engine
"""
function test_rel(
    queries::Vector{String};
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
    location::Union{LineNumberNode,Nothing} = nothing,
)
    steps = Step[]
    for query in queries
        push!(steps, Step(query = query))
    end

    test_rel(
        steps;
        name = name,
        engine = engine,
        location = location,
    )
end

"""
test_rel(query; kwargs...)

Run a Rel testcase.


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
function test_rel(
    steps::Vector{Step};
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
    location::Union{LineNumberNode,Nothing} = nothing,
)
    test_engine = engine
    if isnothing(engine)
        test_engine = create_test_engine()
    end

    try
        test_test_engine_is_valid(test_engine)
    catch
        Base.error("Engine: ", test_engine, " is not valid")
        return
    end

    if isnothing(name)
        name = "Unnamed test"
    end

    _test_rel(
        steps;
        name = name,
        engine = test_engine,
        location = location,
    )

    # Only destroy engines we created
    if isnothing(engine)
        destroy_test_engine(test_engine)
    end

    return nothing
end

# This internal function executes `test_rel`
function _test_rel(
    steps::Vector{Step};
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
        @testset "$(string(name))" begin
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

    step_postfix = steps_length > 1 ? " - step$index" : ""

    @testset "$(string(name))$step_postfix" begin
        try
            if !isempty(step.install)
                response = load_model(get_context(), schema, engine, step.install)
            end

            response = exec(get_context(), schema, engine, program)
            # If there are no expected problems then we expect the transaction to complete
            if isempty(step.expected_problems)
                @test response.transaction.state == "COMPLETED" broken = step.broken
                if response.transaction.state == "ABORTED"
                    for problem in response.problems
                        println("Aborted with problem type: ", problem.type)
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