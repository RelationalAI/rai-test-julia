using RAI

using Random
using Test
using UUIDs


# Generates a database name for the given base name that makes it uniques between multiple
# processing units
function gen_safe_dbname(basename)
    return "$(basename)-p$(getpid())-t$(Base.Threads.threadid())-$(UUIDs.uuid4(Random.MersenneTwister()))"
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
    the fields below represent the keyword arguments used in `test_rel`
    for each transaction step.
    For more information please consult the function keyword arguments
"""
struct Step
    query::String
end

function Step(;
    query::String = nothing,
)
    return Step(
        query,
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

function test_rel(
    query::String;
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
)
    steps = Step[]
    push!(steps, Step(query))

    test_rel(
        steps;
        name = name,
        engine = engine
    )
end

function test_rel(
    queries::Vector{String};
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
)
    steps = Step[]
    for query in queries
        push!(steps, Step(query))
    end

    test_rel(
        steps;
        name = name,
        engine = engine
    )
end

function test_rel(
    steps::Vector{Step};
    name::Union{String,Nothing} = nothing,
    engine::Union{String,Nothing} = nothing,
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
        engine = test_engine
    )

    if isnothing(engine)
        destroy_test_engine(test_engine)
    end

    return nothing
end

# This internal function executes `test_rel`
function _test_rel(
    steps::Vector{Step};
    name::Union{String,Nothing},
    engine::String,
)
 
    schema = create_test_database()

    # TODO: Engine selection
    # engine = ...

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
        println(elapsed_time)
    end
    delete_test_database(schema)

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
    if step.query != nothing
        program = step.query
    else
        program = ""
    end

    step_postfix = steps_length > 1 ? " - step$index" : ""

    @testset "$(string(name))$step_postfix" begin
        try
            response = exec(get_context(), schema, engine, program)
            @test response.transaction.state == "COMPLETED"
        catch e
            Base.display_error(stderr, current_exceptions())
        end
        return nothing
    end
end