using Base: @lock

function release_pooled_test_engine(name::String)
    @lock TEST_SERVER_LOCK begin
        if !haskey(TEST_ENGINE_POOL.engines, name)
            return
        end
        TEST_ENGINE_POOL.engines[name] = 0
    end
end

function get_pooled_test_engine(engine_name::Option{String}=nothing)
    if isnothing(engine_name)
        engine_name = get_free_test_engine_name()
    end

    # If the engine already exists, return it
    is_valid_engine(engine_name) && return engine_name

    # The engine does not exist yet, so create it
    try
        return TEST_ENGINE_PROVISION.creater(engine_name)
    catch
        # Provisioning failed - remove the name from the pool and rethrow
        delete!(TEST_ENGINE_POOL.engines, engine_name)
        rethrow()
    end
end

const TEST_SERVER_LOCK = ReentrantLock()
const TEST_SERVER_ACQUISITION_LOCK = ReentrantLock()

mutable struct TestEnginePool
    engines::Dict{String, Int64}
    # This is used to enable unique, simple, naming of engines
    # Switching to randomly generated UUIDs would be needed if tests are run independently
    next_id::Int64
    generator::Function
end

function get_free_test_engine_name()::String
    delay = 1
    # One lock guards name acquisition, forming a queue
    # The second lock guards modification of the engine pool
    @lock TEST_SERVER_ACQUISITION_LOCK while true
        if (length(TEST_ENGINE_POOL.engines) < 1)
            error("No servers available!")
        end

        @lock TEST_SERVER_LOCK for e in TEST_ENGINE_POOL.engines
            if e.second == 0
                TEST_ENGINE_POOL.engines[e.first] = Base.Threads.threadid()
                return e.first
            end
        end
        # Very naive wait protocol
        sleep(delay)
    end
end

"""
Test if an engine has been created and can be returned via the API.
"""
function is_valid_engine(name::String)
    try
        get_engine(get_context(), name)
        # The engine exists and does not immediately return an error
        return true
    catch
        # Engine does not exist
        return false
    end
end

function replace_engine(name::String)
    @lock TEST_SERVER_LOCK begin
        delete!(TEST_ENGINE_POOL.engines, name)
    end
    # If the engine could not be deleted, notify and continue
    try
        delete_engine(get_context(), name)
    catch
        @warn("Could not delete engine: ", name)
        name = TEST_ENGINE_POOL.generator(TEST_ENGINE_POOL.next_id)
    end

    @lock TEST_SERVER_LOCK begin
        TEST_ENGINE_POOL.engines[name] = 0
    end
end

function list_test_engines()
    @lock TEST_SERVER_LOCK begin
        for e in TEST_ENGINE_POOL.engines
            println(e)
        end
    end
end

"""
    add_test_engine!(name::String)

Add an engine to the pool of test engines
"""
function add_test_engine!(name::String)
    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        engines[name] = 0
    end

    return nothing
end

function get_next_engine_name(id::Int64)
    return "julia-sdk-test-$(id)"
end

"""
Engines are provisioned on first use by default. Calling this method will provision
all engines in the current pool.
"""
function provision_all_test_engines()
    @lock TEST_SERVER_LOCK begin
        Threads.@sync for engine in TEST_ENGINE_POOL.engines
            Threads.@async get_pooled_test_engine(engine.first)
        end
    end
end

"""
    resize_test_engine_pool(5)
    resize_test_engine_pool(5, get_next_engine_name)

Resize the engine pool using the given name generator

If an name generator is given it will be passed a unique id each time it is called.
If the pool size is smaller than the current size, engines will be de-provisioned and
removed from the list until the desired size is reached.
"""
function resize_test_engine_pool(size::Int64, generator::Function=get_next_engine_name)
    if size < 0
        size = 0
    end

    TEST_ENGINE_POOL.generator = generator

    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        # Add engines while length < size
        while (length(engines) < size)
            new_name = TEST_ENGINE_POOL.generator(TEST_ENGINE_POOL.next_id)
            if haskey(engines, new_name)
                throw(ArgumentError("Engine name already exists"))
            end
            engines[new_name] = 0
            TEST_ENGINE_POOL.next_id += 1
        end
        # Move the first length - size engines to the list of engines to delete
        engines_to_delete = String[]
        while length(engines) > size
            engine_name, _ = pop!(engines)
            push!(engines_to_delete, engine_name)
        end
        # Asynchronously delete the engines
        Threads.@sync for engine in engines_to_delete
            @info("Deleting engine", engine)
            @async try
                delete_engine(get_context(), engine)
            catch e
                # The engine may not exist if it hasn't been used yet
                # For other errors, we just report the error and delete what we can
                @info(e)
            end
        end
    end
end

"""
Call delete for any provisioned engines and resize the engine pool to zero.
"""
function destroy_test_engines()
    resize_test_engine_pool(0)
    @info("Destroyed all test engine: ")
end
