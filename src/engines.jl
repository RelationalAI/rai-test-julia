using Base: @lock

#TODO: Redo this as a pool of workers and a queue of jobs. each worker has an engine.
# The below approach has a pool of works and a pool of engines, but there can be only one worker per engine
# Okay, so there could be more, but that would be a different test

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

function replace_engine(name::String)
    @lock TEST_SERVER_LOCK begin
        remove!(TEST_ENGINE_POOL.engines, name)
    end
    try
        delete_engine(get_context(), name)
    catch
        println("Could not delete engine: ", name)
    end

    new_name = getNextEngineName()
    @lock TEST_SERVER_LOCK begin
        TEST_ENGINE_POOL.engines[new_name] = 0
    end
end

function get_or_create_test_engine(name::Union{String, Nothing}, max_wait_time_s = 240)
    time_waited = 1
    engine_name = name
    if isnothing(name)
        engine_name = get_free_test_engine_name()
    end

    size = "XS"
    try
        get_engine(get_context(), engine_name)
        # The engine already exists so return it immediately
        return engine_name
    catch
        # Engine does not exist yet so we'll need to create it
    end

    #TODO: Replace detectably faulty engines - but it seems that transient errors are common

    # The engine does not exist yet, so create it
    create_engine(get_context(), engine_name, size = size)

    # Note that the response format is different from create_engine
    response = get_engine(get_context(), engine_name)

    # The engine exists - now we wait until it is not in the provisioning stage
    while (response.state != "PROVISIONED")
        if (time_waited > max_wait_time_s)
            error("Engine was not provisioned within $max_wait_time_s seconds.")
        end
        println("Waiting for test engine " * engine_name * " to be provisioned...")
        sleep(1)
        time_waited += 1
        response = get_engine(get_context(), engine_name)
    end

    return response.name
end

function release_test_engine(name::Union{String, Nothing})
    if isnothing(name)
        return
    end

    @lock TEST_SERVER_LOCK begin
        if !haskey(TEST_ENGINE_POOL.engines, name)
            return
        end
        TEST_ENGINE_POOL.engines[name] = 0
    end
end

"""
Call delete for any provisioned engines and resize the engine pool to zero.
"""
function destroy_test_engines()
    resize_test_engine_pool(0)
    println("Destroyed all test engine: ")
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

function list_test_engines()
    @lock TEST_SERVER_LOCK begin
        for e in TEST_ENGINE_POOL.engines
            println(e)
        end
    end
end

"""
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
            #TODO: This should be concurrent
            Threads.@async get_or_create_test_engine(engine.first)
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
function resize_test_engine_pool(size::Int64, generator::Function = get_next_engine_name)
    if size < 0
        size = 0
    end

    TEST_ENGINE_POOL.generator = generator

    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        while (length(engines) < size)
            new_name = TEST_ENGINE_POOL.generator(TEST_ENGINE_POOL.next_id)
            if haskey(engines, new_name)
                throw(ArgumentError("Engine name already exists"))
            end
            engines[new_name] = 0
            TEST_ENGINE_POOL.next_id += 1
        end
        Threads.@sync for engine in engines
            if length(engines) > size
                @async try
                    delete_engine(get_context(), engine.first)
                catch
                    # The engine may not exist if it hasn't been used yet
                end
                delete!(engines, engine.first)
            end
        end
    end
end

TEST_ENGINE_POOL = TestEnginePool(Dict{String, Int64}(), 0, get_next_engine_name)
