using Base: @lock

#TODO: Redo this as a pool of workers and a queue of jobs. each worker has an engine.
# The below approach has a pool of works and a pool of engines, but there can be only one worker per engine
# Okay, so there could be more, but that would be a different test

const TEST_SERVER_LOCK = ReentrantLock()

mutable struct NextId; id::Int; end
struct TestEnginePool
    engines::Dict{String, Int64}
    # This is used to enable unique, simple, naming of engines
    # Switching to randomly generated UUIDs would be needed if tests are run independently
    next_id::NextId
end

TEST_ENGINE_POOL = TestEnginePool(Dict{String, Int64}(), NextId(0))


function get_free_test_engine_name()::String
    delay = 1
    while true
        if (length(TEST_ENGINE_POOL.engines) < 1)
            error("No servers available!")
        end

            @lock TEST_SERVER_LOCK begin
            for e in TEST_ENGINE_POOL.engines
                if e.second == 0
                    TEST_ENGINE_POOL.engines[e.first] = Base.Threads.threadid()
                    return e.first
                end
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

function get_or_create_test_engine(name::Union{String, Nothing})
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
        # Don't try to create engines from supplied names
        if !isnothing(name)
            println("Engine does not exist!")
            return nothing
        end
    end

    #TODO: Replace detectably faulty engines - but it seems that transient errors are common

    # The engine does not exist yet, so create it
    create_engine(get_context(), engine_name, size = size)

    # Note that the response format is different from create_engine
    response = get_engine(get_context(), engine_name)

    # The engine exists - now we wait until it is not in the provisioning stage
    while (response.state == "PROVISIONING" || response.state == "REQUESTED")
        println("Waiting for test engine to be provisioned...")
        sleep(5)
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
    println("Released test engine: ", name)
end

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

function get_next_engine_name()
    id = TEST_ENGINE_POOL.next_id.id
    TEST_ENGINE_POOL.next_id.id += 1
    return "julia-sdk-test-$(id)"
end

function provision_all_test_engines()
    @lock TEST_SERVER_LOCK begin
        for engine in TEST_ENGINE_POOL.engines
            get_or_create_test_engine(engine)
        end
    end
end

function resize_test_engine_pool(size::Int64)
    if size < 0
        size = 0
    end

    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        while (length(engines) < size)
            engines[get_next_engine_name()] = 0
        end
        for engine in engines
            if length(engines) > size
                try
                    delete_engine(get_context(), engine.first)
                catch
                    # The engine may not exist if it hasn't been used yet
                end
                delete!(engines, engine.first)
            end
        end
    end
end
