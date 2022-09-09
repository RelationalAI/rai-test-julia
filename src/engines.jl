using Base: @lock

#TODO: Redo this as a pool of workers and a queue of jobs. each worker has an engine.
# The below approach has a pool of works and a pool of engines, but there can be only one worker per engine
# Okay, so there could be more, but that would be a different test

const TEST_SERVER_LOCK = ReentrantLock()

struct TestEnginePool
    engines::Dict{String, Int64}
end

function TestEnginePool(num_servers)
    engines = Dict{String, Int64}()
    for i in 1:num_servers
        #engines[gen_safe_name("mm-test")] = 0
        engines["mm-test" * string(i)] = 0
    end
    return TestEnginePool(engines)
end

const TEST_ENGINE_POOL = TestEnginePool(2)

function get_free_test_engine()::String
    while true
        @lock TEST_SERVER_LOCK begin
            for e in TEST_ENGINE_POOL.engines
                if e.second == 0
                    TEST_ENGINE_POOL.engines[e.first] = Base.Threads.threadid()
                    return e.first
                end
            end
        end
        sleep(1)
    end
end

function claim_test_engine()::String
    @lock TEST_SERVER_LOCK begin
        name = get_free_test_engine()
        size = "XS"
        try
            get_engine(get_context(), name)
            # The engine already exists so return it immediately
            return name
        catch
            # There's no engine yet, so proceed to creating it
        end
        response = create_engine(get_context(), name, size = size)

        println("Claimed test engine: ", response.compute.name)
        return response.compute.name
    end
end

function release_test_engine(name::String)
    @lock TEST_SERVER_LOCK begin
        TEST_ENGINE_POOL.engines[name] = 0
    end
    println("Released test engine: ", name)
end

function destroy_test_engines()
    @lock TEST_SERVER_LOCK begin
        for e in TEST_ENGINE_POOL.engines
            try
                delete_engine(get_context(), e.first)
            catch
            finally
                delete!(TEST_ENGINE_POOL.engines, e.first)
            end
        end
    end
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
