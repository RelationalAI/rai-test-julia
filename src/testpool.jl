using Base: @lock

function release_pooled_test_engine(name::String)
    @lock TEST_SERVER_LOCK begin
        if !haskey(TEST_ENGINE_POOL.engines, name)
            return
        end
        TEST_ENGINE_POOL.engines[name] = 0
    end
end

function get_pooled_test_engine(engine_name::Union{String, Nothing} = nothing)
    # If we're asked to provide a particular engine then we can skip the pool tests
    if !isnothing(engine_name)
        @info("Retrieving named engine ", engine_name)
        return get_engine(get_context(), engine_name)
    end

    # It is valid to keep looping while there are possible names to use
    # When the pool is empty, errors are thrown
    # If errors with provisioning occur they may be ongoing so attempts to provision a new
    # engine are limited.
    max_attempts = 5
    attempts = 0
    while attempts < max_attempts
        attempts = attempts + 1
        engine_name = get_free_test_engine_name()

        # Test if engine already exists and is ready for use
        # If the engine does not exist then an exception is thrown
        try
            is_valid_engine(engine_name) && return engine_name
            @info("<$engine_name> is not in a valid state", response)
        catch
            @info("Engine $engine_name does not exist - creating")
        end

        # The engine does not exist yet, so create it. The engine is potentially in an
        # invalid state, in which case it will be recreated
        try
            return TEST_ENGINE_PROVISION.creater(engine_name)
        catch
            @info("Provisioning for $engine_name failed - attempting to replace")
            # Provisioning failed - replace the engine and try again
            replace_engine(name)
        end
    end

    # max_attempts were made to create a new engine. Give up and return an error
    error("Engine could not be created")

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
Test if an engine has been created and is in the PROVISIONED state. Note that an engine
that is currently being provisioned will fail this test, but may be valid once
provisioning finishes.
"""
function is_valid_engine(name::String)
    try
        response = get_engine(get_context(), name)
        # The engine has been provisioned and did not immediately return an error
        return response.state == "PROVISIONED"
    catch
        # Engine does not exist
        return false
    end
end

function replace_engine(name::String)
    # Remove engine name from pool
    @lock TEST_SERVER_LOCK begin
        delete!(TEST_ENGINE_POOL.engines, name)
    end

    # Attempt to delete the engine. If the engine could not be deleted, notify and continue
    # Note that if an engine cannot be deleted, a new name will be used in the pool.
    try
        delete_engine(get_context(), name)
    catch
        info("Could not delete engine: ", name)
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
        for engine in TEST_ENGINE_POOL.engines
            get_pooled_test_engine(engine.first)
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
                println("Deleting engine ", engine.first)
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

"""
Call delete for any provisioned engines and resize the engine pool to zero.
"""
function destroy_test_engines()
    resize_test_engine_pool(0)
    @info("Destroyed all test engine: ")
end
