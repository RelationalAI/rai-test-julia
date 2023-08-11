using Base: @lock

function release_pooled_test_engine(name::String)
    @lock TEST_SERVER_LOCK begin
        if !haskey(TEST_ENGINE_POOL.engines, name)
            return
        end
        TEST_ENGINE_POOL.engines[name] -= 1
        # Sanity check that the threaded world still makes sense
        @assert TEST_ENGINE_POOL.engines[name] >= 0 "Engine $name over-released"
    end
end

const TEST_SERVER_LOCK = ReentrantLock()
const TEST_SERVER_ACQUISITION_LOCK = ReentrantLock()

mutable struct TestEnginePool
    engines::Dict{String, Int64}
    # This is used to enable unique, simple, naming of engines
    # Switching to randomly generated UUIDs would be needed if tests are run independently
    next_id::Int64
    # Number of tests per engine. Values > 1 invalidate test timing, and require careful
    # attention to engine sizing
    concurrency::Int64
    name_generator::Function
    # Create an engine. This is expected to be used by the provider as needed.
    creater::Function
end

function TestEnginePool(;
    engines::Dict{String, Int64}=Dict{String, Int64}(),
    name_generator::Function=get_next_engine_name,
    creater::Function=create_default_engine,
    concurrency::Int64=1,
)
    return TestEnginePool(engines, 0, concurrency, name_generator, creater)
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
            if e.second < TEST_ENGINE_POOL.concurrency
                TEST_ENGINE_POOL.engines[e.first] += 1
                @info(
                    "Acquired engine $(e.first) with $(TEST_ENGINE_POOL.engines[e.first])"
                )
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
        get_engine(get_context(), name; readtimeout=30)
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
        delete_engine(get_context(), name; readtimeout=30)
    catch
        @warn("Could not delete engine: ", name)
    end

    name = TEST_ENGINE_POOL.name_generator(TEST_ENGINE_POOL.next_id)

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

Add an engine to the pool of test engines. The engine will be provisioned if it is not
already.
"""
function add_test_engine!(name::String)
    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        engines[name] = 0
    end

    # Provision the engine if it does not already exist.
    TEST_ENGINE_POOL.creater(new_name)

    return nothing
end

function get_next_engine_name(id::Int64)
    return "julia-sdk-test-$(id)"
end

"""
    resize_test_engine_pool!(size::Int64, generator::Option{Function}=nothing)

Resize the engine pool

If a name generator is given it will be used to generate all new engine names. When called
it will be passed a unique id each time it is called.

If the pool size is smaller than the current size, engines will be de-provisioned and
removed from the list until the desired size is reached.

# Example

```
resize_test_engine_pool!(5)
resize_test_engine_pool!(10, id->"RAITest-test-\$id")
resize_test_engine_pool!(0)
```
"""
function resize_test_engine_pool!(size::Int64, name_generator::Option{Function}=nothing)
    if size < 0
        size = 0
    end

    if !isnothing(name_generator)
        TEST_ENGINE_POOL.name_generator = name_generator
    end

    @lock TEST_SERVER_LOCK begin
        # Add engines if size > length
        _create_and_add_engines(size)
        _validate_engine_pool()
        _trim_engine_pool!(size)
    end
end

# Test all engines and remove if they are unavailable or not successfully provisioned
function _validate_engine_pool()
    @lock TEST_SERVER_LOCK begin
        @sync for engine in TEST_ENGINE_POOL.engines
            try
                response = get_engine(get_context(), engine.first; readtimeout=30)
                if response.state == "PROVISIONED"
                    # Success! Move on and try the next engine
                    continue
                end
                # The engine exists, but is not provisioned despite our best attempts
            catch
                # The engine does not exist
            end
            # Something went wrong. Remove from the list and attempt to delete
            delete!(TEST_ENGINE_POOL.engines, engine)

            # Note that only the deletion is asynchronous, not the list modification
            @async try
                delete_engine(get_context(), engine.first; readtimeout=30)
                @info("Removed failed engine $engine")
            catch e
                @info("Attempted to remove failed engine $engine", e)
            end
        end
    end
end

function _create_and_add_engines(size::Int64)
    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        increase = size - length(engines)
        increase < 0 && return

        new_names = String[]
        # Add engines while length < size
        while (length(engines) < size)
            new_name = TEST_ENGINE_POOL.name_generator(TEST_ENGINE_POOL.next_id)
            TEST_ENGINE_POOL.next_id += 1

            # Check the engine name generator isn't repeating names.
            if haskey(engines, new_name)
                throw(ArgumentError("Engine name already exists"))
            end
            push!(new_names, new_name)
            engines[new_name] = 0
        end

        @info("Provisioning $(increase) new engines")
        @sync for new_name in new_names
            @async try
                TEST_ENGINE_POOL.creater(new_name)
            catch
                # Ignore any errors here as we check more thoroughly below
            end
        end

        # Test all new engines and remove if they were not successfully provisioned
        for new_name in new_names
            try
                response = get_engine(get_context(), new_name; readtimeout=30)
                if response.state == "PROVISIONED"
                    # Success! Move on and try the next engine
                    continue
                end
                @warn("no bueno", response)
                # The engine exists, but is not provisioned despite our best attempts
            catch e
                @warn("very no bueno", e)
                # The engine does not exist
            end
            # Something went wrong. Remove from the list and attempt to delete
            delete!(engines, new_name)
            try
                delete_engine(get_context(), new_name; readtimeout=30)
            catch
                info("Attempted to remove failed engine provision", e)
            end
        end
    end
end

function _trim_engine_pool!(size::Int64)
    @assert size >= 0

    @lock TEST_SERVER_LOCK begin
        # Remove engines if size < length
        # Move the first length - size engines to the list of engines to delete
        engines_to_delete = String[]
        while length(TEST_ENGINE_POOL.engines) > size
            engine_name, _ = pop!(TEST_ENGINE_POOL.engines)
            push!(engines_to_delete, engine_name)
        end
        # Asynchronously delete the engines
        @sync for engine in engines_to_delete
            @info("Deleting engine", engine)
            @async try
                delete_engine(get_context(), engine; readtimeout=30)
            catch e
                @info(e)
            end
        end
    end
end

"""
Call delete for any provisioned engines and resize the engine pool to zero.
"""
function destroy_test_engines!()
    resize_test_engine_pool!(0)
    @info("Destroyed all test engine: ")
end

"""
    set_engine_creater!(creater::Function)

Set a function used to create engines.

# Examples

```
    set_engine_creater!(create_default_engine)
```
"""
function set_engine_creater!(creater::Function)
    return TEST_ENGINE_POOL.creater = creater
end
