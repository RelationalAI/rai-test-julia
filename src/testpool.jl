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

Base.@kwdef mutable struct TestEnginePool
    engines::Dict{String, Int64} = Dict()
    # This is used to enable unique, simple, naming of engines
    # Switching to randomly generated UUIDs would be needed if tests are run independently
    next_id::Threads.Atomic{Int} = Threads.Atomic{Int}(0)
    # Number of tests per engine. Values > 1 invalidate test timing, and require careful
    # attention to engine sizing
    concurrency::Int64 = 1
    name_generator::Function = get_next_engine_name
    # Create an engine. This is expected to be used by the provider as needed.
    creater::Function = create_default_engine
end

function _get_new_id()
    return Threads.atomic_add!(TEST_ENGINE_POOL.next_id, 1)
end

function get_free_test_engine_name()::String
    delay = 1
    # One lock guards name acquisition, forming a queue
    # The second lock guards modification of the engine pool
    @lock TEST_SERVER_ACQUISITION_LOCK while true
        @lock TEST_SERVER_LOCK begin
            if (length(TEST_ENGINE_POOL.engines) < 1)
                error("No servers available!")
            end

            for e in TEST_ENGINE_POOL.engines
                if e.second < TEST_ENGINE_POOL.concurrency
                    TEST_ENGINE_POOL.engines[e.first] += 1
                    return e.first
                end
            end
        end
        # Very naive wait protocol
        sleep(delay)
    end
end

"""
Test if an engine has been created and can be returned via the API.
"""
function validate_engine(name::String)
    try
        response = get_engine(get_context(), name; readtimeout=30)
        if response.state == "PROVISIONED"
            return true
        end
        # The engine exists, but is not provisioned
        @warn("$name was not provisioned. Reported state was: $(response.state)")
    catch e
        if e isa HTTPError
            @warn("$name was not provisioned. Reported error was: $e")
        else
            rethrow()
        end
    end
    return false
end

function replace_engine(name::String)
    delete_test_engine!(name)
    new_name = TEST_ENGINE_POOL.name_generator(_get_new_id())
    add_test_engine!(new_name)
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
    # Provision the engine if it does not already exist.
    try
        TEST_ENGINE_POOL.creater(name)
    catch
        # Provisioning failed. Attempt to delete the engine
        delete_test_engine!(name)
        @warn("Could not provision engine $name")
        return
    end
    @lock TEST_SERVER_LOCK begin
        engines = TEST_ENGINE_POOL.engines
        engines[name] = 0
    end

    return
end

"""
    delete_test_engine!(name::String)

Delete an engine and remove it from the pool of test engines. The engine will be deleted
whether or not it is in the pool.
"""
function delete_test_engine!(name::String)
    # Remove the engine from the list of available engines
    @lock TEST_SERVER_LOCK begin
        delete!(TEST_ENGINE_POOL.engines, name)
    end
    # Request engine deletion
    try
        delete_engine(get_context(), name; readtimeout=30)
    catch e
        @warn("Could not delete engine $name: ", e)
    end
end

function get_next_engine_name(id::Int64)
    base_name = default_engine_name()
    return "$(base_name)-$(id)"
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

    # Add engines if size > length
    _create_and_add_engines!(size)
    _validate_engine_pool!()
    # Remove engines if size < length
    _trim_engine_pool!(size)
end

# Test all engines and remove if they are unavailable or not successfully provisioned
function _validate_engine_pool!()
    @lock TEST_SERVER_LOCK begin
        for engine in keys(TEST_ENGINE_POOL.engines)
            validate_engine(engine) && continue

            # The engine was not provisioned or does not exist. Remove it from the pool
            @info("Removing failed engine $engine")
            delete_test_engine!(engine)
        end
    end
end

function _create_and_add_engines!(size::Int64)
    new_engine_count = 0
    @lock TEST_SERVER_LOCK begin
        new_engine_count = size - length(TEST_ENGINE_POOL.engines)
        if new_engine_count < 0
            return
        end
    end

    new_names = String[]
    # Generate new names
    for _ in 1:new_engine_count
        push!(new_names, TEST_ENGINE_POOL.name_generator(_get_new_id()))
    end

    @debug("Provisioning $new_engine_count engines")
    @sync for new_name in new_names
        @async try
            add_test_engine!(new_name)
        catch e
            @warn("Could not provision engine $new_name:", e)
        end
    end
end

# Remove engines if size < length(engine_pool)
function _trim_engine_pool!(size::Int64)
    @assert size >= 0
    engines_to_delete = String[]

    @lock TEST_SERVER_LOCK begin
        # Move the first length - size engines to the list of engines to delete
        while length(TEST_ENGINE_POOL.engines) > size
            engine_name, _ = pop!(TEST_ENGINE_POOL.engines)
            push!(engines_to_delete, engine_name)
        end
    end

    @sync for engine in engines_to_delete
        @async delete_test_engine!(engine)
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
