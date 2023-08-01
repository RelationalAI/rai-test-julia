mutable struct TestEngineProvision
    # Returns the name of a provisioned, currently valid, engine
    provider::Function
    # Sends a notification that the engine represented by a string name is no longer in use
    releaser::Function
    # Create an engine. This is expected to be used by the provider as needed.
    creater::Function
end

function _wait_till_provisioned(engine_name, max_wait_time_s=600)
    start_time = time()
    # This should be a rare event, so a coarse-grained period is acceptable
    # Current provisioning time is ~60s
    time_delta_s = 1

    response = get_engine(get_context(), engine_name; readtimeout=max_wait_time_s)

    # The engine exists - now we wait until it is not in the provisioning stage
    while (response.state != "PROVISIONED")
        if (time() - start_time) > max_wait_time_s
            error("Engine was not provisioned within $max_wait_time_s seconds.")
        end
        if response.state == "PROVISION_FAILED"
            error("Provision failed")
        end

        sleep(time_delta_s)
        response = get_engine(get_context(), engine_name; readtimeout=max_wait_time_s)
    end

    return response.name
end

"""
    create_default_engine(name::String)

Create an XS engine with default settings and the provided name.

If the engine already exists, return immediately. If not, create the engine then
return once the provisioning process is complete, or failed.
"""
function create_default_engine(name::String)
    size = "XS"
    max_wait_time_s = 600

    try
        get_engine(get_context(), name; readtimeout=max_wait_time_s)
        # The engine already exists so return it immediately
        return name
    catch
        # Engine does not exist yet so we'll need to create it
    end

    # Request engine creation
    create_engine(get_context(), name; size=size, readtimeout=max_wait_time_s)

    # Wait for engine to be provisioned
    return _wait_till_provisioned(name, max_wait_time_s)
end

# Get test engine. If a name is provided, the corresponding engine will be provided.
get_test_engine()::String = TEST_ENGINE_PROVISION.provider()

# Release test engine. Notifies the provider that this engine is no longer in use.
release_test_engine(name::String) = TEST_ENGINE_PROVISION.releaser(name)

"""
    set_engine_name_provider(provider::Function)

Set a provider for test engine names.

The provider is called by each test to select an engine to run the test with. The default
provider selects a name from a pool of available test engines.

# Examples

```
set_engine_name_provider(() -> "MyEngine")
set_engine_name_provider(() -> my_custom_engine_selector("MyEngine"))

```
"""
function set_engine_name_provider!(provider::Function)
    return TEST_ENGINE_PROVISION.provider = provider
end

"""
    set_engine_name_releaser(releaser::Function)

Set a releaser for test engine names.

The releaser will be called after a test has been run. The default releaser
marks an engine in the test engine pool as available for use by another test.

# Examples

```
set_engine_name_releaser((::String) -> nothing)
set_engine_name_releaser((name::String) -> delete_engine(context, name))
```
"""
function set_engine_name_releaser!(releaser::Function)
    return TEST_ENGINE_PROVISION.releaser = releaser
end

"""
    set_engine_creater(creater::Function)

Set a function used to create engines.

# Examples

```
    set_engine_creater(create_default_engine)
```
"""
function set_engine_creater!(creater::Function)
    return TEST_ENGINE_PROVISION.creater = creater
end

TEST_ENGINE_POOL = TestEnginePool(Dict{String, Int64}(), 0, get_next_engine_name)

TEST_ENGINE_PROVISION = TestEngineProvision(
    get_pooled_test_engine,
    release_pooled_test_engine,
    create_default_engine,
)
