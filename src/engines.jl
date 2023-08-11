mutable struct TestEngineProvision
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
    create_default_engine(name::String, size::String)

Create an XS engine with default settings and the provided name.

If the engine already exists, return immediately. If not, create the engine then
return once the provisioning process is complete, or failed.
"""
function create_default_engine(name::String, size::String="XS")
    max_wait_time_s = 600

    try
        create_engine(get_context(), name; size=size, readtimeout=max_wait_time_s)
    catch e
        # If the status code is 409 then the engine already exists and we can wait for it
        # to be ready
        if e.status_code != 409
            rethrow()
        end
    end

    # Wait for engine to be provisioned
    return _wait_till_provisioned(name, max_wait_time_s)
end

# Get test engine.
get_test_engine()::String = get_free_test_engine_name()

# Release test engine. Notifies the provider that this engine is no longer in use.
release_test_engine(name::String) = release_pooled_test_engine(name)

TEST_ENGINE_POOL = TestEnginePool()
