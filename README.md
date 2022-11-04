# The RelationalAI Testing Kit for Julia

Enables developers to run tests using the RelationalAI SDK for Julia.

RAITest requires a configuration file to use the RAI Julia SDK. Documentation for this can be found at https://docs.relational.ai/rkgms/sdk/julia-sdk

Using a user specified engine to test with
```
using RAITest

@test_rel(
    name = "Descriptive name",
    query = "def output = 1 ic {output = 1}",
    engine = "Name of my pre-existing compute engine",
)
```

Create a pool of engines to test with. Values greater than one are useful for larger test suites with concurrent testing

```
using RAITest

resize_engine_pool(2)

@test_rel("def output = 1 ic {output = 1}")

@test_rel(
    name = "Descriptive name",
    query = "def output = 1 ic {output = 1}",
)

destroy_test_engines()
```

Add existing engines to the pool.

```
using RAITest

add_test_engine!("<Your engine name>")

@test_rel("def output = 1 ic {output = 1}")

```

Run multiple tests concurrently.

```
using RAITest

resize_engine_pool(3)

# Instead of provisioning as needed, provision in advance
provision_all_test_engines()

@testset ConcurrentTestSet "My tests" begin
    for i in 1:10
        query = "def output = $i ic { output = $i }"
        @test_rel(query = query, debug = true)
    end
end

destroy_test_engines()
```
