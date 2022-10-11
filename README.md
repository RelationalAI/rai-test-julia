# The RelationalAI Testing Kit for Julia

Enables developers to run tests using the RelationalAI SDK for Julia.


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
