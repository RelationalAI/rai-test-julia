# The RelationalAI Testing Kit for Julia

Enables developers to run tests using the RelationalAI SDK for Julia.

```
using RAITest

@test_rel("def output = 1 ic {output = 1}")

test_rel(
    name = "Descriptive name",
    query = "def output = 1 ic {output = 1}",
    engine = "Name of my pre-existing compute engine",
)
```
