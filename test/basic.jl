# Basic Rel
# --------------------------------------------------------

@test_rel(
    name = "No input, expected, or output",
    query = """
    def result = 1
    """,
)

@test_rel(
    name = "No input or expected, has output",
    query = """
    def output = 1
    """,
)

@test_rel(
    name = "Input, no expected or output",
    query = """
    def result = input
    """,
    inputs = Dict(:input => [1]),
)

@test_rel(
    name = "Input, output, no expected",
    query = """
    def output = input
    """,
    inputs = Dict(:input => [1]),
)

@test_rel(
    name = "No input, expected output",
    query = """
    def output = 1
    """,
    expected = Dict(:output => [1]),
)

@test_rel(
    name = "Input, expected output",
    query = """
    def output = input
    """,
    inputs = Dict(:input => [1]),
    expected = Dict(:output => [1]),
)

@test_rel(
    name = "Broken expected",
    query = """
    def output = 1
    """,
    expected = Dict(:output => [2]),
    broken = true,
)

 
@test_rel(
    name = "Expected abort",
    query = """
    def result = 1
    ic { result = 2 }
    """,
    expect_abort = true,
)

@test_rel(
    name = "Broken abort",
    query = """
    def result = 1
    ic { result = 2 }
    """,
    broken = true,
)

# Expected problems
# Broken expected problem
# problem + expected
# expected abort + problem
