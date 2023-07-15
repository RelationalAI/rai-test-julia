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

@test_rel(
    name = "Expected problem",
    query = """
    def output = a
    """,
    expected_problems = [(:code => :UNDEFINED, :line => 1)],
)

@test_rel(
    name = "Expected problem, allow all",
    query = """
    def output = a
    """,
    expected_problems = [(:code => :UNDEFINED, :line => 1)],
    allow_unexpected = :error,
)

@test_rel(
    name = "Expected problem, allow none",
    query = """
    def output = a
    """,
    expected_problems = [(:code => :UNDEFINED, :line => 1)],
    allow_unexpected = :none,
)

@test_rel(
    name = "Unexpected problem, ignore all",
    query = """
    def output = a
    """,
    allow_unexpected = :error,
)

@test_rel(
    name = "Unexpected problem, broken",
    query = """
    def output = a
    """,
    broken = true,
)

@test_rel(
    name = "abort_on_error",
    query = "def output = a",
    abort_on_error = true,
    expect_abort = true,
)
