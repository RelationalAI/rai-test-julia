# Basic Rel
# --------------------------------------------------------

@test_rel(
    name = "No input, expected, or output",
    query = """
    def result { 1 }
    """,)

@test_rel(
    name = "No input or expected, has output",
    query = """
    def output { 1 }
    """,
)

@test_rel(
    name = "Input, no expected or output",
    query = """
    def result { input }
    """,
    inputs = Dict(:input => [1]),
)

@test_rel(
    name = "Input, output, no expected",
    query = """
    def output { input }
    """,
    inputs = Dict(:input => [1]),
)

@test_rel(
    name = "No input, expected output",
    query = """
    def output { 1 }
    """,
    expected = Dict(:output => [1]),
)

@test_rel(
    name = "Input, expected output",
    query = """
    def output { input }
    """,
    inputs = Dict(:input => [1]),
    expected = Dict(:output => [1]),
)

@test_rel(
    name = "Empty expected, empty actual",
    query = """
    def output { false }
    """,
    expected = Dict(:output => []),
)

@test_rel(
    name = "Empty expected, present actual",
    query = """
    def output { true }
    """,
    expected = Dict(:output => []),
    broken = true,
)

@test_rel(
    name = "Broken expected",
    query = """
    def output { 1 }
    """,
    expected = Dict(:output => [2]),
    broken = true,
)

@test_rel(
    name = "Expected abort",
    query = """
    def result { 1 }
    ic () requires result = 2
    """,
    expect_abort = true,
)

@test_rel(
    name = "Broken abort",
    query = """
    def result { 1 }
    ic () requires result = 2
    """,
    broken = true,
)

@test_rel(
    name = "Expected problem",
    query = """
    def output { a }
    """,
    expected_problems = [(:code => :UNDEFINED_IDENTIFIER, :line => 1)],
)

@test_rel(
    name = "Expected problem, allow all",
    query = """
    def output { a }
    """,
    expected_problems = [(:code => :UNDEFINED_IDENTIFIER, :line => 1)],
    allow_unexpected = :error,
)

@test_rel(
    name = "Expected problem, allow none",
    query = """
    def output { a }
    """,
    expected_problems = [(:code => :UNDEFINED_IDENTIFIER, :line => 1)],
    allow_unexpected = :none,
)

@test_rel(
    name = "Expected problems, allow none",
    query = """
    // Line 1
    def output { a }
    def output { b }
    """,
    expected_problems = [
        (:code => :UNDEFINED_IDENTIFIER, :line => 2),
        (:code => :UNDEFINED_IDENTIFIER, :line => 3),
    ],
    allow_unexpected = :none,
)

@test_rel(
    name = "Unexpected problem, ignore all",
    query = """
    def output { a }
    """,
    allow_unexpected = :error,
)

@test_rel(
    name = "Unexpected problem, broken",
    query = """
    def output { a }
    """,
    broken = true,
)

@test_rel(
    name = "abort_on_error",
    query = "def output { a }",
    abort_on_error = true,
    expect_abort = true,
)

@test_rel(
    name = "relconfig abort_on_error",
    query = "def output { a }",
    relconfig = Dict(:abort_on_error => true),
    expect_abort = true,
)

@test_rel(
    name = "Install",
    query = """
    def output { install_test }
    """,
    install = Dict("install_test" => "def install_test { 21 }"),
    expected = Dict(:output => [21]),
)

# `setup` and `tags` keywords ignored
# --------------------------------------------------------

@test_rel(name = "tags keyword ignored", query = "def result { 1 }", tags = [:foo],)

module FooSetup end

@test_rel(name = "setup keyword ignored", query = "def result { 1 }", setup = FooSetup,)

@test_rel(
    name = "setup and tags keywords ignored",
    query = "def result { 1 }",
    setup = FooSetup,
    tags = [:foo],
)
