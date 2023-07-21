using Arrow
using Test

enumerate_row(value) = enumerate(value)
enumerate_row(value::String) = [(1, value)]

# Generate a mapping from relation id to Arrow values
# Takes as input a Dict mapping relation symbolic name to expected values
function generate_arrow(results)
    arrowed_results = Dict()
    for (name, expected) in results
        key = RAITest.relation_id(name, expected)
        value = []
        if isempty(expected)
            value = (v1=[],)
        else
            # Generate a v column for each column in multi-value rows
            vs = [[] for _ in 1:length(first(expected))]

            for row in expected
                for (i, v) in enumerate_row(row)
                    push!(vs[i], v)
                end
            end

            ks = []
            for i in 1:length(vs)
                push!(ks, Symbol("v$i"))
            end
            value = NamedTuple{Tuple(ks)}(vs)
        end
        arrowed_results[key] = Arrow.Table(Arrow.tobuffer(value))
    end
    return arrowed_results
end

@testset "expected testing" begin
    expected = Dict(:a => [1, 2, 3])
    actual = nothing
    @test !RAITest.test_expected(expected, actual, "no results")

    expected = Dict()
    actual = generate_arrow(Dict(:a => [1, 2, 3], :b => []))
    @test RAITest.test_expected(expected, actual, "no expected")

    expected = Dict()
    actual = generate_arrow(Dict())
    @test RAITest.test_expected(expected, actual, "no results, no expected")

    expected = Dict(:a => [1, 2, 3])
    actual = generate_arrow(expected)
    @test RAITest.test_expected(expected, actual, "match")

    expected = Dict(:a => [1, 2, 3])
    actual = generate_arrow(Dict(:a => [1, 2, 4]))
    @test !RAITest.test_expected(expected, actual, "!match same type")

    expected = Dict(:a => [1, 2, 3])
    actual = generate_arrow(Dict(:a => ["1", "2", "4"]))
    @test !RAITest.test_expected(expected, actual, "!match diff type")

    expected = Dict(:a => [1, 2, 3])
    actual = generate_arrow(Dict(:a => []))
    @test !RAITest.test_expected(expected, actual, "!match empty a")

    expected = Dict(:a => [])
    actual = generate_arrow(Dict(:a => [1, 2, 3]))
    @test RAITest.test_expected(expected, actual, "!match existence e")

    expected = Dict(:a => [1, 2, 3])
    actual = generate_arrow(Dict(:a => [1, 2, 3], :b => []))
    @test RAITest.test_expected(expected, actual, "match with extra")

    expected = Dict(:a => [1, 2, 3], :b => ["a"])
    actual = generate_arrow(expected)
    @test RAITest.test_expected(expected, actual, "match with extra 2")

    expected = Dict(:a => [(1, "a"), (2, "b"), (3, "c")])
    actual = generate_arrow(expected)
    @test RAITest.test_expected(expected, actual, "match tuple")
end
