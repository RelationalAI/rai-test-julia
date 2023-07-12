using Arrow
using Test

enumerate_row(value) = enumerate(value)
enumerate_row(value::String) = [(1, value)]

function generate_arrow(results)
    arrowed_results = Dict()
    for result in results
        key = RAITest.qualify_name(result[1], result[2])
        value = []
        if isempty(result[2])
            value = (v1=[],)
        else
            # Generate a v column for each column in multi-value rows
            vs = []
            for _ in first(result[2])
                push!(vs, [])
            end
            for row in result[2]
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
    expected = Dict(:a=>[1, 2, 3])
    actual = nothing
    @test !RAITest.test_expected(expected, actual, "no results")

    expected = Dict()
    actual = generate_arrow(Dict(:a=>[1, 2, 3], :b=>[]))
    @test RAITest.test_expected(expected, actual, "no expected")

    expected = Dict()
    actual = generate_arrow(Dict())
    @test RAITest.test_expected(expected, actual, "no results, no expected")

    expected = Dict(:a=>[1, 2, 3])
    actual = generate_arrow(expected)
    @test RAITest.test_expected(expected, actual, "match")

    expected = Dict(:a=>[1, 2, 3])
    actual = generate_arrow(Dict(:a=>[1, 2, 4]))
    @test !RAITest.test_expected(expected, actual, "!match same type")

    expected = Dict(:a=>[1, 2, 3])
    actual = generate_arrow(Dict(:a=>["1", "2", "4"]))
    @test !RAITest.test_expected(expected, actual, "!match diff type")

    expected = Dict(:a=>[1, 2, 3])
    actual = generate_arrow(Dict(:a=>[]))
    @test !RAITest.test_expected(expected, actual, "!match empty a")

    expected = Dict(:a=>[])
    actual = generate_arrow(Dict(:a=>[1, 2, 3]))
    @test RAITest.test_expected(expected, actual, "!match existence e")

    expected = Dict(:a=>[1, 2, 3])
    actual = generate_arrow(Dict(:a=>[1, 2, 3], :b=>[]))
    @test RAITest.test_expected(expected, actual, "match with extra")

    expected = Dict(:a=>[1, 2, 3], :b=>["a"])
    actual = generate_arrow(expected)
    @test RAITest.test_expected(expected, actual, "match with extra 2")

    expected = Dict(:a=>[(1, "a",), (2, "b",), (3, "c",)])
    actual = generate_arrow(expected)
    @test RAITest.test_expected(expected, actual, "match tuple")
end
