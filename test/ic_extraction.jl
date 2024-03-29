using Arrow
using Test

using RAITest: IC_LINE_KEY, IC_OUTPUT_KEY, filter_ic_results

struct HashValue
    h1::UInt64
    h2::UInt64
end

enumerate_row(value) = enumerate(value)
enumerate_row(value::String) = [(1, value)]

# Generate a mapping from relation id to Arrow values
# Takes as input a Dict mapping relation symbolic name to expected values
function generate_arrow(results)
    arrowed_results = Dict{String, Arrow.Table}()
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

@testset "ic extraction testing" begin
    h = (0x0000000000000001, 0x0000000000000000)
    h2 = (0x0000000000000002, 0x0000000000000000)

    # Basic extraction of a single row of results
    arrow = generate_arrow(
        Dict(
            IC_LINE_KEY => [(h, 1)],
            IC_OUTPUT_KEY * "/Float64/Float64" => [(h, 1.0, 2.0), (h, 3.0, 4.0)],
        ),
    )
    @test filter_ic_results(arrow, IC_OUTPUT_KEY, h) == [[1.0, 2.0], [3.0, 4.0]]

    # Extraction of mixed result types
    arrow = generate_arrow(
        Dict(
            IC_LINE_KEY => [(h, 1)],
            IC_OUTPUT_KEY * "/Int64/Int64" => [(h, 1, 2), (h, 3, 4)],
            IC_OUTPUT_KEY * "/Float64/Float64" => [(h, 1.0, 2.0), (h, 3.0, 4.0)],
        ),
    )
    @test filter_ic_results(arrow, IC_OUTPUT_KEY, h) ==
          [[1, 2], [3, 4], [1.0, 2.0], [3.0, 4.0]]

    # Sanity check that we don't extract results for a different hash
    arrow = generate_arrow(
        Dict(
            IC_LINE_KEY => [(h2, 1)],
            IC_OUTPUT_KEY * "/Int64/Int64" => [(h2, 1, 2), (h2, 3, 4)],
            IC_OUTPUT_KEY * "/Float64/Float64" => [(h2, 1.0, 2.0), (h2, 3.0, 4.0)],
        ),
    )
    @test filter_ic_results(arrow, IC_OUTPUT_KEY, h2) ==
          [[1, 2], [3, 4], [1.0, 2.0], [3.0, 4.0]]

    # Differentiate mixed hash results
    arrow = generate_arrow(
        Dict(
            IC_LINE_KEY => [(h, 1), (h2, 2)],
            IC_OUTPUT_KEY * "/Int64/Int64" => [(h2, 1, 2), (h2, 3, 4)],
            IC_OUTPUT_KEY * "/Float64/Float64" => [(h2, 1.0, 2.0), (h2, 3.0, 4.0)],
        ),
    )
    @test filter_ic_results(arrow, IC_OUTPUT_KEY, h2) ==
          [[1, 2], [3, 4], [1.0, 2.0], [3.0, 4.0]]

    # Differentiate mixed hash/mixed type results
    arrow = generate_arrow(
        Dict(
            IC_LINE_KEY => [(h, 1), (h2, 2)],
            IC_OUTPUT_KEY * "/Int64/Int64" => [(h, 1, 2), (h, 3, 4)],
            IC_OUTPUT_KEY * "/Float64/Float64" => [(h2, 1.0, 2.0), (h2, 3.0, 4.0)],
        ),
    )
    @test filter_ic_results(arrow, IC_OUTPUT_KEY, h) == [[1.0, 2.0], [3.0, 4.0]]

    # Limit results
    arrow = generate_arrow(
        Dict(
            IC_LINE_KEY => [(h, 1)],
            IC_OUTPUT_KEY * "/Int64/Int64" =>
                [(h, 1, 2), (h, 3, 4), (h, 5, 6), (h, 7, 8), (h, 9, 10)],
        ),
    )
    @test filter_ic_results(arrow, IC_OUTPUT_KEY, h, 2) == [[1, 2], [3, 4]]
end
