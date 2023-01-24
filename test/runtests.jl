using RAITest
using RAITest: TestRelTestSet
using Test

# Test that results get recorded as expected.
for distributed in (true, false)
    ts = @testset RAITestSet "outer" distributed=distributed begin
        @testset "middle" begin
            @testset TestRelTestSet "inner" begin
                @test (sleep(1); true)
            end
        end
    end
    @testset "record (distributed=$distributed)" begin
        outer = ts.dts
        @test outer.description == "outer"
        @test 1 < outer.time_end - outer.time_start < 2
        @test length(outer.results) == 1
        middle = only(outer.results)
        @test middle.description == "middle"
        @test 1 < middle.time_end - middle.time_start < 2
        @test length(middle.results) == 1
        inner = only(middle.results)
        @test inner.description == "inner"
        @test 1 < inner.time_end - inner.time_start < 2
        @test isempty(inner.results)
        @test inner.n_passed == 1
    end
end

@testset "strip_location" begin
    for line in (0, 42, 999)
        for path in (
            "foo-tests.jl",
            joinpath("bar", "foo.jl"),
            joinpath("qux", "bar", "foo.jl"),
            joinpath("test_dir", "qux", "bar", "foo-test.jl"),
        )
            for name in ("name", "evil name @ looks/like/a/file.jl:123")
                @show full_name = "$name @ $path:$line"
                @test RAITest.strip_location(full_name) == name
            end
        end
    end
end

# Test basic test_rel usage
try
    resize_test_engine_pool(5)

    @testset ConcurrentTestSet "Basics" begin
        include("basic.jl")
    end

finally
    resize_test_engine_pool(0)
end
