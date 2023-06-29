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
