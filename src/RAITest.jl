module RAITest

export test_rel, @test_rel
export Step
export destroy_test_engines, resize_test_engine_pool

include("testrel.jl")

include("engines.jl")

end
