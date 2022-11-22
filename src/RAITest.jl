module RAITest

using Test:@testset

export test_rel, @test_rel, @testset
export Step
export ConcurrentTestSet
export destroy_test_engines, resize_test_engine_pool, provision_all_test_engines, add_test_engine!
export set_engine_name_provider, set_engine_name_releaser, set_engine_creater

include("testsets.jl")

include("code-util.jl")

include("testrel.jl")

include("testpool.jl")

include("engines.jl")

end
