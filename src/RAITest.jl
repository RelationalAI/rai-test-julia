module RAITest

using Test: @testset, TestLogger, LogRecord

import Logging

export test_rel, @test_rel, @testset
export RAITestSet
export Problem, Step
export ConcurrentTestSet

export destroy_test_engines
export resize_test_engine_pool
export provision_all_test_engines
export add_test_engine!

export set_context

export set_engine_name_provider
export set_engine_name_releaser
export set_engine_creater

include("code-util.jl")

include("testsets.jl")

include("testrel.jl")

include("testpool.jl")

include("engines.jl")

end
