module RAITest

using Test: @testset, TestLogger, LogRecord

import Logging
import Pkg

export test_rel, @test_rel, @testset
export RAITestSet
export Step, ReadQuery, WriteQuery, Install

export destroy_test_engines!
export resize_test_engine_pool!
export provision_all_test_engines
export add_test_engine!

export set_context!

export set_engine_name_provider!
export set_engine_name_releaser!
export set_engine_creater!

include("code-util.jl")

include("testsets.jl")

include("testrel.jl")

include("testpool.jl")

include("engines.jl")

function __init__()
    try
        set_context!(Context(load_config()))
    catch
        @warn "No `default` RAI context found. Use `set_context` to pass in a context and enable usage of `RAITest`."
    end
end

end
