rel_code_key = "/:rel/:catalog/:diagnostic/:code/Int64/String"
rel_line_key = "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64"
rel_severity_key = "/:rel/:catalog/:diagnostic/:severity/Int64/String"
rel_message_key = "/:rel/:catalog/:diagnostic/:message/Int64/String"

function generate_arrow(problems)
    code_table = []
    line_table = []
    severity_table = []
    message_table = []
    for entry in problems
        entry = Dict(entry)
        if haskey(entry, :code)
            push!(code_table, (v1=entry[:index], v2=entry[:code]))
        end
        if haskey(entry, :line)
            push!(
                line_table,
                (v1=entry[:index], v2=get(entry, :subindex, 1), v3=entry[:line]),
            )
        end
        if haskey(entry, :severity)
            push!(severity_table, (v1=entry[:index], v2=entry[:severity]))
        end
        if haskey(entry, :message)
            push!(message_table, (v1=entry[:index], v2=entry[:message]))
        end
    end

    results = Dict()
    results[rel_code_key] = Arrow.Table(Arrow.tobuffer(code_table))
    if !isempty(line_table)
        results[rel_line_key] = Arrow.Table(Arrow.tobuffer(line_table))
    end
    if !isempty(severity_table)
        results[rel_severity_key] = Arrow.Table(Arrow.tobuffer(severity_table))
    end
    if !isempty(message_table)
        results[rel_message_key] = Arrow.Table(Arrow.tobuffer(message_table))
    end

    return results
end

@testset "problem extraction" begin

    # Connection errors can result in problems being `nothing``, rather than empty
    @test RAITest.extract_problems(nothing) == []

    # Test matching on code, code + line, code + line + severity
    test_results = generate_arrow([
        (
            :index => 1,
            :code => "UNDEFINED",
            :severity => "error",
            :line => 2,
            :message => "message",
        ),
        (
            :index => 1,
            :code => "UNBOUND_VARIABLE",
            :severity => "error",
            :line => 3,
            :message => "message2",
        ),
    ])
    extracted_problems = RAITest.extract_problems(test_results)
    @test length(extracted_problems) == 2
    extracted_problem = extracted_problems[1]
    @test RAITest.matches_problem((:code => :UNDEFINED), extracted_problem)
    @test RAITest.matches_problem((:code => :UNDEFINED, :line => 2), extracted_problem)
    @test RAITest.matches_problem(
        (:code => :UNDEFINED, :line => 2, :severity => :error),
        extracted_problem,
    )

    # Test matching fails on mismatched code, line, or severity
    @test !RAITest.matches_problem((:code => :UNBOUND_VARIABLE), extracted_problem)
    @test !RAITest.matches_problem(
        (:code => :UNBOUND_VARIABLE, :line => 2),
        extracted_problem,
    )
    @test !RAITest.matches_problem((:code => :UNDEFINED, :line => 1), extracted_problem)
    @test !RAITest.matches_problem(
        (:code => :UNDEFINED, :line => 2, :severity => :warning),
        extracted_problem,
    )

    # Messages are not part of problem comparison, but should still be present
    @test extracted_problem[:message] == "message"

    @test RAITest.contains_problem(extracted_problems, (:code => :UNDEFINED))
    @test RAITest.contains_problem(extracted_problems, (:code => :UNBOUND_VARIABLE))
end
