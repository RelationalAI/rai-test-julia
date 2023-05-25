# helper for optional types
const Option{T} = Union{Nothing, T}

# Extract relation names from the inputs and adds them to the program
# Turns a dict of name=>vector, with names of form :othername/Type,
# into a series of def name = list of tuples
function convert_input_dict_to_string(inputs::AbstractDict)
    program = ""
    for input in inputs
        name = string(input.first)

        program *= "\ndef insert:" * name * " = "

        first = true

        values = input.second

        if isempty(values)
            program *= "{ }"
            continue
        end

        values = to_vector_of_tuples(values)

        for i in values
            if first
                first = false
            else
                program *= "; "
            end

            program *= input_element_to_string(i)
        end
    end
    return program
end

function input_element_to_string(input)
    return repr(input)
end

# Escape strings in a format that is valid rel
# repr() would be nice, but does not produce valid rel-escaped strings
function input_element_to_string(input::String)
    return "\"" * escape_string(input) * "\""
end

function input_element_to_string(input::Tuple)
    if length(input) == 0
        return "()"
    end

    if length(input) == 1
        return input_element_to_string(input...)
    end

    program = "("
    program *= join(input_element_to_string.(input), ",")
    program *= ")"

    return program
end

# Extract relation names from the expected output and append them to output
# Turns a dict of name=>vector, with names of form :othername/Type
# into a series of def output:othername = othername
function generate_output_string_from_expected(expected::AbstractDict)
    program = ""

    for e in expected
        # Only handle symbols. For anything else, assume the path is already as intended
        !isa(e.first, Symbol) && continue
        is_special_symbol(e.first) && continue

        name = string(e.first)
        program *= "\ndef output:" * name * " = " * name
    end
    return program
end

"""
Test if the given symbol is one with special significance for a rel relation.
Current symbols of significance are
:output - standard output relation
:abort - marks the presence of an abort
:rel - used for rel diagnostics
"""
function is_special_symbol(symbol::Symbol)::Bool
    return symbol == :abort || symbol == :output || symbol == :rel
end

# Generate a string representing the Rel type for the input
# Expected inputs are a vector of types, a tuple of types, or a type
function type_string(input::Vector)
    if isempty(input)
        return ""
    end
    return type_string(input[1])
end

function type_string(input::Tuple)
    result = ""
    for t in input
        result *= type_string(t)
    end
    return result
end

function type_string(input)
    return "/" * string(typeof(input))
end

function key_to_array(input::Tuple)
    return collect(input)
end

function key_to_array(input)
    return [input]
end

function value_to_array(input::Pair)
    result = value_to_array(input.first)
    append!(result, value_to_array(input.second))
    return result
end

function to_vector_of_tuples(input::Dict)
    isempty(input) && return []

    result = []
    for v in input
        value_array = []
        append!(value_array, v.first isa Tuple ? collect(v.first) : [v.first])
        # Value is always singular
        push!(value_array, v.second)

        push!(result, Tuple(value_array))
    end
    return result
end

function to_vector_of_tuples(input::Union{Set, Vector})
    isempty(input) && return []

    result = []
    for v in input
        push!(result, v isa Tuple ? v : (v,))
    end
    return result
end

function to_vector_of_tuples(input::Tuple)
    return [input]
end

function to_vector_of_tuples(input)
    return [(input,)]
end

# Chars are serialized as UInt32
function Base.isequal(c::Char, u::UInt32)
    return isequal(UInt32(c), u)
end

# The intermediate Arrow format encodes Int128/UInt128 as a tuple of Int64s
function Base.isequal(i::Int128, t::Tuple{UInt64, UInt64})
    sign = Int128(t[2] >> 63)

    a = Int128(t[2] - (sign << 63))
    a <<= 64
    a += t[1]
    a |= (sign << 127)
    return isequal(i, a)
end

function Base.isequal(expected::UInt128, actual::Tuple{UInt64, UInt64})
    a = UInt128(actual[1]) + UInt128(actual[2]) << 64
    return isequal(expected, a)
end

# In some error cases the results may be nothing, rather than empty
function extract_problems(results::Nothing)
    return []
end

function extract_problems(results)
    problems = []

    rel_code_key = "/:rel/:catalog/:diagnostic/:code/Int64/String"
    rel_line_key = "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64"
    rel_severity_key = "/:rel/:catalog/:diagnostic/:severity/Int64/String"

    if !haskey(results, rel_code_key)
        return problems
    end

    # [index, code]
    problem_codes = results[rel_code_key]

    problem_lines = Dict()
    if haskey(results, rel_line_key)
        # [index, ?, line]
        problem_lines = results[rel_line_key]
    end

    problem_severities = Dict()
    if haskey(results, rel_severity_key)
        # [index, severity]
        problem_severities = results[rel_severity_key]
    end
    if length(problem_codes) > 0
        for i in 1:1:length(problem_codes[1])
            # Not all problems have a line number
            problem_line = nothing
            if haskey(results, rel_line_key)
                problem_line = problem_lines[3][i]
            end
            problem_severity = nothing
            if haskey(results, rel_severity_key)
                problem_severity = problem_severities[2][i]
            end
            problem = Problem(
                :code => problem_codes[2][i],
                :severity => problem_severity,
                :line => problem_line,
            )
            push!(problems, problem)
        end
    end

    return problems
end

function contains_problem(problems, problem_needle)::Bool
    return any(p -> matches_problem(p, problem_needle), problems)
end

function matches_problem(actual, expected)::Bool
    return matches_problem(Dict(actual), Dict(expected))
end

function matches_problem(actual::Dict, expected::Dict)::Bool
    match = string(actual[:code]) == string(expected[:code])
    # TODO: behaviour of line numbering in problem reports needs verification before
    # enabling line number tests
    #haskey(expected, :line) && match &= actual[:line] = expected[:line]

    return match
end

# In some error cases the results may be nothing, rather than empty
function result_table_to_dict(results::Nothing)
    return nothing
end

function result_table_to_dict(results)
    dict_results = Dict{String, Arrow.Table}()
    for result in results
        dict_results[result[1]] = result[2]
    end
    return dict_results
end

# Log a captured log via the current logger
function playback_log(
    io::IO,
    (; level, message, _module, group, id, file, line, kwargs)::LogRecord,
)
    logger = Logging.ConsoleLogger(io)
    Logging.handle_message(
        logger,
        level,
        message,
        _module,
        group,
        id,
        file,
        line;
        kwargs...,
    )
    return nothing
end

# Get an io and ctx that are colored according to the current logger's capabilities
function get_logging_io()
    io = IOBuffer()

    stream = stderr
    logger = Logging.current_logger()
    if hasproperty(logger, :stream) && isopen(logger.stream)
        stream = logger.stream
    end
    ctx = IOContext(io, stream)
    return io, ctx
end
