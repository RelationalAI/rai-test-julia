# helper for optional types
const Option{T} = Union{Nothing, T}

const REL_CODE_KEY = "/:rel/:catalog/:diagnostic/:code/Int64/String"
const REL_LINE_KEY = "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64"
const REL_SEVERITY_KEY = "/:rel/:catalog/:diagnostic/:severity/Int64/String"
const REL_MESSAGE_KEY = "/:rel/:catalog/:diagnostic/:message/Int64/String"

# Convert accepted install source types to Dict{String, String}
convert_to_install_kv(install_dict::Dict{String, String}) = install_dict
convert_to_install_kv(install_pair::Pair{String, String}) = Dict(install_pair)
convert_to_install_kv(install_string::String) = convert_to_install_kv([install_string])
function convert_to_install_kv(install_vector::Vector{String})
    models = Dict{String, String}()
    for (i, src) in enumerate(install_vector)
        models["test_install_$i"] = src
    end
    return models
end

# Build a path/key to identify an expected relation in the output
function build_path(base::Symbol, values)
    name = "/:"
    if !is_special_symbol(base)
        name = "/:output/:"
    end

    name *= string(base)

    # Now determine types
    name *= type_string(values)

    return name
end

build_path(name::Any, ::Any) = string(name)

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

function input_element_to_string(input::Float16)
    # `repr`` will generate non-parseable Float16(1.0)`
    return "float[16, $input]"
end

function input_element_to_string(input::Float32)
    # `repr`` will generate non-parseable `1.0f0`
    return "float[32, $input]"
end

function input_element_to_string(input::Int8)
    # `repr`` will generate non-parseable Float16(1.0)`
    return "int[8, $input]"
end

function input_element_to_string(input::Int16)
    # `repr`` will generate non-parseable Float16(1.0)`
    return "int[16, $input]"
end

function input_element_to_string(input::Int32)
    # `repr`` will generate non-parseable `1.0f0`
    return "int[32, $input]"
end

function input_element_to_string(input::Int128)
    # `repr`` will generate non-parseable `1.0f0`
    return "int[128, $input]"
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

function extract_detail(results, key, cat_index, row_index)
    problem_category = get(results, key, nothing)
    if isnothing(problem_category)
        return nothing
    end
    rows = get(problem_category, cat_index, Dict())

    return get(rows, row_index, nothing)
end

# In some error cases the results may be nothing, rather than empty
function extract_problems(results::Nothing)
    return []
end

function extract_problems(results)
    problems = []

    if !haskey(results, REL_CODE_KEY)
        return problems
    end

    # Diagnostic categories have identical ordering so we can use row to find matches
    # across categories, starting with row 1
    for i in 1:length(results[REL_CODE_KEY][1])
        code = extract_detail(results, REL_CODE_KEY, 2, i)
        # index, subindex, line
        line = extract_detail(results, REL_LINE_KEY, 3, i)
        # index, severity
        severity = extract_detail(results, REL_SEVERITY_KEY, 2, i)
        # index, message
        message = extract_detail(results, REL_MESSAGE_KEY, 2, i)

        problem = (; code, line, severity, message)
        push!(problems, problem)
    end

    return problems
end

function contains_problem(problems, problem_needle)::Bool
    return any(p -> matches_problem(p, problem_needle), problems)
end

matches_problem(prob1::Union{Tuple, Pair}, prob2) = matches_problem(Dict(prob1), prob2)
matches_problem(prob1, prob2::Union{Tuple, Pair}) = matches_problem(prob1, Dict(prob2))

# Match problems based on :code (required) and :line (if present in both)
function matches_problem(prob1, prob2)::Bool
    match = string(prob1[:code]) == string(prob2[:code])

    if haskey(prob1, :line) && haskey(prob2, :line)
        match &= (prob1[:line] == prob2[:line])
    end

    if haskey(prob1, :severity) && haskey(prob2, :severity)
        match &= (string(prob1[:severity]) == string(prob2[:severity]))
    end

    return match
end

# In some error cases the results may be nothing, rather than empty
function result_table_to_dict(results::Nothing)
    return nothing
end

function result_table_to_dict(results)
    return Dict{String, Arrow.Table}(results)
end

relation_id(name, ::Any) = return string(name)

function relation_id(base_name::Symbol, values)
    name = "/:"
    if !is_special_symbol(base_name)
        name = "/:output/:"
    end

    name *= string(base_name)

    # Now determine types
    name *= type_string(values)

    return name
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
