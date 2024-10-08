# helper for optional types
const Option{T} = Union{Nothing, T}

const REL_CODE_KEY = "/:rel/:catalog/:diagnostic/:code/Int64/String"
const REL_LINE_KEY = "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64"
const REL_SEVERITY_KEY = "/:rel/:catalog/:diagnostic/:severity/Int64/String"
const REL_MESSAGE_KEY = "/:rel/:catalog/:diagnostic/:message/Int64/String"

const IC_LINE_KEY = "/:rel/:catalog/:ic_violation/:range/:start/:line/HashValue/Int64"
const IC_OUTPUT_KEY = "/:rel/:catalog/:ic_violation/:output/HashValue/"
const IC_REPORT_KEY = "/:rel/:catalog/:ic_violation/:report/HashValue/String"

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
# into a series of def name { list of tuples }
function convert_input_dict_to_string(inputs::AbstractDict)
    program = ""
    for input in inputs
        name = string(input.first)

        program *= "\ndef insert[:" * name * "]: { "

        values = to_vector_of_tuples(input.second)
        program *= join([input_element_to_string(v) for v in values], "; ")

        program *= " }"
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
        program *= "\ndef output[:" * name * "]: " * name
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

# Values can be a single value, a single tuple, a Vector of values, or a typed but empty
# Vector. Types are extracted directly from single values and recursively from Tuples and
# Vectors.

# :a => [3, 4, 5]
type_string(::Vector{T}) where {T} = type_string(T)

# :a => [(1, 2)]
# :a => (1, 2)
function type_string(::Union{T, Vector{T}}) where {T <: Tuple}
    result = ""
    for e_type in fieldtypes(T)
        result *= type_string(e_type)
    end

    return result
end

# []
type_string(::Type{Any}) = ""

# Generate a string representing the Rel type for single values
# :a => 1
type_string(::Union{T, Type{T}}) where {T} = "/" * string(T)

# The value tuple contains an inner tuple. Recurse into it.
function type_string(::Type{T}) where {T <: Tuple}
    result = "/("
    for e_type in fieldtypes(T)
        result *= type_string(e_type)
    end
    result *= ")"
    return result
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

function filter_by_prefix(results::Dict{String, Arrow.Table}, path::String)
    filtered = Dict()

    for (key, value) in results
        if !startswith(key, path)
            continue
        end

        filtered[key] = value
    end

    return filtered
end

function table_to_rows(table::Arrow.Table)
    rows = []
    if isempty(table)
        return rows
    end

    for i in eachindex(table[1])
        row = []
        for j in eachindex(table)
            push!(row, table[j][i])
        end
        push!(rows, row)
    end

    return rows
end

# Extract the IC results for a given hash
# These are stored in the form:
# /:rel/:catalog/:ic_violation/:xxxx/HashValue/Type[/Type]*
function filter_ic_results(results::Dict, path::String, h, limit::Int=10)
    ics = []

    # Find all the rows with the given path prefix in the key
    for arrow in values(filter_by_prefix(results, path))
        for row in table_to_rows(arrow)
            if row[1] != h
                continue
            end

            # Now that we have a match, extract all the values and construct a tuple
            values = row[2:end]
            push!(ics, values)
            if length(ics) >= limit
                return ics
            end
        end
    end

    return ics
end

# In some error cases the results may be nothing, rather than empty
function extract_ics(results::Nothing)
    return []
end

function extract_ics(results, limit::Int=10)
    ics = []

    if !haskey(results, IC_LINE_KEY)
        return ics
    end

    for i in 1:length(results[IC_LINE_KEY][1])
        line = extract_detail(results, IC_LINE_KEY, 2, i)
        report = extract_detail(results, IC_REPORT_KEY, 2, i)

        # IC Diagnostic values are indexed by hash and type so we extract them separately
        h = extract_detail(results, IC_LINE_KEY, 1, i)
        # Bump the limit by one to see if there are more results than we are showing
        vs = filter_ic_results(results, IC_OUTPUT_KEY, h, limit + 1)
        pretty_vs = [(v...,) for v in vs]
        if length(pretty_vs) > limit
            pretty_vs = vcat(pretty_vs[1:limit], "...")
        end

        values = join(pretty_vs, "; ")
        if isempty(values)
            ic = (; line, report)
        else
            ic = (; line, values, report)
        end
        push!(ics, ic)
    end

    return ics
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

default_timeout() = parse(Int64, get(ENV, "TEST_REL_TEST_TIMEOUT", "300"))
default_allowed() = Symbol(get(ENV, "TEST_REL_SEVERITY_ALLOWED", "warning"))
default_db_name() = get(ENV, "TEST_REL_DB_NAME", "test_rel")
default_engine_name() = get(ENV, "TEST_REL_ENGINE_NAME", "test_rel")
