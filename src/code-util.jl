# Extract relation names from the inputs and adds them to the program
# Turns a dict of name=>vector, with names of form :othername/Type,
# into a series of def name = list of tuples
function convert_input_dict_to_string(inputs::AbstractDict)
    program = ""
    for input in inputs
        name = string(input.first)

        # Dict values represent a functional dependency
        if input.second isa Dict
            program *= "\n@function"
        end
        program *= "\ndef insert:" * name * " = "

        first = true

        values = input.second

        if isempty(values)
            program *= "{ }"
            continue
        end

        values = to_vector_of_tuples( values)

        for i in values
            if first
                first = false
            else
                program *= "; "
            end

            if i isa String
                program *= '"' * i * '"'
            elseif i isa Char
                program *= "'" * i * "'"
            else
                program *= string(i)
            end
        end
    end
    return program
end

# Extract relation names from the expected output and append them to output
# Turns a dict of name=>vector, with names of form :othername/Type
# into a series of def output:othername = othername
function generate_output_string_from_expected(expected::AbstractDict)
    program = ""

    for e in expected
        # If we are already explicitly testing an output relation, we don't need to add it
        if startswith(string(e.first), "/:output")
            continue
        end
        if e.first == :output
            continue
        end

        name = ""

        if e.first isa Symbol
            name = string(e.first)
        else
            # rel path, e.g. ":a/:b/Int64"
            tokens = split(string(e.first), "/")
            for token in tokens
                if startswith(token, ":")
                    name *= token
                else
                    break
                end
            end

            name = SubString(name, 2)
        end
        program *= "\ndef output:" * name * " = " * name
    end
    return program
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

function extract_problems(results)
    problems = Problem[]

    if !haskey(results, "/:rel/:catalog/:diagnostic/:code/Int64/String")
        return problems
    end

    # [index, code]
    problem_codes = results["/:rel/:catalog/:diagnostic/:code/Int64/String"]

    problem_lines = Dict()
    if haskey(results, "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64")
        # [index, ?, line]
        problem_lines = results["/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64"]
    end

    problem_severities = Dict()
    if haskey(results, "/:rel/:catalog/:diagnostic/:severity/Int64/String")
        # [index, severity]
        problem_severities = results["/:rel/:catalog/:diagnostic/:severity/Int64/String"]
    end
    if length(problem_codes) > 0
        for i = 1:1:length(problem_codes[1])
            # Not all problems have a line number
            problem_line = nothing
            if haskey(results, "/:rel/:catalog/:diagnostic/:range/:start/:line/Int64/Int64/Int64")
                problem_line = problem_lines[3][i]
            end
            problem_severity = nothing
            if haskey(results, "/:rel/:catalog/:diagnostic/:severity/Int64/String")
                problem_severity = problem_severities[2][i]
            end
            push!(problems, Problem(problem_codes[2][i], problem_severity, problem_line))
        end
    end

    return problems
end

function result_table_to_dict(results)
    dict_results = Dict{String, Arrow.Table}()
    for result in results
        dict_results[result[1]] = result[2]
    end
    return dict_results
end
