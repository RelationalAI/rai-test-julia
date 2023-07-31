# Single element conversion
using RAITest:input_element_to_string
using Dates

@testset "Input conversion to rel" begin
# Int
@test input_element_to_string(Int128(1)) == "1"
@test input_element_to_string(Int64(1)) == "1"
@test input_element_to_string(Int32(1)) == "1"
@test input_element_to_string(Int16(1)) == "1"
@test input_element_to_string(Int8(1)) == "1"

@test input_element_to_string(Int128(0)) == "0"
@test input_element_to_string(Int64(0)) == "0"
@test input_element_to_string(Int32(0)) == "0"
@test input_element_to_string(Int16(0)) == "0"
@test input_element_to_string(Int8(0)) == "0"

@test input_element_to_string(Int128(-1)) == "-1"
@test input_element_to_string(Int64(-1)) == "-1"
@test input_element_to_string(Int32(-1)) == "-1"
@test input_element_to_string(Int16(-1)) == "-1"
@test input_element_to_string(Int8(-1)) == "-1"

@test input_element_to_string(Int128(170141183460469231731687303715884105727)) == "170141183460469231731687303715884105727"
@test input_element_to_string(Int64(9223372036854775807)) == "9223372036854775807"
@test input_element_to_string(Int32(2147483647)) == "2147483647"
@test input_element_to_string(Int16(32767)) == "32767"
@test input_element_to_string(Int8(127)) == "127"

@test input_element_to_string(Int128(-170141183460469231731687303715884105728)) == "-170141183460469231731687303715884105728"
@test input_element_to_string(Int64(-9223372036854775808)) == "-9223372036854775808"
@test input_element_to_string(Int32(-2147483648)) == "-2147483648"
@test input_element_to_string(Int16(-32768)) == "-32768"
@test input_element_to_string(Int8(-128)) == "-128"

#UInt
@test input_element_to_string(UInt128(1)) == "0x00000000000000000000000000000001"
@test input_element_to_string(UInt64(1)) == "0x0000000000000001"
@test input_element_to_string(UInt32(1)) == "0x00000001"
@test input_element_to_string(UInt16(1)) == "0x0001"
@test input_element_to_string(UInt8(1)) == "0x01"

@test input_element_to_string(UInt128(0)) == "0x00000000000000000000000000000000"
@test input_element_to_string(UInt64(0)) == "0x0000000000000000"
@test input_element_to_string(UInt32(0)) == "0x00000000"
@test input_element_to_string(UInt16(0)) == "0x0000"
@test input_element_to_string(UInt8(0)) == "0x00"

@test input_element_to_string(UInt128(340282366920938463463374607431768211455)) == "0xffffffffffffffffffffffffffffffff"
@test input_element_to_string(UInt64(18446744073709551615)) == "0xffffffffffffffff"
@test input_element_to_string(UInt32(4294967295)) == "0xffffffff"
@test input_element_to_string(UInt16(65535)) == "0xffff"
@test input_element_to_string(UInt8(255)) == "0xff"

# Float
@test input_element_to_string(Float64(1)) == "1.0"
@test input_element_to_string(Float32(1)) == "float[32, 1.0]"
@test input_element_to_string(Float16(1)) == "float[16, 1.0]"

@test input_element_to_string("s") == "\"s\""
@test input_element_to_string("s\ns") == "\"s\\ns\""

@test input_element_to_string('c') == "'c'"
@test input_element_to_string('\n') == "'\\n'"

end
