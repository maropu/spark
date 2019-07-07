-- Creates a test table containing all typed data
CREATE TABLE srcTestTable(
  byteVal byte,
  shortVal short,
  intVal int,
  longVal long,
  doubleVal double,
  floatVal float,
  decimal3_0Val decimal(3, 0),
  decimal5_0Val decimal(5, 0),
  decimal10_0Val decimal(10, 0),
  decimal10_2Val decimal(10, 2),
  decimal20_0Val decimal(20, 0),
  decimal30_15Val decimal(30, 15),
  decimal14_7Val decimal(14, 7),
  binaryVal binary,
  booleanVal boolean,
  stringVal string,
  dateVal date,
  timestampVal timestamp,
  arrayIntVal array<int>,
  arrayDoubleVal array<double>,
  mapStringIntVal map<string, int>,
  mapStringDoubleVal map<string, double>,
  structIntDoubleVal struct<d0: int, d1: double>,
  structStringIntVal struct<d0: string, d1: int>
) USING parquet;

INSERT INTO srcTestTable VALUES(
  1, 1, 1, 1, 1.0, 1.0, 1, 1, 1, 1.0, 1.0, 1.0, 1.0, 'abc', true, 'abc', '1970-01-01', '1970-01-01 00:00:00',
  array(1, 2, 3), array(1.0, 2.0, 3.0), map('k1', 1, 'k2', 2), map('k1', 1.0, 'k2', 2.0),
  struct(1, 1.0), struct('a', 1)
);

INSERT INTO srcTestTable VALUES(
  127, 32767, 2147483647, 9223372036854775807, 1.797693e+308, 3.402823e+38,
  999, 99999, 9999999999, 99999.99, 9999999999999999999, 999999999999999.999999999999999, 9999999.9999999,
  'def', false, 'def', '2018-07-06', '2018-07-06 00:00:00',
  array(4, 5, 6), array(4.0, 5.0, 6.0), map('k3', 3, 'k4', 4), map('k3', 3.0, 'k4', 4.0),
  struct(2, 2.0), struct('b', 2)
);

-- Checks the test data and their types
SELECT * FROM srcTestTable;

-- Turns on ANSI mode
SET spark.sql.ansi.typeCoercion.enabled=true;

-- The following table shows all implicit data type conversions that are not visible to the user:
-- +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
-- | Source Type\CAST TO  | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  | NullType | CalendarIntervalType |     DecimalType     | NumericType | IntegralType |
-- +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
-- | ByteType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(3, 0)   | ByteType    | ByteType     |
-- | ShortType            | X        | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(5, 0)   | ShortType   | ShortType    |
-- | IntegerType          | X        | X         | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(10, 0)  | IntegerType | IntegerType  |
-- | LongType             | X        | X         | X           | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(20, 0)  | LongType    | LongType     |
-- | DoubleType           | X        | X         | X           | X        | DoubleType | X         | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(30, 15) | DoubleType  | IntegerType  |
-- | FloatType            | X        | X         | X           | X        | DoubleType | FloatType | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(14, 7)  | FloatType   | IntegerType  |
-- | Dec(10, 2)           | X        | X         | X           | X        | DoubleType | FloatType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(10, 2)  | Dec(10, 2)  | IntegerType  |
-- | BinaryType           | X        | X         | X           | X        | X          | X         | X          | BinaryType | X           | X          | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
-- | BooleanType          | X        | X         | X           | X        | X          | X         | X          | X          | BooleanType | X          | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
-- | StringType           | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
-- | DateType             | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | DateType | TimestampType | X          | X        | X           | X        | X                    | X                   | X           | X            |
-- | TimestampType        | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | TimestampType | X          | X        | X           | X        | X                    | X                   | X           | X            |
-- | ArrayType            | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | ArrayType* | X        | X           | X        | X                    | X                   | X           | X            |
-- | MapType              | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | X          | MapType* | X           | X        | X                    | X                   | X           | X            |
-- | StructType           | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | X          | X        | StructType* | X        | X                    | X                   | X           | X            |
-- | NullType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  | NullType | CalendarIntervalType | DecimalType(38, 18) | DoubleType  | IntegerType  |
-- | CalendarIntervalType | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | CalendarIntervalType | X                   | X           | X            |
-- +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
-- Note: StructType* is castable when all the internal child types are castable according to the table.
-- Note: ArrayType* is castable when the element type is castable according to the table.
-- Note: MapType* is castable when both the key type and the value type are castable according to the table.

-- The following table shows the result type of binary arithmetic operations:
-- +--------------------------+-------------+
-- | Input Types              | Result Type |
-- +--------------------------+-------------+
-- | ByteType, ShortType      | ShortType   |
-- | ByteType, IntegerType    | IntegerType |
-- | ByteType, IntegerType    | IntegerType |
-- | ByteType, LongType       | LongType    |
-- | ByteType, DoubleType     | DoubleType  |
-- | ByteType, FloatType      | DoubleType  |
-- | ByteType, DecimalType    | DecimalType |
-- | ShortType, IntegerType   | IntegerType |
-- | ShortType, LongType      | LongType    |
-- | ShortType, DoubleType    | DoubleType  |
-- | ShortType, FloatType     | DoubleType  |
-- | ShortType, DecimalType   | DecimalType |
-- | IntegerType, LongType    | LongType    |
-- | IntegerType, DoubleType  | DoubleType  |
-- | IntegerType, FloatType   | DoubleType  |
-- | IntegerType, DecimalType | DecimalType |
-- | LongType, DoubleType     | DoubleType  |
-- | LongType, FloatType      | DoubleType  |
-- | LongType, DecimalType    | DecimalType |
-- | DoubleType, FloatType    | DoubleType  |
-- | DoubleType, DecimalType  | DoubleType  |
-- | FloatType, DecimalType   | DoubleType  |
-- +--------------------------+-------------+

-- PostgreSQL v11.2 output for the almost same queries:
-- implicitCasts-postgresql11.2.sql: https://gist.github.com/maropu/d953c511e11f2c4a07206005f8424618
-- implicitCasts-postgresql11.2.sql.out: https://gist.github.com/maropu/3254c392d197cf4f5e496202da5e253f
--


-- binary arithmetic expression cases

-- (short, *)
SELECT shortVal * intVal FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * longVal FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * doubleVal FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * floatVal FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal3_0Val FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal5_0Val FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal10_0Val FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal10_2Val FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal20_0Val FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal30_15Val FROM srcTestTable WHERE shortVal = 1;
SELECT shortVal * decimal14_7Val FROM srcTestTable WHERE shortVal = 1;

-- (int, *)
SELECT intVal * longVal FROM srcTestTable WHERE intVal = 1;
SELECT intVal * doubleVal FROM srcTestTable WHERE intVal = 1;
SELECT intVal * floatVal FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal3_0Val FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal5_0Val FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal10_0Val FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal10_2Val FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal20_0Val FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal30_15Val FROM srcTestTable WHERE intVal = 1;
SELECT intVal * decimal14_7Val FROM srcTestTable WHERE intVal = 1;

-- (long, *)
SELECT longVal * doubleVal FROM srcTestTable WHERE longVal = 1;
SELECT longVal * floatVal FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal3_0Val FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal5_0Val FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal10_0Val FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal10_2Val FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal20_0Val FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal30_15Val FROM srcTestTable WHERE longVal = 1;
SELECT longVal * decimal14_7Val FROM srcTestTable WHERE longVal = 1;

-- (double, *)
SELECT doubleVal * floatVal FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal3_0Val FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal5_0Val FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal10_0Val FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal10_2Val FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal20_0Val FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal30_15Val FROM srcTestTable WHERE doubleVal < 1.1;
SELECT doubleVal * decimal14_7Val FROM srcTestTable WHERE doubleVal < 1.1;

-- (float, *)
SELECT floatVal * decimal3_0Val FROM srcTestTable WHERE floatVal < 1.1;
SELECT floatVal * decimal5_0Val FROM srcTestTable WHERE floatVal < 1.1;
SELECT floatVal * decimal10_0Val FROM srcTestTable WHERE floatVal < 1.1;
SELECT floatVal * decimal10_2Val FROM srcTestTable WHERE floatVal < 1.1;
SELECT floatVal * decimal20_0Val FROM srcTestTable WHERE floatVal < 1.1;
SELECT floatVal * decimal30_15Val FROM srcTestTable WHERE floatVal < 1.1;
SELECT floatVal * decimal14_7Val FROM srcTestTable WHERE floatVal < 1.1;

-- (decimal(3, 0), *)
SELECT decimal3_0Val * decimal5_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal3_0Val * decimal10_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal3_0Val * decimal10_2Val FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal3_0Val * decimal20_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal3_0Val * decimal30_15Val FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal3_0Val * decimal14_7Val FROM srcTestTable WHERE decimal3_0Val = 1;

-- (decimal(5, 0), *)
SELECT decimal5_0Val * decimal10_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal5_0Val * decimal10_2Val FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal5_0Val * decimal20_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal5_0Val * decimal30_15Val FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal5_0Val * decimal14_7Val FROM srcTestTable WHERE decimal5_0Val = 1;

-- (decimal(10, 0), *)
SELECT decimal10_0Val * decimal10_2Val FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal10_0Val * decimal20_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal10_0Val * decimal30_15Val FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal10_0Val * decimal14_7Val FROM srcTestTable WHERE decimal10_0Val = 1;

-- (decimal(10, 2), *)
SELECT decimal10_2Val * decimal20_0Val FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal10_2Val * decimal30_15Val FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal10_2Val * decimal14_7Val FROM srcTestTable WHERE decimal10_2Val = 1;

-- (decimal(30, 15), *)
SELECT decimal30_15Val * decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal30_15Val * decimal14_7Val FROM srcTestTable WHERE decimal30_15Val = 1;


-- binary arithmetic expression cases

-- (binary, *)
SELECT binaryVal || shortVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || intVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || longVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || doubleVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || floatVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal3_0Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal5_0Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal10_0Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal10_2Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal20_0Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal30_15Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || decimal14_7Val FROM srcTestTable LIMIT 1;
SELECT binaryVal || booleanVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || stringVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || dateVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || timestampVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || arrayIntVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || arrayDoubleVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || mapStringIntVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || mapStringDoubleVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || structIntDoubleVal FROM srcTestTable LIMIT 1;
SELECT binaryVal || structStringIntVal FROM srcTestTable LIMIT 1;

-- (text, *)
SELECT stringVal || shortVal FROM srcTestTable LIMIT 1;
SELECT stringVal || intVal FROM srcTestTable LIMIT 1;
SELECT stringVal || longVal FROM srcTestTable LIMIT 1;
SELECT stringVal || doubleVal FROM srcTestTable LIMIT 1;
SELECT stringVal || floatVal FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal3_0Val FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal5_0Val FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal10_0Val FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal10_2Val FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal20_0Val FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal30_15Val FROM srcTestTable LIMIT 1;
SELECT stringVal || decimal14_7Val FROM srcTestTable LIMIT 1;
SELECT stringVal || booleanVal FROM srcTestTable LIMIT 1;
SELECT stringVal || stringVal FROM srcTestTable LIMIT 1;
SELECT stringVal || dateVal FROM srcTestTable LIMIT 1;
SELECT stringVal || timestampVal FROM srcTestTable LIMIT 1;
SELECT stringVal || arrayIntVal FROM srcTestTable LIMIT 1;
SELECT stringVal || arrayDoubleVal FROM srcTestTable LIMIT 1;
SELECT stringVal || mapStringIntVal FROM srcTestTable LIMIT 1;
SELECT stringVal || mapStringDoubleVal FROM srcTestTable LIMIT 1;
SELECT stringVal || structIntDoubleVal FROM srcTestTable LIMIT 1;
SELECT stringVal || structStringIntVal FROM srcTestTable LIMIT 1;


-- function inputs

-- from * to short
SELECT shortIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT shortIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT shortIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT shortIn(floatVal) FROM srcTestTable WHERE floatVal < 1.1;
SELECT shortIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT shortIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT shortIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT shortIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT shortIn(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT shortIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT shortIn(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT shortIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT shortIn(null);

-- from * to int
SELECT intIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT intIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT intIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT intIn(floatVal) FROM srcTestTable WHERE floatVal < 1.1;
SELECT intIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT intIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT intIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT intIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT intIn(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT intIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT intIn(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT intIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT intIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT intIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT intIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT intIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT intIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT intIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT intIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT intIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT intIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT intIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT intIn(null);

-- from * to long
SELECT longIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT longIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT longIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT longIn(floatVal) FROM srcTestTable WHERE floatVal < 1.1;
SELECT longIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT longIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT longIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT longIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT longIn(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT longIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT longIn(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT longIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT longIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT longIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT longIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT longIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT longIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT longIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT longIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT longIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT longIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT longIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT longIn(null);

-- from * to float8
SELECT doubleIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT doubleIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT doubleIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT doubleIn(floatVal) FROM srcTestTable WHERE floatVal < 1.1;
SELECT doubleIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT doubleIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT doubleIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT doubleIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT doubleIn(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT doubleIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT doubleIn(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT doubleIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT doubleIn(null);

-- from * to float4
SELECT floatIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT floatIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT floatIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT floatIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT floatIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT floatIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT floatIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT floatIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT floatIn(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT floatIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT floatIn(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT floatIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT floatIn(null);

-- from * to decimal(3, 0)
SELECT decimal3_0In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal3_0In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal3_0In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal3_0In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal3_0In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal3_0In(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal3_0In(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal3_0In(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal3_0In(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT decimal3_0In(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal3_0In(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT decimal3_0In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal3_0In(null);

-- from * to decimal(5, 0)
SELECT decimal5_0In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal5_0In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal5_0In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal5_0In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal5_0In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal5_0In(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal5_0In(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal5_0In(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal5_0In(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT decimal5_0In(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal5_0In(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT decimal5_0In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal5_0In(null);

-- from * to decimal(10, 0)
SELECT decimal10_0In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal10_0In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal10_0In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal10_0In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal10_0In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal10_0In(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal10_0In(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal10_0In(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal10_0In(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT decimal10_0In(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal10_0In(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT decimal10_0In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_0In(null);

-- from * to decimal(10, 2)
SELECT decimal10_2In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal10_2In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal10_2In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal10_2In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal10_2In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal10_2In(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal10_2In(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal10_2In(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal10_2In(decimal20_0Val) FROM srcTestTable WHERE decimal20_0Val = 1;
SELECT decimal10_2In(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal10_2In(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT decimal10_2In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal10_2In(null);

-- from * to decimal(20, 0)
SELECT decimal20_0In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal20_0In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal20_0In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal20_0In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal20_0In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal20_0In(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal20_0In(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal20_0In(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal20_0In(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal20_0In(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal20_0In(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT decimal20_0In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal20_0In(null);

-- from * to decimal(30, 15)
SELECT decimal30_15In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal30_15In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal30_15In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal30_15In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal30_15In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal30_15In(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal30_15In(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal30_15In(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal30_15In(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT decimal30_15In(decimal14_7Val) FROM srcTestTable WHERE decimal14_7Val = 1;
SELECT decimal30_15In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal30_15In(null);

-- from * to decimal(14, 7)
SELECT decimal14_7In(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT decimal14_7In(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT decimal14_7In(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT decimal14_7In(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT decimal14_7In(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT decimal14_7In(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT decimal14_7In(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT decimal14_7In(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT decimal14_7In(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT decimal14_7In(binaryVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(booleanVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(stringVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(dateVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(timestampVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT decimal14_7In(null);

-- from * to binary
SELECT binaryIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT binaryIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT binaryIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT binaryIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT binaryIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT binaryIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT binaryIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT binaryIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT binaryIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT binaryIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT binaryIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT binaryIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT binaryIn(null);

-- from * to boolean
SELECT booleanIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT booleanIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT booleanIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT booleanIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT booleanIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT booleanIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT booleanIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT booleanIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT booleanIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT booleanIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT booleanIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT booleanIn(null);

-- from * to string
SELECT stringIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT stringIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT stringIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT stringIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT stringIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT stringIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT stringIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT stringIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT stringIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT stringIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT stringIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT stringIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT stringIn(null);

-- from * to date
SELECT dateIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT dateIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT dateIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT dateIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT dateIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT dateIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT dateIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT dateIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT dateIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT dateIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT dateIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT dateIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT dateIn(null);

-- from * to timestamp
SELECT timestampIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT timestampIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT timestampIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT timestampIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT timestampIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT timestampIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT timestampIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT timestampIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT timestampIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT timestampIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT timestampIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT timestampIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT timestampIn(null);

-- from * to array<int>
SELECT arrayIntIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT arrayIntIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT arrayIntIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT arrayIntIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT arrayIntIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT arrayIntIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT arrayIntIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT arrayIntIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT arrayIntIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT arrayIntIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT arrayIntIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT arrayIntIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT arrayIntIn(null);

-- from * to array<double>
SELECT arrayDoubleIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT arrayDoubleIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT arrayDoubleIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT arrayDoubleIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT arrayDoubleIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT arrayDoubleIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT arrayDoubleIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT arrayDoubleIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT arrayDoubleIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT arrayDoubleIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT arrayDoubleIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT arrayDoubleIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT arrayDoubleIn(null);

-- from * to map<string, int>
SELECT mapStringIntIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT mapStringIntIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT mapStringIntIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT mapStringIntIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT mapStringIntIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT mapStringIntIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT mapStringIntIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT mapStringIntIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT mapStringIntIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT mapStringIntIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT mapStringIntIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT mapStringIntIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(mapStringDoubleVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT mapStringIntIn(null);

-- from * to map<string, double>
SELECT mapStringDoubleIn(shortVal) FROM srcTestTable WHERE shortVal = 1;
SELECT mapStringDoubleIn(intVal) FROM srcTestTable WHERE intVal = 1;
SELECT mapStringDoubleIn(longVal) FROM srcTestTable WHERE longVal = 1;
SELECT mapStringDoubleIn(doubleVal) FROM srcTestTable WHERE doubleVal < 1.1;
SELECT mapStringDoubleIn(floatVal) FROM srcTestTable WHERE floatVal< 1.1;
SELECT mapStringDoubleIn(decimal3_0Val) FROM srcTestTable WHERE decimal3_0Val = 1;
SELECT mapStringDoubleIn(decimal5_0Val) FROM srcTestTable WHERE decimal5_0Val = 1;
SELECT mapStringDoubleIn(decimal10_0Val) FROM srcTestTable WHERE decimal10_0Val = 1;
SELECT mapStringDoubleIn(decimal10_2Val) FROM srcTestTable WHERE decimal10_2Val = 1;
SELECT mapStringDoubleIn(decimal30_15Val) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT mapStringDoubleIn(decimal14_7In) FROM srcTestTable WHERE decimal30_15Val = 1;
SELECT mapStringDoubleIn(binaryVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(booleanVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(stringVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(dateVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(timestampVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(arrayIntVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(arrayDoubleVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(mapStringIntVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(structIntDoubleVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(structStringIntVal) FROM srcTestTable LIMIT 1;
SELECT mapStringDoubleIn(null);

-- Drops all the tables
DROP TABLE srcTestTable;
