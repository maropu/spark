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

-- The following table shows all assignment data type conversions in INSERT/CREATE:
-- +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+
-- | Source Type\CAST TO  | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  |
-- +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+
-- | ByteType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | ShortType            | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | IntegerType          | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | LongType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | DoubleType           | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | FloatType            | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | Dec(10, 2)           | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | BinaryType           | X        | X         | X           | X        | X          | X         | X          | BinaryType | X           | StringType | X        | X             | X          | X        | X           |
-- | BooleanType          | X        | X         | X           | X        | X          | X         | X          | X          | BooleanType | StringType | X        | X             | X          | X        | X           |
-- | StringType           | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | X        | X             | X          | X        | X           |
-- | DateType             | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | DateType | TimestampType | X          | X        | X           |
-- | TimestampType        | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | DateType | TimestampType | X          | X        | X           |
-- | ArrayType            | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | X        | X             | ArrayType* | X        | X           |
-- | MapType              | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | X        | X             | X          | MapType* | X           |
-- | StructType           | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | X        | X             | X          | X        | StructType* |
-- | NullType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  |
-- +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+
-- Note: StructType* is castable when all the internal child types are castable according to the table.
-- Note: ArrayType* is castable when the element type is castable according to the table.
-- Note: MapType* is castable when both the key type and the value type are castable according to the table.


-- PostgreSQL v11.2 output for the almost same queries:
-- assignmentCasts-postgresql11.2.sql: https://gist.github.com/maropu/3d35750c88a5cc659f5d4e58169e8016
-- assignmentCasts-postgresql11.2.sql.out: https://gist.github.com/maropu/7c56c8f372855e713475d21a19bf62a6
--


-- from * to byte
CREATE TABLE dstByteValTable(byteVal byte) USING parquet;

INSERT INTO dstByteValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstByteValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstByteValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstByteValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstByteValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstByteValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstByteValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstByteValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstByteValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstByteValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstByteValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstByteValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstByteValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstByteValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstByteValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstByteValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstByteValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstByteValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstByteValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstByteValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstByteValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstByteValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstByteValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstByteValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstByteValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstByteValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstByteValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstByteValTable SELECT null;

SELECT * FROM dstByteValTable ORDER BY byteVal;

-- from * to short
CREATE TABLE dstShortValTable(shortVal short) USING parquet;

INSERT INTO dstShortValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstShortValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstShortValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstShortValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstShortValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstShortValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstShortValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstShortValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstShortValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstShortValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstShortValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstShortValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstShortValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstShortValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstShortValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstShortValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstShortValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstShortValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstShortValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstShortValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstShortValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstShortValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstShortValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstShortValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstShortValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstShortValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstShortValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstShortValTable SELECT null;

SELECT * FROM dstShortValTable ORDER BY shortVal;

-- from * to int
CREATE TABLE dstIntValTable(intVal int) USING parquet;

INSERT INTO dstIntValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstIntValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstIntValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstIntValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstIntValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstIntValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstIntValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstIntValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstIntValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstIntValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstIntValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstIntValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstIntValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstIntValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstIntValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstIntValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstIntValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstIntValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstIntValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstIntValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstIntValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstIntValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstIntValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstIntValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstIntValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstIntValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstIntValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstIntValTable SELECT null;

SELECT * FROM dstIntValTable ORDER BY intVal;

-- from * to long
CREATE TABLE dstLongValTable(longVal long) USING parquet;

INSERT INTO dstLongValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstLongValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstLongValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstLongValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstLongValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstLongValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstLongValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstLongValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstLongValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstLongValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstLongValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstLongValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstLongValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstLongValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstLongValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstLongValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstLongValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstLongValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstLongValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstLongValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstLongValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstLongValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstLongValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstLongValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstLongValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstLongValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstLongValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstLongValTable SELECT null;

SELECT * FROM dstLongValTable ORDER BY longVal;

-- from * to double
CREATE TABLE dstDoubleValTable(doubleVal double) USING parquet;

INSERT INTO dstDoubleValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDoubleValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDoubleValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDoubleValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDoubleValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDoubleValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDoubleValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDoubleValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDoubleValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDoubleValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDoubleValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDoubleValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDoubleValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDoubleValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDoubleValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDoubleValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDoubleValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDoubleValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDoubleValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDoubleValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDoubleValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDoubleValTable SELECT null;

SELECT * FROM dstDoubleValTable ORDER BY doubleVal;

-- from * to float
CREATE TABLE dstFloatValTable(floatVal float) USING parquet;

INSERT INTO dstFloatValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstFloatValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstFloatValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstFloatValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstFloatValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstFloatValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstFloatValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstFloatValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstFloatValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstFloatValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstFloatValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstFloatValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstFloatValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstFloatValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstFloatValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstFloatValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstFloatValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstFloatValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstFloatValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstFloatValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstFloatValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstFloatValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstFloatValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstFloatValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstFloatValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstFloatValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstFloatValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstFloatValTable SELECT null;

SELECT * FROM dstFloatValTable ORDER BY floatVal;

-- from * to decimal(3, 0)
CREATE TABLE dstDecimal3_0ValTable(decimal3_0Val decimal(3, 0)) USING parquet;

INSERT INTO dstDecimal3_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal3_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal3_0ValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDecimal3_0ValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDecimal3_0ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal3_0ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal3_0ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal3_0ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal3_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal3_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal3_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal3_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal3_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal3_0ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal3_0ValTable SELECT null;

SELECT * FROM dstDecimal3_0ValTable ORDER BY decimal3_0Val;

-- from * to decimal(5, 0)
CREATE TABLE dstDecimal5_0ValTable(decimal5_0Val decimal(5, 0)) USING parquet;

INSERT INTO dstDecimal5_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal5_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal5_0ValTable SELECT shortVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal5_0ValTable SELECT shortVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal5_0ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal5_0ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal5_0ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal5_0ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal5_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal5_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal5_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal5_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal5_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal5_0ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal5_0ValTable SELECT null;

SELECT * FROM dstDecimal5_0ValTable ORDER BY decimal5_0Val;

-- from * to decimal(10, 0)
CREATE TABLE dstDecimal10_0ValTable(decimal10_0Val decimal(10, 0)) USING parquet;

INSERT INTO dstDecimal10_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal10_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal10_0ValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDecimal10_0ValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDecimal10_0ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal10_0ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal10_0ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal10_0ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal10_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal10_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal10_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal10_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal10_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal10_0ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal10_0ValTable SELECT null;

SELECT * FROM dstDecimal10_0ValTable ORDER BY decimal10_0Val;

-- from * to decimal(10, 2)
CREATE TABLE dstDecimal10_2ValTable(decimal10_2Val decimal(10, 2)) USING parquet;

INSERT INTO dstDecimal10_2ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal10_2ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal10_2ValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDecimal10_2ValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDecimal10_2ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal10_2ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal10_2ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal10_2ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal10_2ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal10_2ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal10_2ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal10_2ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal10_2ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal10_2ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal10_2ValTable SELECT null;

SELECT * FROM dstDecimal10_2ValTable ORDER BY decimal10_2Val;

-- from * to decimal(20, 0)
CREATE TABLE dstDecimal20_0ValTable(decimal20_0Val decimal(20, 0)) USING parquet;

INSERT INTO dstDecimal20_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal20_0ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal20_0ValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDecimal20_0ValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDecimal20_0ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal20_0ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal20_0ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal20_0ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal20_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal20_0ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal20_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal20_0ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal20_0ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal20_0ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal20_0ValTable SELECT null;

SELECT * FROM dstDecimal20_0ValTable ORDER BY decimal20_0Val;

-- from * to decimal(30, 15)
CREATE TABLE dstDecimal30_15ValTable(decimal30_15Val decimal(30, 15)) USING parquet;

INSERT INTO dstDecimal30_15ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal30_15ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal30_15ValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDecimal30_15ValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDecimal30_15ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal30_15ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal30_15ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal30_15ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal30_15ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal30_15ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal30_15ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal30_15ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal30_15ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal30_15ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal30_15ValTable SELECT null;

SELECT * FROM dstDecimal30_15ValTable ORDER BY decimal30_15Val;

-- from * to decimal(14, 7)
CREATE TABLE dstDecimal14_7ValTable(decimal14_7Val decimal(14, 7)) USING parquet;

INSERT INTO dstDecimal14_7ValTable SELECT byteVal FROM srcTestTable WHERE byteVal = 1;
INSERT INTO dstDecimal14_7ValTable SELECT byteVal FROM srcTestTable WHERE byteVal != 1;
INSERT INTO dstDecimal14_7ValTable SELECT shortVal FROM srcTestTable WHERE shortVal = 1;
INSERT INTO dstDecimal14_7ValTable SELECT shortVal FROM srcTestTable WHERE shortVal != 1;
INSERT INTO dstDecimal14_7ValTable SELECT intVal FROM srcTestTable WHERE intVal = 1;
INSERT INTO dstDecimal14_7ValTable SELECT intVal FROM srcTestTable WHERE intVal != 1;
INSERT INTO dstDecimal14_7ValTable SELECT longVal FROM srcTestTable WHERE longVal = 1;
INSERT INTO dstDecimal14_7ValTable SELECT longVal FROM srcTestTable WHERE longVal != 1;
INSERT INTO dstDecimal14_7ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal < 1.1;
INSERT INTO dstDecimal14_7ValTable SELECT doubleVal FROM srcTestTable WHERE doubleVal > 1.1;
INSERT INTO dstDecimal14_7ValTable SELECT floatVal FROM srcTestTable WHERE floatVal < 1.1;
INSERT INTO dstDecimal14_7ValTable SELECT floatVal FROM srcTestTable WHERE floatVal > 1.1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal3_0Val FROM srcTestTable WHERE decimal3_0Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal5_0Val FROM srcTestTable WHERE decimal5_0Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal10_0Val FROM srcTestTable WHERE decimal10_0Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal10_2Val FROM srcTestTable WHERE decimal10_2Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal20_0Val FROM srcTestTable WHERE decimal20_0Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal30_15Val FROM srcTestTable WHERE decimal30_15Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val = 1;
INSERT INTO dstDecimal14_7ValTable SELECT decimal14_7Val FROM srcTestTable WHERE decimal14_7Val != 1;
INSERT INTO dstDecimal14_7ValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDecimal14_7ValTable SELECT null;

SELECT * FROM dstDecimal14_7ValTable ORDER BY decimal14_7Val;

-- from * to binary
CREATE TABLE dstBinaryValTable(binaryVal binary) USING parquet;

INSERT INTO dstBinaryValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstBinaryValTable SELECT null;

SELECT * FROM dstBinaryValTable;

-- from * to boolean
CREATE TABLE dstBooleanValTable(booleanVal boolean) USING parquet;

INSERT INTO dstBooleanValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstBooleanValTable SELECT null;

SELECT * FROM dstBooleanValTable;

-- from * to string
CREATE TABLE dstStringValTable(stringVal string) USING parquet;

INSERT INTO dstStringValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstStringValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstStringValTable SELECT null;

SELECT * FROM dstStringValTable;

-- from * to date
CREATE TABLE dstDateValTable(dateVal date) USING parquet;

INSERT INTO dstDateValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstDateValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstDateValTable SELECT null;

SELECT * FROM dstDateValTable;

-- from * to timestamp
CREATE TABLE dstTimestampValTable(timestampVal timestamp) USING parquet;

INSERT INTO dstTimestampValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstTimestampValTable SELECT null;

SELECT * FROM dstTimestampValTable;

-- from * to array<int>
CREATE TABLE dstArrayIntValTable(arrayIntVal array<int>) USING parquet;

INSERT INTO dstArrayIntValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstArrayIntValTable SELECT null;

SELECT * FROM dstArrayIntValTable;

-- from * to array<double>
CREATE TABLE dstArrayDoubleValTable(arrayDoubleVal array<double>) USING parquet;

INSERT INTO dstArrayDoubleValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstArrayDoubleValTable SELECT null;

SELECT * FROM dstArrayDoubleValTable;

-- from * to map<string, int>
CREATE TABLE dstMapStringIntValTable(dstMapStringIntVal map<string, int>) USING parquet;

INSERT INTO dstMapStringIntValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT mapStringINtVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstMapStringIntValTable SELECT null;

SELECT * FROM dstMapStringIntValTable;

-- from * to map<string, double>
CREATE TABLE dstMapStringDoubleValTable(mapStringDoubleVal map<string, double>) USING parquet;

INSERT INTO dstMapStringDoubleValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstMapStringDoubleValTable SELECT null;

SELECT * FROM dstMapStringDoubleValTable;

-- from * to struct<d0: int, d1: double>,
CREATE TABLE dstStructIntDoubleValTable(structIntDoubleVal struct<d0: int, d1: double>) USING parquet;

INSERT INTO dstStructIntDoubleValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstStructIntDoubleValTable SELECT null;

SELECT * FROM dstStructIntDoubleValTable;

-- from * to struct<d0: string, d1: int>,
CREATE TABLE dstStructStringIntValTable(structStringIntVal struct<d0: string, d1: int>) USING parquet;

INSERT INTO dstStructStringIntValTable SELECT byteVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT shortVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT intVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT longVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT doubleVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT floatVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal3_0Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal5_0Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal10_0Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal10_2Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal20_0Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal30_15Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT decimal14_7Val FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT binaryVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT booleanVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT stringVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT dateVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT timestampVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT arrayIntVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT arrayDoubleVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT mapStringIntVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT mapStringDoubleVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT structIntDoubleVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT structStringIntVal FROM srcTestTable;
INSERT INTO dstStructStringIntValTable SELECT null;

SELECT * FROM dstStructStringIntValTable;

-- Drops all the tables
DROP TABLE srcTestTable;
DROP TABLE dstByteValTable;
DROP TABLE dstShortValTable;
DROP TABLE dstIntValTable;
DROP TABLE dstLongValTable;
DROP TABLE dstDoubleValTable;
DROP TABLE dstFloatValTable;
DROP TABLE dstDecimal3_0ValTable;
DROP TABLE dstDecimal5_0ValTable;
DROP TABLE dstDecimal10_0ValTable;
DROP TABLE dstDecimal10_2ValTable;
DROP TABLE dstDecimal20_0ValTable;
DROP TABLE dstDecimal30_15ValTable;
DROP TABLE dstDecimal14_7ValTable;
DROP TABLE dstBinaryValTable;
DROP TABLE dstBooleanValTable;
DROP TABLE dstStringValTable;
DROP TABLE dstDateValTable;
DROP TABLE dstTimestampValTable;
DROP TABLE dstArrayIntValTable;
DROP TABLE dstArrayDoubleValTable;
DROP TABLE dstMapStringIntValTable;
DROP TABLE dstMapStringDoubleValTable;
DROP TABLE dstStructIntDoubleValTable;
DROP TABLE dstStructStringIntValTable;
