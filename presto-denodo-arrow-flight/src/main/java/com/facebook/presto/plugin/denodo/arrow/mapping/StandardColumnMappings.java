/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.denodo.arrow.mapping;

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.primitives.SignedBytes;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.plugin.denodo.arrow.mapping.WriteMapping.createBooleanWriteMapping;
import static com.facebook.presto.plugin.denodo.arrow.mapping.WriteMapping.createDoubleWriteMapping;
import static com.facebook.presto.plugin.denodo.arrow.mapping.WriteMapping.createLongWriteMapping;
import static com.facebook.presto.plugin.denodo.arrow.mapping.WriteMapping.createSliceWriteMapping;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;

public final class StandardColumnMappings
{
    private StandardColumnMappings() {}

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    public static WriteMapping booleanWriteMapping()
    {
        return createBooleanWriteMapping((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            BitVector dataVector = new BitVector(field, new RootAllocator());
            dataVector.setSafe(0, value ? 1 : 0);
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        });
    }

    public static WriteMapping tinyintWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            TinyIntVector dataVector = new TinyIntVector(field, new RootAllocator());
            dataVector.setSafe(0, SignedBytes.checkedCast(value));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping smallintWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            SmallIntVector dataVector = new SmallIntVector(field, new RootAllocator());
            dataVector.setSafe(0, SignedBytes.checkedCast(value));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping integerWriteMapping()
    {
        return createLongWriteMapping((((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            IntVector dataVector = new IntVector(field, new RootAllocator());
            dataVector.setSafe(0, toIntExact(value));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        })));
    }

    public static WriteMapping bigintWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> {
            Field field = statement.getResultSetSchema().getFields().get(0);
            BigIntVector dataVector = new BigIntVector(field, new RootAllocator(Long.MAX_VALUE));
            dataVector.allocateNew(1);
            dataVector.setSafe(0, value);
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping realWriteMapping()
    {
        return createLongWriteMapping((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            Float4Vector dataVector = new Float4Vector(field, new RootAllocator());
            dataVector.setSafe(0, intBitsToFloat(toIntExact(value)));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        });
    }

    public static WriteMapping doubleWriteMapping()
    {
        return createDoubleWriteMapping((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            Float8Vector dataVector = new Float8Vector(field, new RootAllocator());
            dataVector.setSafe(0, value);
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        });
    }

    public static WriteMapping decimalWriteMapping(DecimalType decimalType)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return createLongWriteMapping(((statement, index, value) -> {
                BigInteger unscaledValue = BigInteger.valueOf(value);
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, scale, new MathContext(decimalType.getPrecision()));
                Field field = statement.getParameterSchema().getFields().get(index);
                BigIntVector dataVector = new BigIntVector(field, new RootAllocator());
                dataVector.setSafe(0, bigDecimal.unscaledValue().longValue());
                statement.setParameters(VectorSchemaRoot.of(dataVector));
            }));
        }
        return createSliceWriteMapping(((statement, index, value) -> {
            BigInteger unscaledValue = decodeUnscaledValue(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, scale, new MathContext(decimalType.getPrecision()));
            Field field = statement.getParameterSchema().getFields().get(index);
            BigIntVector dataVector = new BigIntVector(field, new RootAllocator());
            dataVector.setSafe(0, bigDecimal.unscaledValue().longValue());
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping charWriteMapping()
    {
        return createSliceWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            VarCharVector dataVector = new VarCharVector(field, new RootAllocator());
            dataVector.setSafe(0, value.getBytes());
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping varbinaryWriteMapping()
    {
        return createSliceWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            VarBinaryVector dataVector = new VarBinaryVector(field, new RootAllocator());
            dataVector.setSafe(0, value.getBytes());
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping dateWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            DateDayVector dataVector = new DateDayVector(field, new RootAllocator());
            dataVector.setSafe(0, (int) DAYS.toDays(UTC_CHRONOLOGY.getZone().getOffset(value)));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping timeWriteMapping()
    {
        //TODO milli or micro based of connector config
        return createLongWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            TimeMilliVector dataVector = new TimeMilliVector(field, new RootAllocator());
            dataVector.setSafe(0, (int) value);
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping timestampWriteMapping(TimestampType timestampType)
    {
        //TODO milli or micro based of connector config
        return createLongWriteMapping((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            TimeStampMilliVector dataVector = new TimeStampMilliVector(field, new RootAllocator());
            dataVector.setSafe(0, unpackMillisUtc(value));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        });
    }
    public static WriteMapping uuidWriteMapping()
    {
        return createSliceWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            VarCharVector dataVector = new VarCharVector(field, new RootAllocator());
            dataVector.setSafe(0, value.toString().getBytes());
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static WriteMapping timeWithTimeZoneWriteMapping()
    {
        return createLongWriteMapping((((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            TimeStampMilliTZVector dataVector = new TimeStampMilliTZVector(field, new RootAllocator());
            dataVector.setSafe(0, unpackMillisUtc(value));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        })));
    }

    public static WriteMapping timestampWithTimeZoneWriteMapping()
    {
        return createLongWriteMapping(((statement, index, value) -> {
            Field field = statement.getParameterSchema().getFields().get(index);
            TimeStampMilliTZVector dataVector = new TimeStampMilliTZVector(field, new RootAllocator());
            dataVector.setSafe(0, unpackMillisUtc(value));
            statement.setParameters(VectorSchemaRoot.of(dataVector));
        }));
    }

    public static Optional<WriteMapping> prestoTypeToWriteMapping(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Optional.of(booleanWriteMapping());
        }
        else if (type.equals(TINYINT)) {
            return Optional.of(tinyintWriteMapping());
        }
        else if (type.equals(SMALLINT)) {
            return Optional.of(smallintWriteMapping());
        }
        else if (type.equals(BIGINT)) {
            return Optional.of(bigintWriteMapping());
        }
        else if (type.equals(DOUBLE)) {
            return Optional.of(doubleWriteMapping());
        }
        else if (type.equals(INTEGER)) {
            return Optional.of(integerWriteMapping());
        }
        else if (type.equals(REAL)) {
            return Optional.of(realWriteMapping());
        }
        else if (type instanceof DecimalType) {
            return Optional.of(decimalWriteMapping((DecimalType) type));
        }
        else if (type instanceof CharType || type instanceof VarcharType) {
            return Optional.of(charWriteMapping());
        }
        else if (type.equals(VARBINARY)) {
            return Optional.of(varbinaryWriteMapping());
        }
        else if (type instanceof DateType) {
            return Optional.of(dateWriteMapping());
        }
        else if (type instanceof TimestampType) {
            return Optional.of(timestampWriteMapping((TimestampType) type));
        }
        else if (type.equals(TIME)) {
            return Optional.of(timeWriteMapping());
        }
        else if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Optional.of(timeWithTimeZoneWriteMapping());
        }
        else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Optional.of(timestampWithTimeZoneWriteMapping());
        }
        else if (type.equals(UUID)) {
            return Optional.of(uuidWriteMapping());
        }
        return Optional.empty();
    }

    public static Optional<WriteMapping> getWriteMappingForAccumulators(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Optional.of(booleanWriteMapping());
        }
        else if (type.equals(TINYINT)) {
            return Optional.of(tinyintWriteMapping());
        }
        else if (type.equals(SMALLINT)) {
            return Optional.of(smallintWriteMapping());
        }
        else if (type.equals(INTEGER)) {
            return Optional.of(integerWriteMapping());
        }
        else if (type.equals(BIGINT)) {
            return Optional.of(bigintWriteMapping());
        }
        else if (type.equals(REAL)) {
            return Optional.of(realWriteMapping());
        }
        else if (type.equals(DOUBLE)) {
            return Optional.of(doubleWriteMapping());
        }
        else if (type instanceof CharType || type instanceof VarcharType) {
            return Optional.of(charWriteMapping());
        }
        else if (type instanceof DecimalType) {
            return Optional.of(decimalWriteMapping((DecimalType) type));
        }
        else if (type.equals(DateType.DATE)) {
            return Optional.of(dateWriteMapping());
        }
        else if (type.equals(TIME)) {
            return Optional.of(timeWriteMapping());
        }
        else if (type.equals(TIMESTAMP)) {
            return Optional.of(timestampWriteMapping((TimestampType) type));
        }
        else if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Optional.of(timeWithTimeZoneWriteMapping());
        }
        else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Optional.of(timestampWithTimeZoneWriteMapping());
        }
        else if (type instanceof UuidType) {
            return Optional.of(uuidWriteMapping());
        }
        return Optional.empty();
    }
}
