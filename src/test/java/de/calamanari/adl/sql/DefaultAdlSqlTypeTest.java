//@formatter:off
/*
 * DefaultAdlSqlTypeTest
 * Copyright 2024 Karl Eilebrecht
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"):
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//@formatter:on

package de.calamanari.adl.sql;

import static de.calamanari.adl.cnv.tps.DefaultAdlType.BOOL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DATE;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DECIMAL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.INTEGER;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.STRING;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIGINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BOOLEAN;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_CHAR;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DATE;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DECIMAL;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DOUBLE;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_FLOAT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_INTEGER;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_LONGNVARCHAR;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_LONGVARCHAR;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_NCHAR;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_NUMERIC;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_NVARCHAR;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_REAL;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_SMALLINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TIMESTAMP;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TINYINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.DeepCopyUtils;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.cnv.tps.DefaultArgValueFormatter;
import de.calamanari.adl.cnv.tps.NativeTypeCaster;
import de.calamanari.adl.cnv.tps.PassThroughTypeCaster;
import de.calamanari.adl.irl.MatchOperator;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultAdlSqlTypeTest {

    static final Logger LOGGER = LoggerFactory.getLogger(DefaultAdlSqlTypeTest.class);

    @Test
    void testBasics() {
        AdlSqlType type1 = DefaultAdlSqlType.SQL_BOOLEAN;

        AdlSqlType type2 = DefaultAdlSqlType.SQL_BOOLEAN.withFormatter(DefaultArgValueFormatter::formatBool);

        assertNotEquals(type1, type2);
        assertNotEquals(type1.name(), type2.name());

        assertEquals(type1, type2.getBaseType());

        type2 = DefaultAdlSqlType.SQL_BOOLEAN.withFormatter("SQL_BOOLEAN", DefaultArgValueFormatter::formatBool);

        assertNotEquals(type1, type2);
        assertEquals(type1.name(), type2.name());

        type1 = DefaultAdlSqlType.SQL_BIGINT;

        type2 = DefaultAdlSqlType.SQL_BIGINT.withNativeTypeCaster(PassThroughTypeCaster.getInstance());

        assertNotEquals(type1, type2);
        assertNotEquals(type1.name(), type2.name());

        assertEquals(type1, type2.getBaseType());

        type2 = DefaultAdlSqlType.SQL_BIGINT.withNativeTypeCaster("SQL_BIGINT", PassThroughTypeCaster.getInstance());

        assertNotEquals(type1, type2);
        assertEquals(type1.name(), type2.name());

        type1 = DefaultAdlSqlType.SQL_VARCHAR;

        final AtomicInteger idSequence = new AtomicInteger(1000);

        QueryParameterCreator qpc = new QueryParameterCreator() {

            private static final long serialVersionUID = -4738316672211439079L;

            @Override
            public QueryParameter createParameter(String id, ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType) {
                return new DefaultQueryParameter(id, DefaultAdlSqlType.SQL_VARCHAR, argValue, matchOperator);
            }

            @Override
            public boolean isTypeCombinationSupported(AdlType sourceType, AdlSqlType targetType) {
                return true;
            }

            @Override
            public QueryParameter createParameter(ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType) {
                return createParameter("P_" + idSequence.incrementAndGet(), argMetaInfo, argValue, matchOperator, adlSqlType);
            }

        };

        type2 = DefaultAdlSqlType.SQL_VARCHAR.withQueryParameterCreator(qpc);

        assertNotEquals(type1, type2);
        assertNotEquals(type1.name(), type2.name());

        assertEquals(type1, type2.getBaseType());

        type2 = DefaultAdlSqlType.SQL_VARCHAR.withQueryParameterCreator("SQL_VARCHAR", qpc);

        assertNotEquals(type1, type2);
        assertEquals(type1.name(), type2.name());

        QueryParameterApplicator qpa = new QueryParameterApplicator() {

            private static final long serialVersionUID = -6688306052201625015L;

            @Override
            public void apply(PreparedStatement stmt, QueryParameter parameter, int parameterIndex) throws SQLException {
                DefaultQueryParameterApplicator.getInstance().apply(stmt, parameter, parameterIndex);
            }

            @Override
            public void applyUnsafe(StringBuilder sb, QueryParameter parameter, int parameterIndex) {
                DefaultQueryParameterApplicator.getInstance().applyUnsafe(sb, parameter, parameterIndex);
            }

        };

        type2 = DefaultAdlSqlType.SQL_VARCHAR.withQueryParameterApplicator(qpa);

        assertNotEquals(type1, type2);
        assertNotEquals(type1.name(), type2.name());

        assertEquals(type1, type2.getBaseType());

        type2 = DefaultAdlSqlType.SQL_VARCHAR.withQueryParameterApplicator("SQL_VARCHAR", qpa);

        assertNotEquals(type1, type2);
        assertEquals(type1.name(), type2.name());

    }

    @Test
    void testMatchSqlTypes() {
        Map<String, Integer> expectedTypeIdMap = new HashMap<>();
        try {
            Field[] fields = Types.class.getFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) && field.getType() == int.class) {
                    expectedTypeIdMap.put("SQL_" + field.getName(), field.getInt(null));
                }
            }
        }
        catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
        for (DefaultAdlSqlType type : DefaultAdlSqlType.values()) {
            assertEquals(type.getJavaSqlType(), expectedTypeIdMap.get(type.name()));
        }
    }

    @Test
    void testStandardBehavior() {
        for (DefaultAdlSqlType type : DefaultAdlSqlType.values()) {
            assertEquals(expectedFormatter(type), type.getFormatter());
            assertEquals(expectedCoType(type).supportsContains(), type.supportsContains());
            assertEquals(expectedCoType(type).supportsLessThanGreaterThan(), type.supportsLessThanGreaterThan());
            assertEquals(DefaultAdlSqlType.DUMMY_TYPE_CASTER, type.getNativeTypeCaster());
            assertEquals(DefaultQueryParameterCreator.getInstance(), type.getQueryParameterCreator());

        }

    }

    private static ArgValueFormatter expectedFormatter(AdlSqlType adlSqlType) {
        switch (adlSqlType) {
        case DefaultAdlSqlType.SQL_BIT:
            return DefaultSqlFormatter.SQL_BIT;
        case DefaultAdlSqlType.SQL_BOOLEAN:
            return DefaultSqlFormatter.SQL_BOOLEAN;
        case DefaultAdlSqlType.SQL_BIGINT:
            return DefaultSqlFormatter.SQL_BIGINT;
        case DefaultAdlSqlType.SQL_INTEGER:
            return DefaultSqlFormatter.SQL_INTEGER;
        case DefaultAdlSqlType.SQL_SMALLINT:
            return DefaultSqlFormatter.SQL_SMALLINT;
        case DefaultAdlSqlType.SQL_TINYINT:
            return DefaultSqlFormatter.SQL_TINYINT;
        case DefaultAdlSqlType.SQL_CHAR:
            return DefaultSqlFormatter.SQL_CHAR;
        case DefaultAdlSqlType.SQL_LONGNVARCHAR:
            return DefaultSqlFormatter.SQL_LONGNVARCHAR;
        case DefaultAdlSqlType.SQL_LONGVARCHAR:
            return DefaultSqlFormatter.SQL_LONGVARCHAR;
        case DefaultAdlSqlType.SQL_NCHAR:
            return DefaultSqlFormatter.SQL_NCHAR;
        case DefaultAdlSqlType.SQL_NVARCHAR:
            return DefaultSqlFormatter.SQL_NVARCHAR;
        case DefaultAdlSqlType.SQL_VARCHAR:
            return DefaultSqlFormatter.SQL_VARCHAR;
        case DefaultAdlSqlType.SQL_DATE:
            return DefaultSqlFormatter.SQL_DATE_DEFAULT;
        case DefaultAdlSqlType.SQL_TIMESTAMP:
            return DefaultSqlFormatter.SQL_TIMESTAMP_DEFAULT;
        case DefaultAdlSqlType.SQL_DECIMAL:
            return DefaultSqlFormatter.SQL_DECIMAL;
        case DefaultAdlSqlType.SQL_DOUBLE:
            return DefaultSqlFormatter.SQL_DOUBLE;
        case DefaultAdlSqlType.SQL_FLOAT:
            return DefaultSqlFormatter.SQL_FLOAT;
        case DefaultAdlSqlType.SQL_NUMERIC:
            return DefaultSqlFormatter.SQL_NUMERIC;
        case DefaultAdlSqlType.SQL_REAL:
            return DefaultSqlFormatter.SQL_REAL;
        default:
            throw new IllegalStateException("Unknown new AdlSqlType: " + adlSqlType);
        }

    }

    private static AdlType expectedCoType(AdlSqlType adlSqlType) {
        switch (adlSqlType) {
        case DefaultAdlSqlType.SQL_BIT, DefaultAdlSqlType.SQL_BOOLEAN:
            return adlSqlType;
        case DefaultAdlSqlType.SQL_BIGINT, DefaultAdlSqlType.SQL_INTEGER, DefaultAdlSqlType.SQL_SMALLINT, DefaultAdlSqlType.SQL_TINYINT:
            return DefaultAdlType.INTEGER;
        case DefaultAdlSqlType.SQL_CHAR, DefaultAdlSqlType.SQL_LONGNVARCHAR, DefaultAdlSqlType.SQL_LONGVARCHAR, DefaultAdlSqlType.SQL_NCHAR, DefaultAdlSqlType.SQL_NVARCHAR, DefaultAdlSqlType.SQL_VARCHAR:
            return DefaultAdlType.STRING;
        case DefaultAdlSqlType.SQL_DATE, DefaultAdlSqlType.SQL_TIMESTAMP:
            return DefaultAdlType.DATE;
        case DefaultAdlSqlType.SQL_DECIMAL, DefaultAdlSqlType.SQL_DOUBLE, DefaultAdlSqlType.SQL_FLOAT, DefaultAdlSqlType.SQL_NUMERIC, DefaultAdlSqlType.SQL_REAL:
            return DefaultAdlType.DECIMAL;
        default:
            throw new IllegalStateException("Unknown new AdlSqlType: " + adlSqlType);
        }

    }

    @Test
    void testCompatibility1() {

        assertTrue(SQL_BIT.isCompatibleWith(BOOL));
        assertTrue(SQL_BIT.isCompatibleWith(INTEGER));
        assertTrue(SQL_BIT.isCompatibleWith(STRING));
        assertFalse(SQL_BIT.isCompatibleWith(DATE));
        assertFalse(SQL_BIT.isCompatibleWith(DECIMAL));
        assertFalse(SQL_BIT.isCompatibleWith(SQL_BIT));

        assertTrue(SQL_BIGINT.isCompatibleWith(BOOL));
        assertTrue(SQL_BIGINT.isCompatibleWith(INTEGER));
        assertTrue(SQL_BIGINT.isCompatibleWith(STRING));
        assertTrue(SQL_BIGINT.isCompatibleWith(DATE));
        assertTrue(SQL_BIGINT.isCompatibleWith(DECIMAL));
        assertFalse(SQL_BIGINT.isCompatibleWith(SQL_BIGINT));

        assertTrue(SQL_BOOLEAN.isCompatibleWith(BOOL));
        assertTrue(SQL_BOOLEAN.isCompatibleWith(INTEGER));
        assertTrue(SQL_BOOLEAN.isCompatibleWith(STRING));
        assertFalse(SQL_BOOLEAN.isCompatibleWith(DATE));
        assertFalse(SQL_BOOLEAN.isCompatibleWith(DECIMAL));
        assertFalse(SQL_BOOLEAN.isCompatibleWith(SQL_BOOLEAN));

        assertTrue(SQL_CHAR.isCompatibleWith(BOOL));
        assertTrue(SQL_CHAR.isCompatibleWith(INTEGER));
        assertTrue(SQL_CHAR.isCompatibleWith(STRING));
        assertTrue(SQL_CHAR.isCompatibleWith(DATE));
        assertTrue(SQL_CHAR.isCompatibleWith(DECIMAL));
        assertFalse(SQL_CHAR.isCompatibleWith(SQL_CHAR));
    }

    @Test
    void testCompatibility2() {

        assertFalse(SQL_DATE.isCompatibleWith(BOOL));
        assertTrue(SQL_DATE.isCompatibleWith(INTEGER));
        assertTrue(SQL_DATE.isCompatibleWith(STRING));
        assertTrue(SQL_DATE.isCompatibleWith(DATE));
        assertTrue(SQL_DATE.isCompatibleWith(DECIMAL));
        assertFalse(SQL_DATE.isCompatibleWith(SQL_DATE));

        assertFalse(SQL_DECIMAL.isCompatibleWith(BOOL));
        assertTrue(SQL_DECIMAL.isCompatibleWith(INTEGER));
        assertTrue(SQL_DECIMAL.isCompatibleWith(STRING));
        assertTrue(SQL_DECIMAL.isCompatibleWith(DATE));
        assertTrue(SQL_DECIMAL.isCompatibleWith(DECIMAL));
        assertFalse(SQL_DECIMAL.isCompatibleWith(SQL_DECIMAL));

        assertFalse(SQL_DOUBLE.isCompatibleWith(BOOL));
        assertTrue(SQL_DOUBLE.isCompatibleWith(INTEGER));
        assertTrue(SQL_DOUBLE.isCompatibleWith(STRING));
        assertTrue(SQL_DOUBLE.isCompatibleWith(DATE));
        assertTrue(SQL_DOUBLE.isCompatibleWith(DECIMAL));
        assertFalse(SQL_DOUBLE.isCompatibleWith(SQL_DOUBLE));

        assertFalse(SQL_FLOAT.isCompatibleWith(BOOL));
        assertTrue(SQL_FLOAT.isCompatibleWith(INTEGER));
        assertTrue(SQL_FLOAT.isCompatibleWith(STRING));
        assertFalse(SQL_FLOAT.isCompatibleWith(DATE));
        assertTrue(SQL_FLOAT.isCompatibleWith(DECIMAL));
        assertFalse(SQL_FLOAT.isCompatibleWith(SQL_FLOAT));
    }

    @Test
    void testCompatibility3() {

        assertTrue(SQL_INTEGER.isCompatibleWith(BOOL));
        assertTrue(SQL_INTEGER.isCompatibleWith(INTEGER));
        assertTrue(SQL_INTEGER.isCompatibleWith(STRING));
        assertTrue(SQL_INTEGER.isCompatibleWith(DATE));
        assertTrue(SQL_INTEGER.isCompatibleWith(DECIMAL));
        assertFalse(SQL_INTEGER.isCompatibleWith(SQL_INTEGER));

        assertTrue(SQL_LONGNVARCHAR.isCompatibleWith(BOOL));
        assertTrue(SQL_LONGNVARCHAR.isCompatibleWith(INTEGER));
        assertTrue(SQL_LONGNVARCHAR.isCompatibleWith(STRING));
        assertTrue(SQL_LONGNVARCHAR.isCompatibleWith(DATE));
        assertTrue(SQL_LONGNVARCHAR.isCompatibleWith(DECIMAL));
        assertFalse(SQL_LONGNVARCHAR.isCompatibleWith(SQL_LONGNVARCHAR));

        assertTrue(SQL_LONGVARCHAR.isCompatibleWith(BOOL));
        assertTrue(SQL_LONGVARCHAR.isCompatibleWith(INTEGER));
        assertTrue(SQL_LONGVARCHAR.isCompatibleWith(STRING));
        assertTrue(SQL_LONGVARCHAR.isCompatibleWith(DATE));
        assertTrue(SQL_LONGVARCHAR.isCompatibleWith(DECIMAL));
        assertFalse(SQL_LONGVARCHAR.isCompatibleWith(SQL_LONGVARCHAR));

        assertTrue(SQL_NCHAR.isCompatibleWith(BOOL));
        assertTrue(SQL_NCHAR.isCompatibleWith(INTEGER));
        assertTrue(SQL_NCHAR.isCompatibleWith(STRING));
        assertTrue(SQL_NCHAR.isCompatibleWith(DATE));
        assertTrue(SQL_NCHAR.isCompatibleWith(DECIMAL));
        assertFalse(SQL_NCHAR.isCompatibleWith(SQL_NCHAR));
    }

    @Test
    void testCompatibility4() {

        assertFalse(SQL_NUMERIC.isCompatibleWith(BOOL));
        assertTrue(SQL_NUMERIC.isCompatibleWith(INTEGER));
        assertTrue(SQL_NUMERIC.isCompatibleWith(STRING));
        assertTrue(SQL_NUMERIC.isCompatibleWith(DATE));
        assertTrue(SQL_NUMERIC.isCompatibleWith(DECIMAL));
        assertFalse(SQL_NUMERIC.isCompatibleWith(SQL_NUMERIC));

        assertTrue(SQL_NVARCHAR.isCompatibleWith(BOOL));
        assertTrue(SQL_NVARCHAR.isCompatibleWith(INTEGER));
        assertTrue(SQL_NVARCHAR.isCompatibleWith(STRING));
        assertTrue(SQL_NVARCHAR.isCompatibleWith(DATE));
        assertTrue(SQL_NVARCHAR.isCompatibleWith(DECIMAL));
        assertFalse(SQL_NVARCHAR.isCompatibleWith(SQL_NVARCHAR));

        assertFalse(SQL_REAL.isCompatibleWith(BOOL));
        assertTrue(SQL_REAL.isCompatibleWith(INTEGER));
        assertTrue(SQL_REAL.isCompatibleWith(STRING));
        assertTrue(SQL_REAL.isCompatibleWith(DATE));
        assertTrue(SQL_REAL.isCompatibleWith(DECIMAL));
        assertFalse(SQL_REAL.isCompatibleWith(SQL_REAL));

        assertTrue(SQL_SMALLINT.isCompatibleWith(BOOL));
        assertTrue(SQL_SMALLINT.isCompatibleWith(INTEGER));
        assertTrue(SQL_SMALLINT.isCompatibleWith(STRING));
        assertFalse(SQL_SMALLINT.isCompatibleWith(DATE));
        assertTrue(SQL_SMALLINT.isCompatibleWith(DECIMAL));
        assertFalse(SQL_SMALLINT.isCompatibleWith(SQL_SMALLINT));
    }

    @Test
    void testCompatibility5() {

        assertFalse(SQL_TIMESTAMP.isCompatibleWith(BOOL));
        assertTrue(SQL_TIMESTAMP.isCompatibleWith(INTEGER));
        assertTrue(SQL_TIMESTAMP.isCompatibleWith(STRING));
        assertTrue(SQL_TIMESTAMP.isCompatibleWith(DATE));
        assertTrue(SQL_TIMESTAMP.isCompatibleWith(DECIMAL));
        assertFalse(SQL_TIMESTAMP.isCompatibleWith(SQL_TIMESTAMP));

        assertTrue(SQL_TINYINT.isCompatibleWith(BOOL));
        assertTrue(SQL_TINYINT.isCompatibleWith(INTEGER));
        assertTrue(SQL_TINYINT.isCompatibleWith(STRING));
        assertFalse(SQL_TINYINT.isCompatibleWith(DATE));
        assertTrue(SQL_TINYINT.isCompatibleWith(DECIMAL));
        assertFalse(SQL_TINYINT.isCompatibleWith(SQL_TINYINT));

        assertTrue(SQL_VARCHAR.isCompatibleWith(BOOL));
        assertTrue(SQL_VARCHAR.isCompatibleWith(INTEGER));
        assertTrue(SQL_VARCHAR.isCompatibleWith(STRING));
        assertTrue(SQL_VARCHAR.isCompatibleWith(DATE));
        assertTrue(SQL_VARCHAR.isCompatibleWith(DECIMAL));
        assertFalse(SQL_VARCHAR.isCompatibleWith(SQL_VARCHAR));
    }

    @Test
    void testDecoration() {

        ArgValueFormatter formatter = new ArgValueFormatter() {

            private static final long serialVersionUID = 8169081572229587967L;

            @Override
            public String format(String argName, String argValue, MatchOperator operator) {
                return argValue;
            }
        };

        assertDerived(SQL_INTEGER, SQL_INTEGER.withFormatter(formatter));
        assertDerived(SQL_INTEGER, SQL_INTEGER.withFormatter("SQL_INTEGER-new", formatter));
        assertEquals("SQL_INTEGER-new2", SQL_INTEGER.withFormatter("SQL_INTEGER-new2", formatter).name());
        assertEquals(formatter, SQL_INTEGER.withFormatter("SQL_INTEGER-new2", formatter).getFormatter());

        NativeTypeCaster ntc = new NativeTypeCaster() {

            private static final long serialVersionUID = -3716335076800837380L;

            @Override
            public String formatNativeTypeCast(String argName, String nativeFieldName, AdlType argType, AdlType requestedArgType) {
                return "bla";
            }

        };

        assertDerived(SQL_INTEGER, SQL_INTEGER.withNativeTypeCaster(ntc));
        assertDerived(SQL_INTEGER, SQL_INTEGER.withNativeTypeCaster("SQL_INTEGER-new", ntc));
        assertEquals("SQL_INTEGER-new2", SQL_INTEGER.withNativeTypeCaster("SQL_INTEGER-new2", ntc).name());
        assertEquals(ntc, SQL_INTEGER.withNativeTypeCaster("SQL_INTEGER-new2", ntc).getNativeTypeCaster());

        QueryParameterApplicator qpa = new QueryParameterApplicator() {

            private static final long serialVersionUID = 3332122230895749573L;

            @Override
            public void applyUnsafe(StringBuilder sb, QueryParameter parameter, int parameterIndex) {
                // test

            }

            @Override
            public void apply(PreparedStatement stmt, QueryParameter parameter, int parameterIndex) throws SQLException {
                // test

            }
        };

        assertDerived(SQL_INTEGER, SQL_INTEGER.withQueryParameterApplicator(qpa));
        assertDerived(SQL_INTEGER, SQL_INTEGER.withQueryParameterApplicator("SQL_INTEGER-new", qpa));
        assertEquals("SQL_INTEGER-new2", SQL_INTEGER.withQueryParameterApplicator("SQL_INTEGER-new2", qpa).name());
        assertEquals(qpa, SQL_INTEGER.withQueryParameterApplicator("SQL_INTEGER-new2", qpa).getQueryParameterApplicator());

        final AtomicInteger idSequence = new AtomicInteger(1000);

        QueryParameterCreator qpc = new QueryParameterCreator() {

            private static final long serialVersionUID = -3836524296963736662L;

            @Override
            public boolean isTypeCombinationSupported(AdlType sourceType, AdlSqlType targetType) {
                return false;
            }

            @Override
            public QueryParameter createParameter(String id, ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType) {
                return null;
            }

            @Override
            public QueryParameter createParameter(ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType) {
                return createParameter("P_" + idSequence.incrementAndGet(), argMetaInfo, argValue, matchOperator, adlSqlType);
            }
        };

        assertDerived(SQL_INTEGER, SQL_INTEGER.withQueryParameterCreator(qpc));
        assertDerived(SQL_INTEGER, SQL_INTEGER.withQueryParameterCreator("SQL_INTEGER-new", qpc));
        assertEquals("SQL_INTEGER-new2", SQL_INTEGER.withQueryParameterCreator("SQL_INTEGER-new2", qpc).name());
        assertEquals(qpc, SQL_INTEGER.withQueryParameterCreator("SQL_INTEGER-new2", qpc).getQueryParameterCreator());

        AdlSqlType customType = SQL_INTEGER.withFormatter(formatter);

        assertDerived(SQL_INTEGER, customType.withQueryParameterCreator(qpc));
        assertDerived(SQL_INTEGER, customType.withQueryParameterCreator("SQL_INTEGER-new", qpc));
        assertEquals("SQL_INTEGER-new2", customType.withQueryParameterCreator("SQL_INTEGER-new2", qpc).name());
        assertEquals(qpc, customType.withQueryParameterCreator("SQL_INTEGER-new2", qpc).getQueryParameterCreator());

    }

    @Test
    void testSerializableCaster() {

        NativeTypeCaster cs = SQL_INTEGER.getNativeTypeCaster();

        NativeTypeCaster cs2 = DeepCopyUtils.deepCopy(cs);

        assertSame(cs, cs2);

    }

    private static void assertDerived(AdlSqlType baseType, AdlSqlType customType) {

        assertNotEquals(baseType, customType);
        assertNotEquals(baseType.toString(), customType.toString());
        assertEquals(baseType, customType.getBaseType());
        assertTrue(customType.name().startsWith(baseType.name()));

        assertEquals(baseType.supportsContains(), customType.supportsContains());
        assertEquals(baseType.supportsLessThanGreaterThan(), customType.supportsLessThanGreaterThan());
        assertEquals(baseType.getJavaSqlType(), customType.getJavaSqlType());

    }

}
