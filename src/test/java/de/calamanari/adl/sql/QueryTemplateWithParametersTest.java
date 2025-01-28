//@formatter:off
/*
 * QueryTemplateWithParametersTest
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.irl.MatchOperator;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
@ExtendWith(MockitoExtension.class)
class QueryTemplateWithParametersTest {

    static final Logger LOGGER = LoggerFactory.getLogger(QueryTemplateWithParametersTest.class);

    private static final String ALL_SQL_TYPES_TEMPLATE = """
            select id from my_table
            where colBit = ${P_BIT}
              and colBigInt = ${P_BIG_INT}
              and colBoolean = ${P_BOOL}
              and colChar = ${P_CHAR}
              and colDate = ${P_DATE}
              and colDecimal = ${P_DECIMAL}
              and colDouble = ${P_DOUBLE}
              and colFloat = ${P_FLOAT}
              and colInteger = ${P_INTEGER}
              and colLongNVarChar = ${P_LONGNVARCHAR}
              and colLongVarChar = ${P_LONGVARCHAR}
              and colNChar = ${P_NCHAR}
              and colNumeric = ${P_NUMERIC}
              and colNVarChar = ${P_NVARCHAR}
              and colReal = ${P_REAL}
              and colSmallInt = ${P_SMALLINT}
              and colTimestamp = ${P_TIMESTAMP}
              and colTinyInt = ${P_TINYINT}
              and colVarChar = ${P_VARCHAR}
              and colVarChar2 = ${P_VARCHAR}
            """;

    private AtomicInteger paramCounter = new AtomicInteger();

    private StringBuilder sb = new StringBuilder();

    private void init() {
        paramCounter.set(0);
        sb.setLength(0);
    }

    @Test
    void testBasics() {
        String qmTemplate = "select id from my_table where col1=? and col2=?";

        init();
        QueryTemplateWithParameters qtwp = new QueryTemplateWithParameters(qmTemplate, Arrays.asList(sParam("foo"), sParam("bar")), Arrays.asList(35, 46));
        qtwp.applyUnsafe(sb);
        assertEquals("select id from my_table where col1='foo' and col2='bar'", sb.toString());
        assertEquals("select id from my_table where col1='foo' and col2='bar'", qtwp.toDebugString());

        init();

        qtwp = new QueryTemplateWithParameters("", Collections.emptyList(), Collections.emptyList());
        qtwp.applyUnsafe(sb);
        assertEquals("", sb.toString());
        assertEquals("", qtwp.toDebugString());

        init();

        qtwp = new QueryTemplateWithParameters("bla", Collections.emptyList(), Collections.emptyList());
        qtwp.applyUnsafe(sb);
        assertEquals("bla", sb.toString());
        assertEquals("bla", qtwp.toDebugString());

        init();
        QueryTemplateWithParameters qtwpNull = new QueryTemplateWithParameters(qmTemplate, Arrays.asList(sParam(null), sParam("bar")), Arrays.asList(35, 46));
        qtwpNull.applyUnsafe(sb);
        assertEquals("select id from my_table where col1=NULL and col2='bar'", sb.toString());
        assertEquals("select id from my_table where col1=NULL and col2='bar'", qtwpNull.toDebugString());

    }

    @Test
    void testOf() {
        String namedParamsTemplate = "select id from my_table where col1=${P1} and col2=${P2}";

        List<QueryParameter> params = Arrays.asList(sParam("P2", "bar"), sParam("P1", "foo"));

        QueryTemplateWithParameters qtwp = QueryTemplateWithParameters.of(namedParamsTemplate, params);
        qtwp.applyUnsafe(sb);
        assertEquals("select id from my_table where col1='foo' and col2='bar'", sb.toString());
        assertEquals("select id from my_table where col1='foo' and col2='bar'", qtwp.toDebugString());

        init();

        namedParamsTemplate = "${P2}";

        params = Arrays.asList(sParam("P2", "bar"));

        qtwp = QueryTemplateWithParameters.of(namedParamsTemplate, params);
        qtwp.applyUnsafe(sb);
        assertEquals("'bar'", sb.toString());
        assertEquals("'bar'", qtwp.toDebugString());

    }

    @Test
    void testSpecialCase() {
        String qmTemplate = "select id from my_table where col1=? and col2=?";

        init();

        List<QueryParameter> validParams = Arrays.asList(sParam("foo"), sParam("bar"));

        List<Integer> validPositions = Arrays.asList(35, 46);

        List<Integer> invalidPositions1 = validPositions.subList(0, 1);

        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, validParams, invalidPositions1));

        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, validParams, null));

        List<Integer> invalidPositions2 = Arrays.asList(35, 47);
        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, validParams, invalidPositions2));

        List<Integer> invalidPositions3 = Arrays.asList(35, 45);
        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, validParams, invalidPositions3));

        List<Integer> invalidPositions4 = Arrays.asList(-1, 46);
        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, validParams, invalidPositions4));

        List<Integer> invalidPositions5 = Arrays.asList(35, null, 46);
        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, validParams, invalidPositions5));

        List<QueryParameter> invalidParams1 = validParams.subList(0, 1);
        List<QueryParameter> invalidParams2 = Arrays.asList(sParam("foo"), null, sParam("bar"));
        QueryParameter duplicateParam = sParam("bar");
        List<QueryParameter> invalidParams3 = Arrays.asList(sParam("foo"), null, duplicateParam, duplicateParam);

        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, invalidParams1, validPositions));
        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, null, validPositions));
        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(qmTemplate, invalidParams2, validPositions));
        assertThrows(IllegalArgumentException.class, () -> QueryTemplateWithParameters.of(qmTemplate, invalidParams2));
        assertThrows(IllegalArgumentException.class, () -> QueryTemplateWithParameters.of(qmTemplate, invalidParams3));

        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateWithParameters(null, validParams, validPositions));

        assertThrows(QueryPreparationException.class,
                () -> QueryTemplateWithParameters.of("select id from my_table where col1=${P1} and col2=${P2}", invalidParams1));

        assertThrows(IllegalArgumentException.class, () -> QueryTemplateWithParameters.of("select id from my_table where col1=${P1} and col2=${P2}", null));

        init();

        assertThrows(AdlFormattingException.class, () -> iParam("P2", "foo"));

    }

    private List<QueryParameter> createAllSqlTypesParams(boolean withDecimals) {
        List<QueryParameter> params = new ArrayList<>();
        params.add(param("P_BIT", DefaultAdlType.BOOL, "1", DefaultAdlSqlType.SQL_BIT));
        params.add(param("P_BOOL", DefaultAdlType.BOOL, "0", DefaultAdlSqlType.SQL_BOOLEAN));
        params.add(param("P_BIG_INT", DefaultAdlType.INTEGER, "13424", DefaultAdlSqlType.SQL_BIGINT));
        params.add(param("P_INTEGER", DefaultAdlType.INTEGER, "97834", DefaultAdlSqlType.SQL_INTEGER));
        params.add(param("P_SMALLINT", DefaultAdlType.INTEGER, "1254", DefaultAdlSqlType.SQL_SMALLINT));
        params.add(param("P_TINYINT", DefaultAdlType.INTEGER, "212", DefaultAdlSqlType.SQL_TINYINT));
        params.add(param("P_DATE", DefaultAdlType.DATE, "2024-03-04", DefaultAdlSqlType.SQL_DATE));
        params.add(param("P_TIMESTAMP", DefaultAdlType.DATE, "2024-03-04", DefaultAdlSqlType.SQL_TIMESTAMP));
        params.add(param("P_CHAR", DefaultAdlType.STRING, "The quick brown fox jumped over the lazy dog.", DefaultAdlSqlType.SQL_CHAR));
        params.add(param("P_LONGNVARCHAR", DefaultAdlType.STRING, "The quick brown fox jumped over the lazy dog.", DefaultAdlSqlType.SQL_LONGNVARCHAR));
        params.add(param("P_LONGVARCHAR", DefaultAdlType.STRING, "The quick brown fox jumped over the lazy dog.", DefaultAdlSqlType.SQL_LONGVARCHAR));
        params.add(param("P_NCHAR", DefaultAdlType.STRING, "The quick brown fox jumped over the lazy dog.", DefaultAdlSqlType.SQL_NCHAR));
        params.add(param("P_NVARCHAR", DefaultAdlType.STRING, "The quick brown fox jumped over the lazy dog.", DefaultAdlSqlType.SQL_NVARCHAR));
        params.add(param("P_VARCHAR", DefaultAdlType.STRING, "The quick brown fox jumped over the lazy dog.", DefaultAdlSqlType.SQL_VARCHAR));
        if (withDecimals) {
            params.add(param("P_DECIMAL", DefaultAdlType.DECIMAL, "97834.7759871", DefaultAdlSqlType.SQL_DECIMAL));
            params.add(param("P_DOUBLE", DefaultAdlType.DECIMAL, "97111.7756543", DefaultAdlSqlType.SQL_DOUBLE));
            params.add(param("P_REAL", DefaultAdlType.DECIMAL, "56333.1234567", DefaultAdlSqlType.SQL_REAL));
            params.add(param("P_FLOAT", DefaultAdlType.DECIMAL, "6111.773", DefaultAdlSqlType.SQL_FLOAT));
            params.add(param("P_NUMERIC", DefaultAdlType.DECIMAL, "107832.1759877", DefaultAdlSqlType.SQL_NUMERIC));
        }
        else {
            params.add(param("P_DECIMAL", DefaultAdlType.DECIMAL, "97834.0", DefaultAdlSqlType.SQL_DECIMAL));
            params.add(param("P_DOUBLE", DefaultAdlType.DECIMAL, "97111.0", DefaultAdlSqlType.SQL_DOUBLE));
            params.add(param("P_REAL", DefaultAdlType.DECIMAL, "56333.0", DefaultAdlSqlType.SQL_REAL));
            params.add(param("P_FLOAT", DefaultAdlType.DECIMAL, "6111.0", DefaultAdlSqlType.SQL_FLOAT));
            params.add(param("P_NUMERIC", DefaultAdlType.DECIMAL, "107832.0", DefaultAdlSqlType.SQL_NUMERIC));
        }
        return params;
    }

    @Test
    void testApplyUnsafe() {

        init();

        QueryTemplateWithParameters qtwp = QueryTemplateWithParameters.of(ALL_SQL_TYPES_TEMPLATE, createAllSqlTypesParams(true));

        qtwp.applyUnsafe(sb);

        String res = sb.toString();
        assertTrue(res.contains("colBit = 1"));
        assertTrue(res.contains("colBigInt = 13424"));
        assertTrue(res.contains("colBoolean = FALSE"));
        assertTrue(res.contains("colChar = 'The quick brown fox jumped over the lazy dog.'"));
        assertTrue(res.contains("colDate = DATE '2024-03-04'"));
        assertTrue(res.contains("colDecimal = 97834.775987"));
        assertTrue(res.contains("colDouble = 97111.775654"));
        assertTrue(res.contains("colFloat = 6111.772"));
        assertTrue(res.contains("colInteger = 97834"));
        assertTrue(res.contains("colLongNVarChar = 'The quick brown fox jumped over the lazy dog.'"));
        assertTrue(res.contains("colLongVarChar = 'The quick brown fox jumped over the lazy dog.'"));
        assertTrue(res.contains("colNChar = 'The quick brown fox jumped over the lazy dog.'"));
        assertTrue(res.contains("colNumeric = 107832.175987"));
        assertTrue(res.contains("colNVarChar = 'The quick brown fox jumped over the lazy dog.'"));
        assertTrue(res.contains("colReal = 56333.123456"));
        assertTrue(res.contains("colSmallInt = 1254"));
        assertTrue(res.contains("colTimestamp = TIMESTAMP '2024-03-04 00:00:00'"));
        assertTrue(res.contains("colTinyInt = 212"));
        assertTrue(res.contains("colVarChar = 'The quick brown fox jumped over the lazy dog.'"));
        assertTrue(res.contains("colVarChar2 = 'The quick brown fox jumped over the lazy dog.'"));

    }

    @Test
    void testMissingParams() {

        init();
        List<QueryParameter> params = Collections.emptyList();
        assertThrows(QueryPreparationException.class, () -> QueryTemplateWithParameters.of(ALL_SQL_TYPES_TEMPLATE, params));
    }

    @Test
    @SuppressWarnings("resource")
    void testApply() throws SQLException, ParseException {

        init();

        PreparedStatement ps = mock(PreparedStatement.class);

        QueryTemplateWithParameters qtwp = QueryTemplateWithParameters.of(ALL_SQL_TYPES_TEMPLATE, createAllSqlTypesParams(false));

        qtwp.apply(ps);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date checkDate = new Date(sdf.parse("2024-03-04").getTime());
        Timestamp checkTs = new Timestamp(checkDate.getTime());

        verify(ps, times(1)).setBoolean(1, true);
        verify(ps, times(1)).setLong(2, 13424);
        verify(ps, times(1)).setBoolean(3, false);
        verify(ps, times(1)).setString(4, "The quick brown fox jumped over the lazy dog.");
        verify(ps, times(1)).setDate(5, checkDate);
        verify(ps, times(1)).setDouble(6, 97834.0d);
        verify(ps, times(1)).setDouble(7, 97111.0d);
        verify(ps, times(1)).setFloat(8, 6111.0f);
        verify(ps, times(1)).setInt(9, 97834);
        verify(ps, times(1)).setString(10, "The quick brown fox jumped over the lazy dog.");
        verify(ps, times(1)).setString(11, "The quick brown fox jumped over the lazy dog.");
        verify(ps, times(1)).setString(12, "The quick brown fox jumped over the lazy dog.");
        verify(ps, times(1)).setBigDecimal(13, BigDecimal.valueOf(107832d).setScale(7, RoundingMode.HALF_UP));
        verify(ps, times(1)).setString(14, "The quick brown fox jumped over the lazy dog.");
        verify(ps, times(1)).setDouble(15, 56333.0d);
        verify(ps, times(1)).setShort(16, (short) 1254);
        verify(ps, times(1)).setTimestamp(17, checkTs);
        verify(ps, times(1)).setByte(18, (byte) 212);
        verify(ps, times(1)).setString(19, "The quick brown fox jumped over the lazy dog.");
        verify(ps, times(1)).setString(20, "The quick brown fox jumped over the lazy dog.");

    }

    private QueryParameter sParam(String value) {
        int idx = paramCounter.incrementAndGet();
        return DefaultQueryParameterCreator.getInstance().createParameter("P_" + idx, new ArgMetaInfo("Arg" + idx, DefaultAdlType.STRING, false, false), value,
                MatchOperator.EQUALS, DefaultAdlSqlType.SQL_VARCHAR);
    }

    private QueryParameter sParam(String name, String value) {
        int idx = paramCounter.incrementAndGet();
        return DefaultQueryParameterCreator.getInstance().createParameter(name, new ArgMetaInfo("Arg" + idx, DefaultAdlType.STRING, false, false), value,
                MatchOperator.EQUALS, DefaultAdlSqlType.SQL_VARCHAR);
    }

    private QueryParameter iParam(String name, String value) {
        int idx = paramCounter.incrementAndGet();
        return DefaultQueryParameterCreator.getInstance().createParameter(name, new ArgMetaInfo("Arg" + idx, DefaultAdlType.STRING, false, false), value,
                MatchOperator.EQUALS, DefaultAdlSqlType.SQL_INTEGER);
    }

    private QueryParameter param(String name, AdlType type, String value, AdlSqlType sqlType) {
        int idx = paramCounter.incrementAndGet();
        return DefaultQueryParameterCreator.getInstance().createParameter(name, new ArgMetaInfo("Arg" + idx, type, false, false), value, MatchOperator.EQUALS,
                sqlType);
    }

}
