//@formatter:off
/*
 * DefaultQueryParameterTest
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.DeepCopyUtils;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.irl.MatchOperator;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
@ExtendWith(MockitoExtension.class)
class DefaultQueryParameterTest {

    static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryParameterTest.class);

    @Test
    void testBasics() {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, "foobar", MatchOperator.EQUALS);

        assertEquals("p1", param.id());
        assertEquals("${p1}", param.createReference());
        assertEquals(DefaultAdlSqlType.SQL_VARCHAR, param.adlSqlType());
        assertEquals("foobar", param.value());

    }

    @Test
    @SuppressWarnings("resource")
    void testBigInt() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIGINT, 17L, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIGINT, 0L, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIGINT, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setLong(1, 17L);
        verify(ps, times(1)).setLong(2, 0L);
        verify(ps, times(1)).setNull(3, Types.BIGINT);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIGINT, 0, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIGINT, 1.452d, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIGINT, "92336345", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testBit() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIT, true, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIT, false, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIT, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setBoolean(1, true);
        verify(ps, times(1)).setBoolean(2, false);
        verify(ps, times(1)).setNull(3, Types.BIT);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIT, 0, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIT, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BIT, "TRUE", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testBoolean() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BOOLEAN, true, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BOOLEAN, false, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BOOLEAN, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setBoolean(1, true);
        verify(ps, times(1)).setBoolean(2, false);
        verify(ps, times(1)).setNull(3, Types.BOOLEAN);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BOOLEAN, 0, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BOOLEAN, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_BOOLEAN, "TRUE", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testChar() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_CHAR, "foobar", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_CHAR, "", MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_CHAR, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setString(1, "foobar");
        verify(ps, times(1)).setString(2, "");
        verify(ps, times(1)).setNull(3, Types.CHAR);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_CHAR, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_CHAR, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_CHAR, true, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testLongNVarChar() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGNVARCHAR, "foobar", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGNVARCHAR, "", MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGNVARCHAR, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setString(1, "foobar");
        verify(ps, times(1)).setString(2, "");
        verify(ps, times(1)).setNull(3, Types.LONGNVARCHAR);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGNVARCHAR, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGNVARCHAR, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGNVARCHAR, true, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testLongVarChar() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, "foobar", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, "", MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setString(1, "foobar");
        verify(ps, times(1)).setString(2, "");
        verify(ps, times(1)).setNull(3, Types.LONGVARCHAR);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, false, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testNChar() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NCHAR, "foobar", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NCHAR, "", MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NCHAR, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setString(1, "foobar");
        verify(ps, times(1)).setString(2, "");
        verify(ps, times(1)).setNull(3, Types.NCHAR);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NCHAR, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NCHAR, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NCHAR, false, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testNVarChar() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NVARCHAR, "foobar", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NVARCHAR, "", MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NVARCHAR, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setString(1, "foobar");
        verify(ps, times(1)).setString(2, "");
        verify(ps, times(1)).setNull(3, Types.NVARCHAR);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NVARCHAR, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NVARCHAR, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NVARCHAR, false, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testVarChar() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, "foobar", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, "", MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setString(1, "foobar");
        verify(ps, times(1)).setString(2, "");
        verify(ps, times(1)).setNull(3, Types.VARCHAR);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_VARCHAR, true, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testDate() throws SQLException, ParseException {

        SimpleDateFormat nf = new SimpleDateFormat("yyyy-MM-dd");
        nf.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date testDate1 = new Date(nf.parse("2024-10-19").getTime());
        Date testDate2 = new Date(nf.parse("2021-11-23").getTime());

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DATE, testDate1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DATE, testDate2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DATE, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setDate(1, testDate1);
        verify(ps, times(1)).setDate(2, testDate2);
        verify(ps, times(1)).setNull(3, Types.DATE);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DATE, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DATE, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DATE, "2024-12-11", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testTimestamp() throws SQLException, ParseException {

        SimpleDateFormat nf = new SimpleDateFormat("yyyy-MM-dd");
        nf.setTimeZone(TimeZone.getTimeZone("UTC"));

        Timestamp timeStamp1 = new Timestamp(nf.parse("2024-10-19").getTime());
        Timestamp timeStamp2 = new Timestamp(nf.parse("2021-11-23").getTime());

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TIMESTAMP, timeStamp1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TIMESTAMP, timeStamp2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TIMESTAMP, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setTimestamp(1, timeStamp1);
        verify(ps, times(1)).setTimestamp(2, timeStamp2);
        verify(ps, times(1)).setNull(3, Types.TIMESTAMP);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TIMESTAMP, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TIMESTAMP, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TIMESTAMP, true, MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testDecimal() throws SQLException {

        double dValue1 = 8.89128;

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DECIMAL, dValue1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        double dValue2 = 0d;

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DECIMAL, dValue2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DECIMAL, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setDouble(1, dValue1);
        verify(ps, times(1)).setDouble(2, dValue2);
        verify(ps, times(1)).setNull(3, Types.DECIMAL);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DECIMAL, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DECIMAL, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DECIMAL, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testDouble() throws SQLException {

        double dValue1 = 8.89128;

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DOUBLE, dValue1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        double dValue2 = 0d;

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DOUBLE, dValue2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DOUBLE, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setDouble(1, dValue1);
        verify(ps, times(1)).setDouble(2, dValue2);
        verify(ps, times(1)).setNull(3, Types.DOUBLE);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DOUBLE, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DOUBLE, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_DOUBLE, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testReal() throws SQLException {

        double dValue1 = 8.89128;

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_REAL, dValue1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        double dValue2 = 0d;

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_REAL, dValue2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_REAL, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setDouble(1, dValue1);
        verify(ps, times(1)).setDouble(2, dValue2);
        verify(ps, times(1)).setNull(3, Types.REAL);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_REAL, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_REAL, 1.452f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_REAL, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testBigDecimal() throws SQLException {

        double dValue1 = 8.89128;
        BigDecimal bd1 = BigDecimal.valueOf(dValue1).setScale(7, RoundingMode.HALF_UP);

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NUMERIC, bd1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        double dValue2 = 0d;
        BigDecimal bd2 = BigDecimal.valueOf(dValue2).setScale(7, RoundingMode.HALF_UP);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NUMERIC, bd2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NUMERIC, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setBigDecimal(1, bd1);
        verify(ps, times(1)).setBigDecimal(2, bd2);
        verify(ps, times(1)).setNull(3, Types.NUMERIC);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NUMERIC, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NUMERIC, 1.452, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_NUMERIC, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testFloat() throws SQLException {

        float fValue1 = 8.89128f;

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_FLOAT, fValue1, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        float fValue2 = 0f;

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_FLOAT, fValue2, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_FLOAT, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setFloat(1, fValue1);
        verify(ps, times(1)).setFloat(2, fValue2);
        verify(ps, times(1)).setNull(3, Types.FLOAT);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_FLOAT, 300, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_FLOAT, 1.452, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_FLOAT, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testInteger() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_INTEGER, 0, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_INTEGER, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setInt(1, 123);
        verify(ps, times(1)).setInt(2, 0);
        verify(ps, times(1)).setNull(3, Types.INTEGER);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_INTEGER, 300L, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_INTEGER, 1.45f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_INTEGER, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testSmallInt() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_SMALLINT, (short) 12345, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_SMALLINT, (short) 0, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_SMALLINT, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setShort(1, (short) 12345);
        verify(ps, times(1)).setShort(2, (short) 0);
        verify(ps, times(1)).setNull(3, Types.SMALLINT);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_SMALLINT, 254, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_SMALLINT, 1.45f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_SMALLINT, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testTinyInt() throws SQLException {

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TINYINT, (byte) 254, MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        param.apply(ps, 1);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TINYINT, (byte) 0, MatchOperator.EQUALS);

        param.apply(ps, 2);

        param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TINYINT, null, MatchOperator.EQUALS);

        param.apply(ps, 3);

        verify(ps, times(1)).setByte(1, (byte) 254);
        verify(ps, times(1)).setByte(2, (byte) 0);
        verify(ps, times(1)).setNull(3, Types.TINYINT);

        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TINYINT, 254, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TINYINT, 1.45f, MatchOperator.EQUALS));
        assertThrows(ConfigException.class, () -> new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_TINYINT, "", MatchOperator.EQUALS));

    }

    @Test
    @SuppressWarnings("resource")
    void testSpecialCase() throws SQLException {
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter(null, DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("   ", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("P 4", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("p1", null, 254, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("P{4", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("P}4", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new DefaultQueryParameter("P$4", DefaultAdlSqlType.SQL_INTEGER, 123, MatchOperator.EQUALS));

        assertSame(DefaultQueryParameterApplicator.getInstance(), DeepCopyUtils.deepCopy(DefaultQueryParameterApplicator.getInstance()));

        DefaultQueryParameter param = new DefaultQueryParameter("p1", DefaultAdlSqlType.SQL_LONGVARCHAR, "bla", MatchOperator.EQUALS);

        PreparedStatement ps = mock(PreparedStatement.class);

        doThrow(new SQLException()).when(ps).setString(anyInt(), anyString());

        assertThrows(SQLException.class, () -> param.apply(ps, 1));
    }

}
