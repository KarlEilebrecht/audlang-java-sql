//@formatter:off
/*
 * DefaultQueryParameterCreatorTest
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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.DeepCopyUtils;
import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.cnv.tps.DefaultArgValueFormatter;
import de.calamanari.adl.irl.MatchOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultQueryParameterCreatorTest {

    static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryParameterCreatorTest.class);

    private static final List<AdlSqlType> STRING_TYPES = Arrays.asList(DefaultAdlSqlType.SQL_CHAR, DefaultAdlSqlType.SQL_LONGNVARCHAR,
            DefaultAdlSqlType.SQL_LONGVARCHAR, DefaultAdlSqlType.SQL_NCHAR, DefaultAdlSqlType.SQL_NVARCHAR, DefaultAdlSqlType.SQL_VARCHAR);

    private static final List<AdlSqlType> INT_TYPES = Arrays.asList(DefaultAdlSqlType.SQL_BIGINT, DefaultAdlSqlType.SQL_INTEGER, DefaultAdlSqlType.SQL_SMALLINT,
            DefaultAdlSqlType.SQL_TINYINT);

    private static final List<AdlSqlType> DECIMAL_TYPES = Arrays.asList(DefaultAdlSqlType.SQL_DECIMAL, DefaultAdlSqlType.SQL_DOUBLE,
            DefaultAdlSqlType.SQL_FLOAT, DefaultAdlSqlType.SQL_REAL, DefaultAdlSqlType.SQL_NUMERIC);

    DefaultQueryParameterCreator creator = DefaultQueryParameterCreator.getInstance();

    @Test
    void testBasics() {

        assertSame(DefaultQueryParameterCreator.getInstance(), creator);

        assertSame(creator, DeepCopyUtils.deepCopy(creator));

        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.BOOL, false, false);

        DefaultQueryParameterCreator.resetIdSequence();
        QueryParameter param = creator.createParameter(argMetaInfo, "1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);

        assertEquals("P_1001", param.id());

    }

    @Test
    void testBool() {

        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.BOOL, false, false);

        QueryParameter param = null;

        param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);

        assertEquals("TRUE", param.toString());

        param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);

        assertEquals("FALSE", param.toString());

        for (AdlSqlType adlSqlType : STRING_TYPES) {
            param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, adlSqlType);

            assertEquals("'TRUE'", param.toString());

            param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, adlSqlType);

            assertEquals("'FALSE'", param.toString());

        }

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_BIGINT, DefaultAdlSqlType.SQL_BIT, DefaultAdlSqlType.SQL_INTEGER,
                DefaultAdlSqlType.SQL_SMALLINT, DefaultAdlSqlType.SQL_TINYINT)) {
            param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, adlSqlType);

            assertEquals("1", param.toString());

            param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, adlSqlType);

            assertEquals("0", param.toString());

        }

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_DECIMAL, DefaultAdlSqlType.SQL_REAL, DefaultAdlSqlType.SQL_NUMERIC,
                DefaultAdlSqlType.SQL_DOUBLE, DefaultAdlSqlType.SQL_FLOAT, DefaultAdlSqlType.SQL_DATE, DefaultAdlSqlType.SQL_TIMESTAMP)) {

            assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, adlSqlType));

        }

    }

    @Test
    void testString() {

        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.STRING, false, false);

        QueryParameter param = null;

        param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_DATE);

        assertEquals("DATE '2024-12-13'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TIMESTAMP);

        assertEquals("TIMESTAMP '2024-12-13 00:00:00'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "2024-12-13 17:39:12", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TIMESTAMP);

        assertEquals("TIMESTAMP '2024-12-13 17:39:12'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "10000000000", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TIMESTAMP);

        assertEquals("TIMESTAMP '1970-04-26 17:46:40'", param.toString());

        for (AdlSqlType adlSqlType : STRING_TYPES) {
            param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType);

            assertEquals("'2024-12-13'", param.toString());

        }

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_BIGINT, DefaultAdlSqlType.SQL_INTEGER)) {
            param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType);

            assertEquals("1734048000", param.toString());

        }

        param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);

        assertEquals("TRUE", param.toString());

        param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);

        assertEquals("FALSE", param.toString());

        param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BIT);

        assertEquals("1", param.toString());

        param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BIT);

        assertEquals("0", param.toString());

        param = creator.createParameter("p", argMetaInfo, "-255", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_SMALLINT);

        assertEquals("-255", param.toString());

        param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TINYINT);

        assertEquals("255", param.toString());

        param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_DECIMAL);

        assertEquals("255.0", param.toString());

        param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_REAL);

        assertEquals("255.0", param.toString());

        param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_NUMERIC);

        assertEquals("255.0", param.toString());

        param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_FLOAT);

        assertEquals("255.0", param.toString());

        for (AdlSqlType adlSqlType : DefaultAdlSqlType.values()) {

            if (!STRING_TYPES.contains(adlSqlType)) {
                assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "crap", MatchOperator.EQUALS, adlSqlType));
            }

        }

    }

    @Test
    void testDate() {
        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.DATE, false, false);

        QueryParameter param = null;

        param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_DATE);

        assertEquals("DATE '2024-12-13'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TIMESTAMP);

        assertEquals("TIMESTAMP '2024-12-13 00:00:00'", param.toString());

        for (AdlSqlType adlSqlType : STRING_TYPES) {
            param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType);

            assertEquals("'2024-12-13'", param.toString());

        }

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_BIGINT, DefaultAdlSqlType.SQL_INTEGER)) {
            param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType);
            assertEquals("1734048000", param.toString());
        }

        for (AdlSqlType adlSqlType : DECIMAL_TYPES) {
            if (adlSqlType != DefaultAdlSqlType.SQL_FLOAT) {
                param = creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType);
                assertEquals("1734048000000.0", param.toString());
            }
            else {
                assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType));
            }

        }

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_BOOLEAN, DefaultAdlSqlType.SQL_BIT, DefaultAdlSqlType.SQL_SMALLINT,
                DefaultAdlSqlType.SQL_TINYINT)) {

            assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "2024-12-13", MatchOperator.EQUALS, adlSqlType));

        }

    }

    @Test
    void testInteger() {
        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.INTEGER, false, false);

        QueryParameter param = null;

        for (AdlSqlType adlSqlType : INT_TYPES) {

            param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, adlSqlType);
            assertEquals("255", param.toString());

        }

        for (AdlSqlType adlSqlType : DECIMAL_TYPES) {

            param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, adlSqlType);
            assertEquals("255.0", param.toString());

        }

        for (AdlSqlType adlSqlType : STRING_TYPES) {

            param = creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, adlSqlType);
            assertEquals("'255'", param.toString());

        }

        param = creator.createParameter("p", argMetaInfo, "1734048000000", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_DATE);
        assertEquals("DATE '2024-12-13'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "1734048000000", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TIMESTAMP);
        assertEquals("TIMESTAMP '2024-12-13 00:00:00'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);
        assertEquals("TRUE", param.toString());

        param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BOOLEAN);
        assertEquals("FALSE", param.toString());

        param = creator.createParameter("p", argMetaInfo, "1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BIT);
        assertEquals("1", param.toString());

        param = creator.createParameter("p", argMetaInfo, "0", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_BIT);
        assertEquals("0", param.toString());

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_BOOLEAN, DefaultAdlSqlType.SQL_BIT)) {
            assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "255", MatchOperator.EQUALS, adlSqlType));
        }

    }

    @Test
    void testDecimal() {
        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.DECIMAL, false, false);

        QueryParameter param = null;

        for (AdlSqlType adlSqlType : INT_TYPES) {

            param = creator.createParameter("p", argMetaInfo, "255.1234567", MatchOperator.EQUALS, adlSqlType);
            assertEquals("255", param.toString());

        }

        for (AdlSqlType adlSqlType : DECIMAL_TYPES) {

            param = creator.createParameter("p", argMetaInfo, "255.1234567", MatchOperator.EQUALS, adlSqlType);

            if (adlSqlType != DefaultAdlSqlType.SQL_FLOAT) {
                assertEquals(Double.parseDouble("255.1234567"), Double.parseDouble(param.toString()), 0.0000001d);
            }
            else {
                assertEquals(Double.parseDouble("255.1234567"), Double.parseDouble(param.toString()), 0.0001d);
            }

        }

        for (AdlSqlType adlSqlType : STRING_TYPES) {

            param = creator.createParameter("p", argMetaInfo, "255.1234567", MatchOperator.EQUALS, adlSqlType);
            assertEquals("'255.1234567'", param.toString());

        }

        param = creator.createParameter("p", argMetaInfo, "1734048000000.1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_DATE);
        assertEquals("DATE '2024-12-13'", param.toString());

        param = creator.createParameter("p", argMetaInfo, "1734048000000.1", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_TIMESTAMP);
        assertEquals("TIMESTAMP '2024-12-13 00:00:00'", param.toString());

        for (AdlSqlType adlSqlType : Arrays.asList(DefaultAdlSqlType.SQL_BOOLEAN, DefaultAdlSqlType.SQL_BIT)) {
            assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "1.0", MatchOperator.EQUALS, adlSqlType));
        }
    }

    @Test
    void testNull() {
        for (AdlSqlType adlSqlType : DefaultAdlSqlType.values()) {

            ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.STRING, false, false);

            QueryParameter param = creator.createParameter("p", argMetaInfo, null, MatchOperator.EQUALS, adlSqlType);

            assertEquals("NULL", param.toString());

        }
    }

    @Test
    void testSpecialCase() {
        ArgMetaInfo argMetaInfo = new ArgMetaInfo("arg1", DefaultAdlType.STRING, false, false);

        assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", null, "foo", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_VARCHAR));
        assertThrows(AdlFormattingException.class,
                () -> creator.createParameter(null, argMetaInfo, "foo", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_VARCHAR));
        assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "foo", MatchOperator.EQUALS, null));
        assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "foo", null, DefaultAdlSqlType.SQL_VARCHAR));

        AdlType weirdType = new AdlType() {

            private static final long serialVersionUID = 6853692939771236954L;

            @Override
            public boolean supportsLessThanGreaterThan() {
                return false;
            }

            @Override
            public boolean supportsContains() {
                return false;
            }

            @Override
            public String name() {
                return "bla";
            }

            @Override
            public ArgValueFormatter getFormatter() {
                return DefaultArgValueFormatter.STRING_IN_DOUBLE_QUOTES;
            }
        };

        ArgMetaInfo argMetaInfoWeird = new ArgMetaInfo("arg1", weirdType, false, false);

        assertThrows(AdlFormattingException.class,
                () -> creator.createParameter("p", argMetaInfoWeird, "foo", MatchOperator.EQUALS, DefaultAdlSqlType.SQL_VARCHAR));

        AdlSqlType weirdSqlType = new AdlSqlType() {

            private static final long serialVersionUID = 7932934841120622775L;

            @Override
            public boolean supportsLessThanGreaterThan() {
                return false;
            }

            @Override
            public boolean supportsContains() {
                return false;
            }

            @Override
            public String name() {
                return "foobar";
            }

            @Override
            public ArgValueFormatter getFormatter() {
                return DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES;
            }

            @Override
            public QueryParameterCreator getQueryParameterCreator() {
                return DefaultQueryParameterCreator.getInstance();
            }

            @Override
            public QueryParameterApplicator getQueryParameterApplicator() {
                return DefaultQueryParameterApplicator.getInstance();
            }

            @Override
            public int getJavaSqlType() {
                return 0;
            }
        };

        assertThrows(AdlFormattingException.class, () -> creator.createParameter("p", argMetaInfo, "foo", MatchOperator.EQUALS, weirdSqlType));

    }

}
