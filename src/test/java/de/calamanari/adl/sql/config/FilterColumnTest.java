//@formatter:off
/*
 * FilterColumnTest
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

package de.calamanari.adl.sql.config;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.DefaultArgValueFormatter;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class FilterColumnTest {

    @Test
    void testBasics() {

        assertConstruct("COL", DefaultAdlSqlType.SQL_BIGINT, "3");

        assertConstruct("COL", DefaultAdlSqlType.SQL_BIT, "1");
        assertConstruct("COL", DefaultAdlSqlType.SQL_BOOLEAN, "TRUE");

        // tolerance
        assertConstruct("COL", DefaultAdlSqlType.SQL_BIT, "FALSE");
        assertConstruct("COL", DefaultAdlSqlType.SQL_BOOLEAN, "0");

        assertConstruct("COL", DefaultAdlSqlType.SQL_CHAR, "foo");
        assertConstruct("COL", DefaultAdlSqlType.SQL_DATE, "2024-09-12");
        assertConstruct("COL", DefaultAdlSqlType.SQL_DECIMAL, "17.1234567");
        assertConstruct("COL", DefaultAdlSqlType.SQL_DOUBLE, "17.1234567");
        assertConstruct("COL", DefaultAdlSqlType.SQL_FLOAT, "17.123");
        assertConstruct("COL", DefaultAdlSqlType.SQL_INTEGER, "14563");
        assertConstruct("COL", DefaultAdlSqlType.SQL_LONGNVARCHAR, "Fluffy, Tuffy and Muffy went to town.");
        assertConstruct("COL", DefaultAdlSqlType.SQL_LONGVARCHAR, "Fluffy, Tuffy and Muffy went to town.");
        assertConstruct("COL", DefaultAdlSqlType.SQL_NCHAR, "Fluffy, Tuffy and Muffy went to town.");
        assertConstruct("COL", DefaultAdlSqlType.SQL_NUMERIC, "17.1234567");
        assertConstruct("COL", DefaultAdlSqlType.SQL_NVARCHAR, "Fluffy, Tuffy and Muffy went to town.");
        assertConstruct("COL", DefaultAdlSqlType.SQL_REAL, "17.1234567");
        assertConstruct("COL", DefaultAdlSqlType.SQL_SMALLINT, "12345");

        assertConstruct("`COL 2`", DefaultAdlSqlType.SQL_CHAR, "foo");
        assertConstruct("`COL.2`", DefaultAdlSqlType.SQL_CHAR, "foo");

    }

    @Test
    void testBasicsTorance() {
        assertConstruct("COL", DefaultAdlSqlType.SQL_TIMESTAMP, "2024-09-12 17:23:12");
        assertConstruct("COL", DefaultAdlSqlType.SQL_TIMESTAMP, "2024-09-12");
        assertConstruct("COL", DefaultAdlSqlType.SQL_TINYINT, "234");
        assertConstruct("COL", DefaultAdlSqlType.SQL_VARCHAR, "Fluffy, Tuffy and Muffy went to town.");
        assertConstruct("COL", DefaultAdlSqlType.SQL_VARCHAR, "");
        assertConstruct("COL", DefaultAdlSqlType.SQL_VARCHAR, " ");
    }

    @Test
    void testSpecialCase() {

        assertFailedConstruct(null, DefaultAdlSqlType.SQL_CHAR, "foo");
        assertFailedConstruct("", DefaultAdlSqlType.SQL_CHAR, "foo");
        assertFailedConstruct("white space", DefaultAdlSqlType.SQL_CHAR, "foo");

        assertFailedConstruct("COL", null, "foo");

        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_BIGINT, "foo");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_BIT, "2");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_BOOLEAN, "NO");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_DATE, "2024-09-12 12:34:12");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_DECIMAL, "foo");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_DOUBLE, "1a");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_FLOAT, "");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_INTEGER, "145_639_283_222");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_LONGNVARCHAR, null);
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_LONGVARCHAR, null);
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_NCHAR, null);
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_NUMERIC, "hugo");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_NVARCHAR, null);
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_REAL, "");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_SMALLINT, "123456789");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_TIMESTAMP, "12.04.2014");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_TINYINT, "2344");
        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("COL.1", DefaultAdlSqlType.SQL_VARCHAR, "valid");

        assertFailedConstruct("COL", DefaultAdlSqlType.SQL_VARCHAR.withFormatter(DefaultArgValueFormatter.NONE), "");

    }

    @Test
    void testSpecialCase2() {
        assertThrows(ConfigException.class, () -> new FilterColumn("", "C1", DefaultAdlSqlType.SQL_CHAR, "-"));
        assertThrows(ConfigException.class, () -> new FilterColumn(null, "C1", DefaultAdlSqlType.SQL_CHAR, "-"));
        assertThrows(ConfigException.class, () -> new FilterColumn("white space", "C1", DefaultAdlSqlType.SQL_CHAR, "-"));
    }

    private static void assertConstruct(String columnName, AdlSqlType columnType, String filterValue) {

        FilterColumn col = new FilterColumn("TBL", columnName, columnType, filterValue);

        assertSame(columnName, col.columnName());
        assertSame(columnType, col.columnType());
        assertSame(filterValue, col.filterValue());

    }

    private static void assertFailedConstruct(String columnName, AdlSqlType columnType, String filterValue) {

        assertThrows(ConfigException.class, () -> new FilterColumn("TBL", columnName, columnType, filterValue));

    }

}
