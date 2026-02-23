//@formatter:off
/*
 * DataColumnTest
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DataColumnTest {

    @Test
    void testBasics() {

        for (AdlSqlType type : DefaultAdlSqlType.values()) {
            assertConstruct("TBLNAME", "COLNAME", type);
        }

        for (AdlSqlType type : DefaultAdlSqlType.values()) {
            assertConstruct("`TBL NAME`", "COLNAME", type);
        }

        for (AdlSqlType type : DefaultAdlSqlType.values()) {
            assertConstruct("schema.TBLNAME", "COLNAME", type);
        }

        for (AdlSqlType type : DefaultAdlSqlType.values()) {
            assertConstruct("TBLNAME", "`COL NAME`", type);
        }

    }

    @Test
    void testBuilder() {

        // @formatter:off
        DataColumn col = DataColumn.forTable("TBLNAME")
                                        .dataColumn("COLNAME", DefaultAdlSqlType.SQL_INTEGER)
                                            .alwaysKnown()
                                            .multiRow()
                                            .filteredBy("F1", DefaultAdlSqlType.SQL_CHAR, "F")
                                            .filteredBy("F2", DefaultAdlSqlType.SQL_CHAR, "F3")
                                        .get();
        // @formatter:on

        assertEquals("TBLNAME", col.tableName());
        assertEquals("COLNAME", col.columnName());
        assertEquals(true, col.isAlwaysKnown());
        assertEquals(true, col.isMultiRow());
        assertEquals(Arrays.asList(new FilterColumn("TBLNAME", "F1", DefaultAdlSqlType.SQL_CHAR, "F"),
                new FilterColumn("TBLNAME", "F2", DefaultAdlSqlType.SQL_CHAR, "F3")), col.filters());

        // @formatter:off
        DataColumn col2 = DataColumn.forTable("TBLNAME")
                                        .dataColumn("COLNAME", DefaultAdlSqlType.SQL_INTEGER)
                                            .filteredBy("F1", DefaultAdlSqlType.SQL_CHAR, "F")
                                        .get();
        // @formatter:on

        assertEquals("TBLNAME", col2.tableName());
        assertEquals("COLNAME", col2.columnName());
        assertEquals(false, col2.isAlwaysKnown());
        assertEquals(false, col2.isMultiRow());
        assertEquals(Arrays.asList(new FilterColumn("TBLNAME", "F1", DefaultAdlSqlType.SQL_CHAR, "F")), col2.filters());

    }

    @Test
    void testSpecialCase() {

        assertFailedConstruct(null, "COLNAME", DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("TBLNAME", null, DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("TBLNAME", "COLNAME", null, null);

        assertFailedConstruct("", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("TBLNAME", "", DefaultAdlSqlType.SQL_VARCHAR, null);

        assertFailedConstruct("TBL NAME", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("`TBL``NAME`", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("TBLNAME", "COL NAME", DefaultAdlSqlType.SQL_VARCHAR, null);
        assertFailedConstruct("TBLNAME", "COL.NAME", DefaultAdlSqlType.SQL_VARCHAR, null);

        assertFailedConstruct("TBLNAME", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR,
                Arrays.asList(new FilterColumn("TBLNAME", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE1")));

        assertFailedConstruct("TBLNAME", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR,
                Arrays.asList(new FilterColumn("TBLNAME", "F1", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE1"), null));

        assertFailedConstruct("TBLNAME", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR,
                Arrays.asList(new FilterColumn("TBLNAME", "F1", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE1"),
                        new FilterColumn("TBLNAME", "F1", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE2")));

        assertFailedConstruct("TBLNAME", "COLNAME", DefaultAdlSqlType.SQL_VARCHAR,
                Arrays.asList(new FilterColumn("WRONG", "F1", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE1")));

    }

    private static void assertFailedConstruct(String tableName, String columnName, AdlSqlType columnType, List<FilterColumn> filters) {

        assertThrows(ConfigException.class, () -> new DataColumn(tableName, columnName, columnType, false, false, filters));
    }

    private static void assertConstruct(String tableName, String columnName, AdlSqlType columnType) {

        assertConstruct(tableName, columnName, columnType, false, false, null);
        assertConstruct(tableName, columnName, columnType, true, false, null);
        assertConstruct(tableName, columnName, columnType, false, true, null);
        assertConstruct(tableName, columnName, columnType, true, true, null);

        assertConstruct(tableName, columnName, columnType, false, false, Collections.emptyList());
        assertConstruct(tableName, columnName, columnType, false, false,
                Arrays.asList(new FilterColumn(tableName, "F1", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE")));
        assertConstruct(tableName, columnName, columnType, false, false,
                Arrays.asList(new FilterColumn(tableName, "F1", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE1"),
                        new FilterColumn(tableName, "F2", DefaultAdlSqlType.SQL_VARCHAR, "FILTER_VALUE2")));

    }

    private static void assertConstruct(String tableName, String columnName, AdlSqlType columnType, boolean isAlwaysKnown, boolean isMultiRow,
            List<FilterColumn> filters) {

        DataColumn col = new DataColumn(tableName, columnName, columnType, isAlwaysKnown, isMultiRow, filters);

        assertSame(tableName, col.tableName());
        assertSame(columnName, col.columnName());
        assertSame(columnType, col.columnType());
        assertEquals(isAlwaysKnown, col.isAlwaysKnown());
        assertEquals(isMultiRow, col.isMultiRow());
        if (filters != null && !filters.isEmpty()) {
            assertEquals(filters, col.filters());
            assertNotSame(filters, col.filters());
        }
        else {
            assertEquals(Collections.emptyList(), col.filters());
        }

    }

}
