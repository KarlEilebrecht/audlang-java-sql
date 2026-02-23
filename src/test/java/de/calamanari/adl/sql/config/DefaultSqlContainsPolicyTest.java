//@formatter:off
/*
 * DefaultSqlContainsPolicyTest
 * Copyright 2025 Karl Eilebrecht
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

import org.junit.jupiter.api.Test;

import de.calamanari.adl.cnv.tps.ContainsNotSupportedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultSqlContainsPolicyTest {

    @Test
    void testBasics() {

        assertEquals("ABCxyz", DefaultSqlContainsPolicy.MYSQL.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE CONCAT('%', ${bla}, '%')", DefaultSqlContainsPolicy.MYSQL.createInstruction("COL1", "${bla}"));

        assertEquals("ABCxyz", DefaultSqlContainsPolicy.SQL92.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE '%' || ${bla} || '%'", DefaultSqlContainsPolicy.SQL92.createInstruction("COL1", "${bla}"));

        assertEquals("ABCxyz", DefaultSqlContainsPolicy.SQL_SERVER.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE '%' + ${bla} + '%'", DefaultSqlContainsPolicy.SQL_SERVER.createInstruction("COL1", "${bla}"));

        assertEquals("%ABC_xyz%", DefaultSqlContainsPolicy.SQL_SERVER2.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("CHARINDEX(${bla}, COL1, 0) > 0", DefaultSqlContainsPolicy.SQL_SERVER2.createInstruction("COL1", "${bla}"));

        assertEquals("%ABC_xyz%", DefaultSqlContainsPolicy.UNSUPPORTED.prepareSearchSnippet("%ABC_xyz%"));
        assertThrows(ContainsNotSupportedException.class, () -> DefaultSqlContainsPolicy.UNSUPPORTED.createInstruction("COL1", "${bla}"));

    }

    @Test
    void testDecoration() {

        SqlContainsPolicy decorated = DefaultSqlContainsPolicy.SQL92
                .withCreatorFunction((columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')");
        assertEquals("COL1 LIKE CONCAT('%', ${bla}, '%')", decorated.createInstruction("COL1", "${bla}"));

        assertTrue(decorated.name().startsWith("SQL92"));

        decorated = DefaultSqlContainsPolicy.SQL92.withPreparatorFunction(s -> s);
        assertEquals("%ABC_xyz%", decorated.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE '%' || ${bla} || '%'", decorated.createInstruction("COL1", "${bla}"));

        decorated = decorated.withCreatorFunction((columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')");
        assertEquals("%ABC_xyz%", decorated.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE CONCAT('%', ${bla}, '%')", decorated.createInstruction("COL1", "${bla}"));

        assertTrue(decorated.name().startsWith("SQL92"));

        decorated = DefaultSqlContainsPolicy.MYSQL.withPreparatorFunction(s -> s)
                .withCreatorFunction((columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')");
        assertEquals("%ABC_xyz%", decorated.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE CONCAT('%', ${bla}, '%')", decorated.createInstruction("COL1", "${bla}"));

        assertTrue(decorated.name().startsWith("MYSQL"));

        decorated = DefaultSqlContainsPolicy.SQL92.withCreatorFunction("FOOBAR",
                (columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')");
        assertEquals("COL1 LIKE CONCAT('%', ${bla}, '%')", decorated.createInstruction("COL1", "${bla}"));

        assertEquals("FOOBAR", decorated.name());

        decorated = DefaultSqlContainsPolicy.SQL92.withPreparatorFunction("FOOBAR", s -> s);
        assertEquals("%ABC_xyz%", decorated.prepareSearchSnippet("%ABC_xyz%"));

        assertEquals("FOOBAR", decorated.name());

        assertEquals("FOOBAR", decorated.toString());

        decorated = DefaultSqlContainsPolicy.MYSQL.withPreparatorFunction("FOOBAR", s -> s)
                .withCreatorFunction((columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')");
        assertEquals("%ABC_xyz%", decorated.prepareSearchSnippet("%ABC_xyz%"));
        assertEquals("COL1 LIKE CONCAT('%', ${bla}, '%')", decorated.createInstruction("COL1", "${bla}"));

        assertTrue(decorated.name().startsWith("MYSQL"));

        decorated = DefaultSqlContainsPolicy.MYSQL.withPreparatorFunction(s -> s).withCreatorFunction("FOOBAR",
                (columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')");

        assertEquals("FOOBAR", decorated.name());

        assertThrows(IllegalArgumentException.class, () -> DefaultSqlContainsPolicy.SQL92.withCreatorFunction(null));
        assertThrows(IllegalArgumentException.class, () -> DefaultSqlContainsPolicy.SQL92.withPreparatorFunction(null));

        assertThrows(IllegalArgumentException.class, () -> DefaultSqlContainsPolicy.SQL92.withCreatorFunction("Test", null));
        assertThrows(IllegalArgumentException.class, () -> DefaultSqlContainsPolicy.SQL92.withPreparatorFunction("Test", null));
    }

}
