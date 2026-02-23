//@formatter:off
/*
 * SqlFormatUtilsTest
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

package de.calamanari.adl.sql;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.FormatStyle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class SqlFormatUtilsTest {

    @Test
    void testEndsWithOpenBraceOrAllWhitespace() {
        StringBuilder sb = new StringBuilder();

        assertTrue(SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb));

        sb.append("    ");
        assertTrue(SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb));

        sb.append("b");
        assertFalse(SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb));

        sb.append("(");
        assertTrue(SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb));

        sb.append(" ");
        assertTrue(SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb));

        sb.append("\n");
        assertTrue(SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb));
    }

    @Test
    void testAppendOrderBy() {

        StringBuilder sb = new StringBuilder();

        assertSbEquals("ORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.INLINE, 0));
        sb.setLength(0);
        assertSbEquals("ORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.INLINE, 2));
        sb.setLength(0);
        assertSbEquals("\nORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.PRETTY_PRINT, 0));
        sb.setLength(0);
        assertSbEquals("\n        ORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.PRETTY_PRINT, 2));

        sb.setLength(0);
        sb.append("...");

        assertSbEquals("... ORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.INLINE, 0));
        sb.setLength(0);
        sb.append("...");
        assertSbEquals("...\nORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.PRETTY_PRINT, 0));
        sb.setLength(0);
        sb.append("...");
        assertSbEquals("...\n        ORDER BY COL", sb, () -> SqlFormatUtils.appendOrderBy(sb, "COL", FormatStyle.PRETTY_PRINT, 2));

    }

    @Test
    void testAppendQualifiedColumnName() {
        StringBuilder sb = new StringBuilder();
        assertSbEquals("a.b", sb, () -> SqlFormatUtils.appendQualifiedColumnName(sb, "a", "b"));
        sb.setLength(0);
        assertSbEquals(".", sb, () -> SqlFormatUtils.appendQualifiedColumnName(sb, "", ""));

    }

    void testAppendIsNullInversion() {

        StringBuilder sb = new StringBuilder();

        assertSbEquals("IS NULL", sb, () -> SqlFormatUtils.appendIsNullInversion(sb, false));
        assertSbEquals("IS NOT NULL", sb, () -> SqlFormatUtils.appendIsNullInversion(sb, true));

    }

    private static void assertSbEquals(String expected, StringBuilder sb, Runnable runnable) {
        runnable.run();
        assertEquals(expected, sb.toString());
    }

}
