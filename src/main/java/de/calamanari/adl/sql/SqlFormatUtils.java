//@formatter:off
/*
 * SqlFormatUtils
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

import de.calamanari.adl.FormatStyle;
import de.calamanari.adl.FormatUtils;

import static de.calamanari.adl.FormatUtils.appendIndentOrWhitespace;
import static de.calamanari.adl.FormatUtils.appendSpaced;
import static de.calamanari.adl.FormatUtils.space;
import static de.calamanari.adl.sql.SqlFormatConstants.IS_NOT_NULL;
import static de.calamanari.adl.sql.SqlFormatConstants.IS_NULL;
import static de.calamanari.adl.sql.SqlFormatConstants.ORDER_BY;

/**
 * Some additional utilities for formatting SQL
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class SqlFormatUtils {

    /**
     * Same as {@link FormatUtils#appendSpaced(StringBuilder, String...)} but only if the condition is true
     * 
     * @see FormatUtils#space(StringBuilder)
     * 
     * @param sb
     * @param condition only append if the condition is true
     * @param values to append surrounded by space
     */
    public static void appendSpacedIf(StringBuilder sb, boolean condition, String... values) {
        if (condition) {
            appendSpaced(sb, values);
        }
    }

    /**
     * Tells whether ...
     * <ul>
     * <li>sb is empty</li>
     * <li>sb only consist of whitespace</li>
     * <li>sb ends with an opening brace <code><b>(</b><i>[whitespace]</i></code>
     * </ul>
     * 
     * @param sb
     * @return true if the string ends with an opening brace optionally followed by whitespace or only consists of whitespace/empty
     */
    public static boolean endsWithOpenBraceOrAllWhitespace(StringBuilder sb) {
        for (int idx = sb.length() - 1; idx > -1; idx--) {
            char ch = sb.charAt(idx);
            if (ch == '(') {
                return true;
            }
            else if (!Character.isWhitespace(ch)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Appends the order-by in a formatted way
     * 
     * @param sb for appending
     * @param columnName the order-by sql column
     * @param style
     * @param level
     */
    public static void appendOrderBy(StringBuilder sb, String columnName, FormatStyle style, int level) {
        appendIndentOrWhitespace(sb, style, level, true);
        if (style.isMultiLine()) {
            appendIndentOrWhitespace(sb, style, level, true);
        }
        sb.append(ORDER_BY);
        space(sb);
        sb.append(columnName);
    }

    /**
     * Appends the column name <i>qualified by</i> the given alias to the builder.
     * <p>
     * Example: columnName=<code>C71</code>, alias=<code>TABLE3</code> => <code>TABLE3.C71</code>
     * 
     * @param sb
     * @param alias
     * @param columnName
     */
    public static void appendQualifiedColumnName(StringBuilder sb, String alias, String columnName) {
        sb.append(alias);
        sb.append(".");
        sb.append(columnName);
    }

    /**
     * Appends the <i>inversion</i> of an IS (NOT) NULL check.
     * <p>
     * This is sometimes required when we cannot query IS NULL at all. E.g., in a table there might be a column <b>A</b> that always has a value for any given
     * row in that table. There could even be a NOT-NULL SQL-constraint, and we can still not query <code>A IS NULL</code> because that table is only a lookup
     * table and does not contain all IDs.
     * <p>
     * This requires a different query approach which instead queries all rows that are NOT NULL to immediately disqualify them.
     * <p>
     * Summary: this method turns an IS NULL into an IS NOT NULL and vice-versa.
     * <ul>
     * <li><code>negation == true -&gt; IS NULL</code></li>
     * <li><code>negation == false -&gt; IS NOT NULL</code></li>
     * </ul>
     * 
     * @param sb
     * @param negation if true output IS NULL, otherwise output IS NOT NULL
     */
    public static void appendIsNullInversion(StringBuilder sb, boolean negation) {
        space(sb);
        if (negation) {
            sb.append(IS_NULL);
        }
        else {
            sb.append(IS_NOT_NULL);
        }
    }

    private SqlFormatUtils() {
        // static utilities
    }

}
