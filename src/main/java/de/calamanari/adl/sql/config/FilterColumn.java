//@formatter:off
/*
 * FilterColumn
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

import de.calamanari.adl.cnv.TemplateParameterUtils;
import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.sql.AdlSqlType;

import static de.calamanari.adl.sql.config.ConfigUtils.isValidColumnName;
import static de.calamanari.adl.sql.config.ConfigUtils.isValidTableName;

/**
 * A {@link FilterColumn} defines an extra filter condition to be applied to a query, so the scope of the original query will be <i>narrowed</i>.
 * <p>
 * A good example might be a <code>TENANT</code>-column that is not mapped to any attribute. Instead the value of this column will be compared to a process
 * variable.
 * <p>
 * {@link FilterColumn}s also allow for advanced querying on tables with different layouts.<br>
 * Sometimes, a table may be organized in a way that the effective key is composed of two or more columns.<br>
 * A {@link FilterColumn} is the definition of a column name and a <b>fixed</b> value to be included in the query when selecting rows.
 * <p>
 * 
 * <pre>
 *   +------+------------+-------------+
 *   | ID   | ENTRY_TYPE | ENTRY_VALUE |
 *   +------+------------+-------------+
 *   | 8599 | MODEL      | X4886       |
 *   +------+------------+-------------+
 *   | 8599 | BRAND      | RED         |
 *   +------+------------+-------------+
 *   | 4132 | COLOR      | RED         |
 *   +------+------------+-------------+
 *   | ...  | ...        | ...         |
 * </pre>
 * 
 * In the example above, multiple attributes of the logical data model are mapped to the <code>ENTRY_TYPE</code> column, e.g., <code>model</code>,
 * <code>brand</code> and <code>color</code>.<br>
 * However, a query like <code>brand=RED</code> must not be simply translated into <code>SELECT ID FROM ... <b>where ENTRY_VALUE='RED'</b></code> because it
 * would also return ID=4132 - which is wrong. Instead, an additional <i>filtering by ENTRY_TYPE</i> is required:
 * <code>SELECT ID FROM ... <b>where ENTRY_TYPE='BRAND' AND ENTRY_VALUE='RED'</b></code>.
 * <p>
 * To create more expressive configurations your can specify <code>{@value #ARG_NAME_PLACEHOLDER}</code> as the <i>filterValue</i>. Then the converter will
 * fill-in the current argument name from the expression as the filter value. This is meant for flat tables where the argument name acts as a qualifier for the
 * column holding the values.
 * <p>
 * <b>Summary:</b><br>
 * A {@link FilterColumn} is part of the mapping of a field of the logical data model to a column. It expresses an additional filter condition based on a column
 * name and its fixed or dynamic value (global variable reference).<br>
 * {@link FilterColumn}s defined on a table <i>pre-filter</i> the data of that table for any related query.
 * 
 * @param tableName native name of the table
 * @param columnName native name of the filter column
 * @param columnType type of the filter column for sql generation
 * @param filterValue fixed filter value as a condition to be included in the query, or <code>${argName}</code> to fill-in the argument name
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record FilterColumn(String tableName, String columnName, AdlSqlType columnType, String filterValue) implements AdlSqlColumn {

    /**
     * Fixed value that can be specified as a filterValue to tell the system to replace it at runtime with the current argName from the expression.
     * <p>
     * This variable is highly dynamic and only available (of interest) during parameter creation.
     */
    public static final String VAR_ARG_NAME = "argName";

    /**
     * Reference to the variable {@value #VAR_ARG_NAME}
     */
    public static final String ARG_NAME_PLACEHOLDER = "${" + VAR_ARG_NAME + "}";

    /**
     * @param tableName native name of the table
     * @param columnName native name of the filter column
     * @param columnType type of the filter column for sql generation
     * @param filterValue fixed filter value as a condition to be included in the query, or <code>${argName}</code> to fill-in the argument name
     */
    public FilterColumn {
        if (!isValidTableName(tableName) || !isValidColumnName(columnName) || columnType == null || filterValue == null) {
            throw new ConfigException(String.format(
                    "None of the arguments can be null and columnName/tableName must be valid SQL identifiers, given: tableName=%s, columnName=%s, columnType=%s, filterValue=%s.",
                    tableName, columnName, columnType, filterValue));
        }
        try {
            // probing to avoid later surprises
            String value = createNativeValue(tableName, columnName, columnType, filterValue);
            if (value.isBlank()) {
                throw new ConfigException(String.format(
                        "The given filterValue formatted in the given way would result in a broken blank value (>>%s<<), given: tableName=%s, columnName=%s, columnType=%s, filterValue=%s.",
                        value, tableName, columnName, columnType, filterValue));
            }
        }
        catch (AdlFormattingException ex) {
            throw new ConfigException(String.format(
                    "The given filterValue cannot be formatted with the specified type, given: tableName=%s, columnName=%s, columnType=%s, filterValue=%s.",
                    tableName, columnName, columnType, filterValue), ex);
        }
    }

    /**
     * @return the native filter value to be included in the SQL query
     */
    private static String createNativeValue(String tableName, String columnName, AdlSqlType columnType, String filterValue) {
        return TemplateParameterUtils.containsAnyVariables(filterValue) ? filterValue
                : columnType.getFormatter().format("<FilterColumn:" + tableName + "." + columnName + ">", filterValue, MatchOperator.EQUALS);
    }

    /**
     * @return the native filter condition to be included in the SQL query
     */
    private static String createNativeCondition(String tableName, String columnName, AdlSqlType columnType, String filterValue) {
        return tableName + "." + columnName + "=" + createNativeValue(tableName, columnName, columnType, filterValue);
    }

    /**
     * @return the native filter condition to be included in the SQL query
     */
    public String nativeCondition() {
        return createNativeCondition(tableName, columnName, columnType, filterValue);
    }

    @Override
    public String toString() {
        return "?{ " + nativeCondition() + " }";
    }

}
