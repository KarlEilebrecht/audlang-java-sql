//@formatter:off
/*
 * SqlFormatConstants
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

/**
 * A list of SQL-generation related words and phrases etc.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class SqlFormatConstants {

    /**
     * Default alias name for the query alias we are starting the query with
     */
    public static final String DEFAULT_BASE_QUERY_ALIAS = "bq__start";

    /**
     * Constant for cases where we need a dummy argument name for method calls (relevant for debugging only)
     */
    public static final String ARGNAME_DUMMY = "<ARGNAME>";

    /**
     * {@value}
     */
    public static final String SELECT = "SELECT";

    /**
     * {@value}
     */
    public static final String COUNT = "COUNT";

    /**
     * {@value}
     */
    public static final String FROM = "FROM";

    /**
     * {@value}
     */
    public static final String WHERE = "WHERE";

    /**
     * {@value}
     */
    public static final String AND = "AND";

    /**
     * {@value}
     */
    public static final String OR = "OR";

    /**
     * {@value}
     */
    public static final String NOT = "NOT";

    /**
     * {@value}
     */
    public static final String IN = "IN";

    /**
     * {@value}
     */
    public static final String WITH = "WITH";

    /**
     * {@value}
     */
    public static final String AS = "AS";

    /**
     * {@value}
     */
    public static final String INNER_JOIN = "INNER JOIN";

    /**
     * {@value}
     */
    public static final String LEFT_OUTER_JOIN = "LEFT OUTER JOIN";

    /**
     * {@value}
     */
    public static final String ON = "ON";

    /**
     * {@value}
     */
    public static final String UNION = "UNION";

    /**
     * {@value}
     */
    public static final String IS_NULL = "IS NULL";

    /**
     * {@value}
     */
    public static final String IS_NOT_NULL = "IS NOT NULL";

    /**
     * {@value}
     */
    public static final String DISTINCT = "DISTINCT";

    /**
     * {@value}
     */
    public static final String ORDER_BY = "ORDER BY";

    /**
     * {@value}
     */
    public static final String CMP_EQUALS = "=";

    /**
     * {@value}
     */
    public static final String CMP_GREATER_THAN = ">";

    /**
     * {@value}
     */
    public static final String CMP_LESS_THAN = "<";

    /**
     * {@value}
     */
    public static final String CMP_NOT_EQUALS = "<>";

    /**
     * {@value}
     */
    public static final String BRACE_OPEN = "(";

    /**
     * {@value}
     */
    public static final String BRACE_CLOSE = ")";

    /**
     * Alias prefix
     */
    public static final String ALIAS_PREFIX = "sq__";

    /**
     * Alias name for joining a table with itself, {@value}
     */
    public static final String SELF_JOIN_ALIAS = "sq__self";

    /**
     * Default name of the returned ID-column if nothing else is specified: {@value}
     */
    public static final String DEFAULT_ID_COLUMN_NAME = "ID";

    /**
     * {@value}
     */
    public static final String TRUE = "TRUE";

    /**
     * {@value}
     */
    public static final String FALSE = "FALSE";

    private SqlFormatConstants() {
        // Constants
    }

}
