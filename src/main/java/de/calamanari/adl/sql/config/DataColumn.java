//@formatter:off
/*
 * DataColumn
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.DataColumnStep1;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.DataColumnStep2;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.DataColumnStep3;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.DataColumnStep4;

import static de.calamanari.adl.sql.config.ConfigUtils.isValidColumnName;
import static de.calamanari.adl.sql.config.ConfigUtils.isValidTableName;

/**
 * A {@link DataColumn} describes the characteristics of a table colum in a database.
 * <p>
 * <b>Remarks:</b>
 * <p>
 * The flag <b>isAlwaysKnown</b> defines that there is a <b>known value</b> <i>for every record</i> in your database.
 * <p>
 * E.g., be there a record ID=4711 and a column MODEL, you guarantee that there will be a non-null value in that column for ID-4711.
 * <p>
 * <b>Important:</b> There is a subtle difference between a column with the SQL-constraint NOT NULL and <i>isAlwaysKnown</i>.<br>
 * In a database with multiple tables there are two ways a value can be unknown (SQL NULL). First, if a column is nullable, then the value of that column can be
 * SQL NULL. Second, if there is a left join to a table that does not contain a record's id that otherwise matches all query conditions, then this column also
 * appears as SQL NULL value. Be sure, both cannot happen before you set this property. The query builder relies on this setting to decide about the join type.
 * An incorrect setting may lead to wrong query results.
 * <p>
 * <ul>
 * <li>If there is exactly one table in your setup, you can safely set this property if there is a NOT NULL SQL-constraint on the related column.</li>
 * <li>If the table is marked with {@link TableNature#containsAllIds()} (resp. {@link TableNature#isPrimaryTable()}) <b>and</b> there is a NOT NULL
 * SQL-constraint on the related column, then you can safely set this property.</li>
 * <li>If you configure a multi-table setup, please double-check if a particular join path among the configured tables could lead to above mentioned
 * problem.</li>
 * <li>Don't set this property if you are unsure.</li>
 * </ul>
 * The flag <b>isMultiRow</b> defines that the data for a single record can be located on multiple rows.
 * <p>
 * 
 * <pre>
 * +------+-------+---------+
 * |  ID  | ...   | COLOR   |
 * +------+-------+---------+
 * | 8231 | ...   | BLUE    |
 * +------+-------+---------+
 * | 8231 | ...   | RED     |
 * +------+-------+---------+
 * | 8231 | ...   | YELLOW  |
 * +------+-------+---------+
 * | 9555 | ...   | ...     |
 * +------+-------+---------+
 * </pre>
 * 
 * In the example above for the same record (8321) there are 3 different values for the attribute COLOR, which must be considered when generating queries.<br>
 * An SQL-query like <code>COLOR = 'BLUE' AND COLOR = 'RED'</code> would not work because any of these conditions <i>fixes</i> a row set that is incompatible to
 * the other condition. We call this <i>accidental row-pinning</i>.
 * <p>
 * <ul>
 * <li>Unnecessarily setting this property may negatively impact the performance (increased query complexity) but queries will be correct.</li>
 * <li>However, if the values sit on multiple rows <i>not setting</i> this property will inevitably lead to incorrect query results.</li>
 * <li>If you see unexpected query results and you are unsure, then try setting this property to compare the generated queries and the outcome.</li>
 * </ul>
 * <br>
 * If you omit providing an explicit logical data model and instead rely on the the physical model, then any argument mapped to a column marked as multi-row
 * will appear as collection. This is not accurate but may help to understand the implications. In other words: there can be valid scenarios where a multi-row
 * mapping does <i>not</i> imply a collection attribute. This is especially the case for key-value onboarding (flat filtered table) with every key assigned to
 * only one value per record.
 * <p>
 * Each entry in <b>filters</b> defines an <i>extra column filter</i> related to <i>this data column</i>. This is required if <i>this data column</i> stores
 * information effectively qualified or narrowed by values of other column(s).<br>
 * See also {@link FilterColumn}.
 * <p>
 * Instances are <i>deeply immutable</i>.
 * 
 * @param tableName name of the data base table
 * @param columnName name of the column in the table
 * @param columnType type of the column for type alignment with the logical data model
 * @param isAlwaysKnown if true there is always a value for any record existing in the table <b>see {@link DataColumn}</b>
 * @param isMultiRow if true there can be multiple rows (values) in the table related to the same id
 * @param filters optional (null = empty list): columns with fixed values to filter the result <b>see {@link FilterColumn}</b>
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record DataColumn(String tableName, String columnName, AdlSqlType columnType, boolean isAlwaysKnown, boolean isMultiRow, List<FilterColumn> filters)
        implements AdlSqlColumn {

    /**
     * Starts the fluent building process for a data column
     * 
     * @param tableName target table
     * @return builder
     */
    public static DataColumnStep1 forTable(String tableName) {
        return new Builder(tableName);
    }

    /**
     * @param tableName name of the data base table
     * @param columnName name of the column in the table
     * @param columnType type of the column for type alignment with the logical data model
     * @param isAlwaysKnown if true there is always a value for any record existing in the table <b>see {@link DataColumn}</b>
     * @param isMultiRow if true there can be multiple rows (values) in the table related to the same id
     * @param filters optional (null = empty list): columns with fixed values to filter the result <b>see {@link FilterColumn}</b>
     */
    public DataColumn(String tableName, String columnName, AdlSqlType columnType, boolean isAlwaysKnown, boolean isMultiRow, List<FilterColumn> filters) {

        if (!isValidTableName(tableName) || !isValidColumnName(columnName) || columnType == null) {
            throw new ConfigException(String.format(
                    "Arguments tableName, columnName and columnType must not be null, tableName and columnName must be valid SQL identifiers, given: tableName=%s, columnName=%s, columnType=%s",
                    tableName, columnName, columnType));
        }
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.isAlwaysKnown = isAlwaysKnown;
        this.isMultiRow = isMultiRow;
        if (filters != null && !filters.isEmpty()) {
            validateFilters(tableName, columnName, columnType, filters);
            this.filters = Collections.unmodifiableList(new ArrayList<>(filters));
        }
        else {
            this.filters = Collections.emptyList();
        }

    }

    /**
     * @param tableName
     * @param columnName
     * @param columnType
     * @param filters
     */
    private static void validateFilters(String tableName, String columnName, AdlSqlType columnType, List<FilterColumn> filters) {
        Map<String, FilterColumn> filterMap = new HashMap<>();
        for (FilterColumn filter : filters) {
            if (filter == null) {
                throw new ConfigException(
                        String.format("List of filters must not contain any nulls, given: tableName=%s, columnName=%s, columnType=%s, filters=%s", tableName,
                                columnName, columnType, filters));
            }
            if (!filter.tableName().equals(tableName)) {
                throw new ConfigException(
                        String.format("Filter column must belong to the same table, given: tableName=%s, columnName=%s, columnType=%s, filters=%s", tableName,
                                columnName, columnType, filters));
            }
            if (filter.columnName().equals(columnName)) {
                throw new ConfigException(
                        String.format("Filter column must not equal the base column, given: tableName=%s, columnName=%s, columnType=%s, filters=%s", tableName,
                                columnName, columnType, filters));
            }
            FilterColumn prevValue = filterMap.putIfAbsent(filter.columnName(), filter);
            if (prevValue != null) {
                throw new ConfigException(String.format("Duplicate filter column %s, given: tableName=%s, columnName=%s, columnType=%s, filters=%s",
                        filter.columnName(), tableName, columnName, columnType, filters));
            }
        }
    }

    /**
     * Fluent builder for {@link DataColumn}s
     */
    private static class Builder implements DataColumnStep1, DataColumnStep2 {

        private final String tableName;

        private String columnName;

        private AdlSqlType columnType;

        private boolean multiRowFlag = false;

        private boolean alwaysKnownFlag = false;

        private List<FilterColumn> filterColumns = new ArrayList<>();

        private Builder(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public DataColumnStep2 dataColumn(String columnName, AdlSqlType columnType) {
            this.columnName = columnName;
            this.columnType = columnType;
            return this;
        }

        @Override
        public DataColumnStep4 multiRow() {
            multiRowFlag = true;
            return this;
        }

        @Override
        public DataColumnStep4 filteredBy(String columnName, AdlSqlType columnType, String value) {
            filterColumns.add(new FilterColumn(tableName, columnName, columnType, value));
            return this;
        }

        @Override
        public DataColumnStep3 alwaysKnown() {
            this.alwaysKnownFlag = true;
            return this;
        }

        @Override
        public DataColumn get() {
            return new DataColumn(tableName, columnName, columnType, alwaysKnownFlag, multiRowFlag, filterColumns);
        }

    }

}
