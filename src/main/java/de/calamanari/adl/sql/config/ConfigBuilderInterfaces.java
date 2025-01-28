//@formatter:off
/*
 * ConfigBuilders
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

import java.util.function.Function;

import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.cnv.ConversionDirective;
import de.calamanari.adl.sql.config.DefaultAutoMappingPolicy.LocalArgNameExtractor;

/**
 * This class contains the interfaces for the fluent builder flows for creating column and table configurations.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class ConfigBuilderInterfaces {

    private ConfigBuilderInterfaces() {
        // only interfaces for builders
    }

    /**
     * Fluent API to build a {@link DataColumn}
     * <p>
     * Follow the method chain to finally obtain the result with {@link DataColumnStep4#get()}.
     */
    public static interface DataColumnStep1 {

        /**
         * Defines a data column of the given type.
         * 
         * @see DataColumn
         * @param columnName SQL column name in your database table
         * @param columnType type of the column
         * @return builder
         */
        public DataColumnStep2 dataColumn(String columnName, AdlSqlType columnType);

    }

    /**
     * Fluent API to build a {@link DataColumn}
     * <p>
     * Follow the method chain to finally obtain the result with {@link DataColumnStep4#get()}.
     */
    public static interface DataColumnStep2 extends DataColumnStep3 {

        /**
         * Defines that there is a <b>known value</b> <i>for every record</i> in your database.
         * <p>
         * <b>Important:</b> Marking an attribute as <i>always known</i> means that this column information is <i>available and NOT NULL for every record</i> of
         * your base audience. Consequently, if you configure <i>any</i> column in a table as <i>always known</i>, the table will automatically be marked as
         * {@link TableNature#containsAllIds()}.
         * 
         * @see DataColumn
         * @return builder
         */
        public DataColumnStep3 alwaysKnown();

    }

    /**
     * Fluent API to build a {@link DataColumn}
     * <p>
     * Follow the method chain to finally obtain the result with {@link DataColumnStep4#get()}.
     */
    public static interface DataColumnStep3 extends DataColumnStep4 {

        /**
         * Defines that the data for a single record can be <i>spread</i> across multiple rows.
         * <p>
         * This property <b>must</b> be set if the same column is mapped to multiple attributes (with filter column(s), see example below) or if this is a
         * collection attribute and you want to allow AND for two conditions of the same attribute (e.g., <code>color=blue AND color=red</code>.
         * <p>
         * Example:
         * 
         * <pre>
         *   +------+------------+-------------+
         *   | ID   | ENTRY_TYPE | ENTRY_VALUE |
         *   +------+------------+-------------+
         *   | 8599 | MODEL      | X4886       |
         *   +------+------------+-------------+
         *   | 8599 | BRAND      | QUACK       |
         *   +------+------------+-------------+
         *   | 8599 | COLOR      | BLACK       |
         *   +------+------------+-------------+
         *   | ...  | ...        | ...         |
         * </pre>
         * 
         * Let's say we want to query <code>model=X4886 AND brand=QUACK</code>.
         * <p>
         * Obviously, from the table above you <b>cannot</b> query
         * <code>... WHERE ENTRY_TYPE='MODEL' AND ENTRY_VALUE='X4886' AND ENTRY_TYPE='BRAND' AND ENTRY_VALUE='QUACK'</code>
         * <p>
         * The problem is that the first condition would <i>fix</i> the row, so the second condition will never become true (<i>accidental row-pinning</i>).
         * <p>
         * With the multi-row you tell the query builder about this problem and enforce a different query style.
         * 
         * @see DataColumn
         * @return builder
         */
        public DataColumnStep4 multiRow();

    }

    /**
     * Fluent API to build a {@link DataColumn}
     * <p>
     * Follow the method chain to finally obtain the result with {@link DataColumnStep4#get()}.
     */
    public static interface DataColumnStep4 {

        /**
         * Defines an <i>extra column filter</i> on a column. This is required if the target column stores information effectively qualified or narrowed by
         * values of other column(s).
         * <p>
         * <b>Note:</b> The actual <i>value</i> (either hard-coded or from a variable) must be an ADL-value.
         * <p>
         * <b>Example:</b>
         * <ul>
         * <li><b>correct:</b> <code>filteredBy("ACTIVE", SQL_BOOLEAN, <b>"1"</b>)</code></li>
         * <li>incorrect: <code>filteredBy("ACTIVE", SQL_BOOLEAN, <s>"TRUE"</s>)</code></li>
         * </ul>
         * 
         * @see DataColumn
         * 
         * @param columnName name of another column of the <i>same table</i> to be tested for a <b>fixed value</b>
         * @param columnType type of the filter column for sql generation
         * @param value the filter, a <b>fixed value</b> to include into any query related to the data column to filter the results
         * @return builder
         */
        public DataColumnStep4 filteredBy(String columnName, AdlSqlType columnType, String value);

        /**
         * @return the data column
         */
        public DataColumn get();

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableAddColumnOrExit {

        /**
         * Adds a data column of the given type to the current table
         * 
         * @see DataColumn
         * @param columnName SQL column name in your database table
         * @param columnType type of the column
         * @return builder
         */
        public SingleTableDataColumnStep1 dataColumn(String columnName, AdlSqlType columnType);

        /**
         * @return the table configuration
         */
        public SingleTableConfig get();

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableDataColumnStep1 {

        /**
         * Maps the column to an attribute (argName) of the logical data model.<br>
         * You can map the same column to multiple attributes but not vice-versa.
         * <p>
         * This method is meant for setting up the logical and physical data model at the same time.
         * <p>
         * The preferred way should be first setting up an {@link ArgMetaInfoLookup} with the argName's meta data and configuring the table with
         * {@link #mappedToArgName(String)}.
         * 
         * @param argName name of the attribute used in expressions that should be mapped to the column
         * @param argType
         * @return builder
         */
        public SingleTableDataColumnStep2 mappedToArgName(String argName, AdlType argType);

        /**
         * Maps the column to an attribute (argName) of the logical data model which has been configured in advance.<br>
         * You can map the same column to multiple attributes but not vice-versa.
         * 
         * @param argName name of the attribute used in expressions that should be mapped to the column
         * @return builder
         */
        public SingleTableDataColumnStep2 mappedToArgName(String argName);

        /**
         * Maps the column to every attribute (argName) the given extractor can extract a local argName from (not null).
         * <p>
         * In other words: The extractor is an UnaryOperator that at runtime takes the argName as input and either returns null (not applicable) or the
         * extracted <i>local name</i> of the attribute. This value will then be stored as a variable in the settings and can be references as
         * <code>${argName.local}</code> in a filter column definition.
         * <p>
         * <b>Example:</b> You have a number of attributes and the argName follows the pattern <code>section.<i>identifier</i>.int</code>, and you want to map
         * them to column <code>TBL.XYZ</code> of type INTEGER with the filter column F17 set to the the value of <i>identifier</i>.<br>
         * In this case you can specify the an extractor function that either returns null (not applicable, do not map) or the value of the
         * <i>identifier</i>.<br>
         * E.g.: <code>argName -> argName.endsWith(".int") ? argName.substring(argName.indexOf('.') + 1, argName.length-4) : null</code> would return
         * <code>47118</code> for <code>section.<i>47118</i>.int</code>. This value will automatically be assigned to the variable <code>argName.local</code>.
         * Now create a filter-column condition for column F17 and specify <code>${argName.local}</code> as its value.<br>
         * Any argName that is not explicitly mapped and that complies to the extractor will now be mapped to the given column with the filter condition derived
         * from the argName.
         * <p>
         * Explicitly mapped argNames always take precedence over auto-mapped argNames.
         * <p>
         * Should multiple policies be applicable to the same argName, then the <i>first</i> will win (order in the configuration).
         * 
         * @see DefaultAutoMappingPolicy#ARG_NAME_LOCAL_PLACEHOLDER
         * @param extractor custom function to extract the local argName from the argName
         * @param argType
         * @return builder
         */
        public SingleTableDataColumnStep2 autoMapped(LocalArgNameExtractor extractor, AdlType argType);

        /**
         * Maps the column to every attribute (argName) the given policy covers.
         * <p>
         * Explicitly mapped argNames always take precedence over auto-mapped argNames.
         * <p>
         * Should multiple policies be applicable to the same argName, then the <i>first</i> will win (order in the configuration).
         * 
         * @see DefaultAutoMappingPolicy
         * @param policyCreator a function that will be called to create the policy for a given column
         * @return builder
         */
        public SingleTableDataColumnStep2 autoMapped(Function<DataColumn, AutoMappingPolicy> policyCreator);

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableDataColumnStep2 extends SingleTableDataColumnStep3 {

        /**
         * Defines that there is a <b>known value</b> <i>for every record</i> in your database.
         * <p>
         * <b>Important:</b> Marking an attribute as <i>always known</i> means that this column information is <i>available and NOT NULL for every record</i> of
         * your base audience. Consequently, if you configure <i>any</i> column in a table as <i>always known</i>, the table will automatically be marked as
         * {@link TableNature#containsAllIds()}.
         * 
         * @see DataColumn
         * @return builder
         */
        public SingleTableDataColumnStep3 alwaysKnown();

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableDataColumnStep3 extends SingleTableDataColumnStep4 {

        /**
         * Defines that the data for a single record can be <i>spread</i> across multiple rows.
         * <p>
         * This property <b>must</b> be set if the same column is mapped to multiple attributes (with filter column(s), see example below) or if this is a
         * collection attribute and you want to allow AND for two conditions of the same attribute (e.g., <code>color=blue AND color=red</code>.
         * <p>
         * Example:
         * 
         * <pre>
         *   +------+------------+-------------+
         *   | ID   | ENTRY_TYPE | ENTRY_VALUE |
         *   +------+------------+-------------+
         *   | 8599 | MODEL      | X4886       |
         *   +------+------------+-------------+
         *   | 8599 | BRAND      | QUACK       |
         *   +------+------------+-------------+
         *   | 8599 | COLOR      | BLACK       |
         *   +------+------------+-------------+
         *   | ...  | ...        | ...         |
         * </pre>
         * 
         * Let's say we want to query <code>model=X4886 AND brand=QUACK</code>.
         * <p>
         * Obviously, from the table above you <b>cannot</b> query
         * <code>... WHERE ENTRY_TYPE='MODEL' AND ENTRY_VALUE='X4886' AND ENTRY_TYPE='BRAND' AND ENTRY_VALUE='QUACK'</code>
         * <p>
         * The problem is that the first condition would <i>fix</i> the row, so the second condition will never become true (<i>accidental row-pinning</i>).
         * <p>
         * With the multi-row you tell the query builder about this problem and enforce a different query style.
         * 
         * @see DataColumn
         * @return builder
         */
        public SingleTableDataColumnStep4 multiRow();

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableDataColumnStep4 extends SingleTableAddColumnOrExit {

        /**
         * Defines an <i>extra column filter</i> on a column. This is required if the target column stores information effectively qualified or narrowed by
         * values of other column(s).
         * <p>
         * <b>Note:</b> The actual <i>value</i> (either hard-coded or from a variable) must be an ADL-value.
         * <p>
         * <b>Example:</b>
         * <ul>
         * <li><b>correct:</b> <code>filteredBy("ACTIVE", SQL_BOOLEAN, <b>"1"</b>)</code></li>
         * <li>incorrect: <code>filteredBy("ACTIVE", SQL_BOOLEAN, <s>"TRUE"</s>)</code></li>
         * </ul>
         * 
         * @see DataColumn
         * 
         * @param columnName name of another column of the <i>same table</i> to be tested for a <b>fixed value</b>
         * @param columnType type of the filter column for sql generation
         * @param value the filter, a <b>fixed value or variable placeholder ${varName}</b> to include into any query related to the data column to filter the
         *            results
         * @return builder
         */
        public SingleTableDataColumnStep4 filteredBy(String columnName, AdlSqlType columnType, String value);

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableDataColumnStep1 extends MultiTableAddTableOrExit {

        /**
         * Adds a data column of the given type to the current table
         * 
         * @see DataColumn
         * @param columnName SQL column name in your database table
         * @param columnType type of the column
         * @return builder
         */
        public MultiTableDataColumnStep1a dataColumn(String columnName, AdlSqlType columnType);

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableDataColumnStep1a {

        /**
         * Maps the column to an attribute (argName) of the logical data model.<br>
         * You can map the same column to multiple attributes but not vice-versa.
         * <p>
         * This method is meant for setting up the logical and physical data model at the same time.
         * <p>
         * The preferred way should be first setting up an {@link ArgMetaInfoLookup} with the argName's meta data and configuring the table with
         * {@link #mappedToArgName(String)}.
         * 
         * @param argName name of the attribute used in expressions that should be mapped to the column
         * @param argType
         * @return builder
         */
        public MultiTableDataColumnStep2 mappedToArgName(String argName, AdlType argType);

        /**
         * Maps the column to an attribute (argName) of the logical data model which has been configured in advance.<br>
         * You can map the same column to multiple attributes but not vice-versa.
         * 
         * @param argName name of the attribute used in expressions that should be mapped to the column
         * @return builder
         */
        public MultiTableDataColumnStep2 mappedToArgName(String argName);

        /**
         * Maps the column to every attribute (argName) the given extractor can extract a local argName from (not null).
         * <p>
         * In other words: The extractor is an UnaryOperator that at runtime takes the argName as input and either returns null (not applicable) or the
         * extracted <i>local name</i> of the attribute. This value will then be stored as a variable in the settings and can be references as
         * <code>${argName.local}</code> in a filter column definition.
         * <p>
         * <b>Example:</b> You have a number of attributes and the argName follows the pattern <code>section.<i>identifier</i>.int</code>, and you want to map
         * them to column <code>TBL.XYZ</code> of type INTEGER with the filter column F17 set to the the value of <i>identifier</i>.<br>
         * In this case you can specify the an extractor function that either returns null (not applicable, do not map) or the value of the
         * <i>identifier</i>.<br>
         * E.g.: <code>argName -> argName.endsWith(".int") ? argName.substring(argName.indexOf('.') + 1, argName.length-4) : null</code> would return
         * <code>47118</code> for <code>section.<i>47118</i>.int</code>. This value will automatically be assigned to the variable <code>argName.local</code>.
         * Now create a filter-column condition for column F17 and specify <code>${argName.local}</code> as its value.<br>
         * Any argName that is not explicitly mapped and that complies to the extractor will now be mapped to the current column with the filter condition on
         * F17 derived from the argName.
         * <p>
         * Explicitly mapped argNames always take precedence over auto-mapped argNames.
         * 
         * @see DefaultAutoMappingPolicy#ARG_NAME_LOCAL_PLACEHOLDER
         * @param extractor custom function to extract the local argName from the argName
         * @param argType
         * @return builder
         */
        public MultiTableDataColumnStep2 autoMapped(LocalArgNameExtractor extractor, AdlType argType);

        /**
         * Maps the column to every attribute (argName) the given policy covers.
         * <p>
         * Explicitly mapped argNames always take precedence over auto-mapped argNames.
         * <p>
         * Should multiple policies be applicable to the same argName, then the <i>first</i> will win (order in the configuration).
         * 
         * @see DefaultAutoMappingPolicy
         * @param policyCreator a function that will be called to create the policy for a given column
         * @return builder
         */
        public MultiTableDataColumnStep2 autoMapped(Function<DataColumn, AutoMappingPolicy> policyCreator);

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableDataColumnStep2 extends MultiTableDataColumnStep3 {

        /**
         * Defines that there is a <b>known value</b> <i>for every record</i> in your database.
         * <p>
         * <b>Important:</b> Marking an attribute as <i>always known</i> means that this column information is <i>available and NOT NULL for every record</i> of
         * your base audience. Consequently, if you configure <i>any</i> column in a table as <i>always known</i>, the table will automatically be marked as
         * {@link TableNature#containsAllIds()}.
         * 
         * @see DataColumn
         * @return builder
         */
        public MultiTableDataColumnStep3 alwaysKnown();

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableDataColumnStep3 extends MultiTableDataColumnStep4 {

        /**
         * Defines that the data for a single record can be <i>spread</i> across multiple rows.
         * <p>
         * This property <b>must</b> be set if the same column is mapped to multiple attributes (with filter column(s), see example below) or if this is a
         * collection attribute and you want to allow AND for two conditions of the same attribute (e.g., <code>color=blue AND color=red</code>.
         * <p>
         * Example:
         * 
         * <pre>
         *   +------+------------+-------------+
         *   | ID   | ENTRY_TYPE | ENTRY_VALUE |
         *   +------+------------+-------------+
         *   | 8599 | MODEL      | X4886       |
         *   +------+------------+-------------+
         *   | 8599 | BRAND      | QUACK       |
         *   +------+------------+-------------+
         *   | 8599 | COLOR      | BLACK       |
         *   +------+------------+-------------+
         *   | ...  | ...        | ...         |
         * </pre>
         * 
         * Let's say we want to query <code>model=X4886 AND brand=QUACK</code>.
         * <p>
         * Obviously, from the table above you <b>cannot</b> query
         * <code>... WHERE ENTRY_TYPE='MODEL' AND ENTRY_VALUE='X4886' AND ENTRY_TYPE='BRAND' AND ENTRY_VALUE='QUACK'</code>
         * <p>
         * The problem is that the first condition would <i>fix</i> the row, so the second condition will never become true (<i>accidental row-pinning</i>).
         * <p>
         * With the multi-row you tell the query builder about this problem and enforce a different query style.
         * 
         * @see DataColumn
         * @return builder
         */
        public MultiTableDataColumnStep4 multiRow();

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableDataColumnStep4 extends MultiTableDataColumnStep1 {

        /**
         * Defines an <i>extra column filter</i> on a column. This is required if the target column stores information effectively qualified or narrowed by
         * values of other column(s).
         * <p>
         * <b>Note:</b> The actual <i>value</i> (either hard-coded or from a variable) must be an ADL-value.
         * <p>
         * <b>Example:</b>
         * <ul>
         * <li><b>correct:</b> <code>filteredBy("ACTIVE", SQL_BOOLEAN, <b>"1"</b>)</code></li>
         * <li>incorrect: <code>filteredBy("ACTIVE", SQL_BOOLEAN, <s>"TRUE"</s>)</code></li>
         * </ul>
         * 
         * @see DataColumn
         * 
         * @param columnName name of another column of the <i>same table</i> to be tested for a <b>fixed value</b>
         * @param columnType type of the filter column for sql generation
         * @param value the filter, a <b>fixed value or variable placeholder ${varName}</b> to include into any query related to the data column to filter the
         *            results
         * @return builder
         */
        public MultiTableDataColumnStep4 filteredBy(String columnName, AdlSqlType columnType, String value);

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableStep2 extends SingleTableStep3 {

        /**
         * Configures this table as the primary table of a setup, means the table to start the base selection on before joining any other table. This only works
         * if all possible records are present in this table.
         * <p>
         * <ul>
         * <li>Setting this property <i>implicitly</i> configures {@link #thatContainsAllIds()}.</li>
         * <li>Only one table in a setup can be the primary table.</li>
         * </ul>
         * 
         * @return builder
         */
        public SingleTableStep3 asPrimaryTable();

        /**
         * Tells the system that every record is listed in this table. This allows the query builder to start a query on this table before joining any other
         * tables. In other words: it cannot happen that any valid record has no entry in this table.
         * 
         * @return builder
         */
        public SingleTableStep3 thatContainsAllIds();
    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableStep3 extends SingleTableStep4 {

        /**
         * Tells the query builder that the data in this table may be sparse, so data from a single row may not tell the full picture.
         * 
         * @see SingleTableConfig
         * 
         * @return builder
         */
        public SingleTableStep4 withSparseData();

        /**
         * Tells the query builder that for the same record (ID) there is either exactly one row or no row. In other words, selecting the ID column from this
         * won't return any duplicates (a logical unique constraint).
         * <p>
         * This setting is vital for negative queries. Let TBL be a table with the columns <code>ID</code> and <code>COLOR</code> and the record for ID=4711 has
         * two rows in it, one for color=red and another for color=blue. If you now make a query <code>COLOR&lt;&gt;'blue'</code> you get a wrong answer. Based
         * on this setting the converter can decide to add an existence check to avoid this problem.
         * 
         * @see SingleTableConfig
         * 
         * @return builder
         */
        public SingleTableStep4 withUniqueIds();

    }

    /**
     * Fluent API to build a {@link SingleTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link SingleTableAddColumnOrExit#get()}.
     */
    public static interface SingleTableStep4 {

        /**
         * Configures the id-column of the table for joining and result retrieval.
         * <p>
         * <ul>
         * <li>Every table in a setup must have <b>a single id-column</b>.</li>
         * <li>The ID-columns of all tables within a setup must be of <b>the same SQL-type</b>.</li>
         * <li>The concrete type of the ID-column is not relevant and thus not specified here.</li>
         * </ul>
         * 
         * @param idColumnName sql column name
         * @return builder
         */
        public SingleTableAddColumnOrExit idColumn(String idColumnName);

        /**
         * Defines an <i>extra column filter</i> to be applied to each query on this table.
         * <p>
         * This filter affects all data columns added later on. Just in case the table acts as the base query and none of its attributes is part of the where
         * clause then the filter will be added to the global where clause to ensure no record can be selected if it does not match this table condition.
         * <p>
         * Example: Let <code>TBL_BASE</code> be a base table that acts as the primary table, and only this table has a <code>TENANT</code>-column that isolated
         * data spaces from each other. Then you can add a column filter to this table and run all your queries with
         * {@link ConversionDirective#ENFORCE_PRIMARY_TABLE}.<br>
         * This will guarantee that no query can ever select any record not related to this tenant, because every query will start selecting from
         * <code>TBL_BASE</code> filtered by the given filter.
         * <p>
         * The same filter column can <b>either</b> be specified here for the whole table <b>or</b> for a particular column. You cannot define a filter column
         * on table level <i>and</i> specify the same column name again on data column level.
         * <p>
         * <b>Note:</b> The actual <i>value</i> (either hard-coded or from a variable) must be an ADL-value.
         * <p>
         * <b>Example:</b>
         * <ul>
         * <li><b>correct:</b> <code>filteredBy("ACTIVE", SQL_BOOLEAN, <b>"1"</b>)</code></li>
         * <li>incorrect: <code>filteredBy("ACTIVE", SQL_BOOLEAN, <s>"TRUE"</s>)</code></li>
         * </ul>
         * 
         * @see DataColumn
         * 
         * @param columnName name of another column of the <i>same table</i> to be tested for a <b>fixed value</b>
         * @param columnType type of the filter column for sql generation
         * @param value the filter, a <b>fixed value or variable placeholder ${varName}</b> to include into any query related to the data column to filter the
         *            results
         * @return builder
         */
        public SingleTableStep4 filteredBy(String columnName, AdlSqlType columnType, String value);

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableAddTable {

        /**
         * Adds another table to the setup step-by-step
         * 
         * @param tableName
         * @return builder
         */
        public MultiTableStep1 withTable(String tableName);

        /**
         * Adds another readily configured table
         * 
         * @param table
         * @return builder
         */
        public MultiTableAddTableOrExit withTable(SingleTableConfig table);

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableAddTableOrExit extends MultiTableAddTable, MultiTableExit {

        // allows the exit with get() after adding a table
    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableExit {

        /**
         * @return the multi-table configuration
         */
        public MultiTableConfig get();
    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableStep1 extends MultiTableStep2 {

        /**
         * Configures this table as the primary table of a setup, means the table to start the base selection on before joining any other table. This only works
         * if all possible records are present in this table.
         * <p>
         * <ul>
         * <li>Setting this property <i>implicitly</i> configures {@link #thatContainsAllIds()}.</li>
         * <li>Only one table in a setup can be the primary table.</li>
         * </ul>
         * 
         * @return builder
         */
        public MultiTableStep2 asPrimaryTable();

        /**
         * Tells the system that every record is listed in this table. This allows the query builder to start a query on this table before joining any other
         * tables. In other words: it cannot happen that any valid record has no entry in this table.
         * 
         * @return builder
         */
        public MultiTableStep2 thatContainsAllIds();
    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableStep2 extends MultiTableStep3 {

        /**
         * Tells the query builder that the data in this table may be sparse, so data from a single row may not tell the full picture.
         * 
         * @see SingleTableConfig
         * 
         * @return builder
         */
        public MultiTableStep3 withSparseData();

        /**
         * Tells the query builder that for the same record (ID) there is either exactly one row or no row. In other words, selecting the ID column from this
         * won't return any duplicates (a logical unique constraint).
         * <p>
         * This setting is vital for negative queries. Let TBL be a table with the columns <code>ID</code> and <code>COLOR</code> and the record for ID=4711 has
         * two rows in it, one for color=red and another for color=blue. If you now make a query <code>COLOR&lt;&gt;'blue'</code> you get a wrong answer. Based
         * on this setting the converter can decide to add an existence check to avoid this problem.
         * 
         * @see SingleTableConfig
         * 
         * @return builder
         */
        public MultiTableStep3 withUniqueIds();

    }

    /**
     * Fluent API to build a {@link MultiTableConfig}
     * <p>
     * Follow the method chain to finally obtain the result with {@link MultiTableExit#get()}.
     */
    public static interface MultiTableStep3 {

        /**
         * Configures the id-column of the table for joining and result retrieval.
         * <p>
         * <ul>
         * <li>Every table in a setup must have <b>a single id-column</b>.</li>
         * <li>The ID-columns of all tables within a setup must be of <b>the same SQL-type</b>.</li>
         * <li>The concrete type of the ID-column is not relevant and thus not specified here.</li>
         * </ul>
         * 
         * @param idColumnName sql column name
         * @return builder
         */
        public MultiTableDataColumnStep1 idColumn(String idColumnName);

        /**
         * Defines an <i>extra column filter</i> to be applied to each query on this table.
         * <p>
         * This filter affects all data columns added later on. Just in case the table acts as the base query and none of its attributes is part of the where
         * clause then the filter will be added to the global where clause to ensure no record can be selected if it does not match this table condition.
         * <p>
         * Example: Let <code>TBL_BASE</code> be a base table that acts as the primary table, and only this table has a <code>TENANT</code>-column that isolated
         * data spaces from each other. Then you can add a column filter to this table and run all your queries with
         * {@link ConversionDirective#ENFORCE_PRIMARY_TABLE}.<br>
         * This will guarantee that no query can ever select any record not related to this tenant, because every query will start selecting from
         * <code>TBL_BASE</code> filtered by the given filter.
         * <p>
         * The same filter column can <b>either</b> be specified here for the whole table <b>or</b> for a particular column. You cannot define a filter column
         * on table level <i>and</i> specify the same column name again on data column level.
         * <p>
         * <b>Note:</b> The actual <i>value</i> (either hard-coded or from a variable) must be an ADL-value.
         * <p>
         * <b>Example:</b>
         * <ul>
         * <li><b>correct:</b> <code>filteredBy("ACTIVE", SQL_BOOLEAN, <b>"1"</b>)</code></li>
         * <li>incorrect: <code>filteredBy("ACTIVE", SQL_BOOLEAN, <s>"TRUE"</s>)</code></li>
         * </ul>
         * 
         * @see DataColumn
         * 
         * @param columnName name of another column of the <i>same table</i> to be tested for a <b>fixed value</b>
         * @param columnType type of the filter column for sql generation
         * @param value the filter, a <b>fixed value or variable placeholder ${varName}</b> to include into any query related to the data column to filter the
         *            results
         * @return builder
         */
        public MultiTableStep3 filteredBy(String columnName, AdlSqlType columnType, String value);

    }

}
