//@formatter:off
/*
 * SingleTableConfig
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

import static de.calamanari.adl.sql.config.ConfigUtils.assertContextNotNull;
import static de.calamanari.adl.sql.config.ConfigUtils.assertValidArgName;
import static de.calamanari.adl.sql.config.ConfigUtils.isValidArgName;
import static de.calamanari.adl.sql.config.ConfigUtils.isValidColumnName;
import static de.calamanari.adl.sql.config.ConfigUtils.isValidTableName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.TemplateParameterUtils;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.LookupException;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.SingleTableDataColumnStep1;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.SingleTableDataColumnStep2;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.SingleTableStep2;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.SingleTableStep3;

/**
 * A {@link SingleTableConfig} describes the mappings of argNames to columns of one particular table.
 * <p>
 * <b>Hint:</b> If any column is marked {@link DataColumn#isAlwaysKnown()} then the table will be automatically configured with
 * {@link TableNature#containsAllIds()}=<b>true</b>.
 * <p>
 * <b>On sparse data:</b>
 * <p>
 * We call it <b>sparse data</b> if the data of a table is <i>irregular</i>.
 * <p>
 * <i>Example 1: data copied but not merged from multiple sources</i>
 * <p>
 * 
 * <pre>
 * +------+-------+-------+-------+---------+
 * |  ID  | COLOR | SHAPE | RX0   | RX1     |
 * +------+-------+-------+-------+---------+
 * | 8231 | blue  | rect  | NULL  | NULL    |
 * +------+-------+-------+-------+---------+
 * | 8231 | NULL  | NULL  | 1     | NULL    |
 * +------+-------+-------+-------+---------+
 * | 8231 | NULL  | NULL  | NULL  | 1       |
 * +------+-------+-------+-------+---------+
 * | 9555 | ...   | ...   | ...   | ...     |
 * +------+-------+-------+-------+---------+
 * </pre>
 * <p>
 * Interestingly, none of the columns in the example above is {@link DataColumn#isMultiRow()}, or in other words: none of the attributes is a collection
 * ({@link ArgMetaInfo#isCollection()}). The problem is that the data has not been merged. To get the full picture, data <i>from several rows</i> must be
 * considered.
 * <p>
 * <i>Example 2: multi-row (collection) attribute with missing repetition</i>
 * <p>
 * 
 * <pre>
 * +------+-------+-------+--------+---------+
 * |  ID  | NAME  | BRAND | TYPE   | WEEKDAY |
 * +------+-------+-------+--------+---------+
 * | 8231 | CROW  | SKIRT | M365   | SUN     |
 * +------+-------+-------+--------+---------+
 * | 8231 | CROW  | NULL  | NULL   | MON     |
 * +------+-------+-------+--------+---------+
 * | 8231 | CROW  | NULL  | NULL   | WED     |
 * +------+-------+-------+--------+---------+
 * | 9555 | ...   | ...   | ...    | ...     |
 * +------+-------+-------+--------+---------+
 * </pre>
 * <p>
 * Here, WEEKDAY may contain multiple values (in multiple rows), but unfortunately, the values for some other fields have not been repeated since the first
 * occurrence of the record 8231 on Sunday. Again, a query may have to consider multiple rows to get the full picture.
 * <p>
 * In general it is recommended to streamline the onboarding process to avoid any <i>sparse data tables</i> for performance reasons and to avoid logical
 * problems.<br>
 * Sparse data makes all attributes <i>multi-row</i> and thus <i>independent</i> from each other. For example if you query
 * <code>brand = foo <b>AND</b> product = bar</code> with the two columns BRAND and PRODUCT side-by-side you would expect to include rows with both conditions
 * true for a single row. By marking the table as sparse or both columns as multi-row you lose this dependency. Consequently, the query would also match if
 * there are two rows, (BRAND:xyz, PRODUCT:bar) and (BRAND:foo, PRODUCT:hugo).
 * <p>
 * <b>Note:</b> For convenience reasons this class implements {@link ArgMetaInfoLookup} to play nicely with some core features.
 * <p>
 * Instances are <i>deeply immutable</i>.
 * 
 * @param tableName native (fully qualified) name of a table
 * @param idColumnName native name of the id-column in that table
 * @param tableNature characteristics of the table
 * @param tableFilters optional list of filter columns to be added on each selection from this table, null means empty
 * @param argColumnMap argument to column definition mapping
 * @param autoMappingPolicy optional policy for argNames not covered <i>explicitly</i>
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record SingleTableConfig(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
        Map<String, ArgColumnAssignment> argColumnMap, AutoMappingPolicy autoMappingPolicy) implements DataTableConfig, TableMetaInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTableConfig.class);

    /**
     * Entry point for fluently setting up a table in the the physical data model and along with it the logical data model (mapped argNames)
     * <p>
     * This is more for testing convenience. The recommended way is first creating a logical data model with all the argNames and then calling
     * {@link #forTable(String, ArgMetaInfoLookup)} to setup the table(s) and create a mapping.
     * 
     * @param tableName physical table name
     * @return builder
     */
    public static SingleTableStep2 forTable(String tableName) {
        return new Builder(tableName, null);
    }

    /**
     * Entry point for setting up a table and for mapping it to existing argNames
     * 
     * @param tableName
     * @param argMetaInfoLookup (logical data model with all the argNames)
     * @return builder
     */
    public static SingleTableStep2 forTable(String tableName, ArgMetaInfoLookup argMetaInfoLookup) {
        return new Builder(tableName, argMetaInfoLookup);
    }

    /**
     * Performs some essential non-null checks.
     * 
     * @param tableName
     * @param idColumnName
     * @param tableNature
     * @param tableFilters
     * @param argColumnMap
     * @param isSparse
     */
    private static void validateRequiredFields(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
            Map<String, ArgColumnAssignment> argColumnMap) {
        if (!isValidTableName(tableName) || !isValidColumnName(idColumnName)) {
            throw new ConfigException(
                    String.format("Invalid tableName or idColumnName, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                            tableName, idColumnName, tableNature, tableFilters, argColumnMap));
        }
        if (tableNature == null) {
            throw new ConfigException(String.format(
                    "Argument tableNature must not be null, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s", tableName,
                    idColumnName, tableNature, tableFilters, argColumnMap));
        }
    }

    /**
     * Ensures no nulls or contradictions (table names etc.) are sneaking in. Also, the ID-column must not be mapped to attributes.
     * 
     * @param tableName
     * @param idColumnName
     * @param tableNature
     * @param tableFilters
     * @param argColumnMap
     */
    private static void validateArgColumnMap(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
            Map<String, ArgColumnAssignment> argColumnMap) {
        if (argColumnMap == null) {
            throw new ConfigException(String.format(
                    "The parameter argColumnMap can be empty but must not be null, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                    tableName, idColumnName, tableNature, tableFilters, argColumnMap));
        }
        Set<String> allDataColumnNames = argColumnMap.values().stream().filter(Predicate.not(Objects::isNull)).map(ArgColumnAssignment::column)
                .map(DataColumn::columnName).collect(Collectors.toCollection(HashSet::new));

        for (Map.Entry<String, ArgColumnAssignment> entry : argColumnMap.entrySet()) {
            String argName = entry.getKey();
            if (!isValidArgName(argName)) {
                throw new ConfigException(String.format(
                        "Argument names in argColumnMap must not be null or empty, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            if (entry.getValue() == null) {
                throw new ConfigException(String.format(
                        "Assignments in argColumnMap must not be null, given: (%s=null) with tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        argName, tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            if (!argName.equals(entry.getValue().arg().argName())) {
                throw new ConfigException(String.format(
                        "Argument name mismatch in argColumnMap (expected same argument name), given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        tableName, idColumnName, tableNature, tableFilters, argColumnMap),
                        AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
            }
            if (!entry.getValue().column().tableName().equals(tableName)) {
                throw new ConfigException(String.format(
                        "Columns must all be in the same table, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFiltesr=%s, argColumnMap=%s",
                        tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            if (entry.getValue().column().columnName().equals(idColumnName)) {
                throw new ConfigException(String.format(
                        "Columns in argColumnMap must not include the ID-column, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }

            validateFilterColumns(tableName, idColumnName, tableNature, tableFilters, argColumnMap, allDataColumnNames, entry);
        }
    }

    /**
     * Ensures we are never using the ID-column as filter column
     * 
     * @param tableName
     * @param idColumnName
     * @param tableNature
     * @param tableFilters
     * @param argColumnMap
     * @param allDataColumnNames
     * @param entry
     */
    private static void validateFilterColumns(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
            Map<String, ArgColumnAssignment> argColumnMap, Set<String> allDataColumnNames, Map.Entry<String, ArgColumnAssignment> entry) {

        for (FilterColumn filterColumn : entry.getValue().column().filters()) {
            if (filterColumn.columnName().equals(idColumnName)) {
                throw new ConfigException(String.format(
                        "ID-column cannot be defined as a filter column, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            if (allDataColumnNames.contains(filterColumn.columnName())) {
                throw new ConfigException(String.format(
                        "Data column %s cannot be defined as a filter column, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        filterColumn.columnName(), tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }

            if (tableFilters != null) {
                List<String> tableFilterColumnNames = tableFilters.stream().map(FilterColumn::columnName).toList();
                if (entry.getValue().column().filters().stream().map(FilterColumn::columnName).anyMatch(tableFilterColumnNames::contains)) {
                    throw new ConfigException(String.format(
                            "Filters defined for column %s must not overlap with the table filters, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                            entry.getValue().column().columnName(), tableName, idColumnName, tableNature, tableFilters, argColumnMap));

                }
            }

        }
    }

    /**
     * For convenience reasons we auto-set the containsAllIds property if any column in this table is marked {@link DataColumn#isAlwaysKnown()}.
     * 
     * @param containsAllIds
     * @param argColumnMap
     * @return true if the table must contain all IDs.
     */
    private static boolean checkEffectivelyContainsAllIds(boolean containsAllIds, Map<String, ArgColumnAssignment> argColumnMap) {
        return containsAllIds || argColumnMap.values().stream().anyMatch(aca -> aca.column().isAlwaysKnown());
    }

    /**
     * Validates that the table nature does not stay in conflict with the column setting multi-row
     * 
     * @param tableName
     * @param idColumnName
     * @param tableNature
     * @param tableFilters
     * @param argColumnMap
     */
    private static void validateUniqueNotMultiRow(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
            Map<String, ArgColumnAssignment> argColumnMap) {
        if (tableNature.isIdUnique()) {
            for (ArgColumnAssignment assignment : argColumnMap.values()) {
                if (assignment.column().isMultiRow()) {
                    throw new ConfigException(String.format(
                            "Inconsistency detected: The table requires the records-IDs in the table to be unique but column %s is marked multi-row, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                            assignment.column().columnName(), tableName, idColumnName, tableNature, tableFilters, argColumnMap));

                }
            }
        }
    }

    /**
     * @param tableName
     * @param idColumnName
     * @param tableNature
     * @param tableFilters
     * @param argColumnMap
     */
    private static void validateTableFilters(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
            Map<String, ArgColumnAssignment> argColumnMap) {
        if (tableFilters == null) {
            return;
        }

        Set<String> allDataColumnNames = argColumnMap.values().stream().map(ArgColumnAssignment::column).map(DataColumn::columnName)
                .collect(Collectors.toCollection(HashSet::new));

        Map<String, FilterColumn> tableFilterColumns = new HashMap<>();
        for (FilterColumn filterColumn : tableFilters) {
            if (filterColumn.columnName().equals(idColumnName)) {
                throw new ConfigException(String.format(
                        "The ID-column %s cannot be specified as filter column of the table, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        filterColumn.columnName(), tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            if (allDataColumnNames.contains(filterColumn.columnName())) {
                throw new ConfigException(String.format(
                        "The data column %s cannot be specified as filter column the a table, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        filterColumn.columnName(), tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            if (tableFilterColumns.putIfAbsent(filterColumn.columnName(), filterColumn) != null) {
                throw new ConfigException(String.format(
                        "Duplicate table filter column %s detected, given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        filterColumn.columnName(), tableName, idColumnName, tableNature, tableFilters, argColumnMap));
            }
            List<String> variableNames = TemplateParameterUtils.extractVariableNames(filterColumn.filterValue());
            if (variableNames.contains(DefaultAutoMappingPolicy.VAR_ARG_NAME_LOCAL) || variableNames.contains(FilterColumn.VAR_ARG_NAME)) {
                throw new ConfigException(String.format(
                        "Cannot reference variable 'argName' in table filter (only global variables allowed), given: tableName=%s, idColumnName=%s, tableNature=%s, tableFilters=%s, argColumnMap=%s",
                        tableName, idColumnName, tableNature, tableFilters, argColumnMap));

            }
        }
    }

    /**
     * @param tableName native (fully qualified) name of a table
     * @param idColumnName native name of the id-column in that table
     * @param tableNature characteristics of the table
     * @param tableFilters optional list of filter columns to be added on each selection from this table, null means empty
     * @param argColumnMap argument to column definition mapping
     * @param autoMappingPolicy optional policy for argNames not covered <i>explicitly</i>
     */
    public SingleTableConfig(String tableName, String idColumnName, TableNature tableNature, List<FilterColumn> tableFilters,
            Map<String, ArgColumnAssignment> argColumnMap, AutoMappingPolicy autoMappingPolicy) {
        validateRequiredFields(tableName, idColumnName, tableNature, tableFilters, argColumnMap);
        validateTableFilters(tableName, idColumnName, tableNature, tableFilters, argColumnMap);
        validateArgColumnMap(tableName, idColumnName, tableNature, tableFilters, argColumnMap);
        validateUniqueNotMultiRow(tableName, idColumnName, tableNature, tableFilters, argColumnMap);

        Map<String, ArgColumnAssignment> tempMap = new TreeMap<>();
        for (Map.Entry<String, ArgColumnAssignment> entry : argColumnMap.entrySet()) {
            tempMap.put(entry.getKey(), entry.getValue());
        }
        if (checkEffectivelyContainsAllIds(tableNature.containsAllIds(), argColumnMap) && !tableNature.containsAllIds()) {
            TableNature tableNatureBefore = tableNature;
            switch (tableNature) {
            case ID_SUBSET:
                tableNature = TableNature.ALL_IDS;
                break;
            case ID_SUBSET_SPARSE:
                tableNature = TableNature.ALL_IDS_SPARSE;
                break;
            case ID_SUBSET_UNIQUE:
                tableNature = TableNature.ALL_IDS_UNIQUE;
                break;
            // $CASES-OMITTED$
            default:
            }
            if (tableNature != tableNatureBefore) {
                LOGGER.trace("Automatic TableNature adjustment for table {}: {} -> {} (attribute always known)", tableName, tableNatureBefore, tableNature);
            }
        }
        this.tableName = tableName;
        this.idColumnName = idColumnName;
        this.tableNature = tableNature;
        this.tableFilters = Collections.unmodifiableList(tableFilters == null ? Collections.emptyList() : new ArrayList<>(tableFilters));
        this.argColumnMap = Collections.unmodifiableMap(tempMap);
        this.autoMappingPolicy = autoMappingPolicy == null ? DefaultAutoMappingPolicy.NONE : autoMappingPolicy;
    }

    @Override
    public ArgColumnAssignment lookupAssignment(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        assertValidArgName(argName);
        ArgColumnAssignment assignment = argColumnMap.get(argName);
        if (assignment == null && autoMappingPolicy.isApplicable(argName)) {
            assignment = autoMappingPolicy.map(argName, ctx);
            if (assignment != null && !assignment.column().tableName().equals(this.tableName)) {
                throw new LookupException(
                        String.format("Inconsistent auto-mapping detected: The argName=%s was mapped to %s, a column that does not belong to this table (%s).",
                                argName, assignment, this.tableName),
                        AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
            }
        }
        if (assignment == null) {
            throw new LookupException("No meta data available for argName=" + argName);
        }
        return assignment;
    }

    @Override
    public boolean contains(String argName) {
        assertValidArgName(argName);
        return argColumnMap.containsKey(argName) || autoMappingPolicy.isApplicable(argName);
    }

    @Override
    public int numberOfTables() {
        return 1;
    }

    @Override
    public TableMetaInfo lookupTableMetaInfo(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        if (this.contains(argName)) {
            return this;
        }
        else {
            throw new LookupException("No TableMetaInfo available for argName=" + argName,
                    AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
        }
    }

    @Override
    public List<TableMetaInfo> allTableMetaInfos() {
        return Collections.singletonList(this);
    }

    /**
     * Fluent builder implementation to describe and map a table step by step.
     * <p>
     * <b>Note:</b> This class parameterizes the super-class with itself to keep some common code in the parent while staying compatible to individual
     * interfaces.
     * 
     * @see AbstractTableBuilder
     */
    private static class Builder extends AbstractTableBuilder<Builder>
            implements SingleTableDataColumnStep1, SingleTableStep2, SingleTableStep3, SingleTableDataColumnStep2 {

        private Builder(String tableName, ArgMetaInfoLookup argMetaInfoLookup) {
            super(argMetaInfoLookup);
            this.tableName = tableName;
        }

        @Override
        public SingleTableConfig get() {
            addPendingColumn();
            return new SingleTableConfig(tableName, idColumnName, this.determineTableNature(), this.tableFilterColumns, argColumnMap,
                    new CompositeAutoMappingPolicy(autoMappingPolicies));
        }

    }
}
