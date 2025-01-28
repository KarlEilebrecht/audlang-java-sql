//@formatter:off
/*
 * AbstractTableBuilder
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.config.DefaultAutoMappingPolicy.LocalArgNameExtractor;

/**
 * Implementation backing the fluent configuration builders for column and tables.
 * <p>
 * This class avoids code duplication by putting some common logic in the abstract parent class, so the same code can be reused behind different builder
 * interfaces.
 * 
 * @param <T> concrete builder, selects the correct interface
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class AbstractTableBuilder<T extends AbstractTableBuilder<T>> {

    /**
     * builder instance (concrete class is parameterized with itself (T))
     */
    private final T builder;

    /**
     * the lookup (if present) or null if this builder implicitly builds the logical data model
     */
    protected final ArgMetaInfoLookup argMetaInfoLookup;

    /**
     * mappings collected for the current table
     */
    protected final Map<String, ArgColumnAssignment> argColumnMap = new HashMap<>();

    /**
     * Name of the current table
     */
    protected String tableName;

    /**
     * ID-column name for the current table
     */
    protected String idColumnName;

    /**
     * setting for the current table
     */
    protected boolean sparseFlag = false;

    /**
     * setting for the current table
     */
    protected boolean primaryTableFlag = false;

    /**
     * setting for the current table
     */
    protected boolean uniqueIdsFlag = false;

    /**
     * setting for the current table
     */
    protected boolean containsAllIdsFlag = false;

    /**
     * The current column
     */
    protected String columnName;

    /**
     * optional filters for all the columns of the current table.
     */
    protected List<FilterColumn> tableFilterColumns = new ArrayList<>();

    /**
     * type of the current column
     */
    protected AdlSqlType columnType;

    /**
     * setting for the current column
     */
    protected boolean multiRowFlag = false;

    /**
     * setting for the current column
     */
    protected boolean alwaysKnownFlag = false;

    /**
     * optional filters for the current column
     */
    protected List<FilterColumn> filterColumns = new ArrayList<>();

    /**
     * Indicates that the column building has started, this is only to distinguish between the initial state (no pending column) and the working state
     */
    protected boolean haveDataColumns = false;

    /**
     * argName mapped to the current column
     */
    protected String mappedArgName;

    /**
     * type of the arg mapped to the current column
     */
    protected AdlType mappedArgType;

    /**
     * For optional auto-mapping policy for the current column
     */
    protected LocalArgNameExtractor autoMappingExtractor;

    /**
     * For optional auto-mapping policy for the current column
     */
    protected Function<DataColumn, AutoMappingPolicy> autoMappingPolicyFunction;

    /**
     * List of all the policies for the current table
     */
    protected List<AutoMappingPolicy> autoMappingPolicies = new ArrayList<>();

    /**
     * @param argMetaInfoLookup optional meta model, may be null
     */
    protected AbstractTableBuilder(ArgMetaInfoLookup argMetaInfoLookup) {
        this.argMetaInfoLookup = argMetaInfoLookup;
        // need explicit cast, otherwise the compiler does not understand
        // that T must be the "type of this"
        @SuppressWarnings("unchecked")
        T instance = (T) this;
        builder = instance;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T idColumn(String idColumnName) {
        this.idColumnName = idColumnName;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T withSparseData() {
        this.sparseFlag = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T withUniqueIds() {
        this.uniqueIdsFlag = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T asPrimaryTable() {
        this.primaryTableFlag = true;
        this.containsAllIdsFlag = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T thatContainsAllIds() {
        this.containsAllIdsFlag = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T alwaysKnown() {
        this.alwaysKnownFlag = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T multiRow() {
        this.multiRowFlag = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T filteredBy(String columnName, AdlSqlType columnType, String value) {
        if (haveDataColumns) {
            this.filterColumns.add(new FilterColumn(tableName, columnName, columnType, value));
        }
        else {
            this.tableFilterColumns.add(new FilterColumn(tableName, columnName, columnType, value));
        }

        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T dataColumn(String columnName, AdlSqlType columnType) {
        addPendingColumn();
        this.columnName = columnName;
        this.columnType = columnType;
        this.haveDataColumns = true;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T mappedToArgName(String argName, AdlType argType) {
        if (argMetaInfoLookup != null) {
            if (!argMetaInfoLookup.contains(argName)) {
                throw new ConfigException(String.format("Illegal attempt to add a new argName=%s. Logical data model cannot be extended.", argName),
                        AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
            }
            AdlType type = argMetaInfoLookup.typeOf(argName);
            if (argType == null || !type.name().equals(argType.name())) {
                throw new ConfigException(
                        String.format("Illegal attempt to re-define the type of argName=%s. argMetaInfo: %s, given: %s", argName, type, argType),
                        AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
            }
        }
        else {
            this.mappedArgType = argType;
            this.mappedArgName = argName;
        }
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T mappedToArgName(String argName) {
        if (argMetaInfoLookup != null) {
            this.mappedArgType = argMetaInfoLookup.typeOf(argName);
            this.mappedArgName = argName;
        }
        else {
            throw new ConfigException(String.format("Unable to lookup type of argName=%s, logical data model (argMetaInfoLookup) not configured.", argName),
                    AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
        }
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T autoMapped(LocalArgNameExtractor extractor, AdlType argType) {
        this.mappedArgType = argType;
        this.autoMappingExtractor = extractor;
        return builder;
    }

    /**
     * @see ConfigBuilderInterfaces
     */
    public T autoMapped(Function<DataColumn, AutoMappingPolicy> policyCreator) {
        this.autoMappingPolicyFunction = policyCreator;
        return builder;
    }

    /**
     * Takes the collected data and adds a new column, resets the builder for the next column
     */
    protected void addPendingColumn() {
        if (haveDataColumns) {
            DataColumn dataColumn = new DataColumn(tableName, columnName, columnType, alwaysKnownFlag, multiRowFlag, filterColumns);
            if (autoMappingPolicyFunction != null) {
                autoMappingPolicies.add(autoMappingPolicyFunction.apply(dataColumn));
            }
            else if (autoMappingExtractor != null) {
                ArgColumnAssignment templateAssignment = new ArgColumnAssignment(new ArgMetaInfo("dummy", mappedArgType, alwaysKnownFlag, multiRowFlag),
                        dataColumn);
                autoMappingPolicies.add(new DefaultAutoMappingPolicy(autoMappingExtractor, templateAssignment));
            }
            else {
                ArgColumnAssignment assignment = new ArgColumnAssignment(new ArgMetaInfo(mappedArgName, mappedArgType, alwaysKnownFlag, multiRowFlag),
                        dataColumn);
                ArgColumnAssignment prevAssignment = argColumnMap.put(mappedArgName, assignment);
                if (prevAssignment != null) {
                    throw new ConfigException(
                            String.format("Duplicate mapping detected for argName=%s, given: %s vs. %s", mappedArgName, prevAssignment, assignment),
                            AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, mappedArgName));
                }
            }
            this.columnName = null;
            this.columnType = null;
            this.multiRowFlag = false;
            this.alwaysKnownFlag = false;
            this.filterColumns.clear();
            this.mappedArgName = null;
            this.mappedArgType = null;
            this.autoMappingPolicyFunction = null;
            this.autoMappingExtractor = null;
        }

    }

    /**
     * @return derives the table nature from the collected boolean flags
     */
    protected TableNature determineTableNature() {
        if (primaryTableFlag && sparseFlag) {
            return TableNature.PRIMARY_SPARSE;
        }
        else if (primaryTableFlag && uniqueIdsFlag) {
            return TableNature.PRIMARY_UNIQUE;
        }
        else if (primaryTableFlag) {
            return TableNature.PRIMARY;
        }
        else if (containsAllIdsFlag && sparseFlag) {
            return TableNature.ALL_IDS_SPARSE;
        }
        else if (containsAllIdsFlag && uniqueIdsFlag) {
            return TableNature.ALL_IDS_UNIQUE;
        }
        else if (containsAllIdsFlag) {
            return TableNature.ALL_IDS;
        }
        else if (sparseFlag) {
            return TableNature.ID_SUBSET_SPARSE;
        }
        else if (uniqueIdsFlag) {
            return TableNature.ID_SUBSET_UNIQUE;
        }
        else {
            return TableNature.ID_SUBSET;
        }

    }

}
