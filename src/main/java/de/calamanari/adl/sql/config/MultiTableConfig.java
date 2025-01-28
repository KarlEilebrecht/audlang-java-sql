//@formatter:off
/*
 * MultiTableConfig
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.LookupException;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.MultiTableAddTable;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.MultiTableAddTableOrExit;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.MultiTableDataColumnStep1a;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.MultiTableDataColumnStep2;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.MultiTableStep1;
import de.calamanari.adl.sql.config.ConfigBuilderInterfaces.MultiTableStep2;

/**
 * A {@link MultiTableConfig} is composed of {@link SingleTableConfig}s to map argNames <i>uniquely</i> to columns of multiple tables.
 * 
 * @param tableConfigs overlap-free single-table configurations NOT NULL, not empty
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record MultiTableConfig(List<SingleTableConfig> tableConfigs) implements DataTableConfig {

    /**
     * Entry point for fluently setting up a table in the the physical data model and along with it the logical data model (mapped argNames)
     * <p>
     * This is more for testing convenience. The recommended way is first creating a logical data model with all the argNames and then calling
     * {@link #withTable(String, ArgMetaInfoLookup)} to setup the table(s) and create a mapping.
     * 
     * @param tableName physical table name
     * @return builder
     */
    public static MultiTableStep1 withTable(String tableName) {
        return new Builder(null).withTable(tableName);
    }

    /**
     * Entry point to start with the given table and fluently set up tables in the the physical data model and along with it the logical data model (mapped
     * argNames)
     * <p>
     * This is more for testing convenience. The recommended way is first creating a logical data model with all the argNames and then calling
     * {@link #withTable(String, ArgMetaInfoLookup)} to setup the table(s) and create a mapping.
     * 
     * @param table already configured table
     * @return builder
     */
    public static MultiTableAddTableOrExit withTable(SingleTableConfig table) {
        return new Builder(null).withTable(table);
    }

    /**
     * Entry point for setting up a table and for mapping it to existing argNames
     * 
     * @param tableName
     * @param argMetaInfoLookup (logical data model with all the argNames)
     * @return builder
     */
    public static MultiTableStep1 withTable(String tableName, ArgMetaInfoLookup argMetaInfoLookup) {
        return new Builder(argMetaInfoLookup).withTable(tableName);
    }

    /**
     * Entry point to start with the given table and fluently set up tables mapped to existing argNames
     * 
     * @param table first table to add
     * @param argMetaInfoLookup (logical data model with all the argNames)
     * @return builder
     */
    public static MultiTableAddTableOrExit withTable(SingleTableConfig table, ArgMetaInfoLookup argMetaInfoLookup) {
        return new Builder(argMetaInfoLookup).withTable(table);
    }

    /**
     * @param tableConfigs overlap-free single-table configurations NOT NULL, not empty
     */
    public MultiTableConfig(List<SingleTableConfig> tableConfigs) {
        if (tableConfigs == null || tableConfigs.isEmpty()) {
            throw new ConfigException("tableConfigs must not be null or empty, given: " + tableConfigs);
        }
        Map<String, SingleTableConfig> tempMap = new TreeMap<>();

        Set<String> mappedArgNames = new HashSet<>();

        String primaryTable = null;

        for (SingleTableConfig tableConfig : tableConfigs) {
            if (tableConfig == null) {
                throw new ConfigException("tableConfigs contains illegal null-entry, given: " + tableConfigs);
            }
            SingleTableConfig prevEntry = tempMap.putIfAbsent(tableConfig.tableName(), tableConfig);
            if (prevEntry != null) {
                throw new ConfigException(
                        String.format("Duplicate entry detected for tableName=%s, given: tableConfigs=%s", tableConfig.tableName(), tableConfigs));
            }
            primaryTable = validatePrimaryTable(tableConfigs, tableConfig, primaryTable);
            for (String argName : tableConfig.argColumnMap().keySet()) {
                if (mappedArgNames.contains(argName)) {
                    throw new ConfigException(String.format("Duplicate mapping detected for argName=%s, given: tableConfigs=%s", argName, tableConfigs),
                            AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
                }
                mappedArgNames.add(argName);
            }
        }
        this.tableConfigs = Collections.unmodifiableList(new ArrayList<>(tempMap.values()));
    }

    /**
     * @param tableConfigs
     * @param tableConfig
     * @param primaryTable
     * @return primaryTable or null if not yet defined
     * @throws ConfigException if there was a duplicate primary table
     */
    private String validatePrimaryTable(List<SingleTableConfig> tableConfigs, SingleTableConfig tableConfig, String primaryTable) {
        if (tableConfig.tableNature().isPrimaryTable()) {
            if (primaryTable != null) {
                throw new ConfigException(String.format("Duplicate primary table detected: %s vs. %s, given: tableConfigs=%s", primaryTable,
                        tableConfig.tableName(), tableConfigs));
            }
            else {
                primaryTable = tableConfig.tableName();
            }
        }
        return primaryTable;
    }

    @Override
    public boolean contains(String argName) {
        return tableConfigs.stream().anyMatch(tc -> tc.contains(argName));
    }

    @Override
    public ArgColumnAssignment lookupAssignment(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        return tableConfigs.stream().filter(tc -> tc.contains(argName)).findAny()
                .orElseThrow(() -> new LookupException("No meta data available for argName=" + argName,
                        AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName)))
                .lookupAssignment(argName, ctx);
    }

    @Override
    public int numberOfTables() {
        return tableConfigs.size();
    }

    @Override
    public TableMetaInfo lookupTableMetaInfo(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        return tableConfigs.stream().filter(tc -> tc.contains(argName)).findAny()
                .orElseThrow(() -> new LookupException("No TableMetaInfo available for argName=" + argName,
                        AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName)));
    }

    @Override
    public List<TableMetaInfo> allTableMetaInfos() {
        // here we can safely "abuse" type erasure because the list is immutable
        // and any entry in the list is a TableMetaInfo
        @SuppressWarnings("unchecked")
        List<TableMetaInfo> res = (List<TableMetaInfo>) ((List<?>) this.tableConfigs);
        return res;
    }

    /**
     * Fluent builder implementation to describe and map a table step by step.
     * <p>
     * <b>Note:</b> This class parameterizes the super-class <i>with itself</i> to keep some common code in the parent while staying compatible to individual
     * interfaces.
     * 
     * @see AbstractTableBuilder
     */
    private static class Builder extends AbstractTableBuilder<Builder>
            implements MultiTableDataColumnStep1a, MultiTableAddTable, MultiTableStep1, MultiTableStep2, MultiTableDataColumnStep2 {

        private final List<SingleTableConfig> tableConfigs = new ArrayList<>();

        private boolean havePendingTable = false;

        private Builder(ArgMetaInfoLookup argMetaInfoLookup) {
            super(argMetaInfoLookup);
        }

        @Override
        public MultiTableStep1 withTable(String tableName) {
            this.addPendingTable();
            this.tableName = tableName;
            this.havePendingTable = true;
            return this;
        }

        @Override
        public MultiTableAddTableOrExit withTable(SingleTableConfig table) {
            this.addPendingTable();
            tableConfigs.add(table);
            return this;
        }

        private void addPendingTable() {
            if (havePendingTable) {
                super.addPendingColumn();
                tableConfigs.add(new SingleTableConfig(tableName, idColumnName, determineTableNature(), this.tableFilterColumns, argColumnMap,
                        new CompositeAutoMappingPolicy(autoMappingPolicies)));
                this.tableName = null;
                this.argColumnMap.clear();
                this.containsAllIdsFlag = false;
                this.sparseFlag = false;
                this.uniqueIdsFlag = false;
                this.primaryTableFlag = false;
                this.haveDataColumns = false;
                this.autoMappingPolicies.clear();
                this.tableFilterColumns.clear();
                havePendingTable = false;
            }
        }

        @Override
        public MultiTableConfig get() {
            this.addPendingTable();
            return new MultiTableConfig(tableConfigs);
        }

    }

}
