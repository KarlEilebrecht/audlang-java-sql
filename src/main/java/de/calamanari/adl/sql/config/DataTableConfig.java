//@formatter:off
/*
 * DataTableConfig
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

import java.util.Arrays;
import java.util.List;

import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.LookupException;

/**
 * A concrete {@link DataTableConfig} maps columns to argNames.
 * <p>
 * <b>Notes:</b>
 * <ul>
 * <li>The same attribute (argName) cannot be mapped to multiple columns.</li>
 * <li>It is possible to map multiple attributes to the same column.</li>
 * </ul>
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface DataTableConfig extends ArgMetaInfoLookup {

    /**
     * Determines the column assignment for the given argName.
     * 
     * @param argName
     * @param ctx
     * @return the configured {@link ArgColumnAssignment} if {@link #contains(String)}=true, otherwise throws exception
     * @throws LookupException if there is no assignment for the given argName
     * @throws IllegalArgumentException if argName was null
     */
    ArgColumnAssignment lookupAssignment(String argName, ProcessContext ctx);

    /**
     * Returns the total number of tables in this configuration
     * 
     * @return number of tables this config is based on, &gt;=1
     */
    int numberOfTables();

    /**
     * Performs a lookup for each of the given argNames and counts the related tables (distinct count).
     * 
     * @param argNames
     * @param ctx
     * @return the number of tables mapped to the given list of attributes
     * @throws LookupException if there is no assignment for any given argName
     * @throws IllegalArgumentException if any given argName was null
     */
    default int numberOfTablesInvolved(List<String> argNames, ProcessContext ctx) {
        return this.tablesInvolved(argNames, ctx).size();
    }

    /**
     * Returns a list of the names of all table any of the given argNames is mapped to.
     * <p>
     * The result order depends on the order of the provided argNames.
     * 
     * @param argNames
     * @param ctx
     * @return the names of the tables mapped to the given list of attributes, free of duplicates, sorted
     * @throws LookupException if there is no assignment for any given argName
     * @throws IllegalArgumentException if any given argName was null
     */
    default List<String> tablesInvolved(List<String> argNames, ProcessContext ctx) {
        return this.tableMetaInfosInvolved(argNames, ctx).stream().map(TableMetaInfo::tableName).distinct().toList();
    }

    @Override
    default ArgMetaInfo lookup(String argName) {
        if (argName == null) {
            throw new IllegalArgumentException("Parameter argName must not be null.");
        }
        return lookupAssignment(argName, ProcessContext.empty()).arg();
    }

    /**
     * Returns the data column mapped to the given argName.
     * 
     * @param argName
     * @param ctx
     * @return the configured {@link DataColumn} if {@link #contains(String)}=true, otherwise throws exception
     * @throws LookupException if there is no assignment for the given argName
     * @throws IllegalArgumentException if argName was null
     */
    default DataColumn lookupColumn(String argName, ProcessContext ctx) {
        if (argName == null) {
            throw new IllegalArgumentException("Parameter argName must not be null.");
        }
        return lookupAssignment(argName, ctx).column();
    }

    /**
     * Returns the table meta infos for the column that is mapped to the given argName
     * 
     * @param argName
     * @param ctx
     * @return meta data
     * @throws LookupException if there is no {@link TableMetaInfo} for the given argName
     * @throws IllegalArgumentException if argName was null
     */
    TableMetaInfo lookupTableMetaInfo(String argName, ProcessContext ctx);

    /**
     * Returns the meta information for all the tables in this configuration.
     * <p>
     * <b>Important:</b> Although the order is not defined, best case the order in the list reflects the order of configuration.<br>
     * In any case it must be <i>stable</i> and it should be deterministic because this is the order the default implementations of several other methods relies
     * on. If the result is composed from an underlying <i>set</i>, then the list should be sorted before returning to avoid indeterministic behavior like flaky
     * tests or unexplainable variations when generating queries.
     * 
     * @return all configured tables
     */
    List<TableMetaInfo> allTableMetaInfos();

    /**
     * Returns a list of the meta infos of all table any of the given argNames is mapped to.
     * <p>
     * The order result order depends on the order of the provided argNames.
     * 
     * @param argNames
     * @param ctx
     * @return the names of the tables mapped to the given list of attributes, free of duplicates
     * @throws LookupException if there is no assignment for any given argName
     * @throws IllegalArgumentException if any given argName was null
     */
    default List<TableMetaInfo> tableMetaInfosInvolved(List<String> argNames, ProcessContext ctx) {
        return argNames.stream().map(argName -> lookupTableMetaInfo(argName, ctx)).distinct().toList();
    }

    /**
     * Returns all tables from the config that contain all IDs.
     * 
     * @see TableNature#containsAllIds()
     * @return list of the names of all configured tables that are marked {@link TableNature#containsAllIds()} or "the only table at all"
     */
    default List<String> tablesThatContainAllIds() {
        if (allTableMetaInfos().size() == 1) {
            return Arrays.asList(allTableMetaInfos().get(0).tableName());
        }
        return allTableMetaInfos().stream().filter(info -> info.tableNature().containsAllIds()).map(TableMetaInfo::tableName).toList();
    }

    /**
     * Returns the primary table of this configuration.
     * 
     * @see TableNature#isPrimaryTable()
     * @return name of the table that is marked as primary table or <b>null</b> if no table is marked as primary table
     */
    default String primaryTable() {
        return allTableMetaInfos().stream().filter(info -> info.tableNature().isPrimaryTable()).findFirst().map(TableMetaInfo::tableName).orElse(null);
    }

    /**
     * Shorthand for finding the meta info in {@link #allTableMetaInfos()} by table name
     * 
     * @param tableName
     * @return table meta info
     * @throws LookupException if there is no info about the given table name
     * @throws IllegalArgumentException if any given tableName was null
     */
    default TableMetaInfo lookupTableMetaInfoByTableName(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Parameter tableName must not be null.");
        }
        return allTableMetaInfos().stream().filter(info -> info.tableName().equals(tableName)).findFirst()
                .orElseThrow(() -> new LookupException(String.format("No meta information available for table %s", tableName)));
    }
}
