//@formatter:off
/*
 * MultiTableConfigTest
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

import static de.calamanari.adl.cnv.tps.DefaultAdlType.BOOL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DECIMAL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.INTEGER;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.STRING;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIGINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BOOLEAN;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_FLOAT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_INTEGER;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TINYINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.cnv.tps.DefaultArgMetaInfoLookup;
import de.calamanari.adl.sql.DefaultAdlSqlType;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class MultiTableConfigTest {

    @Test
    void testBasics() {

        MultiTableConfig config = makeTripleTable("TBL", "ID");

        assertEquals(3, config.numberOfTables());

        assertTrue(config.contains("arg1"));
        assertTrue(config.contains("arg8"));
        assertTrue(config.contains("arg11"));

        assertEquals("ID_C", config.lookupTableMetaInfo("arg10", ProcessContext.empty()).idColumnName());
        assertEquals("TBL_C", config.lookupTableMetaInfo("arg10", ProcessContext.empty()).tableName());
        assertEquals("ID_A", config.lookupTableMetaInfo("arg1", ProcessContext.empty()).idColumnName());
        assertEquals("TBL_B", config.lookupTableMetaInfo("arg7", ProcessContext.empty()).tableName());

        assertTrue(config.isAlwaysKnown("arg3"));
        assertTrue(config.isCollection("arg3"));
        assertFalse(config.isCollection("arg5"));

        assertEquals(INTEGER, config.lookup("arg9").type());
        assertFalse(config.lookup("arg9").isAlwaysKnown());

        assertEquals("COL4", config.lookupAssignment("arg4", ProcessContext.empty()).column().columnName());
        assertEquals("COL4", config.lookupColumn("arg4", ProcessContext.empty()).columnName());
        assertEquals("F1", config.lookupColumn("arg4", ProcessContext.empty()).filters().get(0).columnName());

        assertEquals(1, config.numberOfTablesInvolved(Arrays.asList("arg1", "arg2", "arg3"), ProcessContext.empty()));
        assertEquals(2, config.numberOfTablesInvolved(Arrays.asList("arg1", "arg2", "arg11"), ProcessContext.empty()));
    }

    @Test
    void testBuilder() {

        // @formatter:off

        SingleTableConfig table2 = SingleTableConfig.forTable("TBL2")
                    .thatContainsAllIds()
                    .withSparseData()
                    .idColumn("ID2")
                    .dataColumn("d1", SQL_FLOAT)
                        .mappedToArgName("arg9", DECIMAL)
                .get();
        
        MultiTableConfig config = MultiTableConfig
                .withTable("TBL1")
                    .asPrimaryTable()
                    .idColumn("ID1")
                    .dataColumn("d1", SQL_VARCHAR)
                        .mappedToArgName("arg1", STRING)
                        .alwaysKnown()
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg2", INTEGER)
                        .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                        .filteredBy("FILTER2", SQL_INTEGER, "3")
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg6", INTEGER)
                        .filteredBy("FILTER1", SQL_BOOLEAN, "0")
                    .dataColumn("d3", SQL_BIT)
                        .mappedToArgName("arg8", BOOL)
                        .multiRow() 
                        .filteredBy("FILTER2", SQL_INTEGER, "2")
                .withTable(table2)
                .withTable("TBL3")
                    .withUniqueIds()
                    .idColumn("ID3")
                    .dataColumn("d6", SQL_BIGINT)
                        .mappedToArgName("arg11", INTEGER)
                .get();
                    
        // @formatter:on

        assertEquals(3, config.numberOfTables());

        assertTrue(config.contains("arg1"));
        assertTrue(config.contains("arg8"));
        assertTrue(config.contains("arg11"));

        assertEquals("ID2", config.lookupTableMetaInfo("arg9", ProcessContext.empty()).idColumnName());
        assertEquals("TBL2", config.lookupTableMetaInfo("arg9", ProcessContext.empty()).tableName());
        assertEquals("ID1", config.lookupTableMetaInfo("arg1", ProcessContext.empty()).idColumnName());
        assertEquals("TBL3", config.lookupTableMetaInfo("arg11", ProcessContext.empty()).tableName());
        assertEquals(TableNature.ID_SUBSET_UNIQUE, config.lookupTableMetaInfo("arg11", ProcessContext.empty()).tableNature());

        assertTrue(config.isAlwaysKnown("arg1"));
        assertTrue(config.isCollection("arg8"));
        assertFalse(config.isCollection("arg11"));

        assertEquals(DECIMAL, config.lookup("arg9").type());
        assertEquals(DECIMAL, config.typeOf("arg9"));
        assertFalse(config.lookup("arg9").isAlwaysKnown());
        assertTrue(config.lookupTableMetaInfo("arg9", ProcessContext.empty()).tableNature().isSparse());
        assertFalse(config.lookupTableMetaInfo("arg9", ProcessContext.empty()).tableNature().isPrimaryTable());

        assertEquals("d2", config.lookupAssignment("arg6", ProcessContext.empty()).column().columnName());
        assertEquals("d2", config.lookupColumn("arg6", ProcessContext.empty()).columnName());
        assertEquals("FILTER2", config.lookupColumn("arg8", ProcessContext.empty()).filters().get(0).columnName());

        assertEquals(1, config.numberOfTablesInvolved(Arrays.asList("arg1", "arg2", "arg8"), ProcessContext.empty()));
        assertEquals(2, config.numberOfTablesInvolved(Arrays.asList("arg1", "arg2", "arg11"), ProcessContext.empty()));

        assertSame(config.tableConfigs(), config.allTableMetaInfos());

    }

    @Test
    void testSpecialCase() {

        assertThrows(ConfigException.class, () -> new MultiTableConfig(null));

        List<SingleTableConfig> emptyConfigs = Collections.emptyList();
        assertThrows(ConfigException.class, () -> new MultiTableConfig(emptyConfigs));

        List<SingleTableConfig> baseList = makeTripleTable("TBL", "ID").tableConfigs();

        Map<String, ArgColumnAssignment> argColumnMapAdd = new HashMap<>();

        argColumnMapAdd.put("argSome",
                new ArgColumnAssignment(new ArgMetaInfo("argSome", INTEGER, false, true), new DataColumn("TBL_B", "COL15", SQL_INTEGER, false, true, null)));

        SingleTableConfig configAdd = new SingleTableConfig("TBL_B", "ID_B", TableNature.ID_SUBSET, null, argColumnMapAdd, DefaultAutoMappingPolicy.NONE);

        List<SingleTableConfig> configsDuplicateTable = copyAdd(baseList, configAdd);
        assertThrows(ConfigException.class, () -> new MultiTableConfig(configsDuplicateTable));

        argColumnMapAdd.clear();

        argColumnMapAdd.put("arg7",
                new ArgColumnAssignment(new ArgMetaInfo("arg7", INTEGER, false, false), new DataColumn("TBL_D", "COL15", SQL_INTEGER, false, true, null)));

        configAdd = new SingleTableConfig("TBL_D", "ID_D", TableNature.ID_SUBSET, null, argColumnMapAdd, DefaultAutoMappingPolicy.NONE);

        List<SingleTableConfig> configsDuplicateArgMapping = copyAdd(baseList, configAdd);
        assertThrows(ConfigException.class, () -> new MultiTableConfig(configsDuplicateArgMapping));

        argColumnMapAdd.clear();

        argColumnMapAdd.put("argNew",
                new ArgColumnAssignment(new ArgMetaInfo("argNew", INTEGER, false, false), new DataColumn("TBL_D", "COL15", SQL_INTEGER, false, true, null)));

        configAdd = new SingleTableConfig("TBL_D", "ID_D", TableNature.PRIMARY, null, argColumnMapAdd, DefaultAutoMappingPolicy.NONE);

        List<SingleTableConfig> configsDuplicatePrimaryTable = copyAdd(baseList, configAdd);
        assertThrows(ConfigException.class, () -> new MultiTableConfig(configsDuplicatePrimaryTable));

        List<SingleTableConfig> configsNullEntry = copyAdd(baseList, null);
        assertThrows(ConfigException.class, () -> new MultiTableConfig(configsNullEntry));

    }

    @Test
    void testBuilderWithLookup() {

        // @formatter:off
        ArgMetaInfoLookup logicalDataModel = DefaultArgMetaInfoLookup
            .withArg("arg1").ofType(STRING)
            .withArg("arg2").ofType(INTEGER)
            .withArg("arg6").ofType(INTEGER)
            .withArg("arg7").ofType(INTEGER)
            .withArg("arg8").ofType(BOOL)
            .get();
                
        // @formatter:on

        // @formatter:off
        
        MultiTableConfig config = MultiTableConfig
                .withTable("TBL1", logicalDataModel)
                    .asPrimaryTable()
                    .idColumn("ID")
                    .dataColumn("d1", SQL_VARCHAR)
                        .mappedToArgName("arg1")
                        .alwaysKnown()
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg2")
                        .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                        .filteredBy("FILTER2", SQL_INTEGER, "3")
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg6")
                        .filteredBy("FILTER1", SQL_BOOLEAN, "0")
                    .dataColumn("d3", SQL_BIT)
                        .mappedToArgName("arg8")
                        .multiRow() 
                        .filteredBy("FILTER2", SQL_INTEGER, "2")
                .withTable("TBL2")
                    .withSparseData()
                    .idColumn("ID2")
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg7")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.primaryTable());
        assertTrue(config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableNature().isPrimaryTable());
        assertTrue(config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableNature().containsAllIds());
        assertFalse(config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableNature().isSparse());
        assertEquals("ID", config.lookupTableMetaInfo("arg1", ProcessContext.empty()).idColumnName());
        assertEquals(INTEGER, config.lookup("arg2").type());
        assertEquals(INTEGER, config.typeOf("arg2"));
        assertEquals("d2", config.lookupAssignment("arg6", ProcessContext.empty()).column().columnName());
        assertEquals("d2", config.lookupAssignment("arg2", ProcessContext.empty()).column().columnName());
        assertEquals(2, config.lookupAssignment("arg2", ProcessContext.empty()).column().filters().size());
        assertEquals(1, config.lookupAssignment("arg6", ProcessContext.empty()).column().filters().size());
        assertEquals(true, config.lookupAssignment("arg8", ProcessContext.empty()).column().isMultiRow());
        assertEquals(2, config.numberOfTables());
        assertEquals(true, config.contains("arg2"));
        assertEquals(false, config.contains("arg17"));
        assertEquals(true, config.isCollection("arg8"));
        assertEquals(true, config.isAlwaysKnown("arg1"));
        assertEquals(config.tableConfigs().get(0), config.lookupTableMetaInfo("arg1", ProcessContext.empty()));
        assertFalse(config.lookupTableMetaInfo("arg7", ProcessContext.empty()).tableNature().isPrimaryTable());
        assertTrue(config.lookupTableMetaInfo("arg7", ProcessContext.empty()).tableNature().isSparse());
        assertEquals("d2", config.lookupAssignment("arg7", ProcessContext.empty()).column().columnName());

    }

    @Test
    void testBuilderWithLookupAndAutoMapping() {

        // @formatter:off
        ArgMetaInfoLookup logicalDataModel = DefaultArgMetaInfoLookup
            .withArg("arg1").ofType(STRING)
            .withArg("arg2").ofType(INTEGER)
            .withArg("arg6").ofType(INTEGER)
            .withArg("arg7").ofType(INTEGER)
            .withArg("arg8").ofType(BOOL)
            .get();
                
        // @formatter:on

        // @formatter:off
        
        MultiTableConfig config = MultiTableConfig
                .withTable("TBL1", logicalDataModel)
                    .asPrimaryTable()
                    .idColumn("ID")
                    .dataColumn("d1", SQL_VARCHAR)
                        .mappedToArgName("arg1")
                        .alwaysKnown()
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg2")
                        .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                        .filteredBy("FILTER2", SQL_INTEGER, "3")
                    .dataColumn("d5", SQL_INTEGER)
                        .autoMapped(argName -> argName.endsWith(".int") ? argName.substring(0, argName.length()-4) : null, INTEGER)
                        .filteredBy("FILTER1", SQL_VARCHAR, "${argName.local}")
                    .dataColumn("d7", SQL_VARCHAR)
                        .autoMapped(dataColumn -> new DefaultAutoMappingPolicy(argName -> argName.endsWith(".s") ? argName.substring(0, argName.length()-2) : null, new ArgColumnAssignment(new ArgMetaInfo("dummy", DefaultAdlType.STRING, false, false), dataColumn)))
                        .filteredBy("FILTER2", SQL_VARCHAR, "${argName.local}")
                .withTable("TBL2")
                    .withSparseData()
                    .idColumn("ID2")
                    .dataColumn("d2", SQL_INTEGER)
                        .mappedToArgName("arg7")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.primaryTable());
        assertTrue(config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableNature().isPrimaryTable());
        assertTrue(config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableNature().containsAllIds());
        assertFalse(config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableNature().isSparse());
        assertEquals("ID", config.lookupTableMetaInfo("arg1", ProcessContext.empty()).idColumnName());
        assertEquals(INTEGER, config.lookup("arg2").type());
        assertEquals(INTEGER, config.typeOf("arg2"));
        assertEquals("d2", config.lookupAssignment("arg2", ProcessContext.empty()).column().columnName());
        assertEquals(2, config.lookupAssignment("arg2", ProcessContext.empty()).column().filters().size());
        assertEquals(2, config.numberOfTables());
        assertEquals(true, config.contains("arg2"));
        assertEquals(true, config.isAlwaysKnown("arg1"));
        assertEquals(true, config.contains("foo.int"));
        assertEquals(true, config.contains("foo.s"));

        assertEquals(config.tableConfigs().get(0), config.lookupTableMetaInfo("arg1", ProcessContext.empty()));
        assertTrue(config.lookupTableMetaInfo("foo.int", ProcessContext.empty()).tableNature().isPrimaryTable());
        assertFalse(config.lookupTableMetaInfo("foo.s", ProcessContext.empty()).tableNature().isSparse());
        assertEquals("d2", config.lookupAssignment("arg7", ProcessContext.empty()).column().columnName());

    }

    @Test
    void testBuilderAutoMappingOnly() {

        // @formatter:off
        
        MultiTableConfig config = MultiTableConfig
                .withTable("TBL1")
                    .asPrimaryTable()
                    .idColumn("ID")
                    .dataColumn("d5", SQL_INTEGER)
                        .autoMapped(argName -> argName.endsWith(".int") ? argName.substring(0, argName.length()-4) : null, INTEGER)
                        .filteredBy("FILTER1", SQL_VARCHAR, "${argName.local}")
                    .dataColumn("d7", SQL_VARCHAR)
                        .autoMapped(dataColumn -> new DefaultAutoMappingPolicy(argName -> argName.endsWith(".s") ? argName.substring(0, argName.length()-2) : null, new ArgColumnAssignment(new ArgMetaInfo("dummy", DefaultAdlType.STRING, false, false), dataColumn)))
                        .alwaysKnown()
                        .filteredBy("FILTER2", SQL_VARCHAR, "${argName.local}")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.primaryTable());
        assertEquals(1, config.numberOfTables());
        assertEquals(true, config.contains("foo.int"));
        assertEquals(true, config.contains("foo.s"));

        assertEquals(DefaultAdlType.STRING, config.lookup("bla.s").type());

        assertEquals(
                "ArgColumnAssignment[arg=ArgMetaInfo[argName=supi.int, type=INTEGER, isAlwaysKnown=false, isCollection=false], column=DataColumn[tableName=TBL1, columnName=d5, columnType=SQL_INTEGER, isAlwaysKnown=false, isMultiRow=false, filters=[?{ TBL1.FILTER1=${argName.local} }]]]",
                config.lookupAssignment("supi.int", ProcessContext.empty()).toString());

        assertTrue(config.lookupTableMetaInfo("foo.int", ProcessContext.empty()).tableNature().isPrimaryTable());
        assertFalse(config.lookupTableMetaInfo("foo.s", ProcessContext.empty()).tableNature().isSparse());

    }

    private static List<SingleTableConfig> copyAdd(List<SingleTableConfig> baseList, SingleTableConfig extra) {
        List<SingleTableConfig> res = new ArrayList<>();
        res.addAll(baseList);
        res.add(extra);
        return res;
    }

    private static MultiTableConfig makeTripleTable(String tableName, String idColumnName) {

        Map<String, ArgColumnAssignment> argColumnMap1 = new HashMap<>();

        argColumnMap1.put("arg1", new ArgColumnAssignment(new ArgMetaInfo("arg1", INTEGER, false, true),
                new DataColumn(tableName + "_A", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMap1.put("arg2", new ArgColumnAssignment(new ArgMetaInfo("arg2", STRING, true, false),
                new DataColumn(tableName + "_A", "COL2", SQL_VARCHAR, false, false, null)));
        argColumnMap1.put("arg3",
                new ArgColumnAssignment(new ArgMetaInfo("arg3", STRING, true, true), new DataColumn(tableName + "_A", "COL3", SQL_VARCHAR, true, true, null)));
        argColumnMap1.put("arg4", new ArgColumnAssignment(new ArgMetaInfo("arg4", STRING, false, false), new DataColumn(tableName + "_A", "COL4",
                DefaultAdlSqlType.SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn(tableName + "_A", "F1", SQL_TINYINT, "126")))));
        argColumnMap1.put("arg5", new ArgColumnAssignment(new ArgMetaInfo("arg5", STRING, true, false),
                new DataColumn(tableName + "_A", "COL2", SQL_VARCHAR, false, false, null)));

        Map<String, ArgColumnAssignment> argColumnMap2 = new HashMap<>();

        argColumnMap2.put("arg7", new ArgColumnAssignment(new ArgMetaInfo("arg7", INTEGER, false, true),
                new DataColumn(tableName + "_B", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMap2.put("arg8", new ArgColumnAssignment(new ArgMetaInfo("arg8", STRING, true, false),
                new DataColumn(tableName + "_B", "COL2", SQL_VARCHAR, false, false, null)));

        Map<String, ArgColumnAssignment> argColumnMap3 = new HashMap<>();

        argColumnMap3.put("arg9", new ArgColumnAssignment(new ArgMetaInfo("arg9", INTEGER, false, true),
                new DataColumn(tableName + "_C", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMap3.put("arg10", new ArgColumnAssignment(new ArgMetaInfo("arg10", STRING, false, false),
                new DataColumn(tableName + "_C", "COL2", SQL_VARCHAR, false, false, null)));
        argColumnMap3.put("arg11", new ArgColumnAssignment(new ArgMetaInfo("arg11", STRING, false, false), new DataColumn(tableName + "_C", "COL4",
                DefaultAdlSqlType.SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn(tableName + "_C", "F1", SQL_TINYINT, "126")))));

        SingleTableConfig config1 = new SingleTableConfig(tableName + "_A", idColumnName + "_A", TableNature.PRIMARY, null, argColumnMap1,
                DefaultAutoMappingPolicy.NONE);
        SingleTableConfig config2 = new SingleTableConfig(tableName + "_B", idColumnName + "_B", TableNature.ALL_IDS_SPARSE, null, argColumnMap2,
                DefaultAutoMappingPolicy.NONE);
        SingleTableConfig config3 = new SingleTableConfig(tableName + "_C", idColumnName + "_C", TableNature.ID_SUBSET, null, argColumnMap3,
                DefaultAutoMappingPolicy.NONE);

        return new MultiTableConfig(Arrays.asList(config1, config2, config3));

    }

}
