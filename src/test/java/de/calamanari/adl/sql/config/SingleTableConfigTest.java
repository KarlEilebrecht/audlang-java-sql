//@formatter:off
/*
 * SingleTableConfigTest
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.Flag;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.cnv.tps.DefaultArgMetaInfoLookup;
import de.calamanari.adl.cnv.tps.LookupException;
import de.calamanari.adl.sql.DefaultAdlSqlType;

import static de.calamanari.adl.cnv.tps.DefaultAdlType.BOOL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.INTEGER;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.STRING;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BOOLEAN;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_INTEGER;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TINYINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class SingleTableConfigTest {

    @Test
    void testBasics() {

        assertConstruct("TBL1", "ID");

    }

    @Test
    void testBuilder() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                .filteredBy("FILTER2", SQL_INTEGER, "3")
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg6", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "FALSE")
            .dataColumn("d3", SQL_BIT)
                .mappedToArgName("arg8", BOOL)
                .multiRow() 
                .filteredBy("FILTER2", SQL_INTEGER, "2")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.tableName());
        assertTrue(config.tableNature().isPrimaryTable());
        assertTrue(config.tableNature().containsAllIds());
        assertFalse(config.tableNature().isSparse());
        assertEquals("ID", config.idColumnName());
        assertEquals(4, config.argColumnMap().size());
        assertEquals(INTEGER, config.lookup("arg2").type());
        assertEquals("d2", config.lookupAssignment("arg6", ProcessContext.empty()).column().columnName());
        assertEquals("d2", config.lookupAssignment("arg2", ProcessContext.empty()).column().columnName());
        assertEquals(2, config.lookupAssignment("arg2", ProcessContext.empty()).column().filters().size());
        assertEquals(1, config.lookupAssignment("arg6", ProcessContext.empty()).column().filters().size());
        assertEquals(true, config.lookupAssignment("arg8", ProcessContext.empty()).column().isMultiRow());
        assertEquals(1, config.numberOfTables());
        assertEquals(true, config.contains("arg2"));
        assertEquals(false, config.contains("arg17"));
        assertEquals(true, config.isCollection("arg8"));
        assertEquals(true, config.isAlwaysKnown("arg1"));
        assertEquals(config, config.lookupTableMetaInfo("arg1", ProcessContext.empty()));

        assertSame(config, config.allTableMetaInfos().get(0));

        assertEquals(1, config.tablesInvolved(Arrays.asList("arg1", "arg2", "arg8"), ProcessContext.empty()).size());
        assertEquals("TBL1", config.tablesInvolved(Arrays.asList("arg1", "arg2", "arg8"), ProcessContext.empty()).get(0));
        assertEquals("TBL1", config.tablesThatContainAllIds().get(0));
        assertEquals("TBL1", config.primaryTable());
        assertSame(config, config.lookupTableMetaInfoByTableName("TBL1"));

    }

    @Test
    void testBuilderUniqueId() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .withUniqueIds()
            .filteredBy("TENANT", SQL_VARCHAR, "${TENANT}")
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                .filteredBy("FILTER2", SQL_INTEGER, "3")
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg6", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "FALSE")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.tableName());
        assertTrue(config.tableNature().isPrimaryTable());
        assertTrue(config.tableNature().containsAllIds());
        assertFalse(config.tableNature().isSparse());
        assertEquals("ID", config.idColumnName());
        assertEquals(3, config.argColumnMap().size());
        assertEquals(INTEGER, config.lookup("arg2").type());
        assertEquals("d2", config.lookupAssignment("arg6", ProcessContext.empty()).column().columnName());
        assertEquals("d2", config.lookupAssignment("arg2", ProcessContext.empty()).column().columnName());
        assertEquals(2, config.lookupAssignment("arg2", ProcessContext.empty()).column().filters().size());
        assertEquals(1, config.lookupAssignment("arg6", ProcessContext.empty()).column().filters().size());
        assertEquals(1, config.numberOfTables());
        assertEquals(true, config.contains("arg2"));
        assertEquals(false, config.contains("arg17"));
        assertEquals(true, config.isAlwaysKnown("arg1"));
        assertEquals(config, config.lookupTableMetaInfo("arg1", ProcessContext.empty()));
        assertEquals(1, config.lookupTableMetaInfo("arg1", ProcessContext.empty()).tableFilters().size());

        assertSame(config, config.allTableMetaInfos().get(0));

        assertEquals(TableNature.PRIMARY_UNIQUE, config.tableNature());
        assertEquals(1, config.tablesInvolved(Arrays.asList("arg1", "arg2"), ProcessContext.empty()).size());
        assertEquals("TBL1", config.tablesInvolved(Arrays.asList("arg1", "arg2"), ProcessContext.empty()).get(0));
        assertEquals("TBL1", config.tablesThatContainAllIds().get(0));
        assertEquals("TBL1", config.primaryTable());
        assertSame(config, config.lookupTableMetaInfoByTableName("TBL1"));

    }

    @Test
    void testBuilderSpecialCase1() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                .filteredBy("FILTER2", SQL_INTEGER, "3")
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg6", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "FALSE")
            .dataColumn("d3", SQL_BIT)
                .mappedToArgName("arg8", BOOL)
                .multiRow() 
                .filteredBy("FILTER2", SQL_INTEGER, "2")
            .get();
        
        // @formatter:on

        ProcessContext emptySettings = ProcessContext.empty();

        assertThrows(IllegalArgumentException.class, () -> config.contains(null));
        assertThrows(IllegalArgumentException.class, () -> config.isAlwaysKnown(null));
        assertThrows(IllegalArgumentException.class, () -> config.isCollection(null));
        assertThrows(IllegalArgumentException.class, () -> config.lookup(null));
        assertThrows(IllegalArgumentException.class, () -> config.lookupAssignment(null, emptySettings));
        assertThrows(IllegalArgumentException.class, () -> config.lookupAssignment("arg1", null));
        assertThrows(IllegalArgumentException.class, () -> config.lookupTableMetaInfo(null, emptySettings));
        assertThrows(LookupException.class, () -> config.lookupTableMetaInfo("unknown", emptySettings));
        assertThrows(IllegalArgumentException.class, () -> config.lookupColumn(null, emptySettings));
        assertThrows(IllegalArgumentException.class, () -> config.lookupColumn("arg6", null));
        assertThrows(IllegalArgumentException.class, () -> config.lookupTableMetaInfoByTableName(null));

    }

    // Warning below ignored (difficult to avoid with fluent API testing)
    @Test
    @SuppressWarnings("java:S5778")
    void testBuilderSpecialCase2() {

        // @formatter:off
        
        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1"));

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                .dataColumn("d2", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .withUniqueIds()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                .dataColumn("d2", SQL_VARCHAR)
                    .mappedToArgName("arg2", STRING)
                    .multiRow()
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                    .alwaysKnown()
                .dataColumn("d2", SQL_INTEGER)
                    .mappedToArgName("arg2", INTEGER)
                    .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                    .filteredBy("ID", SQL_INTEGER, "3")
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                    .alwaysKnown()
                .dataColumn("d2", SQL_INTEGER)
                    .mappedToArgName("arg2", INTEGER)
                    .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                    .filteredBy("d1", SQL_INTEGER, "3")
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .filteredBy("ID", SQL_BOOLEAN, "TRUE")
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                    .alwaysKnown()
                .dataColumn("d2", SQL_INTEGER)
                    .mappedToArgName("arg2", INTEGER)
                    .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                    .filteredBy("FILTER2", SQL_INTEGER, "3")
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .filteredBy("d1", SQL_BOOLEAN, "1")
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                    .alwaysKnown()
                .dataColumn("d2", SQL_INTEGER)
                    .mappedToArgName("arg2", INTEGER)
                    .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                    .filteredBy("FILTER2", SQL_INTEGER, "3")
                .get());

        
        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .filteredBy("FILTER1", SQL_BOOLEAN, "TRUE")
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                .filteredBy("FILTER2", SQL_INTEGER, "3")
            .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .filteredBy("XYZ", SQL_VARCHAR, "${argName}")
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                    .alwaysKnown()
                .dataColumn("d2", SQL_INTEGER)
                    .mappedToArgName("arg2", INTEGER)
                    .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                    .filteredBy("FILTER2", SQL_INTEGER, "3")
                .get());        

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1")
                .asPrimaryTable()
                .filteredBy("XYZ", SQL_VARCHAR, "${argName.local}")
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", STRING)
                    .alwaysKnown()
                .dataColumn("d2", SQL_INTEGER)
                    .mappedToArgName("arg2", INTEGER)
                    .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                    .filteredBy("FILTER2", SQL_INTEGER, "3")
                .get());        

        // @formatter:on

    }

    // Warning below ignored (difficult to avoid with fluent API testing)
    @Test
    @SuppressWarnings("java:S5778")
    void testBuildeDuplicateTableFilter() {

        // @formatter:off
        
        assertThrows(ConfigException.class, ()-> SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .filteredBy("FOO", SQL_VARCHAR, "BAR")
            .filteredBy("FOO", SQL_VARCHAR, "BAR")
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "1")
                .filteredBy("FILTER2", SQL_INTEGER, "3")
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg6", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "FALSE")
            .dataColumn("d3", SQL_BIT)
                .mappedToArgName("arg8", BOOL)
                .multiRow() 
                .filteredBy("FILTER2", SQL_INTEGER, "2")
            .get());
        
        // @formatter:on

    }

    @Test
    void testBuilderWithLookup() {

        // @formatter:off
        ArgMetaInfoLookup logicalDataModel = DefaultArgMetaInfoLookup
            .withArg("arg1").ofType(STRING)
            .withArg("arg2").ofType(INTEGER)
            .withArg("arg6").ofType(INTEGER)
            .withArg("arg8").ofType(BOOL)
            .get();
                
        // @formatter:on

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1", logicalDataModel)
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
                .filteredBy("FILTER1", SQL_BOOLEAN, "FALSE")
            .dataColumn("d3", SQL_BIT)
                .mappedToArgName("arg8")
                .multiRow() 
                .filteredBy("FILTER2", SQL_INTEGER, "2")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.tableName());
        assertTrue(config.tableNature().isPrimaryTable());
        assertTrue(config.tableNature().containsAllIds());
        assertFalse(config.tableNature().isSparse());
        assertEquals("ID", config.idColumnName());
        assertEquals(4, config.argColumnMap().size());
        assertEquals(INTEGER, config.lookup("arg2").type());
        assertEquals(INTEGER, config.typeOf("arg2"));
        assertEquals("d2", config.lookupAssignment("arg6", ProcessContext.empty()).column().columnName());
        assertEquals("d2", config.lookupAssignment("arg2", ProcessContext.empty()).column().columnName());
        assertEquals(2, config.lookupAssignment("arg2", ProcessContext.empty()).column().filters().size());
        assertEquals(1, config.lookupAssignment("arg6", ProcessContext.empty()).column().filters().size());
        assertEquals(true, config.lookupAssignment("arg8", ProcessContext.empty()).column().isMultiRow());
        assertEquals(1, config.numberOfTables());
        assertEquals(true, config.contains("arg2"));
        assertEquals(false, config.contains("arg17"));
        assertEquals(true, config.isCollection("arg8"));
        assertEquals(true, config.isAlwaysKnown("arg1"));
        assertEquals(config, config.lookupTableMetaInfo("arg1", ProcessContext.empty()));
    }

    // Warning below ignored (difficult to avoid with fluent API testing)
    @Test
    @SuppressWarnings("java:S5778")
    void testBuilderWithLookupSpecialCase() {

        // @formatter:off
        ArgMetaInfoLookup logicalDataModel = DefaultArgMetaInfoLookup
            .withArg("arg1").ofType(STRING)
                .thatIsAlwaysKnown()
            .withArg("arg2").ofType(INTEGER)
            .withArg("arg6").ofType(INTEGER)
            .withArg("arg8").ofType(BOOL)
            .get();
                
        // @formatter:on

        // @formatter:off

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1", logicalDataModel)
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("ID", SQL_VARCHAR)
                    .mappedToArgName("arg1")
                .get());

        
        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1", logicalDataModel)
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("someUnknownArg"));

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1", logicalDataModel)
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1", INTEGER));

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1", logicalDataModel)
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1")
                .get());

        assertThrows(ConfigException.class, () -> SingleTableConfig.forTable("TBL1", logicalDataModel)
                .asPrimaryTable()
                .idColumn("ID")
                .dataColumn("d1", SQL_VARCHAR)
                    .mappedToArgName("arg1")
                .dataColumn("d2", SQL_VARCHAR)
                    .mappedToArgName("arg1")
                .get());

        
        // @formatter:on

    }

    @Test
    void testSpecialCase() {

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, null);

        assertFailedConstruct("TBL1", null, TableNature.ID_SUBSET, Collections.emptyMap());

        assertFailedConstruct(null, "ID", TableNature.ID_SUBSET, Collections.emptyMap());

        assertFailedConstruct("", "ID", TableNature.ID_SUBSET, Collections.emptyMap());

        assertFailedConstruct("TBL 1", "ID", TableNature.ID_SUBSET, Collections.emptyMap());

        assertFailedConstruct("TBL1", "", TableNature.ID_SUBSET, Collections.emptyMap());

        assertFailedConstruct("TBL1", "ID NEW", TableNature.ID_SUBSET, Collections.emptyMap());

        assertFailedConstruct("TBL1", "ID", null, Collections.emptyMap());

        Map<String, ArgColumnAssignment> argColumnMapErr = new HashMap<>();

        // ID-column not allowed for filtering
        argColumnMapErr.put("arg4", new ArgColumnAssignment(new ArgMetaInfo("arg4", STRING, false, false),
                new DataColumn("TBL1", "COL4", SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn("TBL1", "ID", SQL_TINYINT, "126")))));

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, argColumnMapErr);

        argColumnMapErr.clear();

        // argument name mismatch
        argColumnMapErr.put("arg4", new ArgColumnAssignment(new ArgMetaInfo("arg5", STRING, false, false),
                new DataColumn("TBL1", "COL4", SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn("TBL1", "F1", SQL_TINYINT, "126")))));

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, argColumnMapErr);

        argColumnMapErr.clear();

        // table mismatch
        argColumnMapErr.put("arg4", new ArgColumnAssignment(new ArgMetaInfo("arg4", STRING, false, false),
                new DataColumn("TBL2", "COL4", SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn("TBL2", "F1", SQL_TINYINT, "126")))));

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, argColumnMapErr);

        argColumnMapErr.clear();

        // null key
        argColumnMapErr.put("arg1",
                new ArgColumnAssignment(new ArgMetaInfo("arg1", INTEGER, false, true), new DataColumn("TBL1", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMapErr.put("arg2",
                new ArgColumnAssignment(new ArgMetaInfo("arg2", STRING, true, false), new DataColumn("TBL1", "COL2", SQL_VARCHAR, false, false, null)));
        argColumnMapErr.put(null,
                new ArgColumnAssignment(new ArgMetaInfo("arg2", STRING, true, false), new DataColumn("TBL1", "COL2", SQL_VARCHAR, false, false, null)));

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, argColumnMapErr);

        argColumnMapErr.clear();

        // empty arg name
        argColumnMapErr.put("arg1",
                new ArgColumnAssignment(new ArgMetaInfo("arg1", INTEGER, false, true), new DataColumn("TBL1", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMapErr.put("",
                new ArgColumnAssignment(new ArgMetaInfo("arg2", STRING, true, false), new DataColumn("TBL1", "COL2", SQL_VARCHAR, false, false, null)));

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, argColumnMapErr);

        argColumnMapErr.clear();

        // null value
        argColumnMapErr.put("arg1",
                new ArgColumnAssignment(new ArgMetaInfo("arg1", INTEGER, false, true), new DataColumn("TBL1", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMapErr.put("arg2", null);

        assertFailedConstruct("TBL1", "ID", TableNature.ID_SUBSET, argColumnMapErr);

        argColumnMapErr.clear();

        Map<String, ArgColumnAssignment> argColumnMap = new HashMap<>();

        argColumnMap.put("arg1",
                new ArgColumnAssignment(new ArgMetaInfo("arg1", INTEGER, false, true), new DataColumn("TBL1", "COL1", SQL_INTEGER, false, true, null)));
        argColumnMap.put("arg2",
                new ArgColumnAssignment(new ArgMetaInfo("arg2", STRING, true, false), new DataColumn("TBL1", "COL2", SQL_VARCHAR, false, false, null)));
        argColumnMap.put("arg3",
                new ArgColumnAssignment(new ArgMetaInfo("arg3", STRING, true, true), new DataColumn("TBL1", "COL3", SQL_VARCHAR, true, true, null)));
        argColumnMap.put("arg4", new ArgColumnAssignment(new ArgMetaInfo("arg4", STRING, false, false),
                new DataColumn("TBL1", "COL4", SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn("TBL1", "F1", SQL_TINYINT, "126")))));

    }

    @Test
    void testBuilderWithAutoMapping() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
                .filteredBy("FILTER1", SQL_BOOLEAN, "TRUE")
                .filteredBy("FILTER2", SQL_INTEGER, "3")
            .dataColumn("d5", SQL_INTEGER)
                .autoMapped(argName -> argName.endsWith(".int") ? argName.substring(0, argName.length()-4) : null, INTEGER)
                .filteredBy("FILTER1", SQL_VARCHAR, "${argName.local}")
            .dataColumn("d7", SQL_VARCHAR)
                .autoMapped(dataColumn -> new DefaultAutoMappingPolicy(argName -> argName.endsWith(".s") ? argName.substring(0, argName.length()-2) : null, new ArgColumnAssignment(new ArgMetaInfo("dummy", DefaultAdlType.STRING, false, false), dataColumn)))
                .filteredBy("FILTER2", SQL_VARCHAR, "${argName.local}")
            .get();
        
        // @formatter:on

        assertEquals("TBL1", config.tableName());
        assertTrue(config.tableNature().isPrimaryTable());
        assertTrue(config.tableNature().containsAllIds());
        assertFalse(config.tableNature().isSparse());
        assertEquals("ID", config.idColumnName());
        assertEquals(2, config.argColumnMap().size());
        assertEquals(INTEGER, config.lookup("arg2").type());
        assertEquals("d2", config.lookupAssignment("arg2", ProcessContext.empty()).column().columnName());
        assertEquals(2, config.lookupAssignment("arg2", ProcessContext.empty()).column().filters().size());
        assertEquals(1, config.numberOfTables());
        assertEquals(true, config.contains("arg2"));
        assertEquals(true, config.isAlwaysKnown("arg1"));
        assertEquals(config, config.lookupTableMetaInfo("arg1", ProcessContext.empty()));
        assertEquals(true, config.contains("foo.int"));
        assertEquals(true, config.contains("foo.s"));
        assertEquals("d5", config.lookupAssignment("foo.int", ProcessContext.empty()).column().columnName());
        assertEquals("d7", config.lookupAssignment("foo.s", ProcessContext.empty()).column().columnName());
        assertEquals(1, config.lookupAssignment("foo.s", ProcessContext.empty()).column().filters().size());

    }

    @Test
    void testBuilderAutoMappingOnly() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d5", SQL_INTEGER)
                .autoMapped(argName -> argName.endsWith(".int") ? argName.substring(0, argName.length()-4) : null, INTEGER)
                .filteredBy("FILTER1", SQL_VARCHAR, "${argName.local}")
            .dataColumn("d7", SQL_VARCHAR)
                .autoMapped(dataColumn -> new DefaultAutoMappingPolicy(argName -> argName.endsWith(".s") ? argName.substring(0, argName.length()-2) : null, new ArgColumnAssignment(new ArgMetaInfo("dummy", DefaultAdlType.STRING, false, false), dataColumn)))
                .filteredBy("FILTER2", SQL_VARCHAR, "${argName.local}")
            .get();
        
        // @formatter:on

        final Map<String, Serializable> globalVariables = new HashMap<>();

        final ProcessContext ctx = new ProcessContext() {

            @Override
            public Map<String, Serializable> getGlobalVariables() {
                return globalVariables;
            }

            @Override
            public Set<Flag> getGlobalFlags() {
                return Collections.emptySet();
            }

        };

        assertEquals("TBL1", config.tableName());
        assertTrue(config.tableNature().isPrimaryTable());
        assertTrue(config.tableNature().containsAllIds());
        assertFalse(config.tableNature().isSparse());
        assertEquals("ID", config.idColumnName());
        assertTrue(config.argColumnMap().isEmpty());
        assertEquals(1, config.numberOfTables());
        assertEquals(true, config.contains("foo.int"));
        assertEquals(true, config.contains("foo.s"));
        assertEquals("d5", config.lookupAssignment("foo.int", ctx).column().columnName());
        assertEquals("d7", config.lookupAssignment("foo.s", ctx).column().columnName());
        assertEquals(1, config.lookupAssignment("foo.s", ctx).column().filters().size());
        assertEquals("foo", globalVariables.get("argName.local"));
        assertEquals(false, config.contains(".int"));

    }

    @Test
    void testBuilderBugInAutoMappingFunction() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d5", SQL_INTEGER)
                .autoMapped(argName -> argName.endsWith(".int") ? argName.substring(0, argName.length()-5) : null, INTEGER)
            .get();
        
        // @formatter:on

        final Map<String, Serializable> globalVariables = new HashMap<>();

        final ProcessContext ctx = new ProcessContext() {

            @Override
            public Map<String, Serializable> getGlobalVariables() {
                return globalVariables;
            }

            @Override
            public Set<Flag> getGlobalFlags() {
                return Collections.emptySet();
            }

        };

        assertThrows(LookupException.class, () -> config.contains(".int"));
        assertThrows(LookupException.class, () -> config.lookupAssignment(".int", ctx));

    }

    @Test
    void testBuilderConfigErrorFromAutoMappingFunction() {

        final AutoMappingPolicy policy = new AutoMappingPolicy() {

            @Override
            public ArgColumnAssignment map(String argName, ProcessContext ctx) {
                throw new ConfigException("Some specific exception");
            }

            @Override
            public boolean isApplicable(String argName) {
                return true;
            }
        };

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d5", SQL_INTEGER)
                .autoMapped(_ -> policy)
            .get();
        
        // @formatter:on

        final Map<String, Serializable> globalVariables = new HashMap<>();

        final ProcessContext ctx = new ProcessContext() {

            @Override
            public Map<String, Serializable> getGlobalVariables() {
                return globalVariables;
            }

            @Override
            public Set<Flag> getGlobalFlags() {
                return Collections.emptySet();
            }

        };

        assertThrows(ConfigException.class, () -> config.lookupAssignment("foo", ctx));

    }

    @Test
    void testBuilderConfigErrorWrongTableAutoMappingFunction() {

        final AutoMappingPolicy policy = new AutoMappingPolicy() {

            @Override
            public ArgColumnAssignment map(String argName, ProcessContext ctx) {
                return new ArgColumnAssignment(new ArgMetaInfo(argName, DefaultAdlType.STRING, false, false),
                        new DataColumn("TBL_WRONG", "col", SQL_VARCHAR, false, false, null));
            }

            @Override
            public boolean isApplicable(String argName) {
                return true;
            }
        };

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d5", SQL_INTEGER)
                .autoMapped(_ -> policy)
            .get();
        
        // @formatter:on

        final Map<String, Serializable> globalVariables = new HashMap<>();

        final ProcessContext ctx = new ProcessContext() {

            @Override
            public Map<String, Serializable> getGlobalVariables() {
                return globalVariables;
            }

            @Override
            public Set<Flag> getGlobalFlags() {
                return Collections.emptySet();
            }

        };
        assertThrows(LookupException.class, () -> config.lookupAssignment("foo", ctx));

    }

    @Test
    void testBadDataBinding() {
        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d5", SQL_INTEGER)
                .autoMapped(argName -> argName.endsWith(".int") ? argName.substring(0, argName.length()-4) : null, INTEGER)
                .filteredBy("FILTER1", SQL_VARCHAR, "${argName.local}")
            .dataColumn("d7", SQL_VARCHAR)
                .autoMapped(dataColumn -> new DefaultAutoMappingPolicy(argName -> argName.endsWith(".s") ? argName.substring(0, argName.length()-2) : null, new ArgColumnAssignment(new ArgMetaInfo("dummy", DefaultAdlType.STRING, false, false), dataColumn)))
                .filteredBy("FILTER2", SQL_VARCHAR, "${argName.local}")
            .get();
        
        // @formatter:on

        assertThrows(IllegalArgumentException.class, () -> new DataBinding(config, null));

        assertThrows(IllegalArgumentException.class, () -> new DataBinding(null, DefaultSqlContainsPolicy.UNSUPPORTED));

    }

    private static void assertConstruct(String tableName, String idColumnName) {

        Map<String, ArgColumnAssignment> argColumnMap = new HashMap<>();

        argColumnMap.put("arg1",
                new ArgColumnAssignment(new ArgMetaInfo("arg1", INTEGER, false, true), new DataColumn(tableName, "COL1", SQL_INTEGER, false, true, null)));
        argColumnMap.put("arg2",
                new ArgColumnAssignment(new ArgMetaInfo("arg2", STRING, true, false), new DataColumn(tableName, "COL2", SQL_VARCHAR, false, false, null)));
        argColumnMap.put("arg3",
                new ArgColumnAssignment(new ArgMetaInfo("arg3", STRING, true, true), new DataColumn(tableName, "COL3", SQL_VARCHAR, true, true, null)));
        argColumnMap.put("arg4", new ArgColumnAssignment(new ArgMetaInfo("arg4", STRING, false, false), new DataColumn(tableName, "COL4",
                DefaultAdlSqlType.SQL_VARCHAR, false, false, Arrays.asList(new FilterColumn(tableName, "F1", SQL_TINYINT, "126")))));
        argColumnMap.put("arg5",
                new ArgColumnAssignment(new ArgMetaInfo("arg5", STRING, true, false), new DataColumn(tableName, "COL2", SQL_VARCHAR, false, false, null)));

        assertConstruct(tableName, idColumnName, argColumnMap);

        assertConstruct(tableName, idColumnName, Collections.emptyMap());

    }

    private static void assertConstruct(String tableName, String idColumnName, Map<String, ArgColumnAssignment> argColumnMap) {

        assertConstruct(tableName, idColumnName, TableNature.ID_SUBSET, argColumnMap);
        assertConstruct(tableName, idColumnName, TableNature.PRIMARY_SPARSE, argColumnMap);
        assertConstruct(tableName, idColumnName, TableNature.PRIMARY, argColumnMap);
        assertConstruct(tableName, idColumnName, TableNature.ID_SUBSET_SPARSE, argColumnMap);

        if (argColumnMap.containsKey("arg1")) {
            assertFailedConstruct(tableName, idColumnName, TableNature.PRIMARY_UNIQUE, argColumnMap);

            Map<String, ArgColumnAssignment> argColumnMap2 = new HashMap<>(argColumnMap);
            argColumnMap2.remove("arg3");
            argColumnMap2.remove("arg1");
            // avoid multi-row conflict
            assertConstruct(tableName, idColumnName, TableNature.PRIMARY_UNIQUE, argColumnMap2);
        }
    }

    private static void assertConstruct(String tableName, String idColumnName, TableNature tableNature, Map<String, ArgColumnAssignment> argColumnMap) {

        SingleTableConfig tableConfig = new SingleTableConfig(tableName, idColumnName, tableNature, null, argColumnMap, null);

        boolean haveIsAlwaysKnownColumn = argColumnMap.values().stream().map(ArgColumnAssignment::column).anyMatch(DataColumn::isAlwaysKnown);

        assertSame(tableName, tableConfig.tableName());
        assertSame(idColumnName, tableConfig.idColumnName());
        assertEquals((tableNature.containsAllIds() || haveIsAlwaysKnownColumn), tableConfig.tableNature().containsAllIds());
        assertEquals(tableNature.isPrimaryTable(), tableConfig.tableNature().isPrimaryTable());
        assertEquals(argColumnMap, tableConfig.argColumnMap());
        assertNotSame(argColumnMap, tableConfig.argColumnMap());
        assertEquals(tableNature.isSparse(), tableConfig.tableNature().isSparse());

        assertEquals(tableConfig, tableConfig.allTableMetaInfos().get(0));
        if (tableNature.isPrimaryTable()) {
            assertEquals(tableConfig.tableName(), tableConfig.primaryTable());
        }
        else {
            assertNull(tableConfig.primaryTable());
        }

    }

    private static void assertFailedConstruct(String tableName, String idColumnName, TableNature tableNature, Map<String, ArgColumnAssignment> argColumnMap) {

        assertThrows(ConfigException.class, () -> new SingleTableConfig(tableName, idColumnName, tableNature, null, argColumnMap, null));

    }

}
