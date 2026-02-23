//@formatter:off
/*
 * DummyDataTableConfigTest
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

import org.junit.jupiter.api.Test;

import de.calamanari.adl.DeepCopyUtils;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DummyDataTableConfigTest {

    @Test
    void testBasics() {

        DummyDataTableConfig dummy = DummyDataTableConfig.getInstance();

        assertSame(dummy, DeepCopyUtils.deepCopy(dummy));
        assertSame(dummy.lookupTableMetaInfo("hugo", ProcessContext.empty()),
                DeepCopyUtils.deepCopy(dummy.lookupTableMetaInfo("hugo", ProcessContext.empty())));

        assertTrue(dummy.contains("arg"));

        assertFalse(dummy.isAlwaysKnown("arg"));
        assertFalse(dummy.isCollection("arg"));

        assertEquals(new ArgMetaInfo("fooBar", DefaultAdlType.STRING, false, false), dummy.lookup("fooBar"));

        ArgColumnAssignment acaExpect = new ArgColumnAssignment(new ArgMetaInfo("fooBar", DefaultAdlType.STRING, false, false),
                new DataColumn("DUMMY_TABLE", "FOOBAR", DefaultAdlSqlType.SQL_VARCHAR, false, false, null));

        assertEquals(acaExpect, dummy.lookupAssignment("fooBar", ProcessContext.empty()));

        assertEquals(acaExpect.column(), dummy.lookupColumn("fooBar", ProcessContext.empty()));

        TableMetaInfo tableInfo = dummy.lookupTableMetaInfo("fooBar", ProcessContext.empty());

        assertNotNull(tableInfo);

        assertEquals("DUMMY_TABLE", tableInfo.tableName());
        assertEquals("ID", tableInfo.idColumnName());

        assertTrue(tableInfo.tableNature().containsAllIds());
        assertFalse(tableInfo.tableNature().isPrimaryTable());
        assertFalse(tableInfo.tableNature().isSparse());
        assertEquals(1, dummy.numberOfTables());

        assertEquals("NAME_WITH_SPACES", dummy.lookupAssignment("name with spaces", ProcessContext.empty()).column().columnName());

        assertSame(dummy.lookupTableMetaInfo("fooBar", ProcessContext.empty()), dummy.allTableMetaInfos().get(0));

    }

    @Test
    void testSpecialCase() {
        DummyDataTableConfig dummy = DummyDataTableConfig.getInstance();
        ProcessContext emptyContext = ProcessContext.empty();
        assertThrows(IllegalArgumentException.class, () -> dummy.contains(null));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupAssignment(null, emptyContext));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupAssignment("foo", null));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupTableMetaInfo(null, emptyContext));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupTableMetaInfo(null, emptyContext));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupTableMetaInfo("foo", null));

        assertThrows(IllegalArgumentException.class, () -> dummy.contains(" "));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupAssignment(" ", emptyContext));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupTableMetaInfo(" ", emptyContext));
        assertThrows(IllegalArgumentException.class, () -> dummy.lookupTableMetaInfo(" ", emptyContext));

    }

}
