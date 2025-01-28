//@formatter:off
/*
 * ArgColumnAssignmentTest
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class ArgColumnAssignmentTest {

    @Test
    void testBasics() {

        DataColumn col1 = new DataColumn("TBL1", "COL1", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        ArgMetaInfo metaInfo1 = new ArgMetaInfo("arg", DefaultAdlType.STRING, false, false);

        ArgColumnAssignment aca = new ArgColumnAssignment(metaInfo1, col1);

        assertEquals(col1, aca.column());
        assertEquals(metaInfo1, aca.arg());

        DataColumn col2 = new DataColumn("TBL2", "COL2", DefaultAdlSqlType.SQL_VARCHAR, true, false, null);

        aca = new ArgColumnAssignment(metaInfo1, col2);

        assertEquals(col2, aca.column());

        ArgMetaInfo metaInfo2 = new ArgMetaInfo("arg", DefaultAdlType.STRING, true, false);

        assertEquals(metaInfo2, aca.arg());

        aca = new ArgColumnAssignment(metaInfo2, col1);

        assertEquals(col1, aca.column());

        assertEquals(metaInfo1, aca.arg());

        DataColumn col3 = new DataColumn("TBL2", "COL2", DefaultAdlSqlType.SQL_VARCHAR, true, true, null);

        aca = new ArgColumnAssignment(metaInfo2, col3);

        ArgMetaInfo metaInfo3 = new ArgMetaInfo("arg", DefaultAdlType.STRING, true, true);

        assertEquals(col3, aca.column());

        assertEquals(metaInfo3, aca.arg());

    }

    @Test
    void testSpecialCase() {

        DataColumn col1 = new DataColumn("TBL1", "COL1", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        ArgMetaInfo metaInfo1 = new ArgMetaInfo("arg", DefaultAdlType.STRING, false, false);

        assertThrows(IllegalArgumentException.class, () -> new ArgColumnAssignment(metaInfo1, null));
        assertThrows(IllegalArgumentException.class, () -> new ArgColumnAssignment(null, col1));

        DataColumn colDate = new DataColumn("TBL1", "COL2", DefaultAdlSqlType.SQL_DATE, false, false, null);

        ArgMetaInfo metaInfoBool = new ArgMetaInfo("arg2", DefaultAdlType.BOOL, false, false);
        assertThrows(ConfigException.class, () -> new ArgColumnAssignment(metaInfoBool, colDate));
    }

}
