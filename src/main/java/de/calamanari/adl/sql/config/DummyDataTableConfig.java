//@formatter:off
/*
 * DummyDataTableConfig
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

import java.util.Collections;
import java.util.List;

import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;

/**
 * This implementation is for testing and debugging, it returns a dummy table name and the argName
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
@SuppressWarnings("java:S6548")
public class DummyDataTableConfig implements DataTableConfig {

    private static final long serialVersionUID = -5545574197848562045L;

    private static final DummyDataTableConfig INSTANCE = new DummyDataTableConfig();

    /**
     * Dummy info
     */
    private static final TableMetaInfo DUMMY_TABLE_META_INFO = new TableMetaInfo() {

        private static final long serialVersionUID = -33333861113941621L;

        @Override
        public String tableName() {
            return "DUMMY_TABLE";
        }

        @Override
        public String idColumnName() {
            return "ID";
        }

        @Override
        public TableNature tableNature() {
            return TableNature.ALL_IDS;
        }

        @Override
        public List<FilterColumn> tableFilters() {
            return Collections.emptyList();
        }

        /**
         * @return singleton instance in JVM
         */
        Object readResolve() {
            return DUMMY_TABLE_META_INFO;
        }
    };

    public static DummyDataTableConfig getInstance() {
        return INSTANCE;
    }

    private DummyDataTableConfig() {
        // singleton
    }

    @Override
    public boolean contains(String argName) {
        assertValidArgName(argName);
        return true;
    }

    @Override
    public ArgColumnAssignment lookupAssignment(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        assertValidArgName(argName);
        return new ArgColumnAssignment(new ArgMetaInfo(argName, DefaultAdlType.STRING, false, false),
                new DataColumn(DUMMY_TABLE_META_INFO.tableName(), cleanUpperCase(argName), DefaultAdlSqlType.SQL_VARCHAR, false, false, null));
    }

    @Override
    public int numberOfTables() {
        return 1;
    }

    @Override
    public TableMetaInfo lookupTableMetaInfo(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        assertValidArgName(argName);
        return DUMMY_TABLE_META_INFO;
    }

    @Override
    public List<TableMetaInfo> allTableMetaInfos() {
        return Collections.singletonList(DUMMY_TABLE_META_INFO);
    }

    /**
     * @return singleton instance in JVM
     */
    Object readResolve() {
        return INSTANCE;
    }

    /**
     * Cosmetics, converts the argName into a dummy field name, upper case, no whitespace for better distinguishing input from output.
     * 
     * @param argName
     * @return pseudo column name
     */
    private static String cleanUpperCase(String argName) {
        StringBuilder sb = new StringBuilder(argName.length());
        argName = argName.toUpperCase();
        for (int i = 0; i < argName.length(); i++) {
            char ch = argName.charAt(i);
            if (ch < 32 || Character.isWhitespace(ch)) {
                sb.append("_");
            }
            else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

}
