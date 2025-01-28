//@formatter:off
/*
 * DefaultAdlSqlType
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

package de.calamanari.adl.sql;

import java.sql.Types;

import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.NativeTypeCaster;

/**
 * The {@link DefaultAdlSqlType} maps a selection of the SQL-types defined in {@link Types} and decorate them with the ability to act as {@link AdlType}.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum DefaultAdlSqlType implements AdlSqlType {

    SQL_BIT(Types.BIT, DefaultSqlFormatter.SQL_BIT),
    SQL_BIGINT(Types.BIGINT, DefaultSqlFormatter.SQL_BIGINT),
    SQL_BOOLEAN(Types.BOOLEAN, DefaultSqlFormatter.SQL_BOOLEAN),
    SQL_CHAR(Types.CHAR, DefaultSqlFormatter.SQL_CHAR),
    SQL_DATE(Types.DATE, DefaultSqlFormatter.SQL_DATE_DEFAULT),
    SQL_DECIMAL(Types.DECIMAL, DefaultSqlFormatter.SQL_DECIMAL),
    SQL_DOUBLE(Types.DOUBLE, DefaultSqlFormatter.SQL_DOUBLE),
    SQL_FLOAT(Types.FLOAT, DefaultSqlFormatter.SQL_FLOAT),
    SQL_INTEGER(Types.INTEGER, DefaultSqlFormatter.SQL_INTEGER),
    SQL_LONGNVARCHAR(Types.LONGNVARCHAR, DefaultSqlFormatter.SQL_LONGNVARCHAR),
    SQL_LONGVARCHAR(Types.LONGVARCHAR, DefaultSqlFormatter.SQL_LONGVARCHAR),
    SQL_NCHAR(Types.NCHAR, DefaultSqlFormatter.SQL_NCHAR),
    SQL_NUMERIC(Types.NUMERIC, DefaultSqlFormatter.SQL_NUMERIC),
    SQL_NVARCHAR(Types.NVARCHAR, DefaultSqlFormatter.SQL_NVARCHAR),
    SQL_REAL(Types.REAL, DefaultSqlFormatter.SQL_REAL),
    SQL_SMALLINT(Types.SMALLINT, DefaultSqlFormatter.SQL_SMALLINT),
    SQL_TIMESTAMP(Types.TIMESTAMP, DefaultSqlFormatter.SQL_TIMESTAMP_DEFAULT),
    SQL_TINYINT(Types.TINYINT, DefaultSqlFormatter.SQL_TINYINT),
    SQL_VARCHAR(Types.VARCHAR, DefaultSqlFormatter.SQL_VARCHAR);

    private final int javaSqlType;

    private final ArgValueFormatter formatter;

    /**
     * There is no default type casting. This instance returns the native field name as-is.
     */
    public static final NativeTypeCaster DUMMY_TYPE_CASTER = new NativeTypeCaster() {

        private static final long serialVersionUID = -3669047015013254849L;

        @Override
        public String formatNativeTypeCast(String argName, String nativeFieldName, AdlType argType, AdlType requestedArgType) {
            return nativeFieldName;
        }

        /**
         * @return singleton instance in JVM
         */
        Object readResolve() {
            return DUMMY_TYPE_CASTER;
        }
    };

    private DefaultAdlSqlType(int javaSqlType, ArgValueFormatter formatter) {
        this.javaSqlType = javaSqlType;
        this.formatter = formatter;
    }

    @Override
    public ArgValueFormatter getFormatter() {
        return formatter;
    }

    @Override
    public boolean supportsContains() {
        return (this == SQL_CHAR || this == SQL_LONGNVARCHAR || this == SQL_LONGVARCHAR || this == SQL_NCHAR || this == SQL_NVARCHAR || this == SQL_VARCHAR);
    }

    @Override
    public boolean supportsLessThanGreaterThan() {
        return (this != SQL_BIT && this != SQL_BOOLEAN);
    }

    /**
     * @return this is the underlying SQL-type for this type, see {@link Types} (mainly for reference and debugging)
     */
    @Override
    public int getJavaSqlType() {
        return javaSqlType;
    }

    @Override
    public QueryParameterCreator getQueryParameterCreator() {
        return DefaultQueryParameterCreator.getInstance();
    }

    @Override
    public QueryParameterApplicator getQueryParameterApplicator() {
        return DefaultQueryParameterApplicator.getInstance();
    }

    @Override
    public NativeTypeCaster getNativeTypeCaster() {
        return DUMMY_TYPE_CASTER;
    }

}
