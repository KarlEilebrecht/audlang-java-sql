//@formatter:off
/*
 * QueryParameter
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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.irl.MatchOperator;

/**
 * Default implementation of a {@link QueryParameter} to pass a value to be set on a prepared statement
 * 
 * @param id identifier of this parameter in the current conversion process
 * @param adlSqlType NOT NULL
 * @param value the value to be set (in-flight transfer type, serializable between creator and applicator)
 * @param operator context information, the operator this parameter is used with, NOT NULL (specify {@link MatchOperator#EQUALS} if you don't know)
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record DefaultQueryParameter(String id, AdlSqlType adlSqlType, Serializable value, MatchOperator operator) implements QueryParameter {

    /**
     * @param id identifier of this parameter in the current conversion process
     * @param adlSqlType NOT NULL
     * @param value the value to be set (in-flight transfer type, serializable between creator and applicator)
     * @param operator context information, the operator this parameter is used with, NOT NULL (specify {@link MatchOperator#EQUALS} if you don't know)
     * @throws ConfigException if there is a mismatch or the sql-type is unsupported (reject early)
     */
    public DefaultQueryParameter {
        assertValidId(id, adlSqlType, value);
        if (adlSqlType == null || operator == null) {
            throw new IllegalArgumentException(String.format(
                    "The arguments adlSqlType and operator of the destination column must not be null, given: id=%s, adlSqlType=null, value=%s, operator=%s.",
                    id, value, operator));
        }
        assertTypesSupported(id, adlSqlType, value);
    }

    /**
     * This method ensures that the given {@link AdlSqlType} is supported and compatible to the given value.
     * 
     * @param adlSqlType
     * @param value
     * @throws ConfigException if there is a mismatch or the sql-type is unsupported
     */
    private static void assertTypesSupported(String id, AdlSqlType adlSqlType, Serializable value) {

        if (adlSqlType.getBaseType() instanceof DefaultAdlSqlType baseType) {

            switch (baseType) {
            case SQL_BIGINT:
                assertTransferTypeSupported(id, adlSqlType, Long.class, value);
                break;
            case SQL_BIT, SQL_BOOLEAN:
                assertTransferTypeSupported(id, adlSqlType, Boolean.class, value);
                break;
            case SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR:
                assertTransferTypeSupported(id, adlSqlType, String.class, value);
                break;
            case SQL_DATE:
                assertTransferTypeSupported(id, adlSqlType, Date.class, value);
                break;
            case SQL_DECIMAL, SQL_DOUBLE, SQL_REAL:
                assertTransferTypeSupported(id, adlSqlType, Double.class, value);
                break;
            case SQL_FLOAT:
                assertTransferTypeSupported(id, adlSqlType, Float.class, value);
                break;
            case SQL_INTEGER:
                assertTransferTypeSupported(id, adlSqlType, Integer.class, value);
                break;
            case SQL_TINYINT:
                assertTransferTypeSupported(id, adlSqlType, Byte.class, value);
                break;
            case SQL_NUMERIC:
                assertTransferTypeSupported(id, adlSqlType, BigDecimal.class, value);
                break;
            case SQL_SMALLINT:
                assertTransferTypeSupported(id, adlSqlType, Short.class, value);
                break;
            case SQL_TIMESTAMP:
                assertTransferTypeSupported(id, adlSqlType, Timestamp.class, value);
                break;
            }
            return;
        }
        throw new ConfigException(String.format("Unsupported AdlSqlType: %s (not implemented, yet)", adlSqlType),
                AudlangMessage.msg(CommonErrors.ERR_3000_MAPPING_FAILED));

    }

    /**
     * Validates that given type of the transfer variable is supported (value can be casted to the required type)
     * 
     * @param type
     * @param value
     */
    private static void assertTransferTypeSupported(String id, AdlSqlType adlSqlType, Class<?> type, Object value) {
        if (value != null) {
            try {
                type.cast(value);
            }
            catch (ClassCastException _) {
                throw new ConfigException(
                        String.format("Transfer type mismatch: id=%s, adlSqlType=%s, expectedType=%s, incompatible value=%s", id, adlSqlType, type, value),
                        AudlangMessage.msg(CommonErrors.ERR_3001_TYPE_MISMATCH));
            }

        }
    }

    /**
     * Ensures the given id is valid, see QueryParamter#id()
     * 
     * @param id
     * @param adlSqlType
     * @param value
     */
    private static void assertValidId(String id, AdlSqlType adlSqlType, Serializable value) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    String.format("The paramteer id must not be null or blank, given: id=%s, adlSqlType=%s, value=%s.", id, adlSqlType, value));
        }
        for (int i = 0; i < id.length(); i++) {
            char ch = id.charAt(i);
            if ("${}".indexOf(ch) > -1 || Character.isWhitespace(ch)) {
                throw new IllegalArgumentException(String.format(
                        "The parameter id must not contain any whitespace, '$', '{' or '}', given: id=%s, adlSqlType=%s, value=%s.", id, adlSqlType, value));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        this.applyUnsafe(sb, 0);
        return sb.toString();
    }
}
