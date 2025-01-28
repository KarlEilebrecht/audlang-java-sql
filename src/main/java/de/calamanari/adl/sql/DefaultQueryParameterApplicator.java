//@formatter:off
/*
 * DefaultQueryParameterApplicator
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

import static de.calamanari.adl.sql.SqlFormatConstants.FALSE;
import static de.calamanari.adl.sql.SqlFormatConstants.TRUE;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.cnv.tps.AdlFormattingException;

/**
 * The {@link DefaultQueryParameterApplicator} exclusively supports the {@link DefaultAdlSqlType} as <i>base types</i>, means the types themselves or any type
 * that returns a {@link DefaultAdlSqlType} as its base type. Thus, it should be suitable for the regular types and most custom types.
 * <p>
 * By replacing the applicator, implementors get full control over the process how values from a {@link QueryParameter} get included in the sql resp. how the
 * processing works to set the parameters on a prepared statement.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
@SuppressWarnings("java:S6548")
public class DefaultQueryParameterApplicator implements QueryParameterApplicator {

    private static final long serialVersionUID = -8256020137586993340L;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryParameterApplicator.class);

    private static final DefaultQueryParameterApplicator INSTANCE = new DefaultQueryParameterApplicator();

    /**
     * @return the only instance of this creator
     */
    public static final DefaultQueryParameterApplicator getInstance() {
        return INSTANCE;
    }

    private DefaultQueryParameterApplicator() {
        // singleton
    }

    @Override
    public void apply(PreparedStatement stmt, QueryParameter parameter, int parameterIndex) throws SQLException {
        try {
            if (parameter.value() == null) {
                stmt.setNull(parameterIndex, parameter.adlSqlType().getJavaSqlType());
            }
            else {
                if (parameter.adlSqlType().getBaseType() instanceof DefaultAdlSqlType baseType) {
                    switch (baseType) {
                    case SQL_BIGINT:
                        stmt.setLong(parameterIndex, (long) parameter.value());
                        break;
                    case SQL_BIT, SQL_BOOLEAN:
                        stmt.setBoolean(parameterIndex, (boolean) parameter.value());
                        break;
                    case SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR:
                        stmt.setString(parameterIndex, (String) parameter.value());
                        break;
                    case SQL_DATE:
                        stmt.setDate(parameterIndex, (Date) parameter.value());
                        break;
                    case SQL_DECIMAL, SQL_DOUBLE, SQL_REAL:
                        stmt.setDouble(parameterIndex, (double) parameter.value());
                        break;
                    case SQL_FLOAT:
                        stmt.setFloat(parameterIndex, (float) parameter.value());
                        break;
                    case SQL_INTEGER:
                        stmt.setInt(parameterIndex, (int) parameter.value());
                        break;
                    case SQL_TINYINT:
                        stmt.setByte(parameterIndex, (byte) parameter.value());
                        break;
                    case SQL_NUMERIC:
                        stmt.setBigDecimal(parameterIndex, (BigDecimal) parameter.value());
                        break;
                    case SQL_SMALLINT:
                        stmt.setShort(parameterIndex, (short) parameter.value());
                        break;
                    case SQL_TIMESTAMP:
                        stmt.setTimestamp(parameterIndex, (Timestamp) parameter.value());
                        break;
                    }
                    return;
                }
                throw new SQLException(String.format("Unsupported type: %s (not implemented, yet)", parameter.adlSqlType()));
            }

        }
        catch (SQLException ex) {
            LOGGER.error(String.format("Unable to set a parameter at a prepared statement, given: adlSqlType=%s, value=%s, stmt=%s, parameterIndex=%s",
                    parameter.adlSqlType(), parameter.value(), stmt, parameterIndex));
            throw ex;
        }

    }

    @Override
    public void applyUnsafe(StringBuilder sb, QueryParameter parameter, int parameterIndex) {
        if (parameter.value() == null) {
            sb.append("NULL");
        }
        else {
            if (parameter.adlSqlType().getBaseType() instanceof DefaultAdlSqlType baseType) {
                switch (baseType) {
                case SQL_BIGINT, SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR, SQL_INTEGER, SQL_SMALLINT:
                    sb.append(formatDefault(parameter));
                    break;
                case SQL_TINYINT:
                    sb.append(formatTinyInt(parameter));
                    break;
                case SQL_BIT, SQL_BOOLEAN:
                    boolean booleanVal = (boolean) parameter.value();
                    sb.append(parameter.adlSqlType().getFormatter().format(parameter.id(), booleanVal ? TRUE : FALSE, parameter.operator()));
                    break;
                case SQL_TIMESTAMP:
                    sb.append(formatTimestamp(parameter));
                    break;
                case SQL_DATE:
                    sb.append(formatDate(parameter));
                    break;
                case SQL_DECIMAL, SQL_DOUBLE, SQL_REAL:
                    sb.append(formatDecimal(parameter, (double) parameter.value()));
                    break;
                case SQL_FLOAT:
                    sb.append(formatDecimal(parameter, (float) parameter.value()));
                    break;
                case SQL_NUMERIC:
                    sb.append(formatDecimal(parameter, ((BigDecimal) parameter.value()).doubleValue()));
                    break;
                }
                return;
            }
            throw new AdlFormattingException(String.format("Unsupported type: %s (not implemented, yet)", parameter));
        }

    }

    /**
     * Formats the value of the parameter again as String (1-7 decimals) and passes it to the formatter of the {@link AdlSqlType}.
     * 
     * @param parameter
     * @param double value of the parameter
     * @return value as string
     */
    private static String formatDecimal(QueryParameter parameter, double value) {
        NumberFormat nfDouble = NumberFormat.getInstance(Locale.US);
        nfDouble.setMaximumFractionDigits(7);
        nfDouble.setMinimumFractionDigits(1);
        nfDouble.setGroupingUsed(false);
        return parameter.adlSqlType().getFormatter().format(parameter.id(), nfDouble.format(value), parameter.operator());
    }

    /**
     * Formats the time or date of the parameter again as String yyyy-MM-dd and passes it to the formatter of the {@link AdlSqlType}.
     * 
     * @param parameter
     * @return sql-formatted value
     */
    private static String formatDate(QueryParameter parameter) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return parameter.adlSqlType().getFormatter().format(parameter.id(), sdf.format((Date) parameter.value()), parameter.operator());
    }

    /**
     * Formats the time or date of the parameter again as String <code>yyyy-MM-dd HH:mm:ss</code> and passes it to the formatter of the {@link AdlSqlType}.
     * 
     * @param parameter
     * @return sql-formatted value
     */
    private static String formatTimestamp(QueryParameter parameter) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return parameter.adlSqlType().getFormatter().format(parameter.id(), sdf.format((Timestamp) parameter.value()), parameter.operator());
    }

    /**
     * Passes the given parameter's value as string over to the formatter of the {@link AdlSqlType}.
     * 
     * @param parameter
     * @return sql-formatted value
     */
    private static String formatTinyInt(QueryParameter parameter) {
        int value = Byte.toUnsignedInt((byte) parameter.value());
        return parameter.adlSqlType().getFormatter().format(parameter.id(), String.valueOf(value), parameter.operator());
    }

    /**
     * Passes the given parameter's value as string over to the formatter of the {@link AdlSqlType}.
     * 
     * @param parameter
     * @return sql-formatted value
     */
    private static String formatDefault(QueryParameter parameter) {
        return parameter.adlSqlType().getFormatter().format(parameter.id(), String.valueOf(parameter.value()), parameter.operator());
    }

    /**
     * @return singleton instance in JVM
     */
    Object readResolve() {
        return INSTANCE;
    }
}
