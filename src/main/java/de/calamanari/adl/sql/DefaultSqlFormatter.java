//@formatter:off
/*
 * DefaultSqlFormatter
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

import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import de.calamanari.adl.cnv.tps.AdlDateUtils;
import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.DefaultArgValueFormatter;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.sql.config.FilterColumn;
import de.calamanari.adl.util.TriFunction;

/**
 * The formatters in this enumeration define the default behavior when formatting values for sql.
 * <p>
 * <b>Important:</b> These formatters usually only come into play when debugging or logging because regularly all values are passed as parameters of
 * {@link PreparedStatement}s, so there is no formatting required.
 * <p>
 * See also remarks at {@link QueryParameterApplicator#applyUnsafe(StringBuilder, QueryParameter, int)}
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum DefaultSqlFormatter implements ArgValueFormatter {

    SQL_BIT(DefaultSqlFormatter::formatSqlBit),
    SQL_BIGINT(DefaultArgValueFormatter.INTEGER::format),
    SQL_BOOLEAN(DefaultSqlFormatter::formatSqlBoolean),
    SQL_CHAR(DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES::format),
    SQL_DATE_PLAIN(DefaultSqlFormatter::formatSqlDatePlain),
    SQL_DATE_DEFAULT(DefaultSqlFormatter::formatSqlDateDefaultStyle),
    SQL_DATE_MYSQL(DefaultSqlFormatter::formatSqlDateMySqlStyle),
    SQL_DATE_ORACLE(DefaultSqlFormatter::formatSqlDateOracleStyle),
    SQL_DATE_SQL_SERVER(DefaultSqlFormatter::formatSqlDateSqlServerStyle),
    SQL_DECIMAL(DefaultArgValueFormatter.DECIMAL::format),
    SQL_DOUBLE(DefaultArgValueFormatter.DECIMAL::format),
    SQL_FLOAT(DefaultArgValueFormatter.DECIMAL::format),
    SQL_INTEGER(DefaultSqlFormatter::formatSqlInteger),
    SQL_LONGNVARCHAR(DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES::format),
    SQL_LONGVARCHAR(DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES::format),
    SQL_NCHAR(DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES::format),
    SQL_NUMERIC(DefaultArgValueFormatter.DECIMAL::format),
    SQL_NVARCHAR(DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES::format),
    SQL_REAL(DefaultArgValueFormatter.DECIMAL::format),
    SQL_SMALLINT(DefaultSqlFormatter::formatSqlSmallInt),
    SQL_TIMESTAMP_PLAIN(DefaultSqlFormatter::formatSqlTimestampPlain),
    SQL_TIMESTAMP_DEFAULT(DefaultSqlFormatter::formatSqlTimestampDefaultStyle),
    SQL_TIMESTAMP_MYSQL(DefaultSqlFormatter::formatSqlTimestampMySqlStyle),
    SQL_TIMESTAMP_ORACLE(DefaultSqlFormatter::formatSqlTimestampOracleStyle),
    SQL_TIMESTAMP_SQL_SERVER(DefaultSqlFormatter::formatSqlTimestampSqlServerStyle),
    SQL_TINYINT(DefaultSqlFormatter::formatSqlTinyInt),
    SQL_VARCHAR(DefaultArgValueFormatter.STRING_IN_SINGLE_QUOTES::format);

    private final TriFunction<String, String, MatchOperator, String> formatterFunction;

    private DefaultSqlFormatter(TriFunction<String, String, MatchOperator, String> formatterFunction) {
        this.formatterFunction = formatterFunction;
    }

    @Override
    public String format(String argName, String argValue, MatchOperator operator) {
        return formatterFunction.apply(argName, argValue, operator);
    }

    /**
     * Formats the given string as a bit-value (0/1) if it is '0', '1', 'TRUE' or 'FALSE'
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return 0 or 1
     * @throws AdlFormattingException if the input does not match
     */
    public static String formatSqlBit(String argName, String argValue, MatchOperator operator) {

        // the tolerant behavior (1/0) is a compromise to avoid confusion

        if (TRUE.equals(argValue) || "1".equals(argValue)) {
            return "1";
        }
        else if (FALSE.equals(argValue) || "0".equals(argValue)) {
            return "0";
        }
        throw new AdlFormattingException(
                String.format("Unable to format argName=%s, argValue=%s, operator=%s (TRUE, 1, FALSE, 0 input expected).", argName, argValue, operator));
    }

    /**
     * Formats the given string as a boolean value (TRUE/FALSE) if it is '0', '1', 'TRUE' or 'FALSE'
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return FALSE or TRUE
     * @throws AdlFormattingException if the input does not match
     */
    public static String formatSqlBoolean(String argName, String argValue, MatchOperator operator) {

        // the tolerant behavior (1/0) is a compromise to avoid confusion

        if (TRUE.equals(argValue) || "1".equals(argValue)) {
            return TRUE;
        }
        else if (FALSE.equals(argValue) || "0".equals(argValue)) {
            return FALSE;
        }
        throw new AdlFormattingException(
                String.format("Unable to format argName=%s, argValue=%s, operator=%s (TRUE, 1, FALSE, 0 input expected).", argName, argValue, operator));
    }

    /**
     * Formats the given string as integer if it is in 32-bit signed integer range
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return int value as string
     * @throws AdlFormattingException if the input does not match
     */
    public static String formatSqlInteger(String argName, String argValue, MatchOperator operator) {
        String formatted = DefaultArgValueFormatter.INTEGER.format(argName, argValue, operator);
        long temp = Long.parseLong(formatted);

        if (temp < Integer.MIN_VALUE || temp > Integer.MAX_VALUE) {
            throw new AdlFormattingException(String.format("Unable to format argName=%s, argValue=%s, operator=%s (value out of sql integer range [%d, %d]).",
                    argName, argValue, operator, Integer.MIN_VALUE, Integer.MAX_VALUE));

        }
        return formatted;
    }

    /**
     * Formats the given string as integer if it is in 16-bit signed short range
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return short value as string
     * @throws AdlFormattingException if the input does not match
     */
    public static String formatSqlSmallInt(String argName, String argValue, MatchOperator operator) {
        String formatted = DefaultArgValueFormatter.INTEGER.format(argName, argValue, operator);
        long temp = Long.parseLong(formatted);

        if (temp < Short.MIN_VALUE || temp > Short.MAX_VALUE) {
            throw new AdlFormattingException(String.format("Unable to format argName=%s, argValue=%s, operator=%s (value out of small int range [%d, %d]).",
                    argName, argValue, operator, Short.MIN_VALUE, Short.MAX_VALUE));

        }
        return formatted;
    }

    /**
     * Formats the given string as integer if it is in 8-bit unsigned byte range
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return byte value as string (0-255)
     * @throws AdlFormattingException if the input does not match
     */
    public static String formatSqlTinyInt(String argName, String argValue, MatchOperator operator) {
        String formatted = DefaultArgValueFormatter.INTEGER.format(argName, argValue, operator);
        long temp = Long.parseLong(formatted);

        if (temp < 0 || temp > 255) {
            throw new AdlFormattingException(String.format("Unable to format argName=%s, argValue=%s, operator=%s (value out of tiny int range [%d, %d]).",
                    argName, argValue, operator, 0, 255));

        }
        return formatted;
    }

    /**
     * Formats the given string if it either matches the format <code>yyyy-MM-dd</code> or <code>yyyy-MM-dd HH:mm:ss</code> and represents a valid UTC-time.
     * <p>
     * <b>Note:</b> The compromise to support full time stamps was introduced to avoid confusion when specifying {@link FilterColumn}s, it should be irrelevant
     * for regular fields (by default Audlang deals with date only).
     * <p>
     * This plain format is accepted by SQLite.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>'yyyy-MM-dd HH:mm:ss'</code>
     */
    public static String formatSqlTimestampPlain(String argName, String argValue, MatchOperator operator) {
        // this lenient behavior was required to fully support Timestamp as FilterColumn condition
        // without this tolerance we would always lose the time portion
        if (isFullTimestamp(argValue)) {
            return "'" + argValue + "'";
        }
        else {
            return "'" + DefaultArgValueFormatter.DATE.format(argName, argValue, operator) + " 00:00:00'";
        }
    }

    /**
     * Formats the given string if it either matches the format <code>yyyy-MM-dd</code> or <code>yyyy-MM-dd HH:mm:ss</code> and represents a valid UTC-time.
     * <p>
     * This "standard" format works with H2 and PostreSql but for example not with SQLite.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>TIMESTAMP '2024-12-12 00:00:00'</code>
     */
    public static String formatSqlTimestampDefaultStyle(String argName, String argValue, MatchOperator operator) {
        return "TIMESTAMP " + formatSqlTimestampPlain(argName, argValue, operator);
    }

    /**
     * Formats the given string if it either matches the format <code>yyyy-MM-dd</code> or <code>yyyy-MM-dd HH:mm:ss</code> and represents a valid UTC-time.
     * <p>
     * Should also work for PostgreSQL
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>TO_TIMESTAMP('2024-12-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS')</code>
     */
    public static String formatSqlTimestampOracleStyle(String argName, String argValue, MatchOperator operator) {
        return "TO_TIMESTAMP(" + formatSqlTimestampPlain(argName, argValue, operator) + ", 'YYYY-MM-DD HH24:MI:SS')";
    }

    /**
     * Formats the given string if it either matches the format <code>yyyy-MM-dd</code> or <code>yyyy-MM-dd HH:mm:ss</code> and represents a valid UTC-time.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>TIMESTAMP('2024-12-12 00:00:00')</code>
     */
    public static String formatSqlTimestampMySqlStyle(String argName, String argValue, MatchOperator operator) {
        return "TIMESTAMP(" + formatSqlTimestampPlain(argName, argValue, operator) + ")";
    }

    /**
     * Formats the given string if it either matches the format <code>yyyy-MM-dd</code> or <code>yyyy-MM-dd HH:mm:ss</code> and represents a valid UTC-time.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>TO_TIMESTAMP('2024-12-12 00:00:00')</code>
     */
    public static String formatSqlTimestampSqlServerStyle(String argName, String argValue, MatchOperator operator) {
        return "TO_TIMESTAMP(" + formatSqlTimestampPlain(argName, argValue, operator) + ")";
    }

    /**
     * Formats the given string if it matches the format <code>yyyy-MM-dd</code> and represents a valid UTC-time.
     * <p>
     * This plain format is accepted by SQLite.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>'yyyy-MM-dd'</code>
     */
    public static String formatSqlDatePlain(String argName, String argValue, MatchOperator operator) {
        return "'" + DefaultArgValueFormatter.DATE.format(argName, argValue, operator) + "'";
    }

    /**
     * Formats the given string if it matches the format <code>yyyy-MM-dd</code> and represents a valid UTC-time.
     * <p>
     * This "standard" format works with H2 and PostreSql but for example not with SQLite.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>DATE '2024-12-12'</code>
     */
    public static String formatSqlDateDefaultStyle(String argName, String argValue, MatchOperator operator) {
        return "DATE " + formatSqlDatePlain(argName, argValue, operator);
    }

    /**
     * Formats the given string if it matches the format <code>yyyy-MM-dd</code> and represents a valid UTC-time.
     * <p>
     * Should also work for PostgreSQL
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>TO_DATE('2024-12-12', 'YYYY-MM-DD')</code>
     */
    public static String formatSqlDateOracleStyle(String argName, String argValue, MatchOperator operator) {
        return "TO_DATE(" + formatSqlDatePlain(argName, argValue, operator) + ", 'YYYY-MM-DD')";
    }

    /**
     * Formats the given string if it matches the format <code>yyyy-MM-dd</code> and represents a valid UTC-time.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>TO_DATE('2024-12-12')</code>
     */
    public static String formatSqlDateSqlServerStyle(String argName, String argValue, MatchOperator operator) {
        return "TO_DATE(" + formatSqlDatePlain(argName, argValue, operator) + ")";
    }

    /**
     * Formats the given string if it matches the format <code>yyyy-MM-dd</code> and represents a valid UTC-time.
     * 
     * @param argName
     * @param argValue
     * @param operator
     * @return stamp of the form <code>DATE('2024-12-12')</code>
     */
    public static String formatSqlDateMySqlStyle(String argName, String argValue, MatchOperator operator) {
        return "DATE(" + formatSqlDatePlain(argName, argValue, operator) + ")";
    }

    /**
     * @param argValue
     * @return true if the given string is a valid timestamp following the pattern <code>yyyy-MM-dd HH:mm:ss</code>
     */
    private static boolean isFullTimestamp(String argValue) {
        if (argValue != null && argValue.length() > 10) {
            SimpleDateFormat sdf = new SimpleDateFormat(AdlDateUtils.AUDLANG_DATE_FORMAT + " HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            try {
                return argValue.equals(sdf.format(sdf.parse(argValue)));
            }
            catch (ParseException ex) {
                return false;
            }
        }
        return false;
    }

}
