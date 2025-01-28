//@formatter:off
/*
 * DefaultQueryParameterCreator
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

import static de.calamanari.adl.sql.SqlFormatConstants.TRUE;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.cnv.tps.AdlDateUtils;
import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.sql.config.FilterColumn;

/**
 * The {@link DefaultQueryParameterCreator} takes the base type ({@link AdlType#getBaseType()}) of an argument's type and tries to create a
 * {@link QueryParameter} applicable to the base type ({@link AdlSqlType#getBaseType()}) of the requested SQL-type.
 * <p>
 * This default implementation is only applicable to base types {@link DefaultAdlType} (input) resp. {@link DefaultAdlSqlType} (output).
 * <p>
 * <b>Note:</b> Part of this implementation are a couple of type alignments (see {@link #isTypeCombinationSupported(AdlType, AdlSqlType)}) to allow lenient
 * handling of differences wherever possible.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
@SuppressWarnings("java:S6548")
public class DefaultQueryParameterCreator implements QueryParameterCreator {

    private static final long serialVersionUID = -1904380668225305221L;

    private static final DefaultQueryParameterCreator INSTANCE = new DefaultQueryParameterCreator();

    /**
     * start with id <code>1000</code>
     */
    private static final AtomicInteger GLOBAL_ID_SEQUENCE = new AtomicInteger(1000);

    /**
     * @return the only instance of this creator
     */
    public static final DefaultQueryParameterCreator getInstance() {
        return INSTANCE;
    }

    private DefaultQueryParameterCreator() {
        // singleton
    }

    /**
     * This is the preferred method to create a valid parameter to ensure compatibility with the target column.
     * 
     * @param argMetaInfo describes the source argument type
     * @param argValue value to be passed as parameter of a query
     * @param matchOperator context
     * @param adlSqlType requested type
     * @return parameter for PreparedStatement execution
     * @throws AdlFormattingException in case of incompatibilities and errors
     */
    @Override
    public DefaultQueryParameter createParameter(String id, ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType) {

        if (id == null || argMetaInfo == null || matchOperator == null || adlSqlType == null) {
            throw new AdlFormattingException(String.format(
                    "Parameters id, argMetaInfo, matchOperator and adlSqlType must not be null, given: id=%s, argMetaInfo=%s, matchOperator=%s, adlSqlType=%s",
                    id, argMetaInfo, matchOperator, adlSqlType), AudlangMessage.msg(CommonErrors.ERR_4003_GENERAL_ERROR));
        }

        Context ctx = new Context(id, argMetaInfo, adlSqlType, matchOperator);

        assertTypesSupported(argValue, ctx);

        if (argValue == null) {
            return new DefaultQueryParameter(id, adlSqlType, null, matchOperator);
        }

        if (argMetaInfo.type().getBaseType() instanceof DefaultAdlType defaultType) {
            switch (defaultType) {
            case DefaultAdlType.STRING:
                return createStringParameter(argValue, ctx);
            case DefaultAdlType.INTEGER:
                return createLongParameter(argValue, ctx);
            case DefaultAdlType.DECIMAL:
                return createDoubleParameter(argValue, ctx);
            case DefaultAdlType.BOOL:
                return createBooleanParameter(argValue, ctx);
            case DefaultAdlType.DATE:
                return createDateParameter(argValue, ctx);
            }
        }
        throw new AdlFormattingException(String.format("Unsupported source argument type: %s (new DefaultAdlType introduced?)", argMetaInfo),
                AudlangMessage.argMsg(CommonErrors.ERR_4003_GENERAL_ERROR, argMetaInfo.argName()));

    }

    @Override
    public boolean isTypeCombinationSupported(AdlType sourceType, AdlSqlType targetType) {
        if (!(sourceType instanceof AdlSqlType) && sourceType.getBaseType() instanceof DefaultAdlType inputType
                && targetType.getBaseType() instanceof DefaultAdlSqlType outputType) {
            switch (inputType) {
            case DefaultAdlType.STRING:
                return true;
            case DefaultAdlType.INTEGER:
                return true;
            case DefaultAdlType.DECIMAL:
                return (outputType != DefaultAdlSqlType.SQL_BIT && outputType != DefaultAdlSqlType.SQL_BOOLEAN);
            case DefaultAdlType.BOOL:
                return (outputType == DefaultAdlSqlType.SQL_BIGINT || outputType == DefaultAdlSqlType.SQL_INTEGER
                        || outputType == DefaultAdlSqlType.SQL_SMALLINT || outputType == DefaultAdlSqlType.SQL_TINYINT
                        || outputType == DefaultAdlSqlType.SQL_BOOLEAN || outputType == DefaultAdlSqlType.SQL_BIT || outputType == DefaultAdlSqlType.SQL_CHAR
                        || outputType == DefaultAdlSqlType.SQL_LONGNVARCHAR || outputType == DefaultAdlSqlType.SQL_LONGVARCHAR
                        || outputType == DefaultAdlSqlType.SQL_NCHAR || outputType == DefaultAdlSqlType.SQL_NVARCHAR
                        || outputType == DefaultAdlSqlType.SQL_VARCHAR);
            case DefaultAdlType.DATE:
                return (outputType == DefaultAdlSqlType.SQL_DATE || outputType == DefaultAdlSqlType.SQL_TIMESTAMP || outputType == DefaultAdlSqlType.SQL_INTEGER
                        || outputType == DefaultAdlSqlType.SQL_BIGINT || outputType == DefaultAdlSqlType.SQL_CHAR
                        || outputType == DefaultAdlSqlType.SQL_LONGNVARCHAR || outputType == DefaultAdlSqlType.SQL_LONGVARCHAR
                        || outputType == DefaultAdlSqlType.SQL_NCHAR || outputType == DefaultAdlSqlType.SQL_NVARCHAR
                        || outputType == DefaultAdlSqlType.SQL_VARCHAR || outputType == DefaultAdlSqlType.SQL_DECIMAL
                        || outputType == DefaultAdlSqlType.SQL_DOUBLE || outputType == DefaultAdlSqlType.SQL_NUMERIC
                        || outputType == DefaultAdlSqlType.SQL_REAL);
            }
        }
        return false;
    }

    @Override
    public QueryParameter createParameter(ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType) {
        return createParameter("P_" + GLOBAL_ID_SEQUENCE.incrementAndGet(), argMetaInfo, argValue, matchOperator, adlSqlType);
    }

    /**
     * This is to avoid later confusion, we require the types used in the logical data model NOT to be {@link AdlSqlType}s because the base type testing would
     * otherwise not work.
     * 
     * @param argValue
     * @param ctx
     */
    private void assertTypesSupported(String argValue, Context ctx) {
        if (!(ctx.argMetaInfo.type().getBaseType() instanceof DefaultAdlType)) {
            throw new AdlFormattingException(String.format(
                    "Unsupported type %s in logical data model. Make sure any custom type's getBaseType() method returns one of the DefaultAdlTypes, given: id=%s, argMetaInfo=%s",
                    ctx.parameterId, ctx.argMetaInfo.type(), ctx.argMetaInfo),
                    AudlangMessage.argMsg(CommonErrors.ERR_4002_CONFIG_ERROR, ctx.argMetaInfo.argName()));
        }
        if (!(ctx.adlSqlType.getBaseType() instanceof DefaultAdlSqlType)) {
            throw new AdlFormattingException(String.format(
                    "Unsupported sql type request. Make sure any custom type's getBaseType() method returns one of the DefaultAdlSqlTypes, given: id=%s, adlSqlType=%s requested for argMetaInfo=%s",
                    ctx.parameterId, ctx.adlSqlType, ctx.argMetaInfo), AudlangMessage.argMsg(CommonErrors.ERR_4002_CONFIG_ERROR, ctx.argMetaInfo.argName()));
        }
        if (!isTypeCombinationSupported(ctx.argMetaInfo.type(), ctx.adlSqlType)) {
            throw createNotAlignableException(argValue, argValue, ctx);
        }
    }

    /**
     * First translates the argument's value by formatting it with its own formatter before selecting the right target format
     * 
     * @param argValue
     * @param ctx
     * @return parameter after basic validation
     */
    private DefaultQueryParameter createStringParameter(String argValue, Context ctx) {
        String formattedValue = ctx.argMetaInfo.type().getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        // Important:
        // Her we pass-through the raw string value from the expression
        // This creates a difference between the behavior when using PreparedStatements (all characters preserved)
        // and the debug output (applyUnsafe) where the formatter will remove characters < 32
        return createTargetParameter(argValue, formattedValue, ctx);
    }

    /**
     * Takes the string value coming from an argument value and tries to find the right way to align its type to a requested target column
     * 
     * @param argValue
     * @param formattedValue
     * @param ctx
     * @return parameter
     */
    private DefaultQueryParameter createTargetParameter(String argValue, String formattedValue, Context ctx) {

        if (ctx.adlSqlType.getBaseType() instanceof DefaultAdlSqlType baseType) {
            switch (baseType) {
            case SQL_BOOLEAN, SQL_BIT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, parseBoolean(argValue, ctx), ctx.matchOperator);
            case SQL_DATE:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, parseDate(argValue, ctx), ctx.matchOperator);
            case SQL_DECIMAL, SQL_DOUBLE, SQL_REAL:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, parseDouble(argValue, ctx), ctx.matchOperator);
            case SQL_NUMERIC:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        BigDecimal.valueOf(parseDouble(argValue, ctx)).setScale(7, RoundingMode.HALF_UP), ctx.matchOperator);
            case SQL_FLOAT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (float) parseDouble(argValue, ctx), ctx.matchOperator);
            case SQL_TIMESTAMP:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        new Timestamp(assertInRange(parseLongOrTimestamp(argValue, ctx), 0, 253_402_300_799000L, formattedValue, ctx)), ctx.matchOperator);
            case SQL_BIGINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, parseLongOrEpochSeconds(argValue, ctx), ctx.matchOperator);
            case SQL_INTEGER:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        (int) assertInRange(parseLongOrEpochSeconds(argValue, ctx), -2_147_483_647, 2_147_483_647, formattedValue, ctx), ctx.matchOperator);
            case SQL_SMALLINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        (short) assertInRange(parseLong(argValue, ctx), -32767, 32767, formattedValue, ctx), ctx.matchOperator);
            case SQL_TINYINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (byte) assertInRange(parseLong(argValue, ctx), 0, 255, formattedValue, ctx),
                        ctx.matchOperator);
            case SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, argValue, ctx.matchOperator);
            }
        }
        throw createNotAlignableException(argValue, formattedValue, ctx);
    }

    /**
     * @param argValue
     * @param ctx
     * @return parameter created based on source type long
     */
    private DefaultQueryParameter createLongParameter(String argValue, Context ctx) {
        String formattedValue = ctx.argMetaInfo.type().getFormatter().format(argValue, argValue, ctx.matchOperator);
        try {
            long value = Long.parseLong(formattedValue);
            return createTargetParameter(value, false, formattedValue, ctx);
        }
        catch (NumberFormatException ex) {
            throw new AdlFormattingException(String.format(
                    "Unexpected formatting exception, %s is not of type long (error in formatter of argMetaInfo=%s with id=%s, argValue=%s, matchOperator=%s, adlSqlType=%s)",
                    formattedValue, ctx.argMetaInfo, ctx.parameterId, argValue, ctx.matchOperator, ctx.adlSqlType), ex,
                    AudlangMessage.argValueMsg(CommonErrors.ERR_2004_VALUE_FORMAT, ctx.argMetaInfo.argName(), argValue));
        }
    }

    /**
     * @param argValue
     * @param ctx
     * @return parameter created based on source type double
     */
    private DefaultQueryParameter createDoubleParameter(String argValue, Context ctx) {
        String formattedValue = ctx.argMetaInfo.type().getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        try {
            double value = Double.parseDouble(formattedValue);
            return createTargetParameter(value, formattedValue, ctx);
        }
        catch (NumberFormatException ex) {
            throw new AdlFormattingException(String.format(
                    "Unexpected formatting exception, %s is not of type double (error in formatter of argMetaInfo=%s with id=%s, argValue=%s, matchOperator=%s, adlSqlType=%s)",
                    formattedValue, ctx.argMetaInfo, ctx.parameterId, argValue, ctx.matchOperator, ctx.adlSqlType), ex,
                    AudlangMessage.argValueMsg(CommonErrors.ERR_2004_VALUE_FORMAT, ctx.argMetaInfo.argName(), argValue));
        }
    }

    /**
     * @param argValue
     * @param formattedValue
     * @param ctx
     * @return parameter created based on converted double and the requested target type
     */
    private DefaultQueryParameter createTargetParameter(double argValue, String formattedValue, Context ctx) {

        if (ctx.adlSqlType.getBaseType() instanceof DefaultAdlSqlType baseType) {
            switch (baseType) {
            case SQL_DATE:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, new Date((long) argValue), ctx.matchOperator);
            case SQL_DECIMAL, SQL_DOUBLE, SQL_REAL:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, argValue, ctx.matchOperator);
            case SQL_NUMERIC:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, BigDecimal.valueOf(argValue).setScale(7, RoundingMode.HALF_UP),
                        ctx.matchOperator);
            case SQL_FLOAT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (float) argValue, ctx.matchOperator);
            case SQL_TIMESTAMP:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        new Timestamp(assertInRange((long) argValue, 0, 253_402_300_799000L, formattedValue, ctx)), ctx.matchOperator);
            case SQL_BIGINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (long) argValue, ctx.matchOperator);
            case SQL_INTEGER:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        (int) assertInRange((long) argValue, -2_147_483_647, 2_147_483_647, formattedValue, ctx), ctx.matchOperator);
            case SQL_SMALLINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (short) assertInRange((long) argValue, -32767, 32767, formattedValue, ctx),
                        ctx.matchOperator);
            case SQL_TINYINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (byte) assertInRange((long) argValue, 0, 255, formattedValue, ctx),
                        ctx.matchOperator);
            case SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, formattedValue, ctx.matchOperator);
            // $CASES-OMITTED$
            default:
            }
        }
        throw createNotAlignableException(argValue, "" + argValue, ctx);

    }

    /**
     * @param argValue
     * @param ctx
     * @return parameter based on adl-boolean input
     */
    private DefaultQueryParameter createBooleanParameter(String argValue, Context ctx) {
        String formattedValue = ctx.argMetaInfo.type().getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        if ("0".equals(argValue)) {
            return createTargetParameter(false, formattedValue, ctx);
        }
        else if ("1".equals(argValue)) {
            return createTargetParameter(true, formattedValue, ctx);
        }
        else {
            throw new AdlFormattingException(String.format(
                    "Unexpected formatting exception, %s is unknown, expected '0' or '1' (error in formatter of argMetaInfo=%s with id=%s, argValue=%s, matchOperator=%s, adlSqlType=%s)",
                    formattedValue, ctx.argMetaInfo, ctx.parameterId, argValue, ctx.matchOperator, ctx.adlSqlType),
                    AudlangMessage.argValueMsg(CommonErrors.ERR_2005_VALUE_FORMAT_BOOL, ctx.argMetaInfo.argName(), argValue));
        }
    }

    /**
     * @param argValue
     * @param formattedValue
     * @param ctx
     * @return parameter created based on boolean value and requested type
     */
    private DefaultQueryParameter createTargetParameter(boolean argValue, String formattedValue, Context ctx) {
        if (ctx.adlSqlType.getBaseType() instanceof DefaultAdlSqlType baseType) {
            switch (baseType) {
            case SQL_BIGINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (argValue ? 1L : 0L), ctx.matchOperator);
            case SQL_INTEGER:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (argValue ? 1 : 0), ctx.matchOperator);
            case SQL_SMALLINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (short) (argValue ? 1 : 0), ctx.matchOperator);
            case SQL_TINYINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (byte) (argValue ? 1 : 0), ctx.matchOperator);
            case SQL_BOOLEAN, SQL_BIT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, argValue, ctx.matchOperator);
            case SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, formattedValue, ctx.matchOperator);
            // $CASES-OMITTED$
            default:
            }
        }
        throw createNotAlignableException(argValue, "" + argValue, ctx);

    }

    /**
     * @param argValue
     * @param matchOperator
     * @param ctx
     * @return parameter based on date field
     */
    private DefaultQueryParameter createDateParameter(String argValue, Context ctx) {
        String formattedValue = ctx.argMetaInfo.type().getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        long utcMillis = AdlDateUtils.tryParseUtcMillis(formattedValue);
        if (utcMillis >= 0) {
            return createTargetParameter(utcMillis, true, formattedValue, ctx);
        }
        else {
            throw new AdlFormattingException(String.format(
                    "Unexpected formatting exception, %s is no valid date value (error in formatter of argMetaInfo=%s with id=%s, argValue=%s, matchOperator=%s, adlSqlType=%s)",
                    formattedValue, ctx.argMetaInfo, ctx.parameterId, argValue, ctx.matchOperator, ctx.adlSqlType),
                    AudlangMessage.argValueMsg(CommonErrors.ERR_2006_VALUE_FORMAT_DATE, ctx.argMetaInfo.argName(), argValue));

        }
    }

    /**
     * @param argValue
     * @param assumeDateMillis if true we assume the given value is a millisecond timestamp (convert to epoch as int)
     * @param formattedValue
     * @param ctx
     * @return parameter based on long value and the requested target type
     */
    private DefaultQueryParameter createTargetParameter(long argValue, boolean assumeUtcMillis, String formattedValue, Context ctx) {
        if (ctx.adlSqlType.getBaseType() instanceof DefaultAdlSqlType baseType) {
            switch (baseType) {
            case SQL_BOOLEAN, SQL_BIT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, assertInRange(argValue, 0, 1, formattedValue, ctx) == 1, ctx.matchOperator);
            case SQL_DATE:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        new Date(assertInRange(argValue, 0, 253_402_300_799_000L, formattedValue, ctx)), ctx.matchOperator);
            case SQL_DECIMAL, SQL_DOUBLE, SQL_REAL:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, Double.valueOf(argValue), ctx.matchOperator);
            case SQL_NUMERIC:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, BigDecimal.valueOf(argValue).setScale(7, RoundingMode.HALF_UP),
                        ctx.matchOperator);
            case SQL_FLOAT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (float) argValue, ctx.matchOperator);
            case SQL_TIMESTAMP:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        new Timestamp(assertInRange(argValue, 0, 253_402_300_799_000L, formattedValue, ctx)), ctx.matchOperator);
            case SQL_BIGINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, toEpochSecondsIfRequired(argValue, assumeUtcMillis), ctx.matchOperator);
            case SQL_INTEGER:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType,
                        (int) assertInRange(toEpochSecondsIfRequired(argValue, assumeUtcMillis), -2_147_483_647, 2_147_483_647, formattedValue, ctx),
                        ctx.matchOperator);
            case SQL_SMALLINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (short) assertInRange(argValue, -32767, 32767, formattedValue, ctx),
                        ctx.matchOperator);
            case SQL_TINYINT:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, (byte) assertInRange(argValue, 0, 255, formattedValue, ctx),
                        ctx.matchOperator);
            case SQL_CHAR, SQL_LONGNVARCHAR, SQL_LONGVARCHAR, SQL_NCHAR, SQL_NVARCHAR, SQL_VARCHAR:
                return new DefaultQueryParameter(ctx.parameterId, ctx.adlSqlType, formattedValue, ctx.matchOperator);
            }
        }
        throw createNotAlignableException(argValue, formattedValue, ctx);

    }

    /**
     * When we store a date in an SQL_INTEGER (32 bit) or SQL_BIGINTEGER (64 bit) we convert from utc millis to epoch seconds.
     * 
     * @param millis
     * @param required
     * @return either the millis or the seconds if required=true
     */
    private static long toEpochSecondsIfRequired(long millis, boolean required) {
        return required ? millis / 1000 : millis;
    }

    /**
     * Tests whether the value is in a certain range
     * 
     * @param value
     * @param min
     * @param max
     * @param formattedValue
     * @param ctx
     * @return value
     * @throws AdlFormattingException if the value was not in range
     */
    private long assertInRange(long value, long min, long max, String formattedValue, Context ctx) {
        if (value >= min && value <= max) {
            return value;
        }
        else {
            throw new AdlFormattingException(String.format(
                    "Unable to align the argument value to the target type (not within in expected range [%d..%d]), given: id=%s, argMetaInfo=%s, argValue=%s (%s), matchOperator=%s, adlSqlType=%s",
                    min, max, ctx.parameterId, ctx.argMetaInfo, formattedValue, formattedValue, ctx.matchOperator, ctx.adlSqlType),
                    AudlangMessage.argValueMsg(CommonErrors.ERR_2003_VALUE_RANGE, ctx.argMetaInfo.argName(), formattedValue));
        }
    }

    /**
     * @param argValue
     * @param ctx
     * @return boolean value parsed from the given string value
     */
    private boolean parseBoolean(String argValue, Context ctx) {
        String formattedValue = DefaultAdlType.BOOL.getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        return TRUE.equals(formattedValue);
    }

    /**
     * @param argValue
     * @param ctx
     * @return Date value (UTC) parsed from the given string value
     */
    private Date parseDate(String argValue, Context ctx) {
        DefaultAdlType.DATE.getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        return new Date(AdlDateUtils.tryParseUtcMillis(argValue));
    }

    /**
     * @param argValue
     * @param ctx
     * @return double value parsed from the given string value
     */
    private double parseDouble(String argValue, Context ctx) {
        String formattedValue = DefaultAdlType.DECIMAL.getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        return Double.parseDouble(formattedValue);
    }

    /**
     * @param argValue
     * @param ctx
     * @return long value parsed from the given string value
     */
    private long parseLong(String argValue, Context ctx) {
        String formattedValue = DefaultAdlType.INTEGER.getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        return Long.parseLong(formattedValue);
    }

    /**
     * The alignment of string timestamps (with time portion) to timestamp millis here is a compromise to fully support {@link FilterColumn}s of type
     * SQL_TIMESTAMP. The method understands <code>yyyy-MM-dd</code> and also <code>yyyy-MM-dd HH:mm:ss</code>.
     * 
     * @param argValue
     * @param ctx
     * @return long value, date millis or full timestamp millis if the given string is a valid timestamp following the pattern <code>yyyy-MM-dd HH:mm:ss</code>
     */
    private long parseLongOrTimestamp(String argValue, Context ctx) {
        if (argValue != null && argValue.length() > 10) {
            SimpleDateFormat sdf = new SimpleDateFormat(AdlDateUtils.AUDLANG_DATE_FORMAT + " HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            try {
                java.util.Date temp = sdf.parse(argValue);
                if (argValue.equals(sdf.format(temp))) {
                    return temp.getTime();
                }
            }
            catch (ParseException ex) {
                // probing failed
            }
        }
        return parseLong(argValue, ctx);
    }

    /**
     * This method adjusts the millis to epoch seconds if the input was a date string yyyy-MM-dd
     * 
     * @param argValue
     * @param ctx
     * @return long value parsed from the given string value
     */
    private long parseLongOrEpochSeconds(String argValue, Context ctx) {
        String formattedValue = DefaultAdlType.INTEGER.getFormatter().format(ctx.argMetaInfo.argName(), argValue, ctx.matchOperator);
        long res = Long.parseLong(formattedValue);
        if (AdlDateUtils.tryParseUtcMillis(argValue) > -1) {
            res = res / 1000;
        }
        return res;
    }

    /**
     * @param argValue
     * @param formattedValue
     * @param ctx
     * @return an exception to be thrown in case of a failed type alignment or formatting issue
     */
    private static AdlFormattingException createNotAlignableException(Object argValue, String formattedValue, Context ctx) {
        return new AdlFormattingException(String.format(
                "Unable to align the argument value to the target type, given: id=%s, argMetaInfo=%s, argValue=%s (%s), matchOperator=%s, adlSqlType=%s",
                ctx.parameterId, ctx.argMetaInfo, formattedValue, argValue, ctx.matchOperator, ctx.adlSqlType),
                AudlangMessage.argValueMsg(CommonErrors.ERR_3001_TYPE_MISMATCH, ctx.argMetaInfo.argName(), formattedValue));
    }

    /**
     * @return singleton instance in JVM
     */
    Object readResolve() {
        return INSTANCE;
    }

    /**
     * Resets the ID-sequence (for parameters) to its initial value (for testing purposes to get reproducible results)
     */
    public static void resetIdSequence() {
        GLOBAL_ID_SEQUENCE.set(1000);
    }

    /**
     * Container for passing a couple of fixed parameters (avoid lengthy parameter lists)
     */
    private record Context(String parameterId, ArgMetaInfo argMetaInfo, AdlSqlType adlSqlType, MatchOperator matchOperator) {

    }

}
