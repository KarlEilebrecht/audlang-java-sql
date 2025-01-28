//@formatter:off
/*
 * MatchCondition
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

package de.calamanari.adl.sql.cnv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ConversionException;
import de.calamanari.adl.cnv.TemplateParameterUtils;
import de.calamanari.adl.cnv.tps.AdlDateUtils;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ContainsNotSupportedException;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.cnv.tps.LessThanGreaterThanNotSupportedException;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.Operand;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;
import de.calamanari.adl.sql.QueryParameter;
import de.calamanari.adl.sql.QueryParameterCreator;
import de.calamanari.adl.sql.config.AdlSqlColumn;
import de.calamanari.adl.sql.config.DataColumn;
import de.calamanari.adl.sql.config.DataTableConfig;
import de.calamanari.adl.sql.config.FilterColumn;
import de.calamanari.adl.sql.config.TableMetaInfo;

/**
 * A {@link MatchCondition} is an instruction to create an SQL term that either ...
 * <ul>
 * <li>compares a column against a value.</li>
 * <li>compares a column against multiple values.</li>
 * <li>compares a column against another column.</li>
 * </ul>
 * By intention the record temporarily introduces some redundancy to make it easier to work with the complex meta data.
 * <p>
 * Instances are deeply immutable.
 * 
 * @param operator comparison operator, equals in case of an IN-clause
 * @param isNegation if true this shall later be a NOT resp. NOT IN
 * @param argNameLeft name of the argument on the left, mandatory
 * @param tableLeft meta data of the table on the left, mandatory
 * @param columnLeft meta data of the column on the left, mandatory
 * @param argNameRight name of the argument on the right in case of a reference match, null by default
 * @param tableRight meta data of the table on the right in case of a reference match, null by default
 * @param columnRight meta data of the column on the right in case of a reference match, null by default
 * @param columnConditions defines the parameters per column
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record MatchCondition(MatchOperator operator, boolean isNegation, String argNameLeft, TableMetaInfo tableLeft, DataColumn columnLeft,
        String argNameRight, TableMetaInfo tableRight, DataColumn columnRight, List<ColumnCondition> columnConditions) implements Serializable {

    /**
     * String type of all the filter values (specified in config)
     */
    private static final ArgMetaInfo FILTER_META_INFO = new ArgMetaInfo("<FILTER>", DefaultAdlType.STRING, false, false);

    /**
     * special case: DATE <br>
     * The filter date (from query) is date-precision 2024-12-10 (00:00:00) while the column may contain a higher precision<br>
     * time like 2024-12-10 02:23:11<br>
     * <ul>
     * <li>2024-12-10 02:23:11 is not equal to 2024-12-10 00:00:00</li>
     * <li>2024-12-10 02:23:11 is greater than 2024-12-10 00:00:00</li>
     * </ul>
     * Both results won't meet the user expectations<br>
     * Thus we create a surrogate parameter with the suffix "_NEXT_DAY" for such dates, so that it can be referenced <b>by name</b> later in the query
     * generation process to create a range.
     */
    private static final String NEXT_DAY_PARAM_SUFFIX = "_NEXT_DAY";

    /**
     * @param operator comparison operator, equals in case of an IN-clause
     * @param isNegation if true this shall later be a NOT resp. NOT IN
     * @param argNameLeft name of the argument on the left, mandatory
     * @param tableLeft meta data of the table on the left, mandatory
     * @param columnLeft meta data of the column on the left, mandatory
     * @param argNameRight name of the argument on the right in case of a reference match, null by default
     * @param tableRight meta data of the table on the right in case of a reference match, null by default
     * @param columnRight meta data of the column on the right in case of a reference match, null by default
     * @param columnConditions defines the parameters per column
     */
    public MatchCondition(MatchOperator operator, boolean isNegation, String argNameLeft, TableMetaInfo tableLeft, DataColumn columnLeft, String argNameRight,
            TableMetaInfo tableRight, DataColumn columnRight, List<ColumnCondition> columnConditions) {
        if (operator == null || argNameLeft == null || tableLeft == null || columnLeft == null || columnConditions == null
                || columnConditions.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(String.format(
                    "The arguments must not be null nor contain any nulls, given: operator=%s, argNameLeft=%s, tableLeft=%s, columnLeft=%s, argNameRight=%s, tableRight=%s, columnRight=%s and columnConditions=%s",
                    operator, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));
        }

        if (!checkColumnsUnique(columnConditions)) {
            throw new IllegalArgumentException(String.format(
                    """
                            A column must be unique inside the columnConditions list resp. for both sides of a reference match, given: operator=%s, argNameLeft=%s, tableLeft=%s, columnLeft=%s, argNameRight=%s, tableRight=%s, columnRight=%s, and columnConditions=%s
                            In this query the same column would be compared twice within the same condition (usually this cannot be fulfilled).
                            This rather indicates a bug in the implementation than a mapping problem.
                            """,
                    operator, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));
        }

        if (tableRight == null) {

            ColumnCondition parameters = columnConditions.stream().filter(cp -> cp.column().columnName().equals(columnLeft.columnName())).findAny()
                    .orElse(null);
            if (parameters == null) {
                throw new IllegalArgumentException(String.format(
                        "For a value match you must specify at least one parameter for column %s, given: operator=%s, argNameLeft=%s, tableLeft=%s, columnLeft=%s, argNameRight=%s, tableRight=%s, columnRight=%s, and columnConditions=%s",
                        columnLeft.columnName(), operator, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));
            }
        }

        if (!checkAllNullOrAllNotNull(argNameRight, tableRight, columnRight)) {
            throw new IllegalArgumentException(String.format(
                    "The arguments argNameRight, tableRight, columnRight either all have to be null or all not null, given: operator=%s, argNameLeft=%s, tableLeft=%s, columnLeft=%s, argNameRight=%s, tableRight=%s, columnRight=%s, and columnConditions=%s",
                    operator, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));
        }
        this.operator = operator;
        this.isNegation = isNegation;
        this.argNameLeft = argNameLeft;
        this.tableLeft = tableLeft;
        this.columnLeft = columnLeft;
        this.argNameRight = argNameRight;
        this.tableRight = tableRight;
        this.columnRight = columnRight;
        this.columnConditions = columnConditions.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(columnConditions));

    }

    /**
     * Tests whether there is a condition conflict.
     * <p>
     * The same column must not be matched to a value twice within the same match condition. If this happens there is either a mapping error or a bug in the
     * implementation.
     * 
     * @param columnConditions
     * @return true if every column is only compared once
     */
    private static boolean checkColumnsUnique(List<ColumnCondition> columnConditions) {

        List<AdlSqlColumn> leftColumns = columnConditions.stream().filter(Predicate.not(condition -> condition.type() == ColumnConditionType.FILTER_RIGHT))
                .map(ColumnCondition::column).toList();
        List<AdlSqlColumn> rightColumns = columnConditions.stream().filter(condition -> condition.type() == ColumnConditionType.FILTER_RIGHT)
                .map(ColumnCondition::column).toList();

        return leftColumns.size() == new HashSet<>(leftColumns).size() && rightColumns.size() == new HashSet<>(rightColumns).size();

    }

    /**
     * For consistency reasons we require some parameters to either be all null or all not-null (avoids ugly NPEs later)
     * 
     * @param params to be tested
     * @return true if all of the elements were null or all of them were not-null
     */
    private static boolean checkAllNullOrAllNotNull(Object... params) {
        boolean anyNull = false;
        boolean anyNotNull = false;
        for (Object param : params) {
            if ((param != null && anyNull) || (param == null && anyNotNull)) {
                return false;
            }
            else if (param == null) {
                anyNull = true;
            }
            else {
                anyNotNull = true;
            }
        }
        return true;
    }

    /**
     * Creates a simple (negated) match condition (against value, or IS NULL) from the given expression
     * 
     * @param expression
     * @param ctx
     * @return condition
     */
    public static MatchCondition createSimpleCondition(SimpleExpression expression, SqlConversionProcessContext ctx) {
        if (expression instanceof NegationExpression neg) {
            return negate(createSimpleCondition(neg.delegate(), ctx));
        }
        else {
            return createMatchConditionInternal(expression, ctx);
        }
    }

    /**
     * Creates a simple match condition (against value, or IS NULL) from the given expression
     * 
     * @param expression
     * @param ctx
     * @return condition
     */
    private static MatchCondition createMatchConditionInternal(SimpleExpression expression, SqlConversionProcessContext ctx) {
        DataTableConfig dataTableConfig = ctx.getDataBinding().dataTableConfig();
        MatchOperator operator = expression.operator();
        String argNameLeft = expression.argName();
        DataColumn columnLeft = dataTableConfig.lookupColumn(argNameLeft, ctx);
        TableMetaInfo tableLeft = dataTableConfig.lookupTableMetaInfo(argNameLeft, ctx);
        String argNameRight = expression.referencedArgName();
        if (argNameRight != null && ConversionDirective.DISABLE_REFERENCE_MATCHING.check(ctx.getGlobalFlags())) {
            AudlangMessage userMessage = AudlangMessage.argRefMsg(CommonErrors.ERR_2101_REFERENCE_MATCH_NOT_SUPPORTED, argNameLeft, argNameRight);
            throw new ConversionException(
                    String.format("Reference matching disabled by global directive, given: expression=%s, argName=%s", expression, argNameLeft), userMessage);
        }
        DataColumn columnRight = argNameRight != null ? dataTableConfig.lookupColumn(argNameRight, ctx) : null;
        TableMetaInfo tableRight = argNameRight != null ? dataTableConfig.lookupTableMetaInfo(argNameRight, ctx) : null;

        List<ColumnCondition> columnParameters = new ArrayList<>();
        if (columnRight == null) {
            columnParameters.add(createPrimaryColumnCondition(expression, argNameLeft, columnLeft, ctx));
        }
        createFilterColumnConditions(expression, columnParameters, ctx);
        return new MatchCondition(operator, false, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnParameters);
    }

    /**
     * Tells whether we should perform an alignment of the given data to a higher resolution stamp before creating the query, to for example turn an equals into
     * a range query.
     * 
     * @param type source type
     * @param columnType destination type
     * @param ctx
     * @return true if the date needs alignment
     */
    public static boolean shouldAlignDate(AdlType type, AdlSqlType columnType, SqlConversionProcessContext ctx) {
        return (type.getBaseType() == DefaultAdlType.DATE
                && (columnType.getBaseType() == DefaultAdlSqlType.SQL_BIGINT || columnType.getBaseType() == DefaultAdlSqlType.SQL_INTEGER
                        || columnType.getBaseType() == DefaultAdlSqlType.SQL_TIMESTAMP)
                && !ConversionDirective.DISABLE_DATE_TIME_ALIGNMENT.check(ctx.getGlobalFlags()));
    }

    /**
     * Composes the primary parameter(s) for direct value matches of the main column (columnLeft)
     * <p>
     * Special handling of dates and preparation of contains snippets happens here.
     * <p>
     * Not applicable to reference matches.
     * 
     * @param expression
     * @param argName
     * @param columnLeft
     * @param ctx
     * @return prepared parameter list for the column
     */
    private static ColumnCondition createPrimaryColumnCondition(SimpleExpression expression, String argName, DataColumn columnLeft,
            SqlConversionProcessContext ctx) {
        AdlSqlType columnType = columnLeft.columnType();
        DataTableConfig dataTableConfig = ctx.getDataBinding().dataTableConfig();
        MatchOperator operator = expression.operator();
        String value = expression.operand() != null ? expression.operand().value() : null;

        if ((expression.operator() == MatchOperator.EQUALS || expression.operator() == MatchOperator.GREATER_THAN)
                && shouldAlignDate(dataTableConfig.typeOf(argName), columnType, ctx)) {

            QueryParameter date = columnType.getQueryParameterCreator().createParameter(dataTableConfig.lookup(argName), value, operator, columnType);
            ctx.registerParameter(date);

            // add supplementary parameter for the NEXT DAY to potentially create a range query later
            // if the underlying type's time resolution is finer than day
            String valueNextDay = AdlDateUtils.computeDayAfter(dataTableConfig.typeOf(argName).getFormatter().format(argName, value, operator));
            QueryParameter nextDate = columnType.getQueryParameterCreator().createParameter(date.id() + MatchCondition.NEXT_DAY_PARAM_SUFFIX,
                    dataTableConfig.lookup(argName), valueNextDay, operator, columnType);
            ctx.registerParameter(nextDate);
            return createAlignedDateColumnCondition(expression, columnLeft, operator, date, nextDate);
        }
        else if (expression.operator() == MatchOperator.CONTAINS) {
            assertContainsSupported(expression, argName, columnType, ctx);
            // prepare the text (user-specified pattern) before including it in the query
            value = ctx.getDataBinding().sqlContainsPolicy().prepareSearchSnippet(value);
        }
        else if (expression.operator() == MatchOperator.LESS_THAN || expression.operator() == MatchOperator.GREATER_THAN) {
            assertLessThanGreaterThanSupported(expression, argName, columnType, ctx);
        }

        // we create a parameter for null even if never used (we write IS NULL instead)
        // this shall help with debugging
        List<QueryParameter> parameters = Collections
                .singletonList(columnType.getQueryParameterCreator().createParameter(dataTableConfig.lookup(argName), value, operator, columnType));
        ctx.registerParameter(parameters.get(0));

        return new ColumnCondition(ColumnConditionType.SINGLE, operator, columnLeft, parameters);

    }

    /**
     * @param expression
     * @param argName
     * @param columnType
     * @param ctx
     * @throws LessThanGreaterThanNotSupportedException if the translation is not possible or not permitted
     */
    private static void assertLessThanGreaterThanSupported(SimpleExpression expression, String argName, AdlSqlType columnType,
            SqlConversionProcessContext ctx) {
        DataTableConfig dataTableConfig = ctx.getDataBinding().dataTableConfig();
        if (!columnType.supportsLessThanGreaterThan() || !dataTableConfig.typeOf(argName).supportsLessThanGreaterThan()) {
            AudlangMessage userMessage = AudlangMessage.argMsg(CommonErrors.ERR_2201_LTGT_NOT_SUPPORTED, argName);
            throw new LessThanGreaterThanNotSupportedException(
                    String.format("LESS/GREATER THAN not supported by type=%s or columnType=%s, given: expression=%s, argName=%s",
                            dataTableConfig.typeOf(argName), columnType, expression, argName),
                    userMessage);
        }
        if (ConversionDirective.DISABLE_LESS_THAN_GREATER_THAN.check(ctx.getGlobalFlags())) {
            AudlangMessage userMessage = AudlangMessage.argMsg(CommonErrors.ERR_2201_LTGT_NOT_SUPPORTED, argName);
            throw new LessThanGreaterThanNotSupportedException(
                    String.format("LESS/GREATER THAN disabled by global directive, given: expression=%s, argName=%s", expression, argName), userMessage);
        }
    }

    /**
     * @param expression
     * @param argName
     * @param columnType
     * @param ctx
     * @throws ContainsNotSupportedException if the translation is not possible or not permitted
     */
    private static void assertContainsSupported(SimpleExpression expression, String argName, AdlSqlType columnType, SqlConversionProcessContext ctx) {
        DataTableConfig dataTableConfig = ctx.getDataBinding().dataTableConfig();
        if (!columnType.supportsContains() || !dataTableConfig.typeOf(argName).supportsContains()) {
            AudlangMessage userMessage = AudlangMessage.argMsg(CommonErrors.ERR_2200_CONTAINS_NOT_SUPPORTED, argName);
            throw new ContainsNotSupportedException(String.format("CONTAINS not supported by type=%s or columnType=%s, given: expression=%s, argName=%s",
                    dataTableConfig.typeOf(argName), columnType, expression, argName), userMessage);
        }
        if (ConversionDirective.DISABLE_CONTAINS.check(ctx.getGlobalFlags())) {
            AudlangMessage userMessage = AudlangMessage.argMsg(CommonErrors.ERR_2200_CONTAINS_NOT_SUPPORTED, argName);
            throw new ContainsNotSupportedException(
                    String.format("CONTAINS disabled by global directive, given: expression=%s, argName=%s", expression, argName), userMessage);
        }
    }

    /**
     * @param expression
     * @param columnLeft
     * @param operator
     * @param parameters
     * @param date
     * @param nextDate
     * @return either a data range column condition (for equals) or "after today" replacement (for greater than)
     */
    private static ColumnCondition createAlignedDateColumnCondition(SimpleExpression expression, DataColumn columnLeft, MatchOperator operator,
            QueryParameter date, QueryParameter nextDate) {
        List<QueryParameter> parameters = new ArrayList<>();
        if (expression.operator() == MatchOperator.EQUALS) {
            parameters.add(date);
            parameters.add(nextDate);
            return new ColumnCondition(ColumnConditionType.DATE_RANGE, operator, columnLeft, parameters);
        }
        else {
            parameters.add(nextDate);
            return new ColumnCondition(ColumnConditionType.AFTER_TODAY, operator, columnLeft, parameters);
        }
    }

    /**
     * This method appends the parameters for the filter columns of the main column(s).
     * 
     * @param expression
     * @param columnConditions
     * @param ctx
     */
    private static void createFilterColumnConditions(SimpleExpression expression, List<ColumnCondition> columnConditions, SqlConversionProcessContext ctx) {
        createFilterColumnConditions(ColumnConditionType.FILTER_LEFT, expression.argName(), columnConditions, ctx);
        if (expression.referencedArgName() != null) {
            createFilterColumnConditions(ColumnConditionType.FILTER_RIGHT, expression.referencedArgName(), columnConditions, ctx);
        }
    }

    /**
     * This method creates the parameters for the filter columns of the main column that belongs to the argName.
     * 
     * @param filterType
     * @param argName
     * @param columnConditions
     * @param ctx
     */
    private static void createFilterColumnConditions(ColumnConditionType filterType, String argName, List<ColumnCondition> columnConditions,
            SqlConversionProcessContext ctx) {

        DataTableConfig dataTableConfig = ctx.getDataBinding().dataTableConfig();

        ctx.getGlobalVariables().put(FilterColumn.VAR_ARG_NAME, argName);
        DataColumn column = dataTableConfig.lookupColumn(argName, ctx);

        for (FilterColumn filterColumn : column.filters()) {
            columnConditions.add(createFilterColumnCondition(filterType, filterColumn, ctx));
        }
        ctx.getGlobalVariables().remove(FilterColumn.VAR_ARG_NAME);

        for (FilterColumn filterColumn : dataTableConfig.lookupTableMetaInfo(argName, ctx).tableFilters()) {
            // also add the filters defined on the table
            columnConditions.add(createFilterColumnCondition(filterType, filterColumn, ctx));
        }

    }

    /**
     * Creates a single filter column condition (column or table filter)
     * 
     * @param filterType
     * @param filterColumn
     * @param ctx
     * @return filter column condition
     */
    public static ColumnCondition createFilterColumnCondition(ColumnConditionType filterType, FilterColumn filterColumn, SqlConversionProcessContext ctx) {
        QueryParameter parameter = null;
        // @formatter:off
        parameter = filterColumn.columnType().getQueryParameterCreator()
                .createParameter(FILTER_META_INFO,
                                 TemplateParameterUtils.replaceVariables(
                                                             filterColumn.filterValue(), 
                                                             ctx.getGlobalVariables()::get), 
                                 MatchOperator.EQUALS,
                                 filterColumn.columnType());
        ctx.registerParameter(parameter);
        // @formatter:on
        return new ColumnCondition(filterType, MatchOperator.EQUALS, filterColumn, Collections.singletonList(parameter));
    }

    /**
     * @param condition
     * @return copy of the the given condition with {@link #isNegation}==true
     */
    public static MatchCondition negate(MatchCondition condition) {
        return new MatchCondition(condition.operator, true, condition.argNameLeft, condition.tableLeft, condition.columnLeft, condition.argNameRight,
                condition.tableRight, condition.columnRight, condition.columnConditions);

    }

    /**
     * Assumes a list of {@link MatchExpression}s <i>or</i> {@link NegationExpression}s all dealing with the same argName, EQUALS-operator and <i>plain
     * values</i>, so the result will be an IN-clause in case of OR vs. a NOT IN clause in case of AND.
     * 
     * @param expressions either the elements of an OR (IN, only {@link MatchExpression}s) or the elements of an AND (NOT IN, only {@link NegationExpression}s)
     * @param ctx
     * @return condition
     * @throws IllegalArgumentException if there are multiple argNames involved, multiple operands reference matches was detected.
     * @throws ClassCastException if expressions are not all of the same type
     */
    public static MatchCondition createInClauseCondition(List<SimpleExpression> expressions, SqlConversionProcessContext ctx) {
        DataTableConfig dataTableConfig = ctx.getDataBinding().dataTableConfig();

        if (expressions.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Cannot creatre IN or NOT IN clause match condition without member expressions, given: expressions=%s", expressions));
        }
        else if (expressions.size() == 1) {
            return createSimpleCondition(expressions.get(0), ctx);
        }

        Class<? extends SimpleExpression> cls = expressions.get(0).getClass();
        boolean negationFlag = (cls == NegationExpression.class);

        if (expressions.stream().anyMatch(Predicate.not(cls::isInstance))) {
            throw new IllegalArgumentException(String.format(
                    "To create IN or NOT IN clause match condition, all member expressions must be of the same type, given: expressions=%s", expressions));
        }

        String argNameLeft = expressions.get(0).argName();

        List<String> uniqueValues = extractInClauseValues(expressions, argNameLeft, negationFlag ? "NOT IN" : "IN");

        if (uniqueValues.size() == 1) {
            return createSimpleCondition(expressions.get(0), ctx);
        }

        MatchOperator operator = MatchOperator.EQUALS;
        DataColumn columnLeft = dataTableConfig.lookupColumn(argNameLeft, ctx);

        List<QueryParameter> parameters = new ArrayList<>(uniqueValues.size());
        QueryParameterCreator creator = columnLeft.columnType().getQueryParameterCreator();
        ArgMetaInfo argMetaInfo = dataTableConfig.lookup(argNameLeft);
        AdlSqlType columnType = columnLeft.columnType();
        for (String value : uniqueValues) {
            QueryParameter parameter = creator.createParameter(argMetaInfo, value, operator, columnType);
            ctx.registerParameter(parameter);
            parameters.add(parameter);
        }

        ColumnCondition inCondition = new ColumnCondition(ColumnConditionType.IN_CLAUSE, operator, columnLeft, parameters);
        List<ColumnCondition> columnConditions = new ArrayList<>();
        columnConditions.add(inCondition);
        createFilterColumnConditions(expressions.get(0), columnConditions, ctx);

        return new MatchCondition(operator, negationFlag, argNameLeft, dataTableConfig.lookupTableMetaInfo(argNameLeft, ctx), columnLeft, null, null, null,
                columnConditions);
    }

    /**
     * @param expressions
     * @param argNameLeft
     * @param clauseInfo type of the clause (for reporting errors)
     * @return list with (NOT) IN-clause values
     */
    private static List<String> extractInClauseValues(List<SimpleExpression> expressions, String argNameLeft, String clauseInfo) {
        if (!expressions.stream().map(SimpleExpression::argName).allMatch(argNameLeft::equals)) {
            throw new IllegalArgumentException(String.format(
                    "To create an %s-clause match condition, all expressions must carry the same argName, given: orExpression=%s", clauseInfo, expressions));
        }
        if (!expressions.stream().map(SimpleExpression::operator).allMatch(MatchOperator.EQUALS::equals)) {
            throw new IllegalArgumentException(String.format(
                    "To create an %s-clause match condition, all expression operators must be 'EQUALS', given: orExpression=%s", clauseInfo, expressions));
        }
        if (expressions.stream().map(SimpleExpression::operand).anyMatch(op -> op == null || op.isReference())) {
            throw new IllegalArgumentException(
                    String.format("To create an %s-clause match condition, all expression operands must be non-null values, given: orExpression=%s", clauseInfo,
                            expressions));
        }

        return expressions.stream().map(SimpleExpression::operand).map(Operand::value).distinct().toList();
    }

    /**
     * Convenience method to obtain the effective data column name
     * 
     * @param qualified if true, we prepend the table name separated by a dot
     * @return sql identifier
     */
    public String dataColumnNameLeft(boolean qualified) {
        if (qualified) {
            return tableLeft.tableName() + "." + columnLeft.columnName();
        }
        else {
            return columnLeft.columnName();
        }
    }

    /**
     * Convenience method to obtain the effective data column name
     * 
     * @param qualified if true, we prepend the table name separated by a dot
     * @return sql identifier
     */
    public String dataColumnNameRight(boolean qualified) {
        if (argNameRight == null) {
            throw new IllegalStateException("This is not a reference match: " + this);
        }
        if (qualified) {
            return tableRight.tableName() + "." + columnRight.columnName();
        }
        else {
            return columnRight.columnName();
        }
    }

    /**
     * Convenience method to obtain the effective ID column name
     * 
     * @param qualified if true, we prepend the table name separated by a dot
     * @return sql identifier
     */
    public String idColumnNameLeft(boolean qualified) {
        if (qualified) {
            return tableLeft.tableName() + "." + tableLeft.idColumnName();
        }
        else {
            return tableLeft.idColumnName();
        }
    }

    /**
     * Convenience method to obtain the effective ID column name
     * 
     * @param qualified if true, we prepend the table name separated by a dot
     * @return sql identifier
     */
    public String idColumnNameRight(boolean qualified) {
        if (qualified) {
            return tableRight.tableName() + "." + tableRight.idColumnName();
        }
        else {
            return tableRight.idColumnName();
        }
    }

    /**
     * @return true if this condition matches two columns against each other
     */
    public boolean isReferenceMatch() {
        return (tableRight != null);
    }

    /**
     * @return true if this condition matches two columns of the same table against each other, and at least one of the columns is marked mult-row
     */
    public boolean isSingleTableReferenceMatchInvolvingMultipleRows() {
        return (tableRight != null) && tableLeft.tableName().equals(tableRight.tableName()) && (columnLeft.isMultiRow() || columnRight.isMultiRow());
    }

    /**
     * @return true if this condition matches two columns from different tables against each other
     */
    public boolean isDualTableReferenceMatch() {
        return tableLeft != null && tableRight != null && !tableLeft.tableName().equals(tableRight.tableName());
    }

    /**
     * @return true if this condition reflects an IS NULL (operator IS UNKNOWN)
     */
    public boolean isNullMatch() {
        return operator == MatchOperator.IS_UNKNOWN;
    }

    /**
     * @return the primary value comparison (e.g., <code>color=blue</code>) or null if this is a reference match.
     */
    public ColumnCondition getPrimaryColumnCondition() {
        if (argNameRight != null) {
            return null;
        }
        return columnConditions.stream().filter(condition -> condition.column().columnName().equals(columnLeft.columnName())).findAny().orElse(null);
    }

    /**
     * @return type of the main condition
     */
    public ColumnConditionType type() {
        ColumnCondition primaryColumnCondition = getPrimaryColumnCondition();
        return primaryColumnCondition == null ? ColumnConditionType.REFERENCE : primaryColumnCondition.type();
    }

    /**
     * @return true if there are any additional filter conditions besides the primary match condition
     */
    public boolean hasAnyFilterColumnConditions() {
        return columnConditions.stream().anyMatch(condition -> condition.column() instanceof FilterColumn);
    }

    /**
     * @return list with optional filter column conditions related to the left column
     */
    public List<ColumnCondition> getLeftFilterColumnConditions() {
        return columnConditions.stream().filter(condition -> condition.type() == ColumnConditionType.FILTER_LEFT).toList();
    }

    /**
     * @return list with optional filter column conditions related to the right column in case of a reference match
     */
    public List<ColumnCondition> getRightFilterColumnConditions() {
        if (tableRight == null) {
            return Collections.emptyList();
        }
        return columnConditions.stream().filter(condition -> condition.type() == ColumnConditionType.FILTER_RIGHT).toList();
    }

}
