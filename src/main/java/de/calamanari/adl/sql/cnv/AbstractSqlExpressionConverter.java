//@formatter:off
/*
 * AbstractSqlExpressionConverter
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

import static de.calamanari.adl.FormatUtils.appendIndentOrWhitespace;
import static de.calamanari.adl.FormatUtils.appendSpaced;
import static de.calamanari.adl.FormatUtils.space;
import static de.calamanari.adl.sql.SqlFormatConstants.AND;
import static de.calamanari.adl.sql.SqlFormatConstants.AS;
import static de.calamanari.adl.sql.SqlFormatConstants.BRACE_CLOSE;
import static de.calamanari.adl.sql.SqlFormatConstants.BRACE_OPEN;
import static de.calamanari.adl.sql.SqlFormatConstants.CMP_EQUALS;
import static de.calamanari.adl.sql.SqlFormatConstants.CMP_GREATER_THAN;
import static de.calamanari.adl.sql.SqlFormatConstants.CMP_LESS_THAN;
import static de.calamanari.adl.sql.SqlFormatConstants.CMP_NOT_EQUALS;
import static de.calamanari.adl.sql.SqlFormatConstants.DISTINCT;
import static de.calamanari.adl.sql.SqlFormatConstants.FROM;
import static de.calamanari.adl.sql.SqlFormatConstants.IN;
import static de.calamanari.adl.sql.SqlFormatConstants.INNER_JOIN;
import static de.calamanari.adl.sql.SqlFormatConstants.IS_NOT_NULL;
import static de.calamanari.adl.sql.SqlFormatConstants.IS_NULL;
import static de.calamanari.adl.sql.SqlFormatConstants.NOT;
import static de.calamanari.adl.sql.SqlFormatConstants.ON;
import static de.calamanari.adl.sql.SqlFormatConstants.OR;
import static de.calamanari.adl.sql.SqlFormatConstants.SELECT;
import static de.calamanari.adl.sql.SqlFormatConstants.SELF_JOIN_ALIAS;
import static de.calamanari.adl.sql.SqlFormatConstants.UNION;
import static de.calamanari.adl.sql.SqlFormatConstants.WHERE;
import static de.calamanari.adl.sql.SqlFormatUtils.appendSpacedIf;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_JOINS_REQUIRED;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ConversionException;
import de.calamanari.adl.Flag;
import de.calamanari.adl.FormatStyle;
import de.calamanari.adl.SpecialSetType;
import de.calamanari.adl.TimeOut;
import de.calamanari.adl.cnv.AbstractCoreExpressionConverter;
import de.calamanari.adl.cnv.IsUnknownRemovalConverter;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.irl.SpecialSetExpression;
import de.calamanari.adl.sql.QueryParameter;
import de.calamanari.adl.sql.QueryTemplateWithParameters;
import de.calamanari.adl.sql.QueryType;
import de.calamanari.adl.sql.SqlFormatConstants;
import de.calamanari.adl.sql.SqlFormatUtils;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DataTableConfig;
import de.calamanari.adl.sql.config.SqlContainsPolicy;
import de.calamanari.adl.sql.config.TableMetaInfo;

/**
 * Base class for creating SQL-converters.
 * <p>
 * A couple of common functions have been identified and realized as template methods to be overridden as desired.<br>
 * In general methods can be found here that are related to preparation steps <i>before</i> composing the final SQL-statement from the prepared data.
 * <p>
 * The state of a conversion is quite complex. Because I wanted to make instances reusable (so state must be reset to initial settings), I moved all the
 * variables into the {@link ResettableScpContext}. Now, each {@link SqlConversionContext} issued by the converter for a particular <i>level</i> of the
 * expression being converted has the full picture in form of the <i>injected</i> process state of the converter. This allows global state communication
 * (variables, flags) throughout a conversion process.
 * 
 * @param <C> {@link SqlConversionContext} or derived type
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public abstract class AbstractSqlExpressionConverter<C extends SqlConversionContext> extends AbstractCoreExpressionConverter<QueryTemplateWithParameters, C> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSqlExpressionConverter.class);

    /**
     * Process context of this converter (across levels), each call to {@link #convert(Object)} resets it to its initial state to avoid artifacts leaking into
     * the next conversion.
     */
    private final ResettableScpContext processContext;

    /**
     * Ensure every newly supplied local level context shares the process context with the converter
     * 
     * @param converter
     * @param processContext
     */
    private static void registerProcessContextProvider(AbstractSqlExpressionConverter<?> converter, ResettableScpContext processContext) {

        // this is by intention in a static method, so that the lambda below has no unintended backward reference to the converter instance
        // references shall all go away from the converter to avoid circles
        converter.setContextPreparator(localContext -> {
            localContext.setProcessContext(processContext);
            return localContext;
        });
    }

    /**
     * Creates a new instance of the converter, fully prepared to call {@link #convert(Object)}.
     * <p>
     * Meant for sub-classes that want to provide an individual processContext (or a sub-class).
     * 
     * @see #getProcessContext()
     * @param contextSupplier to create a context for each level of the expression we visit
     * @param processContext
     */
    protected AbstractSqlExpressionConverter(Supplier<? extends C> contextSupplier, ResettableScpContext processContext) {
        super(contextSupplier);
        this.processContext = processContext;
        registerProcessContextProvider(this, processContext);
    }

    /**
     * Creates a new instance of the converter, fully prepared to call {@link #convert(Object)}.
     * <p>
     * <b>Important:</b> The set of global variables and flags you specify here act as a <i>template</i> for each subsequent run. A conversion run will never
     * modify the map with the variables and the set with the flags provided at construction time because it will run on independent copies.
     * 
     * @see #getProcessContext()
     * @param contextSupplier to create a context for each level of the expression we visit
     * @param dataBinding
     * @param globalVariables optional global variables (null means empty)
     * @param flags directives and dynamic flags (null means empty)
     */
    protected AbstractSqlExpressionConverter(Supplier<? extends C> contextSupplier, DataBinding dataBinding, Map<String, Serializable> globalVariables,
            Set<Flag> flags) {
        super(contextSupplier);
        this.processContext = new ResettableScpContext(dataBinding, globalVariables, flags);
        registerProcessContextProvider(this, processContext);
    }

    /**
     * Creates a new instance of the converter, fully prepared to call {@link #convert(Object)}.
     * 
     * @param contextSupplier to create a context for each level of the expression we visit
     * @param dataBinding
     */
    protected AbstractSqlExpressionConverter(Supplier<? extends C> contextSupplier, DataBinding dataBinding) {
        this(contextSupplier, dataBinding, null, null);
    }

    /**
     * @return augmentation listener of this instance or {@link SqlAugmentationListener#none()} if not installed, not null
     */
    public SqlAugmentationListener getAugmentationListener() {
        return ((ResettableScpContext) getProcessContext()).getAugmentationListener();
    }

    /**
     * @param augmentationListener null means {@link SqlAugmentationListener#none()} (default)
     */
    public void setAugmentationListener(SqlAugmentationListener augmentationListener) {
        ((ResettableScpContext) getProcessContext())
                .setAugmentationListener(augmentationListener == null ? SqlAugmentationListener.none() : augmentationListener);
    }

    /**
     * Returns the converter's <i>process context</i> across the levels of a conversion.
     * <p>
     * While each level of an expression's DAG gets its own level context {@link #getContext()}, {@link #getParentContext()}, {@link #getRootContext()}), the
     * process context (global variables and general converter state) is common to all of them and allows data exchange.
     * <p>
     * By sharing the process context callers have access to the full data and feature set of the converter without sharing the converter instance itself.
     * 
     * @return the current global process context of the converter (variables, flags, etc.), instance of {@link ResettableScpContext}
     */
    public final SqlConversionProcessContext getProcessContext() {
        return this.processContext;
    }

    @Override
    public void init() {
        super.init();
        ((ResettableScpContext) getProcessContext()).reset();
    }

    /**
     * @return id-column name for joining and the returned result, defaults to {@link SqlFormatConstants#DEFAULT_ID_COLUMN_NAME}
     */
    public final String getIdColumnName() {
        return getProcessContext().getIdColumnName();
    }

    /**
     * @param idColumnName id-column name for joining and the returned result, defaults to {@link SqlFormatConstants#DEFAULT_ID_COLUMN_NAME}
     */
    public final void setIdColumnName(String idColumnName) {
        ((ResettableScpContext) getProcessContext()).setIdColumnName(idColumnName);
    }

    /**
     * @return type of the query to be created, defaults to {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     */
    public final QueryType getQueryType() {
        return getProcessContext().getQueryType();
    }

    /**
     * @param queryType type of the query to be created, defaults to {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     */
    public final void setQueryType(QueryType queryType) {
        ((ResettableScpContext) getProcessContext()).setQueryType(queryType);
    }

    /**
     * Returns the variable configuration passed to the constructor
     * <p>
     * This configuration acts <i>as a template</i> for all subsequent calls to {@link #convert(Object)}.
     * <p>
     * You can use this method if you did not specify any variables at construction time but instead want to define them now (before calling
     * {@link #convert(Object)}).
     * 
     * @return mutable map
     */
    public final Map<String, Serializable> getInitialVariables() {
        return ((ResettableScpContext) getProcessContext()).getGlobalVariablesTemplate();
    }

    /**
     * Returns the flag configuration passed to the constructor
     * <p>
     * This configuration acts <i>as a template</i> for all subsequent calls to {@link #convert(Object)}
     * <p>
     * You can use this method if you did not specify any flags at construction time but instead want to define them now (before calling
     * {@link #convert(Object)}).
     * 
     * @return mutable set
     */
    public final Set<Flag> getInitialFlags() {
        return ((ResettableScpContext) getProcessContext()).getGlobalFlagsTemplate();
    }

    /**
     * The main table is the table to start the query with. It is initialized with {@link DataTableConfig#primaryTable()}.
     * <p>
     * Depending on the conversion strategy it can be reasonable to override it with a different table, for example if only a single table is involved without
     * any complex joins.
     * 
     * @return the main table or null if not configured
     */
    public final TableMetaInfo getMainTable() {
        return getProcessContext().getMainTable();
    }

    /**
     * The main table is the table to start the query with. It is initialized with {@link DataTableConfig#primaryTable()}.
     * <p>
     * Depending on the conversion strategy it can be reasonable to override it with a different table, for example if only a single table is involved without
     * any complex joins.
     * 
     * @param mainTable the table to prefer to start the query or null to enforce another query start strategy
     */
    protected final void setMainTable(TableMetaInfo mainTable) {
        ((ResettableScpContext) getProcessContext()).setMainTable(mainTable);
    }

    /**
     * @return configured formatting style (inline or multi-line)
     */
    public final FormatStyle getStyle() {
        return getProcessContext().getStyle();
    }

    /**
     * @param style formatting style (inline or multi-line)
     */
    public final void setStyle(FormatStyle style) {
        ((ResettableScpContext) getProcessContext()).setStyle(style);
    }

    /**
     * @return expression statistics from the expression helper
     */
    protected final CoreExpressionStats stats() {
        return ((ResettableScpContext) getProcessContext()).getExpressionHelper().getStats();
    }

    /**
     * @return alias helper for this conversion, initialized during {@link #prepareRootExpression()}
     */
    protected final AliasHelper aliasHelper() {
        return getProcessContext().getAliasHelper();
    }

    /**
     * @return expression helper for the currently processed expression, initialized during {@link #prepareRootExpression()}
     */
    protected final CoreExpressionSqlHelper expressionHelper() {
        return getProcessContext().getExpressionHelper();
    }

    /**
     * @return initially configured data binding (physical data model)
     */
    protected final DataBinding dataBinding() {
        return getProcessContext().getDataBinding();
    }

    /**
     * @return WHERE-clause builder
     */
    protected final StringBuilder whereClause() {
        return getProcessContext().getWhereClause();
    }

    /**
     * @return match condition factory of the current run
     */
    protected final MatchConditionFactory conditionFactory() {
        return getProcessContext().getConditionFactory();
    }

    /**
     * @return configured format style (pretty or inline)
     */
    protected final FormatStyle style() {
        return getProcessContext().getStyle();
    }

    /**
     * @return process flags (mutable)
     */
    protected final Set<Flag> flags() {
        return getProcessContext().getGlobalFlags();
    }

    /**
     * @return global process variables (mutable)
     */
    protected final Map<String, Serializable> globalVariables() {
        return getProcessContext().getGlobalVariables();
    }

    /**
     * @return collection of the tables referenced in the where clause
     */
    protected final Set<TableMetaInfo> tablesInWhereClause() {
        return getProcessContext().getTablesInWhereClause();
    }

    /**
     * @return collection of the aliases referenced in the where clause
     */
    protected final Set<ExpressionAlias> aliasesInWhereClause() {
        return getProcessContext().getAliasesInWhereClause();
    }

    /**
     * @return the augmentation listener of this converter or {@link SqlAugmentationListener#none()} if not installed, never null
     */
    protected final SqlAugmentationListener augmentationListener() {
        return getAugmentationListener();
    }

    /**
     * Resolves the main id-column for joining further tables.
     * <p>
     * The effective name of the ID-column can vary if the tables have custom-IDs and there are aliases or a renamed base selection. <br>
     * This method simply tests if there is a main table (then the result is the column name) or not (then the ID is {@link #getIdColumnName()}.
     * <p>
     * <b>Note:</b> This is the raw name, of course finally the returned ID-column name is always {@link #getIdColumnName()} (renamed if required).
     * 
     * @return effective main id for joining
     */
    protected final String getMainIdColumnName() {
        return getProcessContext().getMainIdColumnName();
    }

    @Override
    protected CoreExpression prepareRootExpression() {
        CoreExpression rootExpression = getRootExpression();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Preparing \n{}", rootExpression.format(FormatStyle.PRETTY_PRINT));
        }
        IsUnknownRemovalConverter irc = new IsUnknownRemovalConverter(dataBinding().dataTableConfig());
        rootExpression = irc.convert(rootExpression);

        if (rootExpression instanceof SpecialSetExpression spc) {

            if (spc.setType() == SpecialSetType.ALL) {
                String msg = """
                        Unable to convert the given expression because it implies <%s>.
                        This late error can happen if the expression uses IS UNKNOWN on a field that is marked as always known.
                        In this case an expression can "collapse" to <ALL>, for example "arg1 = 2 OR arg3 IS NOT UNKNOWN" with arg3 always known.
                        For details, check the expression and the mapped physical data model.
                        It is recommended to report this error to the user as an "unintentional full audience match" with the advice to rework the condition.

                        given expression:
                        %s
                        """;
                throw new ConversionException(String.format(msg, spc.setType(), getRootExpression().format(FormatStyle.PRETTY_PRINT)),
                        AudlangMessage.msg(CommonErrors.ERR_1001_ALWAYS_TRUE));
            }
            else {
                String msg = """
                        Unable to convert the given expression because it implies <%s>.
                        This late error can happen if the expression uses IS UNKNOWN on a field that is marked as always known.
                        In this case an expression can "collapse" to <NONE>, for example "arg1 = 2 OR arg3 IS UNKNOWN" with arg3 always known.
                        For details, check the expression and the mapped physical data model.
                        It is recommended to gracefully ignore this exception and return an empty result.

                        given expression:
                        %s
                        """;
                throw new ConversionException(String.format(msg, spc.setType(), getRootExpression().format(FormatStyle.PRETTY_PRINT)),
                        AudlangMessage.msg(CommonErrors.ERR_1002_ALWAYS_FALSE));
            }
        }

        ((ResettableScpContext) getProcessContext()).setExpressionHelper(createCoreExpressionSqlHelper(rootExpression));
        ((ResettableScpContext) getProcessContext()).setAliasHelper(createAliasHelper());
        ((ResettableScpContext) getProcessContext()).setConditionFactory(createMatchConditionFactory());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Preparation complete: \n{} \nflags={}", rootExpression, getProcessContext().getGlobalFlags());
        }
        return rootExpression;
    }

    /**
     * This method allows sub-classes to replace the helper with a custom one
     * 
     * @return new helper instance
     */
    protected AliasHelper createAliasHelper() {
        return new AliasHelper();
    }

    /**
     * This method allows sub-classes to replace the helper with a custom one
     * 
     * @param rootExpression the prepared root expression before start
     * @return new helper instance, a {@link CoreExpressionSqlHelper} initialized with the root expression by default
     */
    protected CoreExpressionSqlHelper createCoreExpressionSqlHelper(CoreExpression rootExpression) {
        return new CoreExpressionSqlHelper(rootExpression, TimeOut.createDefaultTimeOut(this.getClass().getSimpleName()), dataBinding(), getProcessContext());
    }

    /**
     * This method allows sub-classes to replace the factory with a custom one
     * 
     * @return new match condition factory, instance of {@link DefaultMatchConditionFactory} by default
     */
    protected MatchConditionFactory createMatchConditionFactory() {
        return new DefaultMatchConditionFactory(getProcessContext());
    }

    @Override
    public void enterCombinedExpression(CombinedExpression expression) {

        getContext().setCombiType(expression.combiType());

        List<List<SimpleExpression>> groups = new ArrayList<>();
        List<CoreExpression> remainder = new ArrayList<>();

        appendCombinerIfRequired(whereClause(), getParentContext().getCombiType().name());

        expressionHelper().groupInClauses(expression, groups, remainder);
        if (groups.size() + remainder.size() > 1) {
            appendSpaced(whereClause(), BRACE_OPEN);
        }
        else {
            getContext().suppressClosingBrace();
        }
        int lastLen = whereClause().length();

        expression.members().stream().filter(Predicate.not(remainder::contains)).forEach(getContext()::skipChildExpression);

        for (int idx = 0; idx < groups.size(); idx++) {
            if (whereClause().length() > lastLen) {
                appendIndentOrWhitespace(whereClause(), style(), getNormalizedDepth() + 1, true);
                whereClause().append(expression.combiType() == CombinedExpressionType.AND ? AND : OR);
                space(whereClause());
                lastLen = whereClause().length();
            }
            List<SimpleExpression> group = groups.get(idx);
            ExpressionAlias alias = aliasHelper().getOrCreateAlias(CombinedExpression.of(new ArrayList<>(group), expression.combiType()));
            if (expression.combiType() == CombinedExpressionType.OR) {
                // IN CLAUSE
                alias.registerPositiveReference();
            }
            else {
                // NOT IN clause
                alias.registerNegativeReference();
            }
            MatchCondition condition = conditionFactory().createInClauseCondition(group);
            appendToGlobalWhereClause(whereClause(), condition, alias);
        }
    }

    @Override
    public void exitCombinedExpression(CombinedExpression expression) {
        if (!getContext().isClosingBraceSuppressed()) {
            appendIndentOrWhitespace(whereClause(), style(), getNormalizedDepth() + 1, true);
            appendSpaced(whereClause(), BRACE_CLOSE);
        }
    }

    @Override
    public void handleMatchExpression(MatchExpression expression) {
        if (!getParentContext().isSkipped(expression)) {
            handleSimpleExpressionInternal(expression);
        }
    }

    @Override
    public void enterNegationExpression(NegationExpression expression) {
        if (!getParentContext().isSkipped(expression)) {
            handleSimpleExpressionInternal(expression);
        }
        getContext().skipChildExpression(expression.delegate());
    }

    @Override
    public void handleSpecialSetExpression(SpecialSetExpression expression) {
        // this code only exists to cover the case that the prepareRootExpression() method has been overwritten in a wrong way
        // otherwise this code is not reachable

        if (expression.setType() == SpecialSetType.ALL) {
            throw new ConversionException("Cannot convert <ALL> to SQL.", AudlangMessage.msg(CommonErrors.ERR_1001_ALWAYS_TRUE));
        }
        else if (expression.setType() == SpecialSetType.NONE) {
            throw new ConversionException("Cannot convert <NONE> to SQL.", AudlangMessage.msg(CommonErrors.ERR_1002_ALWAYS_FALSE));
        }
    }

    /**
     * Handles a (negated) match expression and either counts a positive or negative reference to the corresponding alias
     * 
     * @param expression
     */
    private void handleSimpleExpressionInternal(SimpleExpression expression) {
        ExpressionAlias alias = aliasHelper().getOrCreateAlias(expression);
        if (expression instanceof MatchExpression) {
            alias.registerPositiveReference();
        }
        else {
            alias.registerNegativeReference();
        }
        MatchCondition condition = conditionFactory().createSimpleCondition(expression);

        appendCombinerIfRequired(whereClause(), getParentContext().getCombiType().name());

        this.appendToGlobalWhereClause(whereClause(), condition, alias);
    }

    /**
     * Only appends the given combiner if
     * <ul>
     * <li>we are not right behind an opening brace (start state)</li>
     * <li>the builder is not empty or contains only whitespace</li>
     * </ul>
     * This affects 'AND' / 'OR' in a where-clause.
     * 
     * @param sb
     * @param combiner
     */
    private void appendCombinerIfRequired(StringBuilder sb, String combiner) {
        if (!SqlFormatUtils.endsWithOpenBraceOrAllWhitespace(sb)) {
            appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);
            sb.append(combiner);
            space(sb);
        }
    }

    /**
     * Appends a UNION of the given aliases to compose a super set as base query (part if with)
     * 
     * @param sb
     * @param aliases
     */
    protected void appendAliasUnionBaseQuery(StringBuilder sb, List<ExpressionAlias> aliases) {

        sb.append(aliasHelper().getStartSelectionName());
        appendSpaced(sb, AS, BRACE_OPEN);

        List<List<ExpressionAlias>> aliasGroups = aliasHelper().groupAliasesByTable(conditionFactory(), aliases);
        for (int idx = 0; idx < aliasGroups.size(); idx++) {
            List<ExpressionAlias> aliasGroup = aliasGroups.get(idx);

            if (idx != 0) {
                appendIndentOrWhitespace(sb, style(), getNormalizedDepth() + 2, true);
                sb.append(UNION);
            }
            appendAliasGroupToBaseQuery(sb, aliasGroup);
        }

        appendSpaced(sb, BRACE_CLOSE);

    }

    /**
     * Appends a UNION of all tables defined in the data binding.
     * <p>
     * This "worst-case" scenario happens if no table is marked as primary table nor containing all IDs and the query contains negative or IS UNKNOWN
     * conditions.
     * 
     * @param sb
     */
    protected void appendAllTableUnionBaseQuery(StringBuilder sb) {

        sb.append(aliasHelper().getStartSelectionName());
        appendSpaced(sb, AS, BRACE_OPEN);

        List<TableMetaInfo> allTables = dataBinding().dataTableConfig().allTableMetaInfos();

        for (int idx = 0; idx < allTables.size(); idx++) {
            if (idx != 0) {
                appendIndentOrWhitespace(sb, style(), getNormalizedDepth() + 2, true);
                appendSpaced(sb, UNION);

            }
            appendFullTableQuery(sb, allTables.get(idx));
        }

        appendSpaced(sb, BRACE_CLOSE);

    }

    /**
     * Appends a select that queries all IDs of the given table
     * 
     * @param sb
     * @param table to select all IDs from (with respect to optional table filter conditions)
     */
    protected void appendFullTableQuery(StringBuilder sb, TableMetaInfo table) {
        boolean idColumnRenamingRequired = !table.idColumnName().equals(getIdColumnName());
        sb.append(SELECT);

        augmentationListener().handleAfterWithSelect(sb, getProcessContext(), table.tableName());

        appendSpaced(sb, table.idColumnName());
        appendSpacedIf(sb, idColumnRenamingRequired, AS, getIdColumnName());
        appendSpaced(sb, FROM);
        sb.append(table.tableName());
        if (!table.tableFilters().isEmpty()) {
            appendSpaced(sb, WHERE);
            List<ColumnCondition> tableFilterConditions = table.tableFilters().stream()
                    .map(filterColumn -> MatchCondition.createFilterColumnCondition(ColumnConditionType.FILTER_LEFT, filterColumn, getProcessContext()))
                    .toList();
            appendFilterColumnConditions(sb, table.tableName(), tableFilterConditions, false);
        }
    }

    /**
     * Appends the query related to one alias group (a table or duo of two tables in case of a reference match) to become part of a UNION that shall form the
     * base query.
     * 
     * @param sb
     * @param aliasGroup
     */
    protected void appendAliasGroupToBaseQuery(StringBuilder sb, List<ExpressionAlias> aliasGroup) {
        MatchCondition condition = conditionFactory().createMatchCondition(aliasGroup.get(0).getExpression());

        boolean qualifiedTableNameRequired = condition.isDualTableReferenceMatch() || condition.isSingleTableReferenceMatchInvolvingMultipleRows();
        boolean idColumnRenamingRequired = !condition.idColumnNameLeft(false).equals(getIdColumnName());

        appendSpaced(sb, SELECT);
        if (condition.isReferenceMatch()) {
            augmentationListener().handleAfterWithSelect(sb, getProcessContext(), condition.tableLeft().tableName(), condition.tableRight().tableName());
        }
        else {
            augmentationListener().handleAfterWithSelect(sb, getProcessContext(), condition.tableLeft().tableName());
        }
        sb.append(condition.idColumnNameLeft(qualifiedTableNameRequired));

        appendSpacedIf(sb, idColumnRenamingRequired, AS, getIdColumnName());

        appendSpaced(sb, FROM);
        appendSpaced(sb, condition.tableLeft().tableName());
        appendInnerJoinForReferenceMatchIfRequired(sb, condition);

        // collect all expressions related to the same group (means same table or table-duo)
        List<CoreExpression> aliasExpressions = expressionHelper()
                .consolidateAliasGroupExpressions(aliasGroup.stream().map(ExpressionAlias::getExpression).toList());

        for (int idx = 0; idx < aliasExpressions.size(); idx++) {
            CoreExpression aliasExpression = aliasExpressions.get(idx);
            if (idx == 0) {
                appendSpaced(sb, WHERE);
            }
            else {
                appendSpaced(sb, OR);
            }
            appendSpaced(sb, BRACE_OPEN);
            appendMatchCondition(sb, conditionFactory().createMatchCondition(aliasExpression), qualifiedTableNameRequired);
            appendSpaced(sb, BRACE_CLOSE);
        }
    }

    /**
     * Append an alias query (part of the SQL WITH-section)
     * 
     * @param sb
     * @param alias
     */
    protected void appendAliasQuery(StringBuilder sb, ExpressionAlias alias) {

        if (alias.isReferenceMatch()) {
            appendReferenceMatchAliasQuery(sb, alias);
        }
        else {
            MatchCondition condition = conditionFactory().createMatchCondition(alias.getExpression());
            if (condition.operator() == MatchOperator.IS_UNKNOWN && alias.getNegativeReferenceCount() > 0) {
                // The expression inside an alias is always positive
                // This allows collecting positive and negative references instead of duplicating aliases
                // The problem with this is that you cannot directly re-create a negative expression
                // In the IS-NULL case this causes severe problems.
                // Thus, I mark the condition as negative if we have a negative reference, so that subsequently
                // the correct query can be created. In all other cases it is irrelevant whether the condition
                // is negative or not because the alias will be created and referenced as an existence check in
                // the global WHERE clause.
                condition = MatchCondition.negate(condition);
            }
            boolean idColumnRenamingRequired = !condition.idColumnNameLeft(false).equals(getIdColumnName());

            sb.append(alias.getName());
            appendSpaced(sb, AS, BRACE_OPEN, SELECT);
            augmentationListener().handleAfterWithSelect(sb, getProcessContext(), condition.tableLeft().tableName());
            appendSpacedIf(sb, stats().isMultiRowSensitive(condition.argNameLeft()), DISTINCT);

            sb.append(condition.idColumnNameLeft(false));

            appendSpacedIf(sb, idColumnRenamingRequired, AS, getIdColumnName());

            appendSpaced(sb, FROM);
            appendSpaced(sb, condition.tableLeft().tableName());

            appendSpaced(sb, WHERE);
            appendMatchCondition(sb, condition, false);
            appendSpaced(sb, BRACE_CLOSE);
        }

    }

    /**
     * Appends a reference match alias query.
     * <p>
     * There are three cases:
     * <ul>
     * <li>Single table, and attribute data resides on the same row, e.g. <code>TBL.HOME_COUNTRY = TBL.BIRTH_COUNTRY</code></li>
     * <li>Single table, but at least one attribute is multi-row (or table is marked sparse), so we must join the table with itself
     * (<code>{@value SqlFormatConstants#SELF_JOIN_ALIAS}</code>)</li>
     * <li>Two tables to be joined on a certain condition</li>
     * </ul>
     * 
     * @param sb
     * @param alias
     */
    private void appendReferenceMatchAliasQuery(StringBuilder sb, ExpressionAlias alias) {
        MatchCondition condition = conditionFactory().createMatchCondition(alias.getExpression());

        boolean qualifiedTableNameRequired = condition.isDualTableReferenceMatch() || condition.isSingleTableReferenceMatchInvolvingMultipleRows();

        AdlType commonAdlType = dataBinding().dataTableConfig().lookup(condition.argNameLeft()).type();

        String columnNameLeft = condition.dataColumnNameLeft(qualifiedTableNameRequired);
        String termLeft = condition.columnLeft().columnType().getNativeTypeCaster().formatNativeTypeCast(condition.argNameLeft(), columnNameLeft,
                condition.columnLeft().columnType(), commonAdlType);

        String columnNameRight = condition.dataColumnNameRight(false);

        if (qualifiedTableNameRequired) {
            if (condition.isDualTableReferenceMatch()) {
                columnNameRight = condition.dataColumnNameRight(true);
            }
            else if (condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
                columnNameRight = SELF_JOIN_ALIAS + "." + columnNameRight;
            }
        }

        String termRight = condition.columnRight().columnType().getNativeTypeCaster().formatNativeTypeCast(condition.argNameRight(), columnNameRight,
                condition.columnRight().columnType(), commonAdlType);

        boolean idColumnRenamingRequired = !condition.idColumnNameLeft(false).equals(getIdColumnName());

        sb.append(alias.getName());
        appendSpaced(sb, AS, BRACE_OPEN, SELECT);
        augmentationListener().handleAfterWithSelect(sb, getProcessContext(), condition.tableLeft().tableName(), condition.tableRight().tableName());

        appendSpacedIf(sb, stats().isMultiRowSensitive(condition.argNameLeft()), DISTINCT);

        sb.append(condition.idColumnNameLeft(qualifiedTableNameRequired));

        appendSpacedIf(sb, idColumnRenamingRequired, AS, getIdColumnName());

        appendSpaced(sb, FROM);
        appendSpaced(sb, condition.tableLeft().tableName());
        appendInnerJoinForReferenceMatchIfRequired(sb, condition);

        appendSpaced(sb, WHERE);

        boolean filtersPresent = condition.hasAnyFilterColumnConditions();
        if (filtersPresent) {
            appendSpaced(sb, BRACE_OPEN);
        }

        appendMainCondition(sb, condition, termLeft, termRight);

        if (filtersPresent) {
            appendFilterColumnConditions(sb, condition, true);
            appendSpaced(sb, BRACE_CLOSE);
        }

        appendSpaced(sb, BRACE_CLOSE);

    }

    /**
     * In case of a reference match with the same table (and a multi-row attribute) or two tables we need an additional inner join because the data sits on
     * different rows/tables.
     * 
     * @param sb
     * @param condition
     * @param addTableConditionsToOn if true any table conditions will be added to the ON-part
     */
    private boolean appendInnerJoinForReferenceMatchIfRequired(StringBuilder sb, MatchCondition condition) {
        if (condition.isDualTableReferenceMatch() || condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
            space(sb);
            augmentationListener().handleAppendJoinType(sb, getProcessContext(), condition.tableLeft().tableName(), condition.tableRight().tableName(),
                    INNER_JOIN);
            space(sb);

            sb.append(aliasHelper().determineReferenceMatchTableRight(condition));

            augmentationListener().handleBeforeOnClause(sb, getProcessContext(), condition.tableLeft().tableName(), condition.tableRight().tableName());
            appendSpaced(sb, ON);

            sb.append(condition.idColumnNameLeft(true));
            appendSpaced(sb, CMP_EQUALS);
            sb.append(aliasHelper().determineReferenceMatchIdColumnOrAliasRight(condition));

            if ((!condition.tableLeft().tableFilters().isEmpty() || !condition.tableRight().tableFilters().isEmpty())) {
                List<ColumnCondition> tableFilterConditionsLeft = condition.tableLeft().tableFilters().stream()
                        .map(filterColumn -> MatchCondition.createFilterColumnCondition(ColumnConditionType.FILTER_LEFT, filterColumn, getProcessContext()))
                        .toList();
                List<ColumnCondition> tableFilterConditionsRight = condition.tableRight().tableFilters().stream()
                        .map(filterColumn -> MatchCondition.createFilterColumnCondition(ColumnConditionType.FILTER_RIGHT, filterColumn, getProcessContext()))
                        .toList();

                appendFilterColumnConditions(sb, condition.tableLeft().tableName(), tableFilterConditionsLeft, true);
                appendFilterColumnConditions(sb, aliasHelper().determineReferenceMatchTableOrAliasRight(condition), tableFilterConditionsRight, true);

            }

            return true;
        }
        return false;
    }

    /**
     * Appends the condition to the given <i>alias WHERE-clause or ON-condition</i>.
     * <p>
     * In contrast to the global where clause we don't need extra existence checks in this case (alias is always a simple join).
     * 
     * @param sb
     * @param condition
     * @param qualified
     */
    protected void appendToAliasConditionClause(StringBuilder sb, MatchCondition condition, boolean qualified) {
        appendSpaced(sb, BRACE_OPEN);
        appendMatchCondition(sb, condition, qualified);
        appendSpaced(sb, BRACE_CLOSE);
    }

    /**
     * Appends the condition to the global WHERE-clause.
     * 
     * @param sb
     * @param condition
     * @param qualified
     */
    private void appendToGlobalWhereClause(StringBuilder sb, MatchCondition condition, boolean qualified) {
        appendSpaced(sb, BRACE_OPEN);

        if (condition.operator() == MatchOperator.IS_UNKNOWN && !condition.isNegation() && !expressionHelper().isNullQueryingAllowed(condition.argNameLeft())) {
            // In most cases we cannot safely query IS NULL because we must cover missing rows as well
            // Thus we instead create a supplementary IS NOT NULL alias, join it and add the condition that it does not match.
            ExpressionAlias extraAliasLeft = aliasHelper().getOrCreateAlias(MatchExpression.isUnknown(condition.argNameLeft()));
            extraAliasLeft.registerNegativeReference();
            appendAliasExistenceCheckToWhereClause(sb, extraAliasLeft, true);
        }
        else {
            appendMatchCondition(sb, condition, qualified);
            tablesInWhereClause().add(condition.tableLeft());
            if (condition.isDualTableReferenceMatch()) {
                tablesInWhereClause().add(condition.tableRight());
            }
        }
        appendSpaced(sb, BRACE_CLOSE);

    }

    /**
     * Appends the condition to the global WHERE-clause.
     * <p>
     * We always try to work with the regular table names. The alias name only comes into play if there is any multi-row sensitivity or IS-NULL-check.
     * 
     * @see #tablesInWhereClause
     * @see #aliasesInWhereClause
     * @param sb
     * @param condition
     * @param currentAlias
     */
    protected void appendToGlobalWhereClause(StringBuilder sb, MatchCondition condition, ExpressionAlias currentAlias) {

        if (NO_JOINS_REQUIRED.check(getProcessContext().getGlobalFlags())) {
            this.appendToGlobalWhereClause(sb, condition, false);
        }
        else {
            if (expressionHelper().isMultiRowSensitiveMatch(condition)) {
                // instead of checking the plain condition, we check the existence of the corresponding alias
                appendAliasExistenceCheckWithExtraNullCheckIfRequired(sb, condition, currentAlias);
            }
            else {
                // regular condition qualified by table name
                this.appendToGlobalWhereClause(sb, condition, true);
            }

        }
    }

    /**
     * Appends the given condition to WHERE clause (with/sub query).
     * <p>
     * <b>Note:</b> There is special IS NULL handling, instead of IS NULL we query IS NOT NULL which requires a corresponding handling by the global where
     * clause. Only in the edge case {@link CoreExpressionSqlHelper#isNullQueryingAllowed(String)} NULL will be queried directly to simplify the query.
     * 
     * @param sb to append the condition
     * @param condition to be appended
     * @param qualified tells whether the column names must be qualified or not
     */
    protected void appendMatchCondition(StringBuilder sb, MatchCondition condition, boolean qualified) {

        AdlType commonAdlType = dataBinding().dataTableConfig().lookup(condition.argNameLeft()).type();

        String columnNameLeft = condition.dataColumnNameLeft(qualified);
        String termLeft = condition.columnLeft().columnType().getNativeTypeCaster().formatNativeTypeCast(condition.argNameLeft(), columnNameLeft,
                condition.columnLeft().columnType(), commonAdlType);

        String termRight = null;

        if (condition.isReferenceMatch()) {

            String columnNameRight = aliasHelper().determineReferenceMatchDataColumnOrAliasRight(condition, qualified);

            termRight = condition.columnRight().columnType().getNativeTypeCaster().formatNativeTypeCast(condition.argNameRight(), columnNameRight,
                    condition.columnRight().columnType(), commonAdlType);

        }

        boolean hasFilters = condition.hasAnyFilterColumnConditions();
        if (hasFilters) {
            appendSpaced(sb, BRACE_OPEN);

        }

        if (condition.operator() == MatchOperator.IS_UNKNOWN && !condition.isNegation() && expressionHelper().isNullQueryingAllowed(condition.argNameLeft())) {
            sb.append(termLeft);
            appendSpaced(sb, IS_NULL);
        }
        else if (condition.operator() == MatchOperator.IS_UNKNOWN) {
            // exceptional handling of IS NULL (we always query IS NOT NULL and handle the result in the global where clause)
            sb.append(termLeft);
            appendSpaced(sb, IS_NOT_NULL);
        }
        else {
            appendMainCondition(sb, condition, termLeft, termRight);
        }
        if (hasFilters) {
            appendFilterColumnConditions(sb, condition, qualified);
            appendSpaced(sb, BRACE_CLOSE);

        }

    }

    /**
     * When we query columns with multi-row sensitivity, we perform the condition check within the alias query and perform an alias existence check in the
     * global WHERE clause (we are not <i>attributing</i> the aliases).
     * <p>
     * An extra is-null-check can be required to avoid accidentally including nulls in the result.
     * 
     * @see CoreExpressionSqlHelper#isExtraExistenceMatchRequired(MatchCondition)
     * @param sb
     * @param condition
     * @param alias
     */
    protected void appendAliasExistenceCheckWithExtraNullCheckIfRequired(StringBuilder sb, MatchCondition condition, ExpressionAlias alias) {
        boolean extraCheckRequired = expressionHelper().isExtraExistenceMatchRequired(condition);

        if (extraCheckRequired) {
            appendSpaced(sb, BRACE_OPEN);
            this.appendExtraAliasNullCheck(sb, condition);
        }

        boolean negate = condition.isNegation();

        // Tricky detail: Any IS-NULL query maps to an IS-NOT-NULL-co-query (we usually cannot query IS NULL directly, missing row problem)
        // However that means we must invert the negation in this case
        if (alias.getExpression() instanceof MatchExpression match && match.operator() == MatchOperator.IS_UNKNOWN) {
            negate = !negate;
        }
        appendAliasExistenceCheckToWhereClause(sb, alias, negate);

        if (extraCheckRequired) {
            appendSpaced(sb, BRACE_CLOSE);
        }

    }

    /**
     * Appends an IS-NOT-NULL check (or two in case of reference match) for the condition's argument(s)
     * <p>
     * Remember: In most scenarios querying IS NULL leads to errors, thus we MUST query IS NOT NULL
     * 
     * @param sb
     * @param condition
     */
    private void appendExtraAliasNullCheck(StringBuilder sb, MatchCondition condition) {

        ExpressionAlias extraAliasLeft = aliasHelper().getOrCreateAlias(MatchExpression.isUnknown(condition.argNameLeft()));

        extraAliasLeft.registerNegativeReference();

        appendAliasExistenceCheckToWhereClause(sb, extraAliasLeft, false);
        appendSpaced(sb, AND);

        if (condition.isReferenceMatch()) {
            ExpressionAlias extraAliasRight = aliasHelper().getOrCreateAlias(MatchExpression.isUnknown(condition.argNameRight()));
            extraAliasRight.registerNegativeReference();
            appendAliasExistenceCheckToWhereClause(sb, extraAliasRight, false);
            appendSpaced(sb, AND);
        }

    }

    /**
     * Aliases are always positive queries (instead of IS NULL we query IS NOT NULL).
     * <p>
     * This method takes this into account to create a logical existence check.
     * 
     * @see SqlFormatUtils#appendIsNullInversion(StringBuilder, boolean)
     * @param sb
     * @param extraAlias
     * @param negate
     */
    private void appendAliasExistenceCheckToWhereClause(StringBuilder sb, ExpressionAlias extraAlias, boolean negate) {
        SqlFormatUtils.appendQualifiedColumnName(sb, extraAlias.getName(), getIdColumnName());
        SqlFormatUtils.appendIsNullInversion(sb, negate);
        aliasesInWhereClause().add(extraAlias);
    }

    /**
     * This method deals with the main condition (e.g. <code>TBL.COLOR='red'</code> or <code>TBL1.COLOR = TBL2.COLOR</code>).
     * 
     * @param sb
     * @param condition
     * @param termLeft
     * @param termRight
     */
    private void appendMainCondition(StringBuilder sb, MatchCondition condition, String termLeft, String termRight) {

        if (condition.isNegation() && condition.operator() != MatchOperator.IS_UNKNOWN && condition.operator() != MatchOperator.EQUALS
                && condition.type() != ColumnConditionType.IN_CLAUSE) {
            appendSpaced(sb, NOT);
        }

        switch (condition.type()) {
        case REFERENCE:
            appendReferenceMatchCondition(sb, condition, termLeft, termRight);
            break;
        case SINGLE, FILTER_LEFT, FILTER_RIGHT:
            appendValueMatchCondition(sb, condition, termLeft);
            break;
        case IN_CLAUSE:
            appendInClauseMatchCondition(sb, condition, termLeft);
            break;
        case AFTER_TODAY:
            appendAfterTodayMatchCondition(sb, condition, termLeft);
            break;
        case DATE_RANGE:
            appendDateRangeMatchCondition(sb, condition, termLeft);
            break;
        default:
            throw new IllegalStateException("Not yet implemented: " + condition);
        }

    }

    /**
     * Appends a (NOT) IN clause based on the given condition
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendInClauseMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        sb.append(termLeft);
        if (condition.isNegation()) {
            appendSpaced(sb, NOT);
        }
        appendSpaced(sb, IN);
        List<QueryParameter> parameters = condition.getPrimaryColumnCondition().parameters();
        sb.append(parameters.stream().map(QueryParameter::createReference).collect(Collectors.joining(", ", "(", ")")));
    }

    /**
     * Special date case: turns a GREATER THAN into a GREATER THAN OR EQUALS to the first parameter of the condition which is the start of the next day to
     * correctly mimic <i>after</i> if the underlying column has a higher precision than DATE.
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendAfterTodayMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        sb.append(termLeft);
        space(sb);
        sb.append(CMP_GREATER_THAN);
        sb.append(CMP_EQUALS);
        space(sb);
        sb.append(condition.getPrimaryColumnCondition().parameters().get(0).createReference());
    }

    /**
     * Special date case: turns an equals into a logical between (the first parameter is <i>start of the specified day</i> and the second is <i>start of the day
     * after</i> to mimic correct equals even if the underlying column has a time portion.
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendDateRangeMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {

        if (condition.isNegation()) {
            sb.append("(");
            sb.append(termLeft);
            appendSpaced(sb, CMP_LESS_THAN);
            sb.append(condition.getPrimaryColumnCondition().parameters().get(0).createReference());
            appendSpaced(sb, OR);
            sb.append(termLeft);
            space(sb);
            sb.append(CMP_GREATER_THAN);
            sb.append(CMP_EQUALS);
            space(sb);
            sb.append(condition.getPrimaryColumnCondition().parameters().get(1).createReference());
            sb.append(")");
        }
        else {
            sb.append("(");
            sb.append(termLeft);
            space(sb);
            sb.append(CMP_GREATER_THAN);
            sb.append(CMP_EQUALS);
            space(sb);
            sb.append(condition.getPrimaryColumnCondition().parameters().get(0).createReference());
            appendSpaced(sb, AND);
            sb.append(termLeft);
            appendSpaced(sb, CMP_LESS_THAN);
            sb.append(condition.getPrimaryColumnCondition().parameters().get(1).createReference());
            sb.append(")");
        }
    }

    /**
     * Appends a value match (including IS UNKNOWN) to the where clause
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendValueMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        switch (condition.operator()) {
        case EQUALS:
            appendEqualsValueMatchCondition(sb, condition, termLeft);
            break;
        case CONTAINS:
            appendContainsMatchCondition(sb, condition, termLeft);
            break;
        case IS_UNKNOWN:
            appendIsNullMatchCondition(sb, condition, termLeft);
            break;
        case GREATER_THAN, LESS_THAN:
            appendLessThanGreaterThanMatchCondition(sb, condition, termLeft);
            break;
        // $CASES-OMITTED$
        default:
            throw new IllegalStateException("Unexpected operator in reference match, given: " + condition);
        }

    }

    /**
     * Turns a technical "(not) equals null" into SQL's IS (NOT) NULL
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendIsNullMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        sb.append(termLeft);
        appendSpaced(sb, condition.isNegation() ? IS_NOT_NULL : IS_NULL);
    }

    /**
     * Appends a condition with the operator '=' resp. '<>' (ANSI SQL style) and the first parameter of the condition
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendEqualsValueMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        sb.append(termLeft);
        appendSpaced(sb, condition.isNegation() ? CMP_NOT_EQUALS : CMP_EQUALS);
        sb.append(condition.getPrimaryColumnCondition().parameters().get(0).createReference());
    }

    /**
     * Appends a CONTAINs-condition (typically LIKE, depends on {@link SqlContainsPolicy}) and the first parameter of the condition
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendContainsMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        sb.append(dataBinding().sqlContainsPolicy().createInstruction(termLeft, condition.getPrimaryColumnCondition().parameters().get(0).createReference()));
    }

    /**
     * Appends a condition LESS THAN or GREATER THAN and the first parameter of the condition
     * 
     * @param sb
     * @param condition
     * @param termLeft
     */
    private void appendLessThanGreaterThanMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft) {
        sb.append(termLeft);
        switch (condition.operator()) {
        case GREATER_THAN:
            appendSpaced(sb, CMP_GREATER_THAN);
            break;
        case LESS_THAN:
            appendSpaced(sb, CMP_LESS_THAN);
            break;
        // $CASES-OMITTED$
        default:
            throw new IllegalStateException("Unexpected operator in value match, given: " + condition);
        }
        sb.append(condition.getPrimaryColumnCondition().parameters().get(0).createReference());
    }

    /**
     * Appends a reference match between the two given terms and the operator
     * 
     * @param sb
     * @param condition
     * @param termLeft
     * @param termRight
     */
    private void appendReferenceMatchCondition(StringBuilder sb, MatchCondition condition, String termLeft, String termRight) {
        sb.append(termLeft);
        if (condition.isNegation() && condition.operator() == MatchOperator.EQUALS) {
            appendSpaced(sb, CMP_NOT_EQUALS);
        }
        else {
            switch (condition.operator()) {
            case EQUALS:
                appendSpaced(sb, CMP_EQUALS);
                break;
            case GREATER_THAN:
                appendSpaced(sb, CMP_GREATER_THAN);
                break;
            case LESS_THAN:
                appendSpaced(sb, CMP_LESS_THAN);
                break;
            // $CASES-OMITTED$
            default:
                throw new IllegalStateException("Unexpected operator in reference match, given: " + condition);
            }
        }
        sb.append(termRight);
    }

    /**
     * Appends all the column conditions of the left side of the given match condition
     * 
     * @param sb
     * @param condition
     * @param qualified if true prepend the table name
     */
    private void appendLeftFilterColumnConditions(StringBuilder sb, MatchCondition condition, boolean qualified) {
        List<ColumnCondition> leftColumnConditions = condition.getLeftFilterColumnConditions();
        if (!leftColumnConditions.isEmpty()) {
            String optionalTableOrAliasName = qualified ? condition.tableLeft().tableName() : null;
            appendFilterColumnConditions(sb, optionalTableOrAliasName, leftColumnConditions, true);
        }
    }

    /**
     * Appends all the column conditions of the right side of the given match condition (reference match)
     * <p>
     * The difference compared to the left column conditions is the table name which is either the given name or {@link SqlFormatConstants#SELF_JOIN_ALIAS}
     * (depends whether we have two tables involved or only one)
     * 
     * @param sb
     * @param condition
     * @param qualified if true prepend the table name
     */
    private void appendRightFilterColumnConditions(StringBuilder sb, MatchCondition condition, boolean qualified) {
        List<ColumnCondition> rightColumnConditions = condition.getRightFilterColumnConditions();
        if (!rightColumnConditions.isEmpty()) {
            String optionalTableOrAliasName = null;
            if (qualified) {
                if (condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
                    optionalTableOrAliasName = SELF_JOIN_ALIAS;
                }
                else {
                    optionalTableOrAliasName = condition.tableRight().tableName();
                }
            }
            appendFilterColumnConditions(sb, optionalTableOrAliasName, rightColumnConditions, true);
        }
    }

    /**
     * Appends all filter column conditions (first left and then right (reference match case))
     * 
     * @param sb
     * @param condition
     * @param qualified if true prepend the table name(s)
     */
    private void appendFilterColumnConditions(StringBuilder sb, MatchCondition condition, boolean qualified) {
        appendLeftFilterColumnConditions(sb, condition, qualified);
        appendRightFilterColumnConditions(sb, condition, qualified);
    }

    /**
     * Appends the given list of filter column conditions, connected by AND with the previous content
     * 
     * @param sb
     * @param optionalTableOrAliasName qualifier for the columns or null (pure column name)
     * @param columnConditions
     * @param followUp if true then there is some earlier condition (prepend AND)
     */
    protected void appendFilterColumnConditions(StringBuilder sb, String optionalTableOrAliasName, List<ColumnCondition> columnConditions, boolean followUp) {

        String prefix = optionalTableOrAliasName == null ? "" : (optionalTableOrAliasName + ".");

        for (int idx = 0; idx < columnConditions.size(); idx++) {
            ColumnCondition condition = columnConditions.get(idx);

            if (followUp) {
                appendSpaced(sb, AND);
            }
            else {
                followUp = true;
            }

            sb.append(prefix + condition.column().columnName());

            if (condition.operator() == MatchOperator.IS_UNKNOWN) {
                space(sb);
                sb.append(IS_NULL);
            }
            else {
                appendSpaced(sb, CMP_EQUALS);
                sb.append(condition.parameters().get(0).createReference());
            }

        }

    }

}
