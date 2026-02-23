//@formatter:off
/*
 * DefaultSqlExpressionConverter
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ConversionException;
import de.calamanari.adl.Flag;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.sql.QueryTemplateWithParameters;
import de.calamanari.adl.sql.QueryType;
import de.calamanari.adl.sql.SqlFormatConstants;
import de.calamanari.adl.sql.SqlFormatUtils;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.TableMetaInfo;
import de.calamanari.adl.sql.config.TableNature;

import static de.calamanari.adl.FormatUtils.appendIndentOrWhitespace;
import static de.calamanari.adl.FormatUtils.appendSpaced;
import static de.calamanari.adl.FormatUtils.space;
import static de.calamanari.adl.FormatUtils.stripTrailingWhitespace;
import static de.calamanari.adl.sql.SqlFormatConstants.AND;
import static de.calamanari.adl.sql.SqlFormatConstants.AS;
import static de.calamanari.adl.sql.SqlFormatConstants.BRACE_CLOSE;
import static de.calamanari.adl.sql.SqlFormatConstants.BRACE_OPEN;
import static de.calamanari.adl.sql.SqlFormatConstants.CMP_EQUALS;
import static de.calamanari.adl.sql.SqlFormatConstants.COUNT;
import static de.calamanari.adl.sql.SqlFormatConstants.DISTINCT;
import static de.calamanari.adl.sql.SqlFormatConstants.FROM;
import static de.calamanari.adl.sql.SqlFormatConstants.INNER_JOIN;
import static de.calamanari.adl.sql.SqlFormatConstants.LEFT_OUTER_JOIN;
import static de.calamanari.adl.sql.SqlFormatConstants.ON;
import static de.calamanari.adl.sql.SqlFormatConstants.OR;
import static de.calamanari.adl.sql.SqlFormatConstants.SELECT;
import static de.calamanari.adl.sql.SqlFormatConstants.WHERE;
import static de.calamanari.adl.sql.SqlFormatConstants.WITH;
import static de.calamanari.adl.sql.cnv.ConversionDirective.ENFORCE_PRIMARY_TABLE;
import static de.calamanari.adl.sql.cnv.ConversionHint.LEFT_OUTER_JOINS_REQUIRED;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_JOINS_REQUIRED;
import static de.calamanari.adl.sql.cnv.ConversionHint.SIMPLE_CONDITION;
import static de.calamanari.adl.sql.cnv.ConversionHint.SINGLE_TABLE_CONTAINING_ALL_ROWS;

/**
 * Standard implementation for transforming {@link CoreExpression}s into SQL.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class DefaultSqlExpressionConverter extends AbstractSqlExpressionConverter<SqlConversionContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlExpressionConverter.class);

    /**
     * This flag tells that the system could neither determine a main table nor a primary alias nor any join combination (UNION), so that the base query MUST be
     * any table with all IDs not to "hang in the air".
     */
    private boolean auxiliaryMainTableRequired = false;

    /**
     * This flag tells whether we have added a with-clause to the script or not
     */
    private boolean haveWithClause = false;

    /**
     * @param contextSupplier custom context supplier
     * @param dataBinding physical data binding
     * @param globalVariables initial global variables
     * @param flags initial flags
     */
    public DefaultSqlExpressionConverter(Supplier<? extends SqlConversionContext> contextSupplier, DataBinding dataBinding,
            Map<String, Serializable> globalVariables, Set<Flag> flags) {
        super(contextSupplier, dataBinding, globalVariables, flags);
    }

    /**
     * @param dataBinding physical data binding
     * @param globalVariables initial global variables
     * @param flags initial flags
     */
    public DefaultSqlExpressionConverter(DataBinding dataBinding, Map<String, Serializable> globalVariables, Set<Flag> flags) {
        this(SqlConversionContext::new, dataBinding, globalVariables, flags);
    }

    /**
     * @param dataBinding physical data binding
     * @param globalVariables initial global variables
     * @param flags initial flags
     */
    public DefaultSqlExpressionConverter(DataBinding dataBinding, Map<String, Serializable> globalVariables, Flag... flags) {
        this(SqlConversionContext::new, dataBinding, globalVariables, flags != null ? new HashSet<>(Arrays.asList(flags)) : null);
    }

    /**
     * @param dataBinding physical data binding
     * @param flags initial flags
     */
    public DefaultSqlExpressionConverter(DataBinding dataBinding, Set<Flag> flags) {
        this(SqlConversionContext::new, dataBinding, null, flags);
    }

    /**
     * @param dataBinding physical data binding
     * @param flags initial flags
     */
    public DefaultSqlExpressionConverter(DataBinding dataBinding, Flag... flags) {
        this(SqlConversionContext::new, dataBinding, null, flags != null ? new HashSet<>(Arrays.asList(flags)) : null);
    }

    @Override
    protected QueryTemplateWithParameters finishResult() {
        QueryTemplateWithParameters res = QueryTemplateWithParameters.of(createSqlQueryTemplate(), getProcessContext().getRegisteredParameters());
        LOGGER.trace("Conversion complete:\n{}", res);

        return res;
    }

    @Override
    public void init() {
        super.init();
        this.haveWithClause = false;
        this.auxiliaryMainTableRequired = false;
    }

    /**
     * @return the textual template with named parameters
     */
    protected String createSqlQueryTemplate() {

        StringBuilder sb = new StringBuilder();

        augmentationListener().handleBeforeScript(sb, getProcessContext());
        aliasHelper().determinePrimaryAlias(expressionHelper(), aliasesInWhereClause());
        this.determineMainTable();
        aliasHelper().determineStartSelectionName();

        appendWithClauseIfRequired(sb);

        if (auxiliaryMainTableRequired) {
            determineAuxiliaryMainTable();
        }

        appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);

        augmentationListener().handleBeforeMainStatement(sb, getProcessContext(), haveWithClause);
        appendMainSelectFrom(sb);
        appendAliasJoinsIfRequired(sb);
        appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);
        sb.append(WHERE);
        space(sb);
        appendGlobalWhereCondition(sb);
        if (getQueryType() == QueryType.SELECT_DISTINCT_ID_ORDERED) {
            String idColumnName = getMainIdColumnName();
            if (idColumnName.equals(getIdColumnName())) {
                idColumnName = aliasHelper().getStartSelectionName() + "." + idColumnName;
            }
            else {
                idColumnName = getIdColumnName();
            }
            SqlFormatUtils.appendOrderBy(sb, idColumnName, style(), getCurrentDepth());
        }
        augmentationListener().handleAfterScript(sb, getProcessContext());
        return sb.toString();
    }

    /**
     * Appends the with-clause elements to the query
     * 
     * @param sb
     */
    protected void appendWithClauseIfRequired(StringBuilder sb) {

        List<String> withClauseElements = new ArrayList<>();
        addMainWithClauseElement(withClauseElements);
        addExtraAliasWithClauseElements(withClauseElements);

        Collections.sort(withClauseElements);

        if (!withClauseElements.isEmpty()) {

            sb.append(WITH);
            appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);

            for (int idx = 0; idx < withClauseElements.size(); idx++) {
                if (idx > 0) {
                    sb.append(",");
                    appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);
                }
                sb.append(withClauseElements.get(idx));
            }
            this.haveWithClause = true;
        }

    }

    /**
     * Setup of the SELECT-FROM part of the query (everything before the joins)
     * 
     * @param sb
     */
    protected void appendMainSelectFrom(StringBuilder sb) {
        sb.append(SELECT);
        space(sb);
        augmentationListener().handleAfterMainSelect(sb, getProcessContext());

        if (this.getQueryType() == QueryType.SELECT_DISTINCT_COUNT) {
            sb.append(COUNT);
            appendSpaced(sb, BRACE_OPEN);
        }
        appendSpaced(sb, DISTINCT);

        String idColumnName = getMainIdColumnName();
        if (getMainTable() != null) {
            if (NO_JOINS_REQUIRED.check(flags())) {
                appendSpaced(sb, idColumnName);
            }
            else {
                SqlFormatUtils.appendQualifiedColumnName(sb, aliasHelper().getStartSelectionName(), idColumnName);
            }
        }
        else {
            // there is no main table, the base query is an alias (defined by WITH-clause)
            SqlFormatUtils.appendQualifiedColumnName(sb, aliasHelper().getStartSelectionName(), idColumnName);
        }
        if (this.getQueryType() == QueryType.SELECT_DISTINCT_COUNT) {
            appendSpaced(sb, BRACE_CLOSE);
        }
        else if (!idColumnName.equals(getIdColumnName())) {
            appendSpaced(sb, AS);
            appendSpaced(sb, getIdColumnName());
        }
        appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);

        sb.append(FROM);
        space(sb);

        TableMetaInfo mainTable = getMainTable();
        if (mainTable != null) {
            sb.append(mainTable.tableName());

            if (!NO_JOINS_REQUIRED.check(flags()) && !aliasHelper().getStartSelectionName().equals(mainTable.tableName())
                    && stats().requiredTables().contains(mainTable.tableName())) {
                // the main table name collides with one of the joins
                // we must rename the main selection to avoid that
                space(sb);
                sb.append(aliasHelper().getStartSelectionName());
            }
        }
        else {
            // append just the alias name (alias body to be appended)
            sb.append(aliasHelper().getStartSelectionName());
        }

    }

    /**
     * This is the base query if (and only if) an alias has been chosen as base query
     * <p>
     * If present the element will be added to the list
     * 
     * @param withClauseElements
     */
    protected void addMainWithClauseElement(List<String> withClauseElements) {
        StringBuilder sbMainWith = new StringBuilder();
        if (aliasHelper().getPrimaryAlias() != null && getMainTable() == null) {
            super.appendAliasQuery(sbMainWith, aliasHelper().getPrimaryAlias());
        }
        else if (getMainTable() == null && dataBinding().dataTableConfig().primaryTable() == null) {
            List<ExpressionAlias> aliasesForUnion = aliasHelper().determineAdequateBaseQueryCombination(expressionHelper(),
                    aliasHelper().getOrderedAliasList());
            if (aliasesForUnion.size() == 1) {
                // this is a late promotion because of a complicated analysis-conflict
                // absolute pest to do this here, but I found no way to detect the problem earlier
                aliasHelper().setPrimaryAlias(aliasesForUnion.get(0));
                aliasHelper().setStartSelectionName(aliasHelper().getPrimaryAlias().getName());
                super.appendAliasQuery(sbMainWith, aliasHelper().getPrimaryAlias());
            }
            else if (!aliasesForUnion.isEmpty() && !ConversionDirective.DISABLE_UNION.check(flags())) {
                super.appendAliasUnionBaseQuery(sbMainWith, aliasesForUnion);
            }
            else if (!ConversionDirective.DISABLE_UNION.check(flags()) && dataBinding().dataTableConfig().numberOfTables() > 1
                    && dataBinding().dataTableConfig().tablesThatContainAllIds().isEmpty()) {
                // if there is more than one table, no table has all IDs, and unions are allowed,
                // then we create a "universe union" based on all tables
                super.appendAllTableUnionBaseQuery(sbMainWith);
            }
        }
        String element = sbMainWith.toString().trim();
        if (!element.isEmpty()) {
            withClauseElements.add(sbMainWith.toString().trim());
        }
        else if (getMainTable() == null && aliasHelper().getPrimaryAlias() == null) {
            // there is no WITH for the base query, no primary table and no primary alias
            // Thus we MUST later select from any other table containing IDs
            this.auxiliaryMainTableRequired = true;
        }
    }

    /**
     * Adds further WITH-clause elements to the list. These are the elements for existence checks in case of IS UNKNOWN or multi-row attributes.
     * 
     * @param withClauseElements
     */
    protected void addExtraAliasWithClauseElements(List<String> withClauseElements) {
        List<ExpressionAlias> extraAliases = collectFurtherRequiredAliases();
        if (!extraAliases.isEmpty()) {
            for (int idx = 0; idx < extraAliases.size(); idx++) {
                StringBuilder sb = new StringBuilder();
                super.appendAliasQuery(sb, extraAliases.get(idx));
                withClauseElements.add(sb.toString().trim());
            }
        }
    }

    /**
     * Determines and sets the main table to base the query on (if possible).
     * <p>
     * Most of the time we can start the query from one of the tables referenced in the query.<br>
     * This is either the only table or the table of the primary alias, but only if the global where-clause references the table name (and not the corresponding
     * alias). <br>
     * The latter (no primary table) can happen if only existence check aliases are referenced in the where-clause.
     * <p>
     * If the method is successful, then subsequent calls to {@link #getMainTable()} return the main table.
     */
    protected void determineMainTable() {

        Set<Flag> flags = getProcessContext().getGlobalFlags();

        if (SIMPLE_CONDITION.check(flags)) {
            // no joins required, easy condition on a single main table
            this.setMainTable(dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(stats().requiredTables().iterator().next()));
            aliasHelper().setStartSelectionName(getMainTable().tableName());
        }
        else if (SINGLE_TABLE_CONTAINING_ALL_ROWS.check(flags)) {
            String tableName = stats().requiredTables().iterator().next();
            TableMetaInfo tmi = dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(tableName);
            if (tablesInWhereClause().contains(tmi)) {
                // this is just a beauty thing, it is nicer to start the selection with the table here
                this.setMainTable(tmi);
                aliasHelper().setStartSelectionName(getMainTable().tableName());
                aliasHelper().setPrimaryAlias(null);
            }
        }
        else if (aliasHelper().getPrimaryAlias() != null && (!aliasesInWhereClause().contains(aliasHelper().getPrimaryAlias()))) {
            // we know we can start with this table because it is eligible for the primary alias
            // and the WHERE-clause does not reference the alias name

            if (aliasHelper().isPrimaryAliasRunnningOnPrimaryTable(expressionHelper())) {
                // this covers the case of a reference match where the right table is the primary table
                // here we have a free choice, and the primary table should win
                this.setMainTable(dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(dataBinding().dataTableConfig().primaryTable()));
            }
            else {
                this.setMainTable(conditionFactory().createMatchCondition(aliasHelper().getPrimaryAlias().getExpression()).tableLeft());
            }
            aliasHelper().setStartSelectionName(getMainTable().tableName());
        }

        if (ENFORCE_PRIMARY_TABLE.check(flags)) {
            String primaryTableName = dataBinding().dataTableConfig().primaryTable();
            TableMetaInfo mainTable = getMainTable();
            if (primaryTableName != null && (mainTable == null || !mainTable.tableName().equals(primaryTableName))) {
                this.setMainTable(dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(dataBinding().dataTableConfig().primaryTable()));
                aliasHelper().setStartSelectionName(getMainTable().tableName());
                aliasHelper().setPrimaryAlias(null);
            }
            else if (primaryTableName == null) {
                throw new ConversionException(String.format(
                        "Unable to create base query for expression=%s. Please configure a primary table or remove the directive ENFORCE_PRIMARY_TABLE.",
                        getRootExpression()), AudlangMessage.msg(CommonErrors.ERR_3000_MAPPING_FAILED));

            }
        }

    }

    /**
     * It can happen that we detect during the WITH-clause setup that there is neither a main table nor a primary alias, not even a combination (UNION) of
     * aliases that can serve as base query. <br>
     * The only way to fix this is choosing some table with all IDs to start the query.
     * <p>
     * Resolution order in this case is:
     * <ol>
     * <li>Primary table from configuration.</li>
     * <li>First table with {@link TableNature#containsAllIds()}=true from the tables referenced in the current query.</li>
     * <li>First table with {@link TableNature#containsAllIds()}=true from the configuration.</li>
     * </ol>
     * If this resolution fails, the method throws a {@link ConversionException}.
     * 
     * @throws ConversionException if the resolution was not successful
     */
    protected void determineAuxiliaryMainTable() {
        if (dataBinding().dataTableConfig().primaryTable() != null) {
            this.setMainTable(dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(dataBinding().dataTableConfig().primaryTable()));
        }
        else if (dataBinding().dataTableConfig().numberOfTables() == 1) {
            // this table is our the universe
            this.setMainTable(dataBinding().dataTableConfig().allTableMetaInfos().get(0));
        }
        else {
            List<String> namesOfTablesWithAllIds = dataBinding().dataTableConfig().tablesThatContainAllIds();
            if (namesOfTablesWithAllIds.isEmpty()) {
                throw new ConversionException(String.format(
                        "Unable to create base query for expression=%s. Please mark at least one table as containing all IDs or specify a primary table.",
                        getRootExpression()));
            }

            String tableName = stats().requiredTables().stream().filter(namesOfTablesWithAllIds::contains).findFirst().orElse(namesOfTablesWithAllIds.get(0));
            this.setMainTable(dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(tableName));
        }

        aliasHelper().setStartSelectionName(getMainTable().tableName());
    }

    /**
     * The decision whether a join will be created as INNER JOIN or LEFT OUTER JOIN works as follows:
     * <ul>
     * <li>If possible it will be an INNER JOIN</li>
     * <li>If the table is referenced in the where-clause
     * <ul>
     * <li>If the table contains all IDs and the expression (condition of the join) does not incorrectly limit the result set.</li>
     * </ul>
     * </li>
     * <li>else: LEFT OUTER JOIN</li>
     * </ul>
     * 
     * @param tableOrAliasName
     * @param onRestrictions
     * @return String with the JOIN-type {@link SqlFormatConstants#INNER_JOIN} or {@link SqlFormatConstants#LEFT_OUTER_JOIN}
     */
    protected String detectJoinType(String tableOrAliasName, List<CoreExpression> onRestrictions) {

        TableMetaInfo tmi = stats().requiredTables().contains(tableOrAliasName)
                ? dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(tableOrAliasName)
                : null;

        String joinType = INNER_JOIN;

        if (LEFT_OUTER_JOINS_REQUIRED.check(flags())) {
            joinType = LEFT_OUTER_JOIN;
            if (tmi != null && tablesInWhereClause().contains(tmi) && (tmi.tableNature().containsAllIds()
                    && (onRestrictions.isEmpty() || expressionHelper().isSupersetOfRootExpression(CombinedExpression.orOf(onRestrictions))))) {
                joinType = INNER_JOIN;
            }
        }

        return joinType;
    }

    /**
     * This method consolidates all available conditions related to a given table to construct a consolidated super-condition for the join (ON)
     * <p>
     * The super-condition is a logical OR. Sometimes such a condition cannot be found so that the table will be joined without any condition.
     * 
     * @param table
     * @return list of conditions to be OR'd or empty list if there is no reasonable super-condition
     */
    protected List<CoreExpression> findOnRestrictions(TableMetaInfo table) {

        Set<CoreExpression> expressionCandidates = new TreeSet<>();
        List<ExpressionAlias> aliasCandidates = aliasHelper().getOrderedAliasList().stream().filter(Predicate.not(aliasesInWhereClause()::contains)).toList();

        for (ExpressionAlias alias : aliasCandidates) {
            addOnRestrictionIfApplicable(table, alias, expressionCandidates);
        }
        return expressionHelper().consolidateAliasGroupExpressions(filterApplicableOnConditionExpressions(expressionCandidates));

    }

    /**
     * This method tests the given alias, and if the alias matches the table and certain criteria is fulfilled the expression will be added to the candidates
     * which can later refine the ON-condition.
     * 
     * @see #findOnRestrictions(TableMetaInfo)
     * @param table target table
     * @param alias to be tested
     * @param expressionCandidates to add the alias expression if applicable
     */
    protected void addOnRestrictionIfApplicable(TableMetaInfo table, ExpressionAlias alias, Set<CoreExpression> expressionCandidates) {
        MatchCondition condition = conditionFactory().createMatchCondition(alias.getExpression());
        if (condition.isDualTableReferenceMatch()) {
            if (condition.tableLeft().equals(table)) {
                addIsNotUnknownOnConditionIfApplicable(condition.argNameLeft(), expressionCandidates);
            }
            else if (condition.tableRight().equals(table)) {
                addIsNotUnknownOnConditionIfApplicable(condition.argNameRight(), expressionCandidates);
            }
        }
        else if (condition.tableLeft().equals(table)) {
            if (alias.getPositiveReferenceCount() > 0 && alias.getNegativeReferenceCount() > 0) {
                addIsNotUnknownOnConditionIfApplicable(condition.argNameLeft(), expressionCandidates);
            }
            if (alias.getNegativeReferenceCount() > 0) {
                expressionCandidates.add(NegationExpression.of(alias.getExpression(), true));
            }
            else {
                expressionCandidates.add(alias.getExpression());
            }
        }
    }

    /**
     * Adds an is-no-unknown but only if the argument is not marked as always known (suppress useless condition)
     * 
     * @param argName
     * @param expressionCandidates
     */
    private void addIsNotUnknownOnConditionIfApplicable(String argName, Set<CoreExpression> expressionCandidates) {
        if (!dataBinding().dataTableConfig().isAlwaysKnown(argName)) {
            expressionCandidates.add(MatchExpression.isNotUnknown(argName));
        }
    }

    /**
     * An ON-condition is a logical OR of all the conditions of the aliases' where clauses, which is not always plausible.
     * <p>
     * For example, it does not make any sense to include a condition if there is also IS NOT UNKNOWN included for the same argName.<br>
     * Should there be an IS UNKNOWN OR IS NOT UNKOWN for the same argName, there is no possible ON-condition
     * <p>
     * This method checks the plausibility and returns a simplified list as the lowest common denominator.
     * 
     * @param expressionCandidates
     * @return list of on-conditions or empty list if there is none
     */
    protected List<CoreExpression> filterApplicableOnConditionExpressions(Set<CoreExpression> expressionCandidates) {
        List<CoreExpression> filterExpressions = new ArrayList<>();

        for (CoreExpression candidate : expressionCandidates) {
            switch (candidate) {
            case MatchExpression match when match.operator() == MatchOperator.IS_UNKNOWN
                    && expressionCandidates.contains(MatchExpression.isNotUnknown(match.argName())):
                return Collections.emptyList();
            case NegationExpression neg when neg.operator() == MatchOperator.IS_UNKNOWN:
                filterExpressions.add(candidate);
                break;
            case SimpleExpression simple when !expressionCandidates.contains(MatchExpression.isNotUnknown(simple.argName())):
                filterExpressions.add(candidate);
                break;
            case CombinedExpression cmb when !expressionCandidates.contains(MatchExpression.isNotUnknown(((SimpleExpression) cmb.members().get(0)).argName())):
                filterExpressions.add(candidate);
                break;
            default:
            }
        }
        return filterExpressions;
    }

    /**
     * Appends the join for a table or alias with optional restrictions (ON-condition)
     * 
     * @param sb
     * @param tableOrAliasName
     * @param refIdColumnName
     * @param onRestrictions ON-condition
     */
    protected void appendJoin(StringBuilder sb, String tableOrAliasName, String refIdColumnName, List<CoreExpression> onRestrictions) {

        appendIndentOrWhitespace(sb, style(), getNormalizedDepth(), true);

        augmentationListener().handleAppendJoinType(sb, getProcessContext(), aliasHelper().getStartSelectionName(), tableOrAliasName,
                detectJoinType(tableOrAliasName, onRestrictions));

        appendSpaced(sb, tableOrAliasName);
        appendIndentOrWhitespace(sb, style(), getNormalizedDepth() + 1, true);
        augmentationListener().handleBeforeOnClause(sb, getProcessContext(), aliasHelper().getStartSelectionName(), tableOrAliasName);
        sb.append(ON);
        space(sb);
        augmentationListener().handleBeforeOnConditions(sb, getProcessContext(), aliasHelper().getStartSelectionName(), tableOrAliasName);
        sb.append(tableOrAliasName);
        sb.append(".");
        sb.append(refIdColumnName);
        appendSpaced(sb, CMP_EQUALS);
        sb.append(aliasHelper().getStartSelectionName());
        sb.append(".");
        sb.append(getMainIdColumnName());
        appendFurtherJoinOnRestrictions(sb, tableOrAliasName, onRestrictions);
        augmentationListener().handleAfterOnConditions(sb, getProcessContext(), aliasHelper().getStartSelectionName(), tableOrAliasName);
    }

    /**
     * If the given list of expressions is not empty, each will be expressed as a further ON-restriction of the join, added and combined with AND
     * 
     * @param sb
     * @param onRestrictions
     */
    protected void appendFurtherJoinOnRestrictions(StringBuilder sb, String tableOrAliasName, List<CoreExpression> onRestrictions) {

        if (getMainTable() != null && !getMainTable().tableFilters().isEmpty()) {

            List<ColumnCondition> mainTableFilterConditions = getMainTable().tableFilters().stream()
                    .map(filterColumn -> MatchCondition.createFilterColumnCondition(ColumnConditionType.FILTER_LEFT, filterColumn, getProcessContext()))
                    .toList();

            appendFilterColumnConditions(sb, aliasHelper().getStartSelectionName(), mainTableFilterConditions, true);
        }

        if (onRestrictions.isEmpty()) {
            appendTargetTableConditions(sb, tableOrAliasName);
        }
        else {
            appendIndentOrWhitespace(sb, style(), getNormalizedDepth() + 1, true);
            sb.append(AND);
            space(sb);
            if (onRestrictions.size() > 1) {
                sb.append(BRACE_OPEN);
                space(sb);
            }
            for (int idx = 0; idx < onRestrictions.size(); idx++) {
                if (idx > 0) {
                    appendIndentOrWhitespace(sb, style(), getNormalizedDepth() + 2, true);
                    sb.append(OR);
                    space(sb);
                }
                appendToAliasConditionClause(sb, conditionFactory().createMatchCondition(onRestrictions.get(idx)), true);
            }
            if (onRestrictions.size() > 1) {
                appendIndentOrWhitespace(sb, style(), getNormalizedDepth() + 1, true);
                sb.append(BRACE_CLOSE);
            }
        }
    }

    /**
     * There is an ON with no restrictions on the target table. If this table has table column conditions, we apply them (potentially this helps during
     * execution to consider indexes). On the other hand if there is an explicit join condition present then we skip this step as the match condition will
     * consider the table column conditions anyway.
     * 
     * @param sb
     * @param tableOrAliasName
     */
    private void appendTargetTableConditions(StringBuilder sb, String tableOrAliasName) {
        Optional<TableMetaInfo> targetTableOpt = dataBinding().dataTableConfig().allTableMetaInfos().stream()
                .filter(tmi -> tmi.tableName().equals(tableOrAliasName)).findFirst();

        if (targetTableOpt.isPresent() && !targetTableOpt.get().tableFilters().isEmpty()) {
            List<ColumnCondition> targetTableFilterConditions = targetTableOpt.get().tableFilters().stream()
                    .map(filterColumn -> MatchCondition.createFilterColumnCondition(ColumnConditionType.FILTER_RIGHT, filterColumn, getProcessContext()))
                    .toList();

            appendFilterColumnConditions(sb, tableOrAliasName, targetTableFilterConditions, true);
        }
    }

    /**
     * Collects further aliases.
     * <p>
     * These are aliases other than the primary alias which are referenced in the where-clause, so they MUST be included in WITH and/or joins.
     * <p>
     * This method is required for the WITH-clause creation and for the joins
     * 
     * @return list of aliases
     */
    protected List<ExpressionAlias> collectFurtherRequiredAliases() {
        // @formatter:off
        return aliasHelper().getOrderedAliasList()
                .stream()
                    .filter(alias -> !alias.equals(aliasHelper().getPrimaryAlias()))
                    .filter(aliasesInWhereClause()::contains)
                .toList();
        // @formatter:on
    }

    /**
     * Here we add the joins to connect tables and (if defined) aliases from the WITH-clause to the base query
     * 
     * @param sb final sql script
     */
    protected void appendAliasJoinsIfRequired(StringBuilder sb) {

        if (NO_JOINS_REQUIRED.check(flags())) {
            return;
        }

        List<String> joinElements = new ArrayList<>();

        // @formatter:off
        tablesInWhereClause().stream().sorted(Comparator.comparing(TableMetaInfo::tableName))
                                    .map(TableMetaInfo::tableName)
                                    .filter(tableName -> !tableName.equals(aliasHelper().getStartSelectionName()))
                                    .forEach(tableName -> addTableJoinElement(tableName, joinElements));
        // @formatter:on

        if (!aliasesInWhereClause().isEmpty()) {
            StringBuilder sbJoin = new StringBuilder();
            for (ExpressionAlias alias : collectFurtherRequiredAliases()) {
                appendJoin(sbJoin, alias.getName(), getIdColumnName(), Collections.emptyList());
                joinElements.add(sbJoin.toString());
                sbJoin.setLength(0);
            }
        }

        // print all inner joins before the left outer joins
        Collections.sort(joinElements);
        for (String joinElement : joinElements) {
            if (!style().isMultiLine()) {
                space(sb);
            }
            sb.append(joinElement);
            stripTrailingWhitespace(sb);
        }
    }

    /**
     * Prepares a single join element for the given table
     * 
     * @param tableName
     * @param joinElements to add the element
     */
    private void addTableJoinElement(String tableName, List<String> joinElements) {
        StringBuilder sbJoin = new StringBuilder();
        TableMetaInfo table = dataBinding().dataTableConfig().lookupTableMetaInfoByTableName(tableName);
        appendJoin(sbJoin, tableName, table.idColumnName(), findOnRestrictions(table));
        joinElements.add(sbJoin.toString());
    }

    /**
     * This method appends the global WHERE-condition (the part after the 'WHERE'), composed in {@link #whereClause()} to the given builder
     * <p>
     * It performs a last check if the table filter conditions for the main table are present and adds these if required.
     * 
     * @param sb the final sql script
     */
    protected void appendGlobalWhereCondition(StringBuilder sb) {
        String whereCondition = whereClause().toString().trim();
        if (getMainTable() != null && !getMainTable().tableFilters().isEmpty() && !tablesInWhereClause().contains(getMainTable())) {
            // we are selecting from a table with configured table filters which has no direct conditions in the WHERE-clause
            // means: the configured filters have not yet been applied
            StringBuilder extendedWhere = new StringBuilder();
            extendedWhere.append(BRACE_OPEN);
            appendSpaced(extendedWhere, whereCondition);
            extendedWhere.append(BRACE_CLOSE);
            List<ColumnCondition> tableFilterConditions = getMainTable().tableFilters().stream()
                    .map(filterColumn -> MatchCondition.createFilterColumnCondition(ColumnConditionType.FILTER_LEFT, filterColumn, getProcessContext()))
                    .toList();

            appendFilterColumnConditions(extendedWhere, aliasHelper().getStartSelectionName(), tableFilterConditions, true);
            whereCondition = extendedWhere.toString().trim();
        }

        sb.append(whereCondition);
    }

}
