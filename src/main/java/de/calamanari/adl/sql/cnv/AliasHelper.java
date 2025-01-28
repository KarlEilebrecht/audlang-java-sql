//@formatter:off
/*
 * AliasHelper
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.sql.SqlFormatConstants;

/**
 * During a conversion run an {@link AliasHelper} keeps track of all the created aliases. There is only one alias for the same expression and its negation.
 * <p>
 * Instances are meant to exist local to a conversion process and especially <b>not</b> safe to be accessed (modified) concurrently by multiple threads.
 * 
 * @see ExpressionAlias
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class AliasHelper {

    /**
     * count of aliases created so far, for creating unique alias names within a query
     */
    private int aliasId = 0;

    /**
     * Lookup from sub-expression to its alias, even if the same sub-expression occurs multiple times in a query this will ensure we have only a single alias.
     */
    protected final Map<CoreExpression, ExpressionAlias> aliasMap = new HashMap<>();

    /**
     * Alias that has been identified as the best mandatory alias for the full expression.
     * <p>
     * Then primary alias can serve as start selection because it covers all possible IDs (like a primary table).
     */
    private ExpressionAlias primaryAlias = null;

    /**
     * This is the identifier for the main query, it can be a table or an alias
     */
    private String startSelectionName = null;

    /**
     * @return new alias name, this implementation adds an upcounting number to {@link SqlFormatConstants#ALIAS_PREFIX}, e.g., <code>sq__001, sq__002</code> ...
     */
    public String createAliasName() {
        aliasId++;
        return String.format("sq__%03d", aliasId);
    }

    /**
     * Returns an alias for the given expression. For the same expression we always return the same alias instance (cached). A new aliases will only be created
     * if there was no alias, yet for the given expression.
     * <p>
     * <b>Important:</b> If the given expression is a negative expression we will return the <i>positive</i> alias, so we can better consolidate aliases. In
     * other words, for a {@link NegationExpression} we return the alias for the internal delegate and for any AND of {@link NegationExpression}s (means a NOT
     * IN), we return the alias for the corresponding IN-expression (OR).
     * 
     * @param expression the expression an alias is required for
     * @return alias
     */
    public ExpressionAlias getOrCreateAlias(CoreExpression expression) {
        switch (expression) {
        case CombinedExpression cmb when cmb.combiType() == CombinedExpressionType.AND:
            // this is a NOT IN, we will use the OR (IN) instead to create the alias
            List<CoreExpression> orMembers = cmb.members().stream().map(NegationExpression.class::cast).map(NegationExpression::delegate)
                    .collect(Collectors.toCollection(ArrayList::new));
            CoreExpression aliasInExpression = CombinedExpression.orOf(orMembers);
            return aliasMap.computeIfAbsent(aliasInExpression, e -> new ExpressionAlias(createAliasName(), aliasInExpression));
        case NegationExpression neg:
            return aliasMap.computeIfAbsent(neg.delegate(), e -> new ExpressionAlias(createAliasName(), neg.delegate()));
        case CombinedExpression cmb when cmb.combiType() == CombinedExpressionType.OR:
            return aliasMap.computeIfAbsent(cmb, e -> new ExpressionAlias(createAliasName(), cmb));
        case MatchExpression match:
            return aliasMap.computeIfAbsent(match, e -> new ExpressionAlias(createAliasName(), match));
        default:
            throw new IllegalArgumentException("Cannot create alias for expression (unsupported type), given: " + expression);
        }
    }

    /**
     * Returns the primary alias if it is available.
     * <p>
     * The primary alias is the table or alias defined in the WITH-section we will start the query with.
     * 
     * @see #determinePrimaryAlias(CoreExpressionSqlHelper, Set)
     * @return primary alias or null if undefined
     */
    public ExpressionAlias getPrimaryAlias() {
        return primaryAlias;
    }

    /**
     * Sets the primary alias
     * <p>
     * The primary alias is the table or alias defined in the WITH-section we will start the query with. <br>
     * It must be guaranteed that any possible match of the query is covered by this selection.
     * 
     * @param primaryAlias
     */
    public void setPrimaryAlias(ExpressionAlias primaryAlias) {
        this.primaryAlias = primaryAlias;
    }

    /**
     * This method tests all available aliases if they <i>have to be fulfilled</i> to let the root expression to become true.<br>
     * In case of success the method {@link #getPrimaryAlias()} will return the identified alias.
     * <p>
     * If the primary alias is already set, this method returns immediately (no update).
     * <p>
     * Primary aliases can serve as a base query because we know that the whole query won't succeed if this particular alias is not fulfilled. If there are
     * multiple candidates, we pick the <i>most complex</i> one (restrict result set early).
     * 
     * @param expressionHelper
     * @param aliasesInWhereClause references to alias names in the where clause
     */
    public void determinePrimaryAlias(CoreExpressionSqlHelper expressionHelper, Set<ExpressionAlias> aliasesInWhereClause) {

        if (this.primaryAlias != null || expressionHelper.getStats().isSeparateBaseTableRequired()) {
            return;
        }

        List<ExpressionAlias> aliasList = getOrderedAliasList();

        // @formatter:off
        List<ExpressionAlias> candidates = aliasList.stream()
                                            .filter(alias -> isPrimaryAliasCandidate(expressionHelper, aliasesInWhereClause, alias))
                                            .toList();
        // @formatter:on

        List<CoreExpression> requiredCandidates = filterRequiredAliasExpressions(expressionHelper, candidates);

        // we take the most complex condition to reduce the number of rows early
        expressionHelper.sortByComplexityDescending(requiredCandidates);

        prioritizePrimaryTableIfPresent(expressionHelper, requiredCandidates);

        if (!requiredCandidates.isEmpty()) {
            CoreExpression selectedCandidate = requiredCandidates.get(0);
            for (ExpressionAlias alias : aliasList) {
                if (alias.getExpression().equals(selectedCandidate)) {
                    setPrimaryAlias(alias);
                }
            }
        }
    }

    /**
     * This method checks if any of the candidates runs on the primary table. If so, it removes the others.
     * <p>
     * The idea here is that a primary table may be the best option to start a query on, even if other candidates are more complex.<br>
     * For example if the primary table has drastically fewer rows or the columns are better indexed.
     * 
     * @param expressionHelper
     * @param candidates to be filtered
     */
    private void prioritizePrimaryTableIfPresent(CoreExpressionSqlHelper expressionHelper, List<CoreExpression> candidates) {

        List<CoreExpression> candidatesOnPrimaryTable = candidates.stream().filter(expressionHelper::isSimpleExpressionOnPrimaryTable).toList();

        if (!candidatesOnPrimaryTable.isEmpty()) {
            candidates.clear();
            candidates.addAll(candidatesOnPrimaryTable);
        }
    }

    /**
     * Checks the requirements to promote an alias to primary alias.
     * <p>
     * An alias can only become primary alias if:
     * <ul>
     * <li>It has either only positive or only negative references (otherwise we would need to query without any condition).</li>
     * <li>Either it is a positive reference (e.g., <code>color = red</code>) or (negative query) the alias name is not referenced
     * (<code>ALIAS_NAME.ID IS NULL</code>) in the where-clause.</li>
     * <li>The alias query is logically eligible, see {@link CoreExpressionSqlHelper#isEligibleForBaseQuery(CoreExpression)}</li> </lu>
     * 
     * @param expressionHelper
     * @param aliasesInWhereClause
     * @param alias
     * @return true if the given alias is a valid primary alias candidate
     */
    private boolean isPrimaryAliasCandidate(CoreExpressionSqlHelper expressionHelper, Set<ExpressionAlias> aliasesInWhereClause, ExpressionAlias alias) {
        if (!alias.requiresAllRowsOfTableQueryInUnion()) {
            CoreExpression expression = alias.getExpression();
            if (alias.getNegativeReferenceCount() > 0 && expression instanceof MatchExpression match && match.operator() == MatchOperator.IS_UNKNOWN) {
                expression = NegationExpression.of(expression, true);
                return expressionHelper.isEligibleForBaseQuery(expression);
            }
            else {
                return (alias.getNegativeReferenceCount() == 0 || !aliasesInWhereClause.contains(alias))
                        && expressionHelper.isEligibleForBaseQuery(alias.getExpression());
            }
        }
        return false;
    }

    /**
     * Tests each candidate in the list whether it is a <b>required</b> candidate for the root expression. If so, the alias expression will be included in the
     * returned list.
     * 
     * @see CoreExpressionSqlHelper#isSupersetOfRootExpression(CoreExpression)
     * @param expressionHelper
     * @param candidates
     * @return list of candidate expressions, may be empty
     */
    private List<CoreExpression> filterRequiredAliasExpressions(CoreExpressionSqlHelper expressionHelper, List<ExpressionAlias> candidates) {

        List<CoreExpression> res = new ArrayList<>();

        for (ExpressionAlias candidate : candidates) {

            CoreExpression effectiveExpression = candidate.getExpression();
            if (effectiveExpression instanceof MatchExpression match && !candidate.isReferenceMatch() && candidate.getPositiveReferenceCount() == 0
                    && candidate.getNegativeReferenceCount() > 0) {
                // a simple negation can serve as primary alias
                effectiveExpression = NegationExpression.of(match, true);
            }

            if (expressionHelper.isSupersetOfRootExpression(effectiveExpression)) {
                res.add(candidate.getExpression());
            }
        }
        return res;
    }

    /**
     * At the begin of every SQL-query there must be a base selection to start with. Any ID <b>not</b> included in this selection becomes <i>unreachable</i>
     * because further joins can only further restrict the selection.
     * <p>
     * If we cannot identify a primary table or alias then one option is creating a UNION of all aliases identified for the given query.
     * <p>
     * However, unnecessary UNIONs are expensive. This method aims to find a sufficient combination of as few as possible aliases that covers the desired
     * entirety of IDs and can serve as a base query.
     * <p>
     * The approach is prone to combinatoric explosion. Thus, the search for adequate combinations is limited and may not always find the best combination.<br>
     * This method intentionally <i>excludes</i> any alias with {@link ExpressionAlias#requiresAllRowsOfTableQueryInUnion()} because I assume that in this case
     * it makes more sense to start with a base table that contains all IDs rather than building a UNION.
     * <p>
     * <b>Important:</b> Should this helper have a {@link #getPrimaryAlias()} then this method will always return a list with the primary alias as its only
     * element <i>independent</i> from the aliases list provided as input to this method.
     * 
     * @param expressionHelper
     * @param aliases
     * @return either a list with the {@link #getPrimaryAlias()} (if present), or a list list with one or more aliases that combined by OR meet the base query
     *         requirements, or empty list if there was no primary alias defined and the combination search was unsuccessful
     */
    public List<ExpressionAlias> determineAdequateBaseQueryCombination(CoreExpressionSqlHelper expressionHelper, List<ExpressionAlias> aliases) {

        ExpressionAlias priorityAlias = getPrimaryAlias();
        if (priorityAlias != null) {
            return Collections.singletonList(priorityAlias);
        }

        Map<CoreExpression, ExpressionAlias> eligibleAliasMap = new HashMap<>();

        // @formatter:off
        aliases.stream().filter(alias -> !alias.requiresAllRowsOfTableQueryInUnion() && expressionHelper.isEligibleForBaseQuery(alias.getExpression()))
                        .forEach(alias -> eligibleAliasMap.put(alias.getExpression(), alias));
        // @formatter:on

        List<CoreExpression> res = expressionHelper.findMinimumRequiredOrCombination(new ArrayList<>(eligibleAliasMap.keySet()), 5);
        return res.stream().map(eligibleAliasMap::get).toList();
    }

    /**
     * Determines the identifier of the main query <i>if not already present</i>: either name of {@link #getPrimaryAlias()} or {@link #getBaseQueryAliasName()}
     */
    public void determineStartSelectionName() {
        if (this.startSelectionName == null) {
            this.startSelectionName = this.primaryAlias != null ? primaryAlias.getName() : getBaseQueryAliasName();
        }
    }

    /**
     * Returns the name to be used for the base query alias if required.
     * <p>
     * Technical detail, sub-classes may decide to choose a different name.
     * 
     * @return name of the base query alias, returns {@link SqlFormatConstants#DEFAULT_BASE_QUERY_ALIAS} by default
     */
    protected String getBaseQueryAliasName() {
        return SqlFormatConstants.DEFAULT_BASE_QUERY_ALIAS;
    }

    /**
     * Returns the identifier of the main query in the statement
     * <p>
     * The start selection name is either a table name (simple query case) or a name that stands for a renamed table or one of the alias names defined in the
     * WITH-section of the query.
     * 
     * @return alias or table name the query is based on
     */
    public String getStartSelectionName() {
        return this.startSelectionName;
    }

    /**
     * Sets the identifier of the main query in the statement
     * <p>
     * The start selection name is either a table name (simple query case) or a name that stands for a renamed table or one of the alias names defined in the
     * WITH-section of the query.
     * 
     * @param startQueryAliasName
     */
    public void setStartSelectionName(String startQueryAliasName) {
        this.startSelectionName = startQueryAliasName;
    }

    /**
     * Returns all aliases (main aliases and supplementary aliases) ordered by name
     * 
     * @return ordered list of aliases (mutable)
     */
    public List<ExpressionAlias> getOrderedAliasList() {
        List<ExpressionAlias> aliasList = new ArrayList<>(aliasMap.values());
        Collections.sort(aliasList);
        return aliasList;
    }

    /**
     * Groups any aliases by table or table-duo (reference matches on two tables)
     * <p>
     * Each sub-list represents the aliases for a table or specific table combination.
     * 
     * @param conditionFactory
     * @param aliases
     * @return list of alias lists
     */
    public List<List<ExpressionAlias>> groupAliasesByTable(MatchConditionFactory conditionFactory, List<ExpressionAlias> aliases) {
        Map<String, List<ExpressionAlias>> temp = new TreeMap<>();
        for (ExpressionAlias alias : aliases) {
            MatchCondition condition = conditionFactory.createMatchCondition(alias.getExpression());
            String tableKey = condition.isDualTableReferenceMatch() ? createTableKey(condition.tableLeft().tableName(), condition.tableRight().tableName())
                    : condition.tableLeft().tableName();
            List<ExpressionAlias> aliasGroup = temp.computeIfAbsent(tableKey, key -> new ArrayList<>());
            aliasGroup.add(alias);
        }
        return new ArrayList<>(temp.values());
    }

    /**
     * Creates a temporary unique key to identify a set of tables
     * <p>
     * The key generation returns a unique name <i>independent from the order of the input table names</i>.
     * <ul>
     * <li>(TBL1, TBL2) -> TBL1::TBL2</li>
     * <li>(TBL2, TBL1) -> TBL1::TBL2</li>
     * </ul>
     * 
     * @param tableNames to be included
     * @return unique key for the set
     */
    private static String createTableKey(String... tableNames) {
        return Arrays.stream(tableNames).sorted().collect(Collectors.joining("::"));
    }

    /**
     * Determines the effective name of the right side table or alias of a reference match.
     * 
     * @param condition
     * @return right table name or {@link SqlFormatConstants#SELF_JOIN_ALIAS} if required
     */
    public String determineReferenceMatchTableOrAliasRight(MatchCondition condition) {
        String res = null;
        if (condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
            res = SqlFormatConstants.SELF_JOIN_ALIAS;
        }
        else {
            res = condition.tableRight().tableName();
        }
        return res;
    }

    /**
     * Determines the effective name of the right side of a reference match.
     * 
     * @param condition
     * @param qualified columns must be qualified (e.g. in global where and ON-condition)
     * @return column qualified by {@link SqlFormatConstants#SELF_JOIN_ALIAS} if required
     */
    public String determineReferenceMatchDataColumnOrAliasRight(MatchCondition condition, boolean qualified) {
        String res = null;
        if (condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
            res = SqlFormatConstants.SELF_JOIN_ALIAS + "." + condition.dataColumnNameRight(false);
        }
        else {
            res = condition.dataColumnNameRight(qualified);
        }
        return res;
    }

    /**
     * Determines the effective right ID column in a reference match
     * 
     * @param condition
     * @return column qualified by {@link SqlFormatConstants#SELF_JOIN_ALIAS} or the source table
     */
    public String determineReferenceMatchIdColumnOrAliasRight(MatchCondition condition) {
        String res = null;
        if (condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
            res = SqlFormatConstants.SELF_JOIN_ALIAS + "." + condition.idColumnNameRight(false);
        }
        else {
            res = condition.idColumnNameRight(true);
        }
        return res;
    }

    /**
     * Determines the effective name or alias of the right table in a reference match
     * 
     * @param condition
     * @return the table name or "renamed" to {@link SqlFormatConstants#SELF_JOIN_ALIAS}
     */
    public String determineReferenceMatchTableRight(MatchCondition condition) {
        String res = condition.tableRight().tableName();
        if (condition.isSingleTableReferenceMatchInvolvingMultipleRows()) {
            res = res + " " + SqlFormatConstants.SELF_JOIN_ALIAS;
        }
        return res;
    }

    /**
     * This method determines if there is a primary alias <i>and</i> this alias runs on the primary table.
     * <p>
     * An alias runs on the primary table if the primary table is involved in the corresponding SQL.
     * 
     * @see CoreExpressionSqlHelper#isPrimaryTableInvolved(SimpleExpression)
     * @param expressionHelper
     * @return if there is a primary alias and either the left or the right table (if reference match) of the primary alias is the primary table
     */
    public boolean isPrimaryAliasRunnningOnPrimaryTable(CoreExpressionSqlHelper expressionHelper) {
        return (primaryAlias != null && primaryAlias.getExpression() instanceof SimpleExpression simple && expressionHelper.isPrimaryTableInvolved(simple));
    }

}
