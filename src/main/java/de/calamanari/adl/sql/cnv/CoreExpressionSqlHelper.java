//@formatter:off
/*
 * CoreExpressionSqlHelper
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.Flag;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.TimeOut;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.irl.biceps.EncodedExpressionTree;
import de.calamanari.adl.irl.biceps.ExpressionLogicHelper;
import de.calamanari.adl.irl.biceps.ImplicationResolver;
import de.calamanari.adl.irl.biceps.NodeType;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.config.DataBinding;

import static de.calamanari.adl.irl.biceps.MemberUtils.EMPTY_MEMBERS;
import static de.calamanari.adl.sql.cnv.ConversionHint.SIMPLE_CONDITION;

/**
 * The {@link CoreExpressionSqlHelper} provides a couple of utilities to support the conversion of a {@link CoreExpression} into an SQL-expression (e.g.
 * construction of IN-clauses)
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class CoreExpressionSqlHelper {

    /**
     * reference to the expression currently being converted, required for logic checks
     */
    protected final CoreExpression rootExpression;

    /**
     * binary root node for quick access
     */
    protected final int rootNode;

    /**
     * Tree from the root expression, not meant to be modified but for comparison
     */
    protected final EncodedExpressionTree tree;

    /**
     * Safety: avoid combinatoric explosion and runaway
     */
    protected final TimeOut timeout;

    /**
     * Statistics about the current expression
     */
    protected final CoreExpressionStats stats;

    /**
     * Data binding (from context)
     */
    protected final DataBinding dataBinding;

    /**
     * Reference to variables and flags
     */
    protected final ProcessContext processContext;

    /**
     * A comparator that <i>ascendingly</i> orders expressions first after {@link #complexityOf(CoreExpression)}
     */
    protected final Comparator<CoreExpression> complexityComparator = Comparator.comparing(this::complexityOf);

    /**
     * Analyzes the given expression, gathers statistics and creates a fresh helper instance.
     * 
     * @param rootExpression
     * @param timeout if null we will use the default: {@link TimeOut#createDefaultTimeOut(String)}
     * @param dataBinding physical table binding
     * @param processContext (hints will be added to global flags)
     */
    public CoreExpressionSqlHelper(CoreExpression rootExpression, TimeOut timeout, DataBinding dataBinding, ProcessContext processContext) {
        this.rootExpression = rootExpression;
        this.tree = EncodedExpressionTree.fromCoreExpression(rootExpression);
        this.rootNode = tree.getRootNode();
        this.timeout = timeout == null ? TimeOut.createDefaultTimeOut(ImplicationResolver.class.getSimpleName()) : timeout;
        CoreExpressionStats expressionStats = CoreExpressionStats.from(rootExpression, dataBinding, processContext);

        // add all hints as global flags for easier access
        processContext.getGlobalFlags().addAll(expressionStats.hints());

        this.stats = expressionStats;
        this.dataBinding = dataBinding;
        this.processContext = new MinimalProcessContext(processContext.getGlobalVariables(), processContext.getGlobalFlags());
    }

    /**
     * @param expression
     * @return the node that corresponds to the given expression
     */
    private int getNode(CoreExpression expression) {
        return tree.createNode(expression);
    }

    /**
     * @param expressions to be mapped
     * @return ordered map, node back to expression candidate
     */
    private Map<Integer, CoreExpression> getSortedNodeMap(List<CoreExpression> expressions) {
        Map<Integer, CoreExpression> res = new TreeMap<>();
        for (int i = 0; i < expressions.size(); i++) {
            CoreExpression candidate = expressions.get(i);
            int node = getNode(candidate);
            res.putIfAbsent(node, candidate);
        }
        return res;
    }

    /**
     * Tries to find the shortest OR-combination of candidates that guarantees the root expression to be true.
     * <p>
     * To avoid combinatoric explosion and excessive execution times it is <b>strongly recommended</b> to set a <b>limit</b> which defines the maximum number of
     * elements in a group (OR) to be tested.
     * <p>
     * The success of this approach is limited in the same way as {@link ExpressionLogicHelper#leftImpliesRight(int, int)}, so it is neither guaranteed that we
     * can find any solution nor that it will be the shortest possible.
     * 
     * @param candidates
     * @param limit maximum number of elements in a test group (OR members), implicitly limited by the number of candidates
     * @return list of candidates that forms an OR that must be true to let the root expression become true, may be empty, not null
     */
    public List<CoreExpression> findMinimumRequiredOrCombination(List<CoreExpression> candidates, int limit) {

        if (limit <= 0 || limit > candidates.size()) {
            limit = candidates.size();
        }

        List<CoreExpression> res = Collections.emptyList();

        if (limit > 0) {
            int[] match = EMPTY_MEMBERS;

            // Note:
            // I order the nodes only to stabilize the algorithm (make it independent from the input order)
            // Otherwise the search result might become indeterministic

            Map<Integer, CoreExpression> nodeMap = getSortedNodeMap(candidates);
            int[] candidateNodes = nodeMap.keySet().stream().mapToInt(i -> i).toArray();

            // Below, I increase the numberOfElements in each "round", accepting that we will iterate (not test!) several times
            // This guarantees that we see the shorter alternatives first
            // The approach should still be cheaper than collecting all matching alternatives first and sorting them by length
            for (int numberOfElements = 1; numberOfElements <= limit && match == EMPTY_MEMBERS; numberOfElements++) {
                match = findRequiredOrCombination(candidateNodes, 0, EMPTY_MEMBERS, numberOfElements);
            }

            // get rid of the temporary ORs if there were many
            tree.getMemberArrayRegistry().triggerHousekeeping(rootNode);
            res = new ArrayList<>(match.length);
            for (int idx = 0; idx < match.length; idx++) {
                res.add(nodeMap.get(match[idx]));
            }
        }
        return res;

    }

    /**
     * Recursive implementation that takes candidates (left to right) and appends them to the combination until the requested number of OR-members is reached.
     * The resulting OR gets tests if it must be true to let the root expression become true.
     * 
     * @param candidates the full list of candidates
     * @param currentIdx current position for left-to-right processing (avoids testing <code>a=1 OR b=1</code> and later <i>b=1 OR a=1</i>)
     * @param currentCombination combination of a given length, initially empty
     * @param numberOfElements length of the OR-combination
     * @return match or empty array if not found
     */
    private int[] findRequiredOrCombination(int[] candidates, int currentIdx, int[] currentCombination, int numberOfElements) {

        int[] match = EMPTY_MEMBERS;

        for (int idx = currentIdx; match.length == 0 && idx < candidates.length; idx++) {
            timeout.assertHaveTime();
            int[] newCombination = combine(currentCombination, candidates[idx]);
            if (newCombination.length == numberOfElements && checkLeftSupersetOfRight(tree.createNode(NodeType.OR, newCombination), rootNode)) {
                match = newCombination;
            }
            else if (newCombination.length < numberOfElements) {
                match = findRequiredOrCombination(candidates, idx + 1, newCombination, numberOfElements);
            }
        }

        return match;
    }

    /**
     * @param currentCombination
     * @param candidate
     * @return new array with the candidate appended (size increased by 1)
     */
    private int[] combine(int[] currentCombination, int candidate) {
        int[] res = Arrays.copyOf(currentCombination, currentCombination.length + 1);
        res[res.length - 1] = candidate;
        return res;
    }

    /**
     * Performs a <i>logical</i> check if the given expression is <i>required to be true</i> by the root expression resp. if the ID-set covered by the given
     * expression is the same as or a superset of the ID-set covered by the root expression.
     * <p>
     * This information is required to decide if the expression (resp. the related SQL query) can serve as a base query to start the selection.
     * <p>
     * <b>Example:</b> If <code>color = blue AND shape = circle</code> is the <i>root expression</i> then <i>logically</i> <code>color = blue</code> is
     * <i>required</i> by the root expression. <br>
     * The related SQL-query could start with the sub set of records with blue color and then apply <i>further</i> restrictions (<code>shape = circle</code>).
     * <p>
     * In other words: the given expression cannot be false if the root expression is fulfilled.
     * 
     * @param expression
     * @return true if the given expression must be true to fulfill the root expression
     */
    public boolean isSupersetOfRootExpression(CoreExpression expression) {
        return checkLeftSupersetOfRight(expression, rootExpression);
    }

    /**
     * Tests whether the left expression must be true if the right shall be true, in other words <i>right implies left</i>.
     * 
     * @param left
     * @param right
     * @return true if left cannot be false if right shall be true
     */
    public boolean checkLeftSupersetOfRight(CoreExpression left, CoreExpression right) {
        return checkLeftSupersetOfRight(getNode(left), getNode(right));
    }

    /**
     * Tests whether the left expression must be true if the right shall be true, in other words <i>right implies left</i>.
     * 
     * @param left
     * @param right
     * @return true if left cannot be false if right shall be true
     */
    private boolean checkLeftSupersetOfRight(int leftNode, int rightNode) {
        timeout.assertHaveTime();
        return tree.getLogicHelper().leftImpliesRight(rightNode, leftNode);
    }

    /**
     * This method takes a {@link CombinedExpression} (AND vs. OR) and tries to find groups of members related to the same argName to form an IN (in case of OR)
     * vs. a NOT IN (in case of AND).
     * <p>
     * Each group is represented by list of {@link SimpleExpression}. In case of a given OR the group members are all {@link MatchExpression}s (positive), if
     * the expression was an AND, then the group members will be {@link NegationExpression}s (negative).
     * <p>
     * Example:
     * <p>
     * 
     * <pre>
     *        a=1 OR a=2 OR a=3 OR b=6 OR c=7 OR c=8
     *        => groups: (a=1, a=2, a=3), (c=7, c=8); remaining: b=6
     *          
     *        a!=1 AND a!=2 AND a!=3 AND b=6 AND c!=7 AND c!=9 AND d=8
     *        => groups: (a!=1, a!=2, a!=3), (c!=7, c!=9); remaining: b=6, d=8
     * </pre>
     * 
     * Any members of type {@link CombinedExpression} always go to the remaining list. This method does not operate recursively.
     * 
     * @param expression
     * @param groups each list represents a group (IN or NOT IN clause)
     * @param remaining all members of the given combined expression that were not assigned to any group
     * @return true if at least one group has been identified
     */
    public boolean groupInClauses(CombinedExpression expression, List<List<SimpleExpression>> groups, List<CoreExpression> remaining) {
        return performGrouping(expression.childExpressions(), groups, remaining, expression.combiType() == CombinedExpressionType.AND);
    }

    /**
     * Checks whether there is type alignment suggested, this disqualifies the candidate from becoming member of an IN or NOT IN
     * 
     * @param candidate
     * @return true if there is alignment required
     */
    private boolean isDateAlignmentRequired(SimpleExpression candidate) {
        AdlType type = dataBinding.dataTableConfig().typeOf(candidate.argName());
        AdlSqlType columnType = dataBinding.dataTableConfig().lookupColumn(candidate.argName(), processContext).columnType();
        return MatchCondition.shouldAlignDate(type, columnType, processContext);
    }

    /**
     * Separates the potential IN-group candidates (match expressions with values or negation expressions with values) from all the remaining candidates.
     * 
     * @param candidates
     * @param confirmed initially empty, will be filled with the confirmed candidates
     * @param remaining initially empty, all remaining expressions go here
     * @param filterNegations if true, we are looking for {@link NegationExpression}s, otherwise for {@link MatchExpression}s
     */
    private void filterInGroupingCandidates(List<CoreExpression> candidates, List<SimpleExpression> confirmed, List<CoreExpression> remaining,
            boolean filterNegations) {

        for (CoreExpression candidate : candidates) {
            // @formatter:off
            if (candidate instanceof SimpleExpression simple 
                    && simple.operator() == MatchOperator.EQUALS 
                    && simple.referencedArgName() == null
                    && ((filterNegations && candidate instanceof NegationExpression) 
                            || (!filterNegations && candidate instanceof MatchExpression))
                    && !isDateAlignmentRequired(simple)) {
                confirmed.add(simple);
                // @formatter:on
            }
            else {
                remaining.add(candidate);
            }
        }

    }

    /**
     * Groups elements from the candidates list together by argName
     * 
     * @param candidates
     * @param groups identified groups for an IN resp. NOT IN
     * @param remaining other candidates
     */
    private void performGrouping(List<SimpleExpression> candidates, List<List<SimpleExpression>> groups, List<CoreExpression> remaining) {
        List<SimpleExpression> currentGroup = new ArrayList<>();
        List<SimpleExpression> consumed = new ArrayList<>();
        for (int idxLeft = 0; idxLeft < candidates.size(); idxLeft++) {
            SimpleExpression left = candidates.get(idxLeft);
            if (consumed.contains(left)) {
                continue;
            }
            currentGroup.add(left);
            for (int idxRight = idxLeft + 1; idxRight < candidates.size(); idxRight++) {
                SimpleExpression right = candidates.get(idxRight);
                if (consumed.contains(right)) {
                    continue;
                }
                if (right.argName().equals(left.argName())) {
                    currentGroup.add(right);
                    consumed.add(right);
                }
            }
            if (currentGroup.size() > 1) {
                groups.add(currentGroup);
            }
            else {
                remaining.add(left);
            }
            currentGroup = new ArrayList<>();
        }
    }

    /**
     * Either searches for groups of {@link MatchExpression}s or groups of {@link NegationExpression}s
     * 
     * @param members
     * @param groups identified groups for an IN resp. NOT IN
     * @param remaining other members
     * @param filterNegations
     * @return if true, we are grouping {@link NegationExpression}s, otherwise {@link MatchExpression}s
     */
    private boolean performGrouping(List<CoreExpression> members, List<List<SimpleExpression>> groups, List<CoreExpression> remaining,
            boolean filterNegations) {
        List<SimpleExpression> candidates = new ArrayList<>();
        filterInGroupingCandidates(members, candidates, remaining, filterNegations);
        if (candidates.size() > 1) {
            performGrouping(candidates, groups, remaining);
        }
        else {
            remaining.addAll(candidates);
        }
        return !groups.isEmpty();
    }

    /**
     * Returns the initially gathered statistics about the root expression currently being converted.
     * 
     * @return expression statistics
     */
    public CoreExpressionStats getStats() {
        return stats;
    }

    /**
     * Tells whether we must add an extra "has any value check".
     * <p>
     * In case of collections (multi-row) a simple condition like <code>TBL.COLOR != 'blue'</code> turns into the more complicated question <i>NOT has any row
     * with</i> <code>TBL.COLOR = 'blue'</code>.
     * <p>
     * This creates a serious problem: any row with <code>TBL.COLOR = NULL</code> (either explicitly or via join) gets <i>included</i> in the result
     * (wrong!).<br>
     * In such a scenario we need an extra check for existence to correctly <i>exclude</i> the NULLs again.
     * 
     * @param condition
     * @return true if an extra existence match is required
     */
    public boolean isExtraExistenceMatchRequired(MatchCondition condition) {
        return !SIMPLE_CONDITION.check(processContext.getGlobalFlags()) && condition.isNegation() && condition.operator() != MatchOperator.IS_UNKNOWN;
    }

    /**
     * This method checks if there are multi-row sensitive arguments involved in the given expression.
     * <p>
     * Example I: <i>IS NULL problem</i>
     * <p>
     * 
     * <pre>
     * +------+-------+--------+
     * | ID   | COLOR | SHAPE  |
     * +------+-------+--------+
     * | 1356 | red   | circle |
     * +------+-------+--------+
     * | 1356 | blue  | NULL   |
     * +------+-------+--------+
     * | ...  | ...   | ...    |
     * </pre>
     * 
     * Let's say you query <code>TBL.SHAPE IS NULL</code> for "shape IS UNKNOWN". This does not reflect the truth because there is some other row with shape =
     * circle (so shape is not unknown for 1356). You must ensure that there is not any row with a value for shape.
     * <p>
     * Example II: <i>Accidental condition row-pinning</i>
     * <p>
     * 
     * <pre>
     * +------+-------+--------+
     * | ID   | COLOR | SHAPE  |
     * +------+-------+--------+
     * | 1356 | red   | circle |
     * +------+-------+--------+
     * | 1356 | blue  | NULL   |
     * +------+-------+--------+
     * | ...  | ...   | ...    |
     * </pre>
     * 
     * Let's say there are two conditions <code>TBL.SHAPE = 'circle'</code> and (different MatchCondition) <code>TBL.COLOR = 'blue'</code>.<br>
     * If you now write <code>TBL.SHAPE = 'circle' AND TBL.COLOR = 'blue'</code> the alias would become <code>TBL.SHAPE = 'circle' OR TBL.COLOR = 'blue'</code>
     * and the global WHERE would contain <code>TBL.SHAPE = 'circle' AND TBL.COLOR = 'blue'</code>. This is wrong, because the data sits on different rows, not
     * a single row of the table meets the combined condition. To address this problem you must INTERSECT the IDs with <code>TBL.SHAPE = 'circle'</code> and
     * <code>TBL.COLOR = 'blue'</code>.
     * <p>
     * Example III: <i>Accidental reference row-pinning</i>
     * <p>
     * 
     * <pre>
     * +------+--------+--------+
     * | ID   | SHAPE1 | SHAPE2 |
     * +------+--------+--------+
     * | 1356 | box    | circle |
     * +------+--------+--------+
     * | 1356 | circle | NULL   |
     * +------+--------+--------+
     * | ...  | ...    | ...    |
     * </pre>
     * <p>
     * This time you query <code>TBL.SHAPE1 = TBL.SHAPE2</code>. This is incorrect, again because the data does not sit on the same row. It is required to
     * restructure the query to select all IDs which have any match with <code>SHAPE1 = SHAPE2</code>.
     * <p>
     * Example IV: <i>Has not any</i>
     * <p>
     * 
     * <pre>
     * +------+-------+--------+
     * | ID   | COLOR | SHAPE  |
     * +------+-------+--------+
     * | 1356 | red   | circle |
     * +------+-------+--------+
     * | 1356 | blue  | square |
     * +------+-------+--------+
     * | ...  | ...   | ...    |
     * </pre>
     * <p>
     * This time you query <code>TBL.COLOR != 'red'</code>. This is wrong again. Here you must <i>exclude</i> any row with the value 'red'.
     * <p>
     * In all the examples the generated query must consider extra joins to produce correct results.
     * 
     * @param condition
     * @return true if any of the involved argNames is marked multi-row sensitive
     */
    public boolean isMultiRowSensitiveMatch(MatchCondition condition) {
        if (!condition.isNegation() && !condition.isReferenceMatch()) {
            // see examples I, II
            return stats.isMultiRowSensitive(condition.argNameLeft());
        }
        else if (!condition.isNegation()) {
            // see example III
            return condition.tableLeft().equals(condition.tableRight());
        }
        else {
            // see example II, II, IV
            return stats.isMultiRowSensitive(condition.argNameLeft())
                    || (condition.argNameRight() != null && stats.isMultiRowSensitive(condition.argNameRight()))
                    || (condition.isNegation() && !condition.tableLeft().tableNature().isIdUnique())
                    || (condition.isNegation() && condition.isReferenceMatch() && !condition.tableRight().tableNature().isIdUnique());
        }
    }

    /**
     * The query related to an expression is eligible to serve as a base query if it is either a (negated) match against a non-null value (except for
     * {@link #isNullQueryingAllowed(String)}) or a reference match or a (NOT) IN clause.
     * <p>
     * The record set returned by a base query must be superset of the records the root expression wants to select.
     * 
     * @see CoreExpressionSqlHelper#isNullQueryingAllowed(String)
     * @param expression
     * @return true if the given expression can serve as base query
     */
    public boolean isEligibleForBaseQuery(CoreExpression expression) {
        switch (expression) {
        case MatchExpression match:
            return (match.operator() != MatchOperator.IS_UNKNOWN || isNullQueryingAllowed(match.argName()));
        case NegationExpression _:
            return true;
        case CombinedExpression _ when !isSubNested(expression):
            return true;
        default:
            throw new IllegalStateException("Unexpected expression type to base query eligibility, given: " + expression);
        }
    }

    /**
     * Shorthand for checking if the given expression is a {@link CombinedExpression} that itself contains further levels of {@link CombinedExpression}s
     * 
     * @param expression
     * @return true if there is further nesting
     */
    public static boolean isSubNested(CoreExpression expression) {
        if (expression instanceof CombinedExpression cmb) {
            return cmb.members().stream().anyMatch(CombinedExpression.class::isInstance);
        }
        else {
            return false;
        }
    }

    /**
     * Tells whether we can safely translate IS UNKNOWN into IS NULL for a given argName, and the result could serve as a base query.
     * <p>
     * This is only the case if the argument is not multi-row sensitive and the table contains all rows, because only under these conditions we know there is
     * exactly one row in that table for any valid ID in the base audience. If the value is unknown, the column value will be null, so that a query with
     * <code>TBL.COL IS NULL</code> will correctly identify the records we are looking for.
     * 
     * @param argName
     * @return true if the argument can be queried with IS NULL for a base query
     */
    public boolean isNullQueryingAllowed(String argName) {
        return !stats.isMultiRowSensitive(argName) && (dataBinding.dataTableConfig().numberOfTables() == 1
                || dataBinding.dataTableConfig().lookupTableMetaInfo(argName, processContext).tableNature().containsAllIds());
    }

    /**
     * Computes a penalty factor in case of reference matches and multi-row sensitive arguments
     * 
     * @param expression
     * @return penalty factor &gt;=1.0
     */
    private double computeDbPenaltyFactor(MatchExpression expression) {
        double res = 1.0;
        if (expression.referencedArgName() != null) {
            if (stats.isMultiRowSensitive(expression.argName()) && stats.isMultiRowSensitive(expression.referencedArgName())) {
                res = res * 19;
            }
            else if (stats.isMultiRowSensitive(expression.argName()) || stats.isMultiRowSensitive(expression.referencedArgName())) {
                res = res * 11;
            }
            else {
                res = res * 2.0;
            }
        }
        else if (stats.isMultiRowSensitive(expression.argName())) {
            res = res * 7;
        }
        return res;
    }

    /**
     * Estimates the complexity of the given expression based on the nesting, type of sub-expressions and the database mapping
     * 
     * @param expression
     * @return complexity value &gt;=1.0
     */
    public double complexityOf(CoreExpression expression) {
        switch (expression) {
        case MatchExpression match: {
            switch (match.operator()) {
            case LESS_THAN, GREATER_THAN:
                return 1.2 * computeDbPenaltyFactor(match);
            case CONTAINS:
                return 1.8 * computeDbPenaltyFactor(match);
            // $CASES-OMITTED$
            default:
                return 1.0 * computeDbPenaltyFactor(match);
            }
        }
        case NegationExpression neg:
            // high penalty weight for NOT
            return 1.5 * complexityOf(neg.delegate());
        case CombinedExpression comb: {
            if (comb.combiType() == CombinedExpressionType.AND) {
                // the complexity of an AND is just a sum of its member complexities
                return comb.members().stream().map(this::complexityOf).collect(Collectors.summingDouble(d -> d));
            }
            else {
                // For an OR we assign a penalty weight to each member before summing
                return comb.members().stream().map(e -> complexityOf(e) * 1.1).collect(Collectors.summingDouble(d -> d));
            }
        }
        default:
            return 1.0;
        }

    }

    /**
     * Consolidates the given members to prepare the expressions for the creation of a union or an ON-join-condition.
     * <p>
     * An SQL UNION is nothing but a logical OR, so we may be able to merge IN-clauses, create these or eliminate redundancies
     * 
     * @param members expressions all related to the same table, to be consolidated for the inclusion in a UNION or an ON-join-condition
     * @return consolidated list of expressions
     */
    public List<CoreExpression> consolidateAliasGroupExpressions(List<CoreExpression> members) {

        int sizeBefore = 0;
        do {
            sizeBefore = members.size();
            members = consolidateUnionGroupMembersInternal(members);
        } while (members.size() < sizeBefore);
        return members;
    }

    /**
     * Performs a single consolidation run
     * 
     * @param members
     * @return consolidated list or the input if there was nothing to consolidate
     */
    private List<CoreExpression> consolidateUnionGroupMembersInternal(List<CoreExpression> members) {

        List<CoreExpression> candidates = new ArrayList<>(members);
        List<CoreExpression> res = new ArrayList<>(members.size());

        for (int idxLeft = 0; idxLeft < candidates.size(); idxLeft++) {
            CoreExpression left = candidates.get(idxLeft);
            boolean merged = false;
            for (int idxRight = idxLeft + 1; idxRight < candidates.size(); idxRight++) {
                CoreExpression right = candidates.get(idxRight);
                CoreExpression updatedMember = tryMergeForUnion(left, right);
                if (updatedMember != null) {
                    candidates.set(idxRight, updatedMember);
                    merged = true;
                    break;
                }
            }
            if (!merged) {
                res.add(left);
            }
        }
        if (res.size() < members.size()) {
            return res;
        }
        return members;

    }

    /**
     * Takes two expressions and merges them, (NOT) IN clause if possible.
     * 
     * @param left
     * @param right
     * @return merged expression or null if not possible
     */
    private CoreExpression tryMergeForUnion(CoreExpression left, CoreExpression right) {
        if (left.equals(right)) {
            return left;
        }
        if (left instanceof CombinedExpression cmbLeft && right instanceof CombinedExpression cmbRight) {
            return tryMergeInClauses(cmbLeft, cmbRight);
        }
        else if (left instanceof CombinedExpression cmbLeft && right instanceof SimpleExpression simpleRight) {
            return tryMergeInOrNotInClauseWithSimple(cmbLeft, simpleRight);
        }
        else if (left instanceof SimpleExpression simpleLeft && right instanceof CombinedExpression cmbRight) {
            return tryMergeInOrNotInClauseWithSimple(cmbRight, simpleLeft);
        }
        else if (left instanceof MatchExpression matchLeft && right instanceof MatchExpression matchRight) {
            return tryMergeMatchExpressions(matchLeft, matchRight);
        }
        return null;
    }

    /**
     * Checks whether two match expressions can form an IN-clause (same argName, multiple values)
     * 
     * @param left
     * @param right
     * @return merged expression or null if not possible
     */
    private CoreExpression tryMergeMatchExpressions(MatchExpression left, MatchExpression right) {

        // @formatter:off
        if (left.argName().equals(right.argName())
                && left.operator() == MatchOperator.EQUALS 
                && right.operator() == MatchOperator.EQUALS 
                && left.referencedArgName() == null
                && right.referencedArgName() == null 
                && !isDateAlignmentRequired(left)
                && !isDateAlignmentRequired(right)) {
            return CombinedExpression.orOf(Arrays.asList(left, right));
        // @formatter:on
        }
        return null;

    }

    /**
     * Assumes a combined expression on the left that either represents an IN (type OR) or a NOT IN (type AND).
     * <p>
     * If the simple expression on the right side is already member of the AND on the left (NOT IN case), then we return the simple expression because it is
     * shorter.<br>
     * If the simple expression on the right side is already member of the OR on the left (IN case), then we return the combined expression because the simple
     * one is redundant.<br>
     * If the simple expression on the right side is compatible (same argName, equals) to the OR on the left (IN case), then we return a merged combined OR
     * expression.<br>
     * 
     * @param combined
     * @param simple
     * @return merged expression or null if not possible
     */
    private CoreExpression tryMergeInOrNotInClauseWithSimple(CombinedExpression combined, SimpleExpression simple) {
        if (combined.members().contains(simple)) {
            if (combined.combiType() == CombinedExpressionType.AND) {
                return simple;
            }
            else {
                return combined;
            }
        }
        // @formatter:off
        else if (combined.combiType() == CombinedExpressionType.OR 
                && simple instanceof MatchExpression match 
                && combined.members().get(0) instanceof MatchExpression member
                && member.referencedArgName() == null
                && member.argName().equals(match.argName())
                && match.operator() == MatchOperator.EQUALS
                && !isDateAlignmentRequired(simple)) {
        // @formatter:on
            List<CoreExpression> updatedMembers = new ArrayList<>(combined.members());
            updatedMembers.add(simple);
            return CombinedExpression.orOf(updatedMembers);
        }
        return null;
    }

    /**
     * Assumes two combined expressions, each representing a (NOT) IN-clause and merges them into a single OR/AND-expression if possible.
     * 
     * @param left
     * @param right
     * @return merged OR or null if not possible
     */
    private CoreExpression tryMergeInClauses(CombinedExpression left, CombinedExpression right) {

        // @formatter:off
        if (left.combiType() == CombinedExpressionType.OR
                && right.combiType() == CombinedExpressionType.OR
                && left.members().get(0) instanceof MatchExpression memberLeft
                && right.members().get(0) instanceof MatchExpression memberRight
                && memberLeft.argName().equals(memberRight.argName())
                && memberLeft.operator() == MatchOperator.EQUALS
                && memberRight.operator() == MatchOperator.EQUALS) {
        // @formatter:on
            List<CoreExpression> updatedMembers = new ArrayList<>(left.members());
            updatedMembers.addAll(right.members());
            return CombinedExpression.orOf(updatedMembers);
        }
        // @formatter:off
        else if (left.combiType() == CombinedExpressionType.AND
                && right.combiType() == CombinedExpressionType.AND) {
        // @formatter:on

            // check if any is fully contained in the other
            List<CoreExpression> membersLeft = left.members();
            List<CoreExpression> membersRight = right.members();
            if (membersLeft.size() > membersRight.size() && membersLeft.containsAll(membersRight)) {
                // right is less restrictive and wins
                return right;
            }
            else if (membersLeft.size() < membersRight.size() && membersRight.containsAll(membersLeft)) {
                // left is less restrictive and wins
                return left;
            }
        }

        return null;
    }

    /**
     * Sorts the given list by complexity descending, so the most complex expression is at the top.
     * 
     * @see #complexityComparator
     * @param expressions to be sorted in descending order
     */
    public void sortByComplexityDescending(List<CoreExpression> expressions) {
        Collections.sort(expressions, complexityComparator.reversed().thenComparing(Function.identity()));
    }

    /**
     * Determines if the given expression is a {@link SimpleExpression} related to the primary table (either value match or by reference, left or right).
     * 
     * @param expression to be checked
     * @return true if this expression only involves the primary table
     */
    public boolean isSimpleExpressionOnPrimaryTable(CoreExpression expression) {
        return (expression instanceof SimpleExpression simple && isPrimaryTableInvolved(simple));
    }

    /**
     * Determines if the primary table is involved in the SQL that maps to the given expression.
     * 
     * @param expression to be checked
     * @return true if either the left table or the right table (in case of a reference match) is the primary table
     */
    public boolean isPrimaryTableInvolved(SimpleExpression expression) {
        String primaryTableName = dataBinding.dataTableConfig().primaryTable();
        return (primaryTableName != null
                && (dataBinding.dataTableConfig().lookupTableMetaInfo(expression.argName(), processContext).tableName().equals(primaryTableName)
                        || (expression.referencedArgName() != null && dataBinding.dataTableConfig()
                                .lookupTableMetaInfo(expression.referencedArgName(), processContext).tableName().equals(primaryTableName))));
    }

    /**
     * This avoids that the helper holds a reference to the context while the context knows the helper.
     * 
     * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
     */
    protected static record MinimalProcessContext(Map<String, Serializable> globalVariables, Set<Flag> globalFlags) implements ProcessContext {

        @Override
        public Map<String, Serializable> getGlobalVariables() {
            return this.globalVariables;
        }

        @Override
        public Set<Flag> getGlobalFlags() {
            return this.globalFlags;
        }

    }

}
