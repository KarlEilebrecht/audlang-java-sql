//@formatter:off
/*
 * CoreExpressionStats
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

import static de.calamanari.adl.sql.cnv.ConversionHint.LEFT_OUTER_JOINS_REQUIRED;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_AND;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_IS_UNKNOWN;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_JOINS_REQUIRED;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_MULTI_ROW_REFERENCE_MATCH;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_MULTI_ROW_SENSITIVITY;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_OR;
import static de.calamanari.adl.sql.cnv.ConversionHint.NO_REFERENCE_MATCH;
import static de.calamanari.adl.sql.cnv.ConversionHint.SIMPLE_CONDITION;
import static de.calamanari.adl.sql.cnv.ConversionHint.SINGLE_ATTRIBUTE;
import static de.calamanari.adl.sql.cnv.ConversionHint.SINGLE_TABLE;
import static de.calamanari.adl.sql.cnv.ConversionHint.SINGLE_TABLE_CONTAINING_ALL_ROWS;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.Flag;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.ParentAwareExpressionNode;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.TableMetaInfo;

/**
 * {@link CoreExpressionStats} is a report with meta information about an expression to be converted.
 * <p>
 * The provided statistics later influence the way how the expression can be translated into a rather simplistic or complex SQL-expression.
 * <p>
 * The members (sets) of this record are all <b>mutable</b> to allow refinement.
 * 
 * @param hints see {@link ConversionHint}
 * @param argNames all the arguments involved in a given expression
 * @param argNamesMarkedMultiRow arguments in the expression that are directly or indirectly (sparse table) marked as multi-row
 * @param argNamesWithMultiRowSensitivity arguments with multi-row characteristics <i>relevant</i> to the expression
 * @param argNamesInPositiveValueMatches arguments that occur in matches against values
 * @param argNamesInNegativeValueMatches arguments that occur in negated matches against values
 * @param argNamesInPositiveIsUnknownMatches arguments that occur in IS UNKNOWN matches
 * @param argNamesInNegativeIsUnknownMatches aruments that occur in negated IS UNKNOWN matches
 * @param requiredTables list of all tables referenced by arguments in the expression
 * @param isSeparateBaseTableRequired true if an extra table is required to cover all IDs and the query cannot be based on a union of all related tables
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record CoreExpressionStats(Set<Flag> hints, Set<String> argNames, Set<String> argNamesMarkedMultiRow, Set<String> argNamesWithMultiRowSensitivity,
        Set<String> argNamesInPositiveValueMatches, Set<String> argNamesInNegativeValueMatches, Set<String> argNamesInPositiveIsUnknownMatches,
        Set<String> argNamesInNegativeIsUnknownMatches, Set<String> requiredTables, boolean isSeparateBaseTableRequired) {

    /**
     * @param hints see {@link ConversionHint}
     * @param argNames all the arguments involved in a given expression
     * @param argNamesMarkedMultiRow arguments in the expression that are directly or indirectly (sparse table) marked as multi-row
     * @param argNamesWithMultiRowSensitivity arguments with multi-row characteristics <i>relevant</i> to the expression
     * @param argNamesInPositiveValueMatches arguments that occur in matches against values
     * @param argNamesInNegativeValueMatches arguments that occur in negated matches against values
     * @param argNamesInPositiveIsUnknownMatches arguments that occur in IS UNKNOWN matches
     * @param argNamesInNegativeIsUnknownMatches aruments that occur in negated IS UNKNOWN matches
     * @param requiredTables list of all tables referenced by arguments in the expression
     * @param isSeparateBaseTableRequired true if an extra table is required to cover all IDs and the query cannot be based on a union of all related tables
     */
    public CoreExpressionStats {
        if (hints == null || argNames == null || argNamesMarkedMultiRow == null || argNamesWithMultiRowSensitivity == null
                || argNamesInPositiveValueMatches == null || argNamesInNegativeValueMatches == null || argNamesInPositiveIsUnknownMatches == null
                || argNamesInNegativeIsUnknownMatches == null || requiredTables == null) {
            throw new IllegalArgumentException("Internal sets must not be null, given: " + this);
        }
    }

    /**
     * Analyzes the given root expression considering the configured data binding and returns statistics to be used during expression to SQL conversion.
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @return statistics
     */
    public static CoreExpressionStats from(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx) {

        Set<String> multiRowArgNames = new TreeSet<>();
        Set<String> argNamesInPositiveValueMatches = new TreeSet<>();
        Set<String> argNamesInNegativeValueMatches = new TreeSet<>();
        Set<String> argNamesInPositiveIsUnknownMatches = new TreeSet<>();
        Set<String> argNamesInNegativeIsUnknownMatches = new TreeSet<>();
        Set<String> argNamesWithMultiRowSensitivity = new TreeSet<>();
        Set<String> requiredTables = new TreeSet<>(expression.allArgNames().stream()
                .map(argName -> dataBinding.dataTableConfig().lookupTableMetaInfo(argName, ctx)).map(TableMetaInfo::tableName).distinct().toList());

        collectArgNamesInValueMatches(expression, (e -> e.operator() != MatchOperator.IS_UNKNOWN), false, argNamesInPositiveValueMatches);
        collectArgNamesInValueMatches(expression, (e -> e.operator() != MatchOperator.IS_UNKNOWN), true, argNamesInNegativeValueMatches);
        collectArgNamesInValueMatches(expression, (e -> e.operator() == MatchOperator.IS_UNKNOWN), false, argNamesInPositiveIsUnknownMatches);
        collectArgNamesInValueMatches(expression, (e -> e.operator() == MatchOperator.IS_UNKNOWN), true, argNamesInNegativeIsUnknownMatches);

        collectArgNamesMultiRowOrSparse(expression, dataBinding, ctx, multiRowArgNames);
        collectArgNamesWithIsNullMultiRowSensitivity(dataBinding, ctx, argNamesInPositiveIsUnknownMatches, argNamesWithMultiRowSensitivity);
        collectArgNamesWithMultiRowSensitivity(expression, dataBinding, ctx, multiRowArgNames, argNamesWithMultiRowSensitivity);

        CoreExpressionStats stats = new CoreExpressionStats(new TreeSet<>(), argNamesWithMultiRowSensitivity, multiRowArgNames, argNamesWithMultiRowSensitivity,
                argNamesInPositiveValueMatches, argNamesInNegativeValueMatches, argNamesInPositiveIsUnknownMatches, argNamesInNegativeIsUnknownMatches,
                requiredTables, checkSeparateBaseTableRequired(expression, dataBinding, ctx));
        computeHints(expression, dataBinding, ctx, stats);

        return stats;
    }

    /**
     * @param stats
     * @param dataBinding
     * @param ctx
     * @return if there is only one table involved in the query
     */
    private static boolean checkSingleTableQuery(CoreExpressionStats stats, DataBinding dataBinding, ProcessContext ctx) {
        String primaryTableName = dataBinding.dataTableConfig().primaryTable();
        return stats.requiredTables.size() == 1 && (!ConversionDirective.ENFORCE_PRIMARY_TABLE.check(ctx.getGlobalFlags())
                || (primaryTableName != null && stats.requiredTables.contains(primaryTableName)));
    }

    /**
     * Based on the collected data we create hints that can later help to write simplified queries.
     * <p>
     * Hints are updated on the given stats instance.
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @param stats
     */
    private static void computeHints(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx, CoreExpressionStats stats) {
        if (expression.collectExpressions(e -> e instanceof MatchExpression match && match.operator() == MatchOperator.IS_UNKNOWN).isEmpty()) {
            stats.hints.add(NO_IS_UNKNOWN);
        }
        if (expression.collectExpressions(e -> e instanceof MatchExpression match && match.referencedArgName() != null).isEmpty()) {
            stats.hints.add(NO_REFERENCE_MATCH);
        }

        computeAndOrHints(expression, stats);
        if (expression.allArgNames().size() == 1) {
            stats.hints.add(SINGLE_ATTRIBUTE);
        }
        if (checkSingleTableQuery(stats, dataBinding, ctx)) {
            stats.hints.add(SINGLE_TABLE);
            TableMetaInfo tmi = dataBinding.dataTableConfig().lookupTableMetaInfoByTableName(stats.requiredTables.iterator().next());
            if (dataBinding.dataTableConfig().numberOfTables() == 1 || tmi.tableNature().containsAllIds()) {
                stats.hints.add(SINGLE_TABLE_CONTAINING_ALL_ROWS);
            }
        }

        if (!containsMultiRowSensitiveReferenceMatch(expression, stats)) {
            stats.hints.add(NO_MULTI_ROW_REFERENCE_MATCH);
        }

        if (checkSimpleSingleTableCondition(expression, dataBinding, ctx, stats)) {
            stats.hints.add(SIMPLE_CONDITION);
        }

        computeComplexHints(dataBinding, ctx, stats);

    }

    /**
     * Checks the structure of the expression to derive hints
     * 
     * @param expression
     * @param stats
     */
    private static void computeAndOrHints(CoreExpression expression, CoreExpressionStats stats) {
        if (expression instanceof SimpleExpression) {
            stats.hints.add(NO_AND);
            stats.hints.add(NO_OR);
        }
        else if (expression instanceof CombinedExpression cmb && !CoreExpressionSqlHelper.isSubNested(expression)
                && cmb.combiType() == CombinedExpressionType.OR) {
            stats.hints.add(NO_AND);
        }
        else if (expression instanceof CombinedExpression cmb && !CoreExpressionSqlHelper.isSubNested(expression)
                && cmb.combiType() == CombinedExpressionType.AND) {
            stats.hints.add(NO_OR);
        }
    }

    /**
     * Bases on the data collected so far this method adds some complex findings as hints
     * 
     * @param dataBinding
     * @param ctx
     * @param stats
     */
    private static void computeComplexHints(DataBinding dataBinding, ProcessContext ctx, CoreExpressionStats stats) {

        if (stats.argNamesWithMultiRowSensitivity.isEmpty()) {
            stats.hints.add(NO_MULTI_ROW_SENSITIVITY);
        }

        // @formatter:off
        if (SIMPLE_CONDITION.check(stats.hints) 
                || (SINGLE_TABLE_CONTAINING_ALL_ROWS.check(stats.hints) && ((NO_AND.check(stats.hints) && ConversionHint.NO_MULTI_ROW_REFERENCE_MATCH.check(stats.hints) && NO_IS_UNKNOWN.check(stats.hints)) || NO_MULTI_ROW_SENSITIVITY.check(stats.hints)))
                || (SINGLE_TABLE.check(stats.hints) && ((NO_AND.check(stats.hints) && ConversionHint.NO_MULTI_ROW_REFERENCE_MATCH.check(stats.hints)) || NO_MULTI_ROW_SENSITIVITY.check(stats.hints)) && NO_IS_UNKNOWN.check(stats.hints))
                ) {
        // @formatter:on
            addSimpleJoinTypeHint(dataBinding, ctx, stats);
        }
        else {

            addComplexJoinTypeHint(dataBinding, ctx, stats);
        }
    }

    /**
     * Adds a LEFT OUTER JOIN if no inner join is possible due to other conditions
     * 
     * @param dataBinding
     * @param ctx
     * @param stats
     */
    private static void addComplexJoinTypeHint(DataBinding dataBinding, ProcessContext ctx, CoreExpressionStats stats) {

        // when we query a negation color != red, and the underlying table can have multiple entries for the id then
        // we must ensure that none of the matching records has red color
        boolean multiJoinConflictPossible = stats.argNamesInNegativeValueMatches.stream()
                .map(argName -> dataBinding.dataTableConfig().lookupTableMetaInfo(argName, ctx)).anyMatch(tmi -> !tmi.tableNature().isIdUnique());

        boolean innerJoinsPossible = (NO_MULTI_ROW_SENSITIVITY.check(stats.hints) && NO_OR.check(stats.hints)
                && ConversionHint.NO_MULTI_ROW_REFERENCE_MATCH.check(stats.hints) && NO_IS_UNKNOWN.check(stats.hints) && !multiJoinConflictPossible);
        if (!innerJoinsPossible) {
            stats.hints.add(LEFT_OUTER_JOINS_REQUIRED);
        }
    }

    /**
     * Decides whether we need joins at all and otherwise advises to use LEFT OUTER JOIN
     * 
     * @param ctx
     * @param stats
     */
    private static void addSimpleJoinTypeHint(DataBinding dataBinding, ProcessContext ctx, CoreExpressionStats stats) {
        TableMetaInfo tmi = dataBinding.dataTableConfig().lookupTableMetaInfoByTableName(stats.requiredTables().iterator().next());
        if (stats.argNamesInNegativeValueMatches.isEmpty() || tmi.tableNature().isIdUnique()) {
            stats.hints.add(NO_JOINS_REQUIRED);
        }
        else {
            stats.hints.add(LEFT_OUTER_JOINS_REQUIRED);
        }
    }

    /**
     * @param argName
     * @return true if the given argument is explicitly or implicitly sensitive to multi-row scenarios
     */
    public boolean isMultiRowSensitive(String argName) {
        return argNamesWithMultiRowSensitivity.contains(argName);
    }

    /**
     * @return true if there are any multi-row sensitive arguments in the current query
     */
    public boolean hasAnyMultiRowSensitiveArgs() {
        return !argNamesWithMultiRowSensitivity.isEmpty();
    }

    /**
     * @param expression
     * @param dataBinding
     * @param ctx
     * @param stats
     * @return true id this condition can be written in a simplistic way (SELECT-FROM-WHERE) on a single table
     */
    private static boolean checkSimpleSingleTableCondition(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx, CoreExpressionStats stats) {
        if (!SINGLE_TABLE.check(stats.hints)) {
            return false;
        }
        if (stats.hasAnyMultiRowSensitiveArgs()) {
            TableMetaInfo tmi = dataBinding.dataTableConfig().lookupTableMetaInfoByTableName(stats.requiredTables.iterator().next());
            return (dataBinding.dataTableConfig().numberOfTables() == 1 || tmi.tableNature().containsAllIds()
                    || stats.argNamesInPositiveIsUnknownMatches.isEmpty()) && ConversionHint.NO_AND.check(stats.hints)
                    && ConversionHint.NO_MULTI_ROW_REFERENCE_MATCH.check(stats.hints)
                    && (stats.argNamesInNegativeValueMatches.isEmpty() || tmi.tableNature().isIdUnique());
        }
        else {
            return checkSimpleSingleTableCondition(expression, dataBinding, ctx);
        }
    }

    /**
     * Determines if the expression contains any reference match with a multi-row-sensitive argument involved
     * <p>
     * Especially in simple queries like <code>fact.color1 = &#64;fact.color2</code> it could be that both are mapped to the same column (with different filter
     * condition). In this case we cannot query directly, we must join this fact-table with left outer joins.
     * 
     * @param expression
     * @param stats
     * @return true if the expression or any of its sub-expressions is a reference match with a multi-row attribute involved
     */
    private static boolean containsMultiRowSensitiveReferenceMatch(CoreExpression expression, CoreExpressionStats stats) {
        switch (expression) {
        case SimpleExpression simple when simple.referencedArgName() != null
                && (stats.isMultiRowSensitive(simple.argName()) || stats.isMultiRowSensitive(simple.referencedArgName())):
            return true;
        case CombinedExpression cmb:
            return cmb.members().stream().anyMatch(e -> containsMultiRowSensitiveReferenceMatch(e, stats));
        default:
        }
        return false;
    }

    /**
     * Given the information that there is only a single table involved and we don't have any multi-row-sensitive arguments in this query we check if the whole
     * expression can be written <i>inline</i>.
     * <p>
     * Example I: <code>color=blue AND (shape=circle OR material=wood)</code> on the following table
     * <p>
     * 
     * <pre>
     *   +------+-------+--------+----------+
     *   | ID   | COLOR | SHAPE  | MATERIAL |
     *   +------+-------+--------+----------+
     *   | 8599 | blue  | circle | wood     |
     *   +------+-------+--------+----------+
     *   | 4132 | red   | square | metal    |
     *   +------+-------+--------+----------+
     *   | ...  | ...   | ...    | ...      |
     * </pre>
     * <p>
     * The related query is <i>simple</i>, because there are no additional joins required: <code>COLOR='blue' AND (SHAPE='circle' OR MATERIAL='wood')</code>.
     * <p>
     * Example II: <code>color IS UNKNOWN AND (shape=circle OR material=wood)</code> on the following table
     * <p>
     * 
     * <pre>
     *   +------+-------+--------+----------+
     *   | ID   | COLOR | SHAPE  | MATERIAL |
     *   +------+-------+--------+----------+
     *   | 8599 | blue  | circle | wood     |
     *   +------+-------+--------+----------+
     *   | 4132 | red   | square | metal    |
     *   +------+-------+--------+----------+
     *   | ...  | ...   | ...    | ...      |
     * </pre>
     * <p>
     * We can only translate this into the simple query <code>COLOR IS NULL AND (SHAPE='circle' OR MATERIAL='wood')</code> if we know that this table
     * <i>contains all IDs</i>. Otherwise we would have to query a different base table and create a join to this table not to miss any records.
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @return true if this is a simple single table condition
     */
    private static boolean checkSimpleSingleTableCondition(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx) {
        switch (expression) {
        case MatchExpression match:
            return checkSimpleConditionStraightMatch(match, dataBinding, ctx);
        case CombinedExpression cmb:
            return cmb.members().stream().allMatch(e -> checkSimpleSingleTableCondition(e, dataBinding, ctx));
        default:
            return true;
        }
    }

    /**
     * Checks if the match can be written as simple condition (single table)
     * <p>
     * This is the case if none of the conditions requires any individual join.
     * 
     * @param match
     * @param dataBinding
     * @param ctx
     * @return true if the match is simple, e.g. <code>COLOR='red'</code>, <code>COLOR IS NULL</code>
     */
    private static boolean checkSimpleConditionStraightMatch(MatchExpression match, DataBinding dataBinding, ProcessContext ctx) {
        return (match.operator() != MatchOperator.IS_UNKNOWN
                || dataBinding.dataTableConfig().lookupTableMetaInfo(match.argName(), ctx).tableNature().containsAllIds()
                || dataBinding.dataTableConfig().numberOfTables() == 1);
    }

    /**
     * Collects any argument that is explicitly marked multi-row or implicitly multi-row because the table is marked sparse.
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @param multiRowArgNames for result collection
     */
    private static void collectArgNamesMultiRowOrSparse(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx, Set<String> multiRowArgNames) {

        // @formatter:off
        expression.allArgNames().stream()
                    .filter(argName -> (dataBinding.dataTableConfig().lookupColumn(argName, ctx).isMultiRow()
                                     || dataBinding.dataTableConfig().lookupTableMetaInfo(argName, ctx).tableNature().isSparse()))
                    .forEach(multiRowArgNames::add);
        // @formatter:on
    }

    /**
     * There are a few hard indicators that an argName definitely needs multi-value handling. Here we identify and immediately copy these into the sensitive
     * collection.
     * 
     * @param expression
     * @param multiRowArgNames
     * @param multiRowSensitiveArgNames
     */
    private static void collectArgNamesWithDirectMultiRowSensitivity(CoreExpression expression, Set<String> multiRowArgNames,
            Set<String> multiRowSensitiveArgNames) {
        switch (expression) {
        case CombinedExpression cmb:
            cmb.members().forEach(e -> collectArgNamesWithDirectMultiRowSensitivity(e, multiRowArgNames, multiRowSensitiveArgNames));
            break;
        case MatchExpression match when match.operator() == MatchOperator.IS_UNKNOWN && multiRowArgNames.contains(match.argName()):
            // in a multi-row case you cannot query with TBL.COLOR IS NULL, instead you must select all IDs where you don't get
            // any value for COLOR (explicitly or caused by a failed join)
            multiRowSensitiveArgNames.add(match.argName());
            break;
        case MatchExpression match when match.referencedArgName() != null:
            if (multiRowArgNames.contains(match.argName())) {
                multiRowSensitiveArgNames.add(match.argName());
            }
            if (multiRowArgNames.contains(match.referencedArgName())) {
                multiRowSensitiveArgNames.add(match.referencedArgName());
            }
            break;
        case NegationExpression neg:
            // In a multi-row case, negation is problematic
            // Example:
            // ROWID | color
            // ---------------
            // 88761 | red
            // 88761 | yellow
            //
            // If you query color != red, you would still get 88761 (wrong). Query needs to explicitly exclude any existing rows with color=red.

            if (multiRowArgNames.contains(neg.argName())) {
                multiRowSensitiveArgNames.add(neg.argName());
            }
            if (neg.referencedArgName() != null && multiRowArgNames.contains(neg.referencedArgName())) {
                multiRowSensitiveArgNames.add(neg.referencedArgName());
            }
            break;
        default:
        }
    }

    /**
     * Safety: Mark an argName as multi-row-sensitive if there is an IS UNKNOWN condition and the column has filters attached.
     * <p>
     * If an argName is not marked multi-row-sensitive and the related table contains all ids, we usually assume that we can query IS NULL. This is true if the
     * related column is a regular column among others. However, if the column has filters assigned then the likelihood is high that it can happen that <i>the
     * null is not present</i>. Thus, for safety reasons we mark this argName as multi-row-sensitive.
     * 
     * @param dataBinding
     * @param ctx
     * @param argNamesInPositiveIsUnknownMatches
     * @param multiRowSensitiveArgNames
     */
    private static void collectArgNamesWithIsNullMultiRowSensitivity(DataBinding dataBinding, ProcessContext ctx,
            Set<String> argNamesInPositiveIsUnknownMatches, Set<String> multiRowSensitiveArgNames) {
        for (String argName : argNamesInPositiveIsUnknownMatches) {
            if (!multiRowSensitiveArgNames.contains(argName) && !dataBinding.dataTableConfig().lookupAssignment(argName, ctx).column().filters().isEmpty()) {
                multiRowSensitiveArgNames.add(argName);
            }
        }
    }

    /**
     * There is a difference if an argument is marked multi-row and its practical impact on the current query.<br>
     * For example, if you query <code>color=blue</code> (no other conditions) it is irrelevant if color is marked multi-row or not.
     * <p>
     * This method investigates all argNames marked multi-row and only marks them as <i>multi-row-sensitive</i> if there is a potential impact.
     * <p>
     * Additionally, arguments may suffer from multi-row-sensitivity in a <i>transitive</i> way (same table or reference matches). Then these arguments will be
     * marked as well.
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @param multiRowArgNames argNames known/marked in advance as multi-row
     * @param multiRowSensitiveArgNames to collect the results
     */
    private static void collectArgNamesWithMultiRowSensitivity(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx,
            Set<String> multiRowArgNames, Set<String> multiRowSensitiveArgNames) {
        int sizeBefore = 0;

        // iterative approach to consider transitivity
        do {
            sizeBefore = multiRowSensitiveArgNames.size();
            collectArgNamesWithDirectMultiRowSensitivity(expression, multiRowArgNames, multiRowSensitiveArgNames);
            collectArgNamesMultiRowSensitive(expression, dataBinding, ctx, multiRowArgNames, multiRowSensitiveArgNames);
        } while (multiRowSensitiveArgNames.size() > sizeBefore);
    }

    /**
     * Performs a single run, multiple may be required due to transitivity
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @param multiRowArgNames
     * @param multiRowSensitiveArgNames
     */
    private static void collectArgNamesMultiRowSensitive(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx, Set<String> multiRowArgNames,
            Set<String> multiRowSensitiveArgNames) {

        List<ParentAwareExpressionNode> candidates = ParentAwareExpressionNode.collectLeafNodes(expression);

        for (int idxLeft = 0; idxLeft < candidates.size() - 1; idxLeft++) {
            ParentAwareExpressionNode leftNode = candidates.get(idxLeft);
            for (int idxRight = idxLeft + 1; idxRight < candidates.size(); idxRight++) {
                ParentAwareExpressionNode rightNode = candidates.get(idxRight);
                // @formatter:off
                if (leftNode.hasCommonAndParentWith(rightNode) 
                        && leftNode.expression() instanceof SimpleExpression left
                        && rightNode.expression() instanceof SimpleExpression right 
                        && !right.equals(left)
                        && !isAlreadyMarkedMultiRowSensitive(left, right, multiRowSensitiveArgNames)
                        && isTableOverlap(left, right, dataBinding, ctx)) {
                    // @formatter:on

                    collectArgNamesMultiRowDueToImplication(left, right, multiRowArgNames, multiRowSensitiveArgNames);

                }
            }

        }

    }

    /**
     * Whenever two conditions (implied by left and right) operate on the same table (either directly or through reference matching) there is serious problem if
     * left and right are connected by AND and the data physically sits on two different rows.
     * <p>
     * 
     * <pre>
     *   +------+------------+-------------+
     *   | ID   | ENTRY_TYPE | ENTRY_VALUE |
     *   +------+------------+-------------+
     *   | 8599 | MODEL      | X4886       |
     *   +------+------------+-------------+
     *   | 8599 | BRAND      | FOO         |
     *   +------+------------+-------------+
     *   | 4132 | COLOR      | RED         |
     *   +------+------------+-------------+
     *   | ...  | ...        | ...         |
     * </pre>
     * 
     * <p>
     * In the example above, a naive query for <code>model=X4886 AND brand=FOO</code> would be
     * <code>ENTRY_TYPE = 'MODEL' AND ENTRY_VALUE='X4886' AND ENTRY_TYPE = 'BRAND' AND ENTRY_VALUE='FOO'</code> which cannot be fulfilled for a single row of
     * that table.
     * <p>
     * This we call <i>accidental row pinning</i>, and it can only be avoided by later generating multiple joins (arguments are multi-row-sensitive). <br>
     * In this case we must further investigate the involved argNames.
     * 
     * @param left
     * @param right
     * @param dataBinding
     * @param ctx
     * @return true if left and right operate on the same table
     */
    private static boolean isTableOverlap(SimpleExpression left, SimpleExpression right, DataBinding dataBinding, ProcessContext ctx) {
        String tableLeft = dataBinding.dataTableConfig().lookupTableMetaInfo(left.argName(), ctx).tableName();
        String tableRight = dataBinding.dataTableConfig().lookupTableMetaInfo(right.argName(), ctx).tableName();
        String referencedTableLeft = left.referencedArgName() == null ? null
                : dataBinding.dataTableConfig().lookupTableMetaInfo(left.referencedArgName(), ctx).tableName();
        String referencedTableRight = right.referencedArgName() == null ? null
                : dataBinding.dataTableConfig().lookupTableMetaInfo(right.referencedArgName(), ctx).tableName();
        return (tableLeft.equals(tableRight) || tableLeft.equals(referencedTableRight) || tableRight.equals(referencedTableLeft)
                || (referencedTableLeft != null && referencedTableLeft.equals(referencedTableRight)));

    }

    /**
     * @param left
     * @param right
     * @param multiRowSensitiveArgNames
     * @return true if already all involved argNames are marked as multi-row sensitive so we can skip further analysis
     */
    private static boolean isAlreadyMarkedMultiRowSensitive(SimpleExpression left, SimpleExpression right, Set<String> multiRowSensitiveArgNames) {
        return multiRowSensitiveArgNames.contains(left.argName()) && multiRowSensitiveArgNames.contains(right.argName())
                && (left.referencedArgName() == null || multiRowSensitiveArgNames.contains(left.argName()))
                && (right.referencedArgName() == null || multiRowSensitiveArgNames.contains(right.argName()));
    }

    /**
     * Two expressions, left and right, conditions on the <i>same table</i> involved in an <i>AND</i>.
     * <p>
     * Then every attribute that is <i>marked</i> multi-row among them becomes multi-row sensitive.
     * <p>
     * For the conditions multiple joins are required because otherwise a single condition would <i>pin</i> a particular row, so we could no longer find the
     * multiple rows required for the remainder of the <i>AND</i>-condition.
     * 
     * @param left
     * @param right
     * @param multiRowArgNames argNames we know they are marked multi-row
     * @param multiRowSensitiveArgNames result
     */
    private static void collectArgNamesMultiRowDueToImplication(SimpleExpression left, SimpleExpression right, Set<String> multiRowArgNames,
            Set<String> multiRowSensitiveArgNames) {

        if (multiRowArgNames.contains(left.argName())) {
            multiRowSensitiveArgNames.add(left.argName());
        }
        if (left.referencedArgName() != null && multiRowArgNames.contains(left.referencedArgName())) {
            multiRowSensitiveArgNames.add(left.referencedArgName());
        }
        if (multiRowArgNames.contains(right.argName())) {
            multiRowSensitiveArgNames.add(right.argName());
        }
        if (right.referencedArgName() != null && multiRowArgNames.contains(right.referencedArgName())) {
            multiRowSensitiveArgNames.add(right.referencedArgName());
        }

    }

    /**
     * Walks recursively through the expression and collects the argument names involved in value matches
     * 
     * @param expression to be analyzed
     * @param filter extra condition to be fulfilled for a particular value match
     * @param negated if true we <i>only consider</i> {@link NegationExpression}s, otherwise we <i>ignore</i> any {@link NegationExpression}s
     * @param result collection to append the involved argNames
     */
    private static void collectArgNamesInValueMatches(CoreExpression expression, Predicate<SimpleExpression> filter, boolean negated, Set<String> result) {
        switch (expression) {
        case MatchExpression match when (filter.test(match) && !negated):
            result.add(match.argName());
            break;
        case NegationExpression neg when (filter.test(neg) && negated):
            result.add(neg.argName());
            break;
        case CombinedExpression cmb:
            cmb.childExpressions().stream().forEach(e -> collectArgNamesInValueMatches(e, filter, negated, result));
            break;
        default:
        }
    }

    /**
     * Checks whether this query can be based solely on the tables referenced by the expression or if this super-set is insufficient.
     * <p>
     * This can happen if none of the tables contains all IDs and the query has at least one IS UNKNOWN. In this case there may exist records in the base
     * audience that are not present in the UNION of these tables, so they could be overlooked.
     * 
     * @param expression
     * @param dataBinding
     * @param ctx
     * @return true if an extra table is required to cover all IDs and the query cannot be based on a union of all related tables
     */
    private static boolean checkSeparateBaseTableRequired(CoreExpression expression, DataBinding dataBinding, ProcessContext ctx) {
        return dataBinding.dataTableConfig().numberOfTables() > 1
                && expression.allArgNames().stream().map(argName -> dataBinding.dataTableConfig().lookupTableMetaInfo(argName, ctx))
                        .noneMatch(info -> info.tableNature().containsAllIds())
                && !expression.collectExpressions(e -> e instanceof MatchExpression match && match.operator() == MatchOperator.IS_UNKNOWN).isEmpty();
    }

}
