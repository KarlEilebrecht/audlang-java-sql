//@formatter:off
/*
 * ExpressionAlias
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

import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;

/**
 * An {@link ExpressionAlias} represents what we know (information gathered while building the query) about an alias query (WITH) for certain detail expression.
 * <p>
 * The term <i>alias</i> refers to an entity that identifies a <b>positive condition</b> on a single attribute in memory.<br>
 * Aliases are always created from a given {@link CoreExpression}. The latter is either a {@link SimpleExpression} or a <i>not further nested</i>
 * {@link CombinedExpression}. Such a combined expression can be either an OR of {@link MatchExpression}s related to the same argument or an AND of
 * {@link NegationExpression}s related to the same argument reflecting an SQL IN resp. NOT IN clause.
 * <p>
 * <b>Important:</b> As mentioned the expression {@link #expression} stored in the alias is always positive. So, even if you created the alias from an AND of
 * negations (aka NOT IN) then the expression will be the corresponding OR.
 * <p>
 * The motivation for this approach was to avoid having the same logical condition multiple times (negated). The alias allows to keep track across a larger
 * expression if the same condition is referenced again (or its inversion).
 * <p>
 * Not all aliases later occur with their name in the created SQL. If possible the raw table names will be preferred over referencing the aliases.
 * <p>
 * The identity of an alias is solely based on its name.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class ExpressionAlias implements Comparable<ExpressionAlias> {

    /**
     * There should be only one alias per expression at a time.
     */
    private final CoreExpression expression;

    /**
     * SQL-name of the alias to be generated, defines the identity (equals, hashcode, compareTo) of this instance.
     */
    private final String name;

    /**
     * Number of positive references (means argName = value) this alias is involved
     */
    private int positiveReferenceCount = 0;

    /**
     * Number of negative references (means argName != value) this alias is involved
     */
    private int negativeReferenceCount = 0;

    /**
     * 
     * @param name <b>the identity</b>, two instances with the same name are equal, NOT NULL
     * @param expression the sub-expression this alias stands for, NOT NULL
     */
    public ExpressionAlias(String name, CoreExpression expression) {
        if (name == null || name.isBlank() || expression == null) {
            throw new IllegalArgumentException(String.format("Arguments must not be null, empty or blank, given: name=%s, expression=%s", name, expression));
        }

        if (!(expression instanceof MatchExpression) && !(expression instanceof CombinedExpression cmb && cmb.combiType() == CombinedExpressionType.OR
                && cmb.members().stream().allMatch(MatchExpression.class::isInstance))) {
            // early fail if an unexpected expression sneaks in to prevent later confusion
            throw new IllegalArgumentException(String.format(
                    "Aliases can only be positive match expressions or combined expressions of type OR of positive match expressions, given: name=%s, expression=%s",
                    name, expression));
        }
        this.name = name;
        this.expression = expression;
    }

    /**
     * This returns the expression (resp. the positive form) behind this alias. Even if you have created the alias with an AND of negations this method will
     * return an OR of match expressions. The negation is counted here: {@link #getNegativeReferenceCount()}
     * 
     * @return the core expression backing this alias
     */
    public CoreExpression getExpression() {
        return expression;
    }

    /**
     * @return technical (SQL-conforming) name of this alias
     */
    public String getName() {
        return name;
    }

    /**
     * @return Number of times this alias was referenced positively (e.g. <code>color = red</code>)
     */
    public int getPositiveReferenceCount() {
        return positiveReferenceCount;
    }

    /**
     * @return Number of times this alias was referenced negatively (e.g. <code>color != red</code>)
     */
    public int getNegativeReferenceCount() {
        return negativeReferenceCount;
    }

    /**
     * Increments the number of positive references to this alias
     */
    public void registerPositiveReference() {
        positiveReferenceCount++;
    }

    /**
     * Increments the number of negative references to this alias (counts a negation)
     */
    public void registerNegativeReference() {
        negativeReferenceCount++;
    }

    /**
     * This method determines if a query required the positive result set and its inversion within the same query.
     * <p>
     * When we select rows from a table given a certain condition (the alias stands for, e.g. color=blue) and in the same query anywhere else we reference the
     * inversion of that selection (here: color!=blue), then obviously the result set of the alias cannot serve as (part of) a base query that consists of an
     * <i>alias union</i>.
     * <p>
     * In this case the alias condition must be dropped and the whole table must be included in the union not to miss any rows.
     * 
     * @return true if the alias must be replaced with its full table row set (select *) if the alias should be included in a base query union
     */
    public boolean requiresAllRowsOfTableQueryInUnion() {
        return positiveReferenceCount > 0
                && (negativeReferenceCount > 0 || (this.expression instanceof MatchExpression match && match.operator() == MatchOperator.IS_UNKNOWN));
    }

    /**
     * @return checks if the expression of this alias is a reference match
     */
    public boolean isReferenceMatch() {
        return expression instanceof SimpleExpression simple && simple.referencedArgName() != null;
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ExpressionAlias other = (ExpressionAlias) obj;
        return name.equals(other.name);
    }

    @Override
    public int compareTo(ExpressionAlias o) {
        return this.name.compareTo(o.name);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [name=" + name + ", positiveReferenceCount=" + positiveReferenceCount + ", negativeReferenceCount="
                + negativeReferenceCount + ", expression=" + expression + "]";
    }

}
