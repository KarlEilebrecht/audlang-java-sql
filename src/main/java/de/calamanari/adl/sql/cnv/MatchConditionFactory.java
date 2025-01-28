//@formatter:off
/*
 * MatchConditionFactory
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
import java.util.List;
import java.util.function.Predicate;

import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;

/**
 * A {@link MatchConditionFactory} abstracts the preparation step of a {@link MatchCondition}.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface MatchConditionFactory {

    /**
     * @see MatchCondition#createSimpleCondition(SimpleExpression, SqlConversionProcessContext)
     * @param expression
     * @return condition
     */
    MatchCondition createSimpleCondition(SimpleExpression expression);

    /**
     * @see MatchCondition#createInClauseCondition(List, SqlConversionProcessContext)
     * @param expressions list with expressions, all related to the same argName, either all matches or all negations
     * @return condition
     * @throws IllegalArgumentException if there are multiple argNames involved, multiple operands reference matches was detected.
     * @throws ClassCastException if expressions are not all of the same type
     */
    MatchCondition createInClauseCondition(List<SimpleExpression> expressions);

    /**
     * Auto-detects the form of match-condition from the given expression which can either be a {@link MatchExpression}, a {@link NegationExpression} or a
     * {@link CombinedExpression} representing an IN (OR or matches) resp. a NOT IN (AND of negations)
     * 
     * @param expression
     * @return condition
     * @throws IllegalArgumentException if there are multiple argNames involved, multiple operands reference matches was detected.
     * @throws ClassCastException if the expression is structured in an unexpected way and contains unsupported children
     */
    default MatchCondition createMatchCondition(CoreExpression expression) {
        switch (expression) {
        case SimpleExpression simple:
            return this.createSimpleCondition(simple);
        case CombinedExpression cmb:
            List<SimpleExpression> temp = new ArrayList<>(cmb.members().size());
            if (cmb.members().stream().anyMatch(Predicate.not(SimpleExpression.class::isInstance))) {
                throw new ClassCastException("CombinedExpressions representing IN or NOT IN must not contain any complex members, given: " + expression);
            }
            cmb.members().stream().map(SimpleExpression.class::cast).forEach(temp::add);
            return this.createInClauseCondition(temp);
        default:
            throw new IllegalArgumentException("Expecting either a SimpleExpression or a CombinedExpression representing an IN or NOT IN");
        }
    }

}
