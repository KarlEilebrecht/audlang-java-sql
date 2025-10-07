//@formatter:off
/*
 * DefaultMatchConditionFactory
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.SimpleExpression;

/**
 * The {@link DefaultMatchConditionFactory} provides integrated caching, so the exact same comparison or (NOT) IN-clause won't be prepared twice which also
 * reduces the number of different parameters.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class DefaultMatchConditionFactory implements MatchConditionFactory {

    /**
     * physical data model mapping, variables and flags
     */
    private final SqlConversionProcessContext ctx;

    /**
     * Caches the already created match conditions by the input expression(s) they are based on
     */
    private final Map<CoreExpression, MatchCondition> cache = new HashMap<>();

    public DefaultMatchConditionFactory(SqlConversionProcessContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public MatchCondition createSimpleCondition(SimpleExpression expression) {
        return cache.computeIfAbsent(expression, _ -> MatchCondition.createSimpleCondition(expression, ctx));
    }

    @Override
    public MatchCondition createInClauseCondition(List<SimpleExpression> expressions) {
        if (expressions == null || expressions.isEmpty() || expressions.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Argument expressions must not be null or empty, given: " + expressions);
        }

        // it is either a list of OR'd positive MatchExpressions or a list of AND'd NegationExpressions
        // thus we can use a combined expression of the members as the key
        // error handling (crap input) we leave to the record validation
        CombinedExpressionType combiType = (expressions.get(0) instanceof NegationExpression) ? CombinedExpressionType.AND : CombinedExpressionType.OR;
        CoreExpression keyExpression = CombinedExpression.of(new ArrayList<>(expressions), combiType);

        return cache.computeIfAbsent(keyExpression, _ -> MatchCondition.createInClauseCondition(expressions, ctx));
    }

}
