//@formatter:off
/*
 * CoreExpressionSqlHelperTest
 * Copyright 2025 Karl Eilebrecht
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

import static de.calamanari.adl.cnv.StandardConversions.parseCoreExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.NegationExpression;
import de.calamanari.adl.irl.Operand;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DefaultSqlContainsPolicy;
import de.calamanari.adl.sql.config.DummyDataTableConfig;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class CoreExpressionSqlHelperTest {

    static final Logger LOGGER = LoggerFactory.getLogger(CoreExpressionSqlHelperTest.class);

    @Test
    void testBasics() {

        CoreExpression expression = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));

        SqlConversionProcessContext ctx = new ResettableScpContext(new DataBinding(DummyDataTableConfig.getInstance(), DefaultSqlContainsPolicy.SQL92),
                new HashMap<>(), new HashSet<>());

        CoreExpressionSqlHelper helper = new CoreExpressionSqlHelper(expression, null, ctx);

        assertFalse(CoreExpressionSqlHelper.isSubNested(expression));

        assertTrue(helper.complexityOf(expression) > 0);

        CoreExpression expression2 = NegationExpression.of(expression, true);

        assertTrue(helper.complexityOf(expression2) > helper.complexityOf(expression));

        CoreExpression red = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));
        CoreExpression blue = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("blue", false));
        CoreExpression yellow = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("yellow", false));
        CoreExpression green = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("green", false));
        CoreExpression val1Gt3 = MatchExpression.of("val1", MatchOperator.GREATER_THAN, Operand.of("3", false));
        CoreExpression commentContainsBlue = MatchExpression.of("comment", MatchOperator.CONTAINS, Operand.of("blue", false));

        CoreExpression parent0 = CombinedExpression.andOf(red, yellow, val1Gt3);

        CoreExpression parent1 = CombinedExpression.orOf(parent0, green);

        CoreExpression parent2 = CombinedExpression.orOf(yellow, blue);

        CoreExpression parent3 = CombinedExpression.andOf(parent1, parent2);

        CoreExpression parent4 = CombinedExpression.orOf(parent3, red);

        CoreExpression rootExpression = CombinedExpression.orOf(parent4, commentContainsBlue);

        assertTrue(helper.complexityOf(rootExpression) > helper.complexityOf(parent4));

    }

    @Test
    void testMerge() {

        CoreExpression expression = parseCoreExpression("color any of (red, green, blue, yellow, black) and country any of (USA, Germany, UK, France)");

        SqlConversionProcessContext ctx = new ResettableScpContext(new DataBinding(DummyDataTableConfig.getInstance(), DefaultSqlContainsPolicy.SQL92),
                new HashMap<>(), new HashSet<>());

        CoreExpressionSqlHelper helper = new CoreExpressionSqlHelper(expression, null, ctx);

        List<CoreExpression> candidates = Arrays.asList(parseCoreExpression("color any of (red, green)"), parseCoreExpression("country any of (USA, Germany)"),
                parseCoreExpression("color any of (green, blue)"), parseCoreExpression("country any of (Germany, UK)"), parseCoreExpression("color = black"),
                parseCoreExpression("country = France"));

        assertEquals(
                Arrays.asList(parseCoreExpression("color any of (black, blue, green, red)"), parseCoreExpression("country any of (France, Germany, UK, USA)")),
                helper.consolidateAliasGroupExpressions(candidates));

        candidates = Arrays.asList(parseCoreExpression("color STRICT not any of (red, green)"), parseCoreExpression("country STRICT not any of (USA, Germany)"),
                parseCoreExpression("color STRICT not any of (red, green, blue)"), parseCoreExpression("country STRICT not any of (Germany, UK, USA)"),
                parseCoreExpression("STRICT color != red"), parseCoreExpression("strict country != USA"), parseCoreExpression("country is unknown"));

        assertEquals(Arrays.asList(parseCoreExpression("strict color != red"), parseCoreExpression("strict country != USA"),
                parseCoreExpression("country is unknown")), helper.consolidateAliasGroupExpressions(candidates));

        candidates = Arrays.asList(parseCoreExpression("color STRICT NOT any of (black, blue, green, red, yellow)"),
                parseCoreExpression("color STRICT NOT any of (blue, yellow)"));

        assertEquals(Arrays.asList(parseCoreExpression("color STRICT NOT any of (blue, yellow)")), helper.consolidateAliasGroupExpressions(candidates));

        candidates = Arrays.asList(parseCoreExpression("color any of (black, blue, green, red, yellow)"), parseCoreExpression("color = blue"));

        assertEquals(Arrays.asList(parseCoreExpression("color any of (black, blue, green, red, yellow)")), helper.consolidateAliasGroupExpressions(candidates));

        candidates = Arrays.asList(parseCoreExpression("color any of (black, blue, green)"), parseCoreExpression("color any of (black, blue, green)"));

        assertEquals(Arrays.asList(parseCoreExpression("color any of (black, blue, green)")), helper.consolidateAliasGroupExpressions(candidates));

        candidates = Arrays.asList(parseCoreExpression("color = blue"), parseCoreExpression("color = blue"), parseCoreExpression("color = blue"));

        assertEquals(Arrays.asList(parseCoreExpression("color = blue")), helper.consolidateAliasGroupExpressions(candidates));

    }

}
