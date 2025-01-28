//@formatter:off
/*
 * MatchConditionTest
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

import static de.calamanari.adl.cnv.tps.DefaultAdlType.INTEGER;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.STRING;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_INTEGER;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.ConversionException;
import de.calamanari.adl.irl.CombinedExpression;
import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.Operand;
import de.calamanari.adl.irl.SimpleExpression;
import de.calamanari.adl.irl.SpecialSetExpression;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DataColumn;
import de.calamanari.adl.sql.config.DefaultSqlContainsPolicy;
import de.calamanari.adl.sql.config.DummyDataTableConfig;
import de.calamanari.adl.sql.config.SingleTableConfig;
import de.calamanari.adl.sql.config.TableMetaInfo;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class MatchConditionTest {

    @Test
    void testBasics() {

        SqlConversionProcessContext ctx = new ResettableScpContext(new DataBinding(DummyDataTableConfig.getInstance(), DefaultSqlContainsPolicy.SQL92),
                new HashMap<>(), new HashSet<>());

        SimpleExpression red = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));

        MatchCondition tpl = MatchCondition.createSimpleCondition(red, ctx);

        String argNameLeft = tpl.argNameLeft();
        String argNameRight = tpl.argNameRight();
        List<ColumnCondition> columnConditions = tpl.columnConditions();
        DataColumn columnLeft = tpl.columnLeft();
        DataColumn columnRight = tpl.columnRight();
        TableMetaInfo tableLeft = tpl.tableLeft();
        TableMetaInfo tableRight = tpl.tableRight();

        MatchOperator operator = tpl.operator();
        boolean negation = tpl.isNegation();

        List<ColumnCondition> emptyColumnConditions = Collections.emptyList();

        MatchCondition tpl2 = new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight,
                columnConditions);

        assertEquals(tpl, tpl2);

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(null, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, null, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, null, argNameRight, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, null));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, emptyColumnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, "unexpected", tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableLeft, columnRight, columnConditions));

        List<ColumnCondition> duplicateConditions = new ArrayList<>();
        duplicateConditions.addAll(columnConditions);
        duplicateConditions.addAll(columnConditions);

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnLeft, duplicateConditions));

    }

    @Test
    void testBasics2() {

        SqlConversionProcessContext ctx = new ResettableScpContext(new DataBinding(DummyDataTableConfig.getInstance(), DefaultSqlContainsPolicy.SQL92),
                new HashMap<>(), new HashSet<>());

        SimpleExpression refMatch = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("color2", true));

        MatchCondition tpl = MatchCondition.createSimpleCondition(refMatch, ctx);

        String argNameLeft = tpl.argNameLeft();
        String argNameRight = tpl.argNameRight();
        List<ColumnCondition> columnConditions = tpl.columnConditions();
        DataColumn columnLeft = tpl.columnLeft();
        DataColumn columnRight = tpl.columnRight();
        TableMetaInfo tableLeft = tpl.tableLeft();
        TableMetaInfo tableRight = tpl.tableRight();

        MatchOperator operator = tpl.operator();
        boolean negation = tpl.isNegation();

        MatchCondition tpl2 = new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight,
                columnConditions);

        assertEquals(tpl, tpl2);

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(null, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, null, tableLeft, columnLeft, argNameRight, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, null, argNameRight, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, columnRight, null));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, null, tableRight, columnRight, columnConditions));

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, null, columnRight, columnConditions));

        List<ColumnCondition> duplicateConditions = new ArrayList<>();
        duplicateConditions.addAll(columnConditions);
        duplicateConditions.addAll(columnConditions);

        assertThrows(IllegalArgumentException.class,
                () -> new MatchCondition(operator, negation, argNameLeft, tableLeft, columnLeft, argNameRight, tableRight, null, duplicateConditions));

        ctx.getGlobalFlags().add(ConversionDirective.DISABLE_REFERENCE_MATCHING);

        assertThrows(ConversionException.class, () -> MatchCondition.createSimpleCondition(refMatch, ctx));

    }

    @Test
    void testBasics3() {

        // @formatter:off
        
        SingleTableConfig config = SingleTableConfig.forTable("TBL1")
            .asPrimaryTable()
            .idColumn("ID")
            .dataColumn("d1", SQL_VARCHAR)
                .mappedToArgName("arg1", STRING)
                .alwaysKnown()
            .dataColumn("d2", SQL_INTEGER)
                .mappedToArgName("arg2", INTEGER)
            .get();
        
        // @formatter:on

        SqlConversionProcessContext ctx = new ResettableScpContext(new DataBinding(config, DefaultSqlContainsPolicy.SQL92), new HashMap<>(), new HashSet<>());

        SimpleExpression greaterThan = (SimpleExpression) MatchExpression.of("arg2", MatchOperator.GREATER_THAN, Operand.of("500", false));

        MatchCondition.createSimpleCondition(greaterThan, ctx);

        ctx.getGlobalFlags().add(ConversionDirective.DISABLE_LESS_THAN_GREATER_THAN);

        assertThrows(ConversionException.class, () -> MatchCondition.createSimpleCondition(greaterThan, ctx));

        SimpleExpression contains = (SimpleExpression) MatchExpression.of("arg2", MatchOperator.CONTAINS, Operand.of("foobar", false));

        assertThrows(ConversionException.class, () -> MatchCondition.createSimpleCondition(contains, ctx));

        SimpleExpression contains2 = (SimpleExpression) MatchExpression.of("arg1", MatchOperator.CONTAINS, Operand.of("foobar", false));

        MatchCondition.createSimpleCondition(contains2, ctx);

        ctx.getGlobalFlags().add(ConversionDirective.DISABLE_CONTAINS);

        assertThrows(ConversionException.class, () -> MatchCondition.createSimpleCondition(contains2, ctx));

    }

    @Test
    void testBasics4() {

        SqlConversionProcessContext ctx = new ResettableScpContext(new DataBinding(DummyDataTableConfig.getInstance(), DefaultSqlContainsPolicy.SQL92),
                new HashMap<>(), new HashSet<>());

        SimpleExpression red = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));
        SimpleExpression blue = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("blue", false));
        SimpleExpression yellow = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("yellow", false));
        SimpleExpression green = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("green", false));
        SimpleExpression refColor = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("color2", true));

        SimpleExpression budgetEq3000 = (SimpleExpression) MatchExpression.of("budget", MatchOperator.EQUALS, Operand.of("3000", false));
        SimpleExpression budgetEq4000 = (SimpleExpression) MatchExpression.of("budget", MatchOperator.EQUALS, Operand.of("4000", false));
        SimpleExpression budgetGt4000 = (SimpleExpression) MatchExpression.of("budget", MatchOperator.GREATER_THAN, Operand.of("4000", false));

        List<SimpleExpression> inList = Arrays.asList(red, blue, yellow, green);

        MatchCondition.createInClauseCondition(inList, ctx);

        List<SimpleExpression> emptyInList = Collections.emptyList();

        assertThrows(IllegalArgumentException.class, () -> MatchCondition.createInClauseCondition(emptyInList, ctx));

        List<SimpleExpression> singletonInList = Collections.singletonList(red);
        MatchCondition.createInClauseCondition(singletonInList, ctx);

        List<SimpleExpression> effectiveSingletonInList = Arrays.asList(red, red);
        MatchCondition.createInClauseCondition(effectiveSingletonInList, ctx);

        List<SimpleExpression> mixedInList = Arrays.asList(red, blue, budgetEq3000);
        assertThrows(IllegalArgumentException.class, () -> MatchCondition.createInClauseCondition(mixedInList, ctx));

        MatchCondition.createInClauseCondition(Arrays.asList(budgetEq3000, budgetEq4000), ctx);

        List<SimpleExpression> inListBadOp = Arrays.asList(budgetEq3000, budgetGt4000);
        assertThrows(IllegalArgumentException.class, () -> MatchCondition.createInClauseCondition(inListBadOp, ctx));

        List<SimpleExpression> inListBadRef = Arrays.asList(red, blue, refColor);
        assertThrows(IllegalArgumentException.class, () -> MatchCondition.createInClauseCondition(inListBadRef, ctx));

    }

    @Test
    void testFactory() {

        SqlConversionProcessContext ctx = mock(SqlConversionProcessContext.class);
        DefaultMatchConditionFactory factory = new DefaultMatchConditionFactory(ctx);

        SimpleExpression red = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));
        SimpleExpression blue = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("blue", false));
        SimpleExpression yellow = (SimpleExpression) MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("yellow", false));

        CoreExpression redOrBlue = CombinedExpression.orOf(red, blue);
        CoreExpression illegalComplexAnd = CombinedExpression.andOf(redOrBlue, yellow);

        assertThrows(ClassCastException.class, () -> factory.createMatchCondition(illegalComplexAnd));

        CoreExpression redAndBlue = CombinedExpression.andOf(red, blue);
        CoreExpression illegalComplexOr = CombinedExpression.orOf(redAndBlue, yellow);

        assertThrows(ClassCastException.class, () -> factory.createMatchCondition(illegalComplexOr));

        CoreExpression illegalSpecialSet = SpecialSetExpression.none();

        assertThrows(IllegalArgumentException.class, () -> factory.createMatchCondition(illegalSpecialSet));

        assertThrows(IllegalArgumentException.class, () -> factory.createInClauseCondition(null));

        List<SimpleExpression> empty = Collections.emptyList();

        assertThrows(IllegalArgumentException.class, () -> factory.createInClauseCondition(empty));

        List<SimpleExpression> bad = Arrays.asList(red, blue, null);

        assertThrows(IllegalArgumentException.class, () -> factory.createInClauseCondition(bad));

    }

}
