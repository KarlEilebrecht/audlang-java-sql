//@formatter:off
/*
 * ExpressionAliasTest
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.Operand;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class ExpressionAliasTest {

    @Test
    void testBasics() {

        CoreExpression expression = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));

        ExpressionAlias alias = new ExpressionAlias("bla", expression);

        assertEquals("ExpressionAlias [name=bla, positiveReferenceCount=0, negativeReferenceCount=0, expression=color = red]", alias.toString());

        assertThrows(IllegalArgumentException.class, () -> new ExpressionAlias("bla", null));
        assertThrows(IllegalArgumentException.class, () -> new ExpressionAlias(null, expression));

        assertEquals(0, alias.getNegativeReferenceCount());
        assertEquals(0, alias.getPositiveReferenceCount());

        alias.registerNegativeReference();

        assertEquals(1, alias.getNegativeReferenceCount());
        assertEquals(0, alias.getPositiveReferenceCount());

        alias.registerPositiveReference();

        assertEquals(1, alias.getNegativeReferenceCount());
        assertEquals(1, alias.getPositiveReferenceCount());

        assertFalse(alias.isReferenceMatch());

        CoreExpression expression2 = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("color2", true));

        ExpressionAlias alias2 = new ExpressionAlias("bla", expression2);

        assertEquals("ExpressionAlias [name=bla, positiveReferenceCount=0, negativeReferenceCount=0, expression=color = @color2]", alias2.toString());

        assertEquals(alias, alias2);

        assertTrue(alias2.isReferenceMatch());

        ExpressionAlias alias3 = new ExpressionAlias("bla1", expression2);

        assertTrue(alias3.compareTo(alias) > 0);
    }

}
