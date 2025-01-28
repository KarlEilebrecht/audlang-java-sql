//@formatter:off
/*
 * SqlConversionContextTest
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.irl.CoreExpression;
import de.calamanari.adl.irl.MatchExpression;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.irl.Operand;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class SqlConversionContextTest {

    @Test
    void testBasics() {

        CoreExpression red = MatchExpression.of("color", MatchOperator.EQUALS, Operand.of("red", false));

        SqlConversionContext ctx = new SqlConversionContext();

        SqlConversionProcessContext processContext = mock(SqlConversionProcessContext.class);

        ctx.setProcessContext(processContext);

        assertEquals(processContext, ctx.getProcessContext());

        ctx.markNegation();

        assertTrue(ctx.isNegation());

        ctx.skipChildExpression(red);

        assertEquals(red, ctx.getSkippedChildExpressions().get(0));

        ctx.clear();

        assertEquals(processContext, ctx.getProcessContext());

        assertTrue(ctx.getSkippedChildExpressions().isEmpty());

        assertFalse(ctx.isNegation());

    }

}
