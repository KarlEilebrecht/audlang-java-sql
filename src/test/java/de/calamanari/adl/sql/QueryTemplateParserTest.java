//@formatter:off
/*
 * QueryTemplateParserTest
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

package de.calamanari.adl.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.sql.QueryTemplateParser.ParameterListener;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
@ExtendWith(MockitoExtension.class)
class QueryTemplateParserTest {

    static final Logger LOGGER = LoggerFactory.getLogger(QueryTemplateParserTest.class);

    ParameterListener listener = null;

    QueryTemplateParser parser = null;

    String template = null;

    private void init(String template) {
        this.listener = mock(ParameterListener.class);
        this.parser = new QueryTemplateParser(listener);
        this.template = template;
    }

    @Test
    void testBasics() {

        init("");

        parser.parseSqlTemplate("");

        verify(listener, never()).handleParameter(anyString(), anyString(), anyInt(), anyInt());

        init("${p}");

        parser.parseSqlTemplate(template);

        verify(listener, times(1)).handleParameter("p", template, 0, 4);

        init("${p}${x}");

        parser.parseSqlTemplate(template);

        verify(listener, times(1)).handleParameter("p", template, 0, 4);
        verify(listener, times(1)).handleParameter("x", template, 4, 8);

        init("${p} ${x}");

        parser.parseSqlTemplate(template);

        verify(listener, times(1)).handleParameter("p", template, 0, 4);
        verify(listener, times(1)).handleParameter("x", template, 5, 9);

        init("${ longP} ${xLong } $$$ {} ${ y_Long }");

        parser.parseSqlTemplate(template);

        verify(listener, times(1)).handleParameter("longP", template, 0, 9);
        verify(listener, times(1)).handleParameter("xLong", template, 10, 19);
        verify(listener, times(1)).handleParameter("y_Long", template, 27, 38);

    }

    @Test
    void testSpecialCase() {

        assertThrows(IllegalArgumentException.class, () -> new QueryTemplateParser(null));

        init(null);

        assertThrows(IllegalArgumentException.class, () -> parser.parseSqlTemplate(template));

        init("${a");

        assertThrows(QueryPreparationException.class, () -> parser.parseSqlTemplate(template));

        init("${} bla");

        assertThrows(QueryPreparationException.class, () -> parser.parseSqlTemplate(template));

        assertEquals("msg", new QueryPreparationException("msg", new RuntimeException()).getMessage());

        RuntimeException ex = new RuntimeException();

        assertEquals(ex, new QueryPreparationException(ex).getCause());

    }

}
