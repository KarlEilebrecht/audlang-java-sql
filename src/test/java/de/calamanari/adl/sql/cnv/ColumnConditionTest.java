//@formatter:off
/*
 * ColumnConditionTest
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.sql.DefaultAdlSqlType;
import de.calamanari.adl.sql.DefaultQueryParameter;
import de.calamanari.adl.sql.QueryParameter;
import de.calamanari.adl.sql.config.AdlSqlColumn;
import de.calamanari.adl.sql.config.FilterColumn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class ColumnConditionTest {

    @Test
    void testBasics() {

        List<QueryParameter> emptyParams = Collections.emptyList();

        List<QueryParameter> singleParams = Arrays.asList(new DefaultQueryParameter("xyz", DefaultAdlSqlType.SQL_VARCHAR, "xyz", MatchOperator.EQUALS));
        List<QueryParameter> twoParams = Arrays.asList(new DefaultQueryParameter("xyz", DefaultAdlSqlType.SQL_VARCHAR, "xyz", MatchOperator.EQUALS),
                new DefaultQueryParameter("abc", DefaultAdlSqlType.SQL_VARCHAR, "abc", MatchOperator.EQUALS));
        List<QueryParameter> threeParams = Arrays.asList(new DefaultQueryParameter("xyz", DefaultAdlSqlType.SQL_VARCHAR, "xyz", MatchOperator.EQUALS),
                new DefaultQueryParameter("abc", DefaultAdlSqlType.SQL_VARCHAR, "abc", MatchOperator.EQUALS),
                new DefaultQueryParameter("stu", DefaultAdlSqlType.SQL_VARCHAR, "stu", MatchOperator.EQUALS));

        AdlSqlColumn column = new FilterColumn("TBL", "FILTER", DefaultAdlSqlType.SQL_VARCHAR, "bla");

        ColumnCondition condition = new ColumnCondition(ColumnConditionType.FILTER_LEFT, MatchOperator.EQUALS, column, singleParams);

        assertEquals(ColumnConditionType.FILTER_LEFT, condition.type());

        condition = new ColumnCondition(ColumnConditionType.DATE_RANGE, MatchOperator.EQUALS, column, twoParams);

        assertEquals(ColumnConditionType.DATE_RANGE, condition.type());

        assertThrows(IllegalArgumentException.class, () -> new ColumnCondition(ColumnConditionType.FILTER_LEFT, MatchOperator.EQUALS, column, emptyParams));
        assertThrows(IllegalArgumentException.class, () -> new ColumnCondition(ColumnConditionType.FILTER_LEFT, MatchOperator.EQUALS, column, twoParams));
        assertThrows(IllegalArgumentException.class, () -> new ColumnCondition(ColumnConditionType.FILTER_RIGHT, MatchOperator.EQUALS, column, twoParams));
        assertThrows(IllegalArgumentException.class, () -> new ColumnCondition(ColumnConditionType.DATE_RANGE, MatchOperator.EQUALS, column, singleParams));
        assertThrows(IllegalArgumentException.class, () -> new ColumnCondition(ColumnConditionType.DATE_RANGE, MatchOperator.EQUALS, column, threeParams));
        assertThrows(IllegalArgumentException.class, () -> new ColumnCondition(ColumnConditionType.IN_CLAUSE, MatchOperator.EQUALS, column, singleParams));

    }

}
