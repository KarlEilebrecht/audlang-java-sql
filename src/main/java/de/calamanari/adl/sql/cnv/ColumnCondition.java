//@formatter:off
/*
 * ColumnCondition
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

import java.util.List;

import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.sql.QueryParameter;
import de.calamanari.adl.sql.config.AdlSqlColumn;
import de.calamanari.adl.sql.config.DataColumn;
import de.calamanari.adl.sql.config.FilterColumn;

/**
 * A {@link ColumnCondition} carries the meta data and the prepared parameters related to a single column, which can either be a {@link DataColumn} (means a
 * column related to an argName) or a {@link FilterColumn} to be tested against a value.
 * <p>
 * Column conditions apply to value matches and also to reference matches if the related column or referenced column has filters.<br>
 * Additional column conditions can occur in the main WHERE-clause if the table the query is starting with has table filter columns.
 * 
 * @param type tells how this condition should be translated
 * @param operator the operator to compare the column against the value
 * @param column to be compared
 * @param parameters one or multiple prepared parameters, see {@link ColumnConditionType}
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record ColumnCondition(ColumnConditionType type, MatchOperator operator, AdlSqlColumn column, List<QueryParameter> parameters) {

    /**
     * @param type tells how this condition should be translated
     * @param operator the operator to compare the column against the value
     * @param column to be compared
     * @param parameters one or multiple prepared parameters, see {@link ColumnConditionType}
     */
    public ColumnCondition {
        if (operator == null || type == null || column == null || parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Arguments must not be null, parameters must not be empty, given: type=%s, operator=%s, column=%s, parameters=%s", type,
                            operator, column, parameters));
        }
        if ((type == ColumnConditionType.SINGLE || type == ColumnConditionType.FILTER_LEFT || type == ColumnConditionType.FILTER_RIGHT
                || type == ColumnConditionType.AFTER_TODAY) && parameters.size() > 1) {
            throw new IllegalArgumentException(
                    String.format("When this type is specified, only one parameter can be provided, given: type=%s, operator=%s, column=%s, parameters=%s",
                            type, operator, column, parameters));
        }
        if (type == ColumnConditionType.IN_CLAUSE && parameters.size() < 2) {
            throw new IllegalArgumentException(String.format(
                    "When type INCLAUSE is specified, at least two parameters must be provided, given: type=%s, operator=%s, column=%s, parameters=%s", type,
                    operator, column, parameters));
        }
        if (type == ColumnConditionType.DATE_RANGE && parameters.size() != 2) {
            throw new IllegalArgumentException(String.format(
                    "When type DATE_RANGE is specified, two parameters are expected, the original date and the day after, given: type=%s, operator=%s, column=%s, parameters=%s",
                    type, operator, column, parameters));
        }
    }

}