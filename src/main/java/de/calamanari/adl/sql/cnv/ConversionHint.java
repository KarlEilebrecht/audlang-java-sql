//@formatter:off
/*
 * ConversionHint
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

import de.calamanari.adl.Flag;
import de.calamanari.adl.irl.MatchOperator;

/**
 * Enumeration with findings that can influence SQL expression generation
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum ConversionHint implements Flag {

    /**
     * The expression to be converted does not contain any (negated) {@link MatchOperator#IS_UNKNOWN}
     * <p>
     * This hint can help simplifying queries because IS UNKNOWN (SQL IS NULL) can be evil in conjunction with multi-row-sensitivity where IS UNKNOWN
     * effectively means <i>not any row where this field as any value</i>. If we know that the whole query does not suffer from this problem we might create a
     * simpler query.
     */
    NO_IS_UNKNOWN,

    /**
     * The expression to be converted does not compare any arguments to other arguments
     * <p>
     * Reference matches often require extra joins if the arguments are mapped to columns of the same table and the values sit on different rows. <br>
     * Knowing that there are no reference matches in a query may lead to simpler queries.
     */
    NO_REFERENCE_MATCH,

    /**
     * The expression to be converted is either an atom (e.g., <code>color=red</code>) or an OR of atoms (e.g.,
     * <code>color=red OR shape=circle OR size=XXL</code>)
     */
    NO_AND,

    /**
     * The expression to be converted is either an atom (e.g., <code>color=red</code>) or an AND of atoms (e.g.,
     * <code>color=red AND shape=circle AND size=XXL</code>)
     */
    NO_OR,

    /**
     * None of the involved args is sensitive to multi-row
     */
    NO_MULTI_ROW_SENSITIVITY,

    /**
     * There is no reference match with or against a multi-row argument
     */
    NO_MULTI_ROW_REFERENCE_MATCH,

    /**
     * Derived hint: this query does not require any joins, all conditions can be applied as-is
     */
    NO_JOINS_REQUIRED,

    /**
     * Derived hint: this query cannot be created with all inner joins in the main query
     */
    LEFT_OUTER_JOINS_REQUIRED,

    /**
     * The expression to be converted only deals with a single argName (attribute).
     * <p>
     * This hint can lead to simplistic queries, e.g. if it is a simple OR of match conditions, it often does not matter if the attribute is multi-row or not.
     */
    SINGLE_ATTRIBUTE,

    /**
     * All attributes involved in the query to be converted are mapped to the same table.
     * <p>
     * Knowing that only a single table is involved can help to write simple queries. E.g., if there are no IS UNKNOWNs involved and the related attributes are
     * not multi-row, a simple select will do.
     */
    SINGLE_TABLE,

    /**
     * All attributes involved in the query to be converted are mapped to the same table, and this table has all rows.
     * <p>
     * In this case, and if there is no multi-row-sensitivity, then we can query IS NULL safely.
     */
    SINGLE_TABLE_CONTAINING_ALL_ROWS,

    /**
     * The expression to be converted does not require any joins.
     * <p>
     * Simple conditions can be quite complex (AND/OR) but they only deal with a single table that contains all IDs, and none of their arguments is
     * multi-row-sensitive.
     */
    SIMPLE_CONDITION;

}
