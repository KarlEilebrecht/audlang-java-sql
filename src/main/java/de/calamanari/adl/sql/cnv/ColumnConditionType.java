//@formatter:off
/*
 * ColumnConditionType
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

/**
 * Defines the type of the parameter set for a column, information for later expression setup
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum ColumnConditionType {

    /**
     * Identifies a match of two columns against each other
     */
    REFERENCE,

    /**
     * A filter condition for the main column resp. the left side of a reference match, also used for table filter conditions
     */
    FILTER_LEFT,

    /**
     * A filter condition for the reference column resp. the right side of a reference match
     */
    FILTER_RIGHT,

    /**
     * A single value comparison, parameter list contains one element
     */
    SINGLE,

    /**
     * Values for an IN-clause, list contains at least two elements
     */
    IN_CLAUSE,

    /**
     * Exactly one value with the <i>aligned</i> date after the specified one, to query <i>greater than or equals</i> rather than <i>greater than</i>
     */
    AFTER_TODAY,

    /**
     * List contains two date values, first is the main date (the user-specified date), second is the <i>aligned</i> date after for creating a range query
     * instead of an equals
     */
    DATE_RANGE;
}