//@formatter:off
/*
 * QueryType
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

/**
 * Identifies the kind of query
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum QueryType {

    /**
     * Returns the matching unique IDs without special sorting
     */
    SELECT_DISTINCT_ID,

    /**
     * Returns the matching unique IDs in ascending order
     */
    SELECT_DISTINCT_ID_ORDERED,

    /**
     * A query that returns the number of unique matching rows
     */
    SELECT_DISTINCT_COUNT;

}
