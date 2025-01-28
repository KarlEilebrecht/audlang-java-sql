//@formatter:off
/*
 * TableMetaInfo
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

package de.calamanari.adl.sql.config;

import java.io.Serializable;
import java.util.List;

/**
 * This interface defines a few methods that returns extra infos (hints) about a table.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface TableMetaInfo extends Serializable {

    /**
     * @return name of the SQL-table
     */
    String tableName();

    /**
     * Returns the id-column name of the table for joining and result retrieval.
     * <p>
     * <ul>
     * <li>Every table in a setup must have <b>a single id-column</b>.</li>
     * <li>The ID-columns of all tables within a setup must be of <b>the same SQL-type</b>.</li>
     * <li>The concrete type of the ID-column is not relevant and thus not specified here.</li>
     * </ul>
     * 
     * @return name of the SQL-table's ID-column
     */
    String idColumnName();

    /**
     * @return information about the characteristics of this table
     */
    TableNature tableNature();

    /**
     * List of filter columns defined on the table, these filters will be applied whenever a query selects data from the table.
     */
    List<FilterColumn> tableFilters();

}
