//@formatter:off
/*
 * AdlSqlColumn
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

import de.calamanari.adl.sql.AdlSqlType;

/**
 * Interface to for columns, introduced to handle {@link DataColumn}s and {@link FilterColumn}s in a common way.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface AdlSqlColumn extends Serializable {

    /**
     * @return name of the table this column is located
     */
    String tableName();

    /**
     * @return name of the db-column
     */
    String columnName();

    /**
     * @return type of the db-column
     */
    AdlSqlType columnType();

}
