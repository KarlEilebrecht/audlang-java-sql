//@formatter:off
/*
 * ConversionDirective
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
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;
import de.calamanari.adl.sql.config.TableNature;

/**
 * {@link ConversionDirective}s are {@link Flag}s that influence details of the query generation process.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum ConversionDirective implements Flag {

    /**
     * By default the converter will align {@link DefaultAdlType#DATE} to higher resolution types {@link DefaultAdlSqlType#SQL_TIMESTAMP},
     * {@link DefaultAdlSqlType#SQL_INTEGER} or {@link DefaultAdlSqlType#SQL_BIGINT} by turning the query into a range query.
     * <p>
     * This directive turns off this feature. <br>
     * Be aware that with this directive dates will be matched against timestamps assuming a 00:00:00 as the time portion.<br>
     * For example <code>2024-12-13</code> will no longer match (equal) the db-timestamp <code>2024-12-13 12:30:12</code>.
     */
    DISABLE_DATE_TIME_ALIGNMENT,

    /**
     * This disallows any CONTAINS to SQL-translation, no matter how the data binding is configured.
     */
    DISABLE_CONTAINS,

    /**
     * This disallows any LESS THAN or GREATER THAN to be translated, no matter how the data binding is configured
     */
    DISABLE_LESS_THAN_GREATER_THAN,

    /**
     * This disallows any reference matching, no matter how the data binding is configured.
     */
    DISABLE_REFERENCE_MATCHING,

    /**
     * This disallows the union of aliases as base query (always use any table with {@link TableNature#containsAllIds()} instead
     * <p>
     * UNIONs appear in a base selection if none of the aliases in the WITH-clause alone can serve as a base selection but the converter knows that a
     * combination of some aliases would work. On some systems the UNION-approach may lead to bad performance.
     * <p>
     * This directive is not required if your configuration contains a primary table because the primary table takes precedence over UNIONs as base selection.
     */
    DISABLE_UNION,

    /**
     * This directive enforces the usage of the primary table as start selection, no matter if the table is required in the current query or not.
     * <p>
     * Without this directive the query builder may decide not to query from the primary table if it is logically not required.<br>
     * Use this directive if you want to be sure the primary table will be involved in <i>every</i> query.<br>
     * For example, set this directive if you have an <code>IS_ACTIVE</code> column only in your base table and this shall be always considered.<br>
     * This directive can also be useful if the primary table is drastically smaller then some of the other tables.
     */
    ENFORCE_PRIMARY_TABLE;

}
