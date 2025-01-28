//@formatter:off
/*
 * SqlAugmentationListener
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

import de.calamanari.adl.sql.SqlFormatConstants;

/**
 * A concrete {@link SqlAugmentationListener} observes the creation of an SQL-script with the opportunity to apply changes/extensions.
 * <p>
 * Several callback methods allow influencing certain aspects of the SQL without changing the overall structure.
 * <p>
 * Examples:
 * <ul>
 * <li>Add a header or footer to each SQL script.</li>
 * <li>Change the join type for a particular critical table combination.</li>
 * <li>Extend the SQL so that it returns data for this selection of IDs instead.</li>
 * <li>Apply native hints.</li>
 * </ul>
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface SqlAugmentationListener {

    /**
     * Called before starting the SQL-script
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     */
    default void handleBeforeScript(StringBuilder sb, SqlConversionProcessContext ctx) {
        // by default no action
    }

    /**
     * Called before the main SELECT line
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     * @param withClausePresent true if there is already a WITH-section
     */
    default void handleBeforeMainStatement(StringBuilder sb, SqlConversionProcessContext ctx, boolean withClausePresent) {
        // by default no action
    }

    /**
     * The default implementation just appends the suggested join type e.g. <code>"INNER JOIN"</code> to the builder.
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     * @param tableOrAliasFrom name of the table or alias we are coming from
     * @param tableOrAliasTo name of the table or alias we are joining
     * @param suggestedJoinType e.g. <code>"INNER JOIN"</code> or <code>"LEFT OUTER JOIN"</code>
     */
    default void handleAppendJoinType(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo,
            String suggestedJoinType) {
        sb.append(suggestedJoinType);
    }

    /**
     * Called right before printing the text "ON"
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     * @param tableOrAliasFrom source table or alias
     * @param tableOrAliasTo table or alias to be joined
     */
    default void handleBeforeOnClause(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo) {
        // by default no action
    }

    /**
     * Called before printing an ON-condition, means right after the text "ON"
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     * @param tableOrAliasFrom source table or alias
     * @param tableOrAliasTo table or alias to be joined
     */
    default void handleBeforeOnConditions(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo) {
        // by default no action
    }

    /**
     * Called after finishing the ON-part of a join
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     * @param tableOrAliasFrom source table or alias
     * @param tableOrAliasTo table or alias to be joined
     */
    default void handleAfterOnConditions(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo) {
        // by default no action
    }

    /**
     * Called right after appending {@link SqlFormatConstants#SELECT} to the main selection part
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     */
    default void handleAfterMainSelect(StringBuilder sb, SqlConversionProcessContext ctx) {
        // by default no action
    }

    /**
     * Called right after appending {@link SqlFormatConstants#SELECT} to selection part of a WITH-element
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     * @param tables the tables involved in the select
     */
    default void handleAfterWithSelect(StringBuilder sb, SqlConversionProcessContext ctx, String... tables) {
        // by default no action
    }

    /**
     * Called after the SQL-script is complete (after finishing the main WHERE-clause)
     * 
     * @param sb SQL-script string builder
     * @param ctx process context providing access to the state and utilities of the converter
     */
    default void handleAfterScript(StringBuilder sb, SqlConversionProcessContext ctx) {
        // by default no action
    }

    /**
     * This method is called to initialize/reset the listener to avoid mixing state related to different conversion runs.
     */
    default void init() {
        // no-op
    }

    /**
     * Returns a dummy instance
     * 
     * @return listener instance that does not augment anything
     */
    public static SqlAugmentationListener none() {
        return new SqlAugmentationListener() {
            // dummy
        };
    }

}
