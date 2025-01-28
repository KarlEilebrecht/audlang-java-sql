//@formatter:off
/*
 * SqlContainsPolicy
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
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import de.calamanari.adl.irl.MatchExpression;

/**
 * A concrete {@link SqlContainsPolicy} formats an SQL LIKE instruction (or anything similar) to realize a CONTAINS condition.
 * <p>
 * Unfortunately, this cannot be done database-independently because major databases support different syntax.<br>
 * There can even be subtle differences related to the JDBC-driver. Some databases work nicely with like <i>patterns</i> as arguments to a prepared statement,
 * other will escape the percentage signs and this way break the pattern.<br>
 * This makes it difficult to format a LIKE in a generic way.
 * <p>
 * The {@link SqlContainsPolicy} takes care of both problems, the search pattern preparation and the SQL-syntax to express the LIKE.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface SqlContainsPolicy extends Serializable {

    /**
     * @return unique name (identifier) of this policy
     */
    String name();

    /**
     * This method allows to modify the snippet to conform to a specific database.
     * <p>
     * E.g., given as a prepared statement parameter one would assume that further percentage signs or underscores in the snippet would be escaped by the
     * driver. However, some databases interpret them as further wildcards.
     * <p>
     * This method allows intercepting the snippet preparation (e.g., remove such problematic characters).
     * <p>
     * <b>Important:</b><br>
     * The return value of this method is still a <i>text snippet</i> and not yet the SQL-pattern to be included in the statement.<br>
     * So, typically, you should NOT surround the text here with percentage signs but. Otherwise the driver will most likely escape them and this way destroy
     * the search pattern.
     * 
     * @param value the text value (snippet) of a CONTAINS {@link MatchExpression}
     * @return cleaned value
     */
    String prepareSearchSnippet(String value);

    /**
     * Returns the SQL-instruction to compare a column against a pattern.
     * <p>
     * Here are some examples:
     * <ul>
     * <li><code>COL123 LIKE '%' || ${PATTERN_1011} || '%'</code> should work well with Oracle, SQLite, PostgreSQL</li>
     * <li><code>COL123 LIKE CONCAT('%', ${PATTERN_1011}, '%')</code> should do with MySQL</li>
     * <li><code>COL123 LIKE '%' + ${PATTERN_1011} + '%'</code> should work with SQL Server</li>
     * <li><code>CHARINDEX(${PATTERN_1011}, COL123, 0) > 0</code> may also work with SQL Server</li>
     * </ul>
     * <b>Notes:</b>
     * <ul>
     * <li>The percentage signs are not part of the prepared statement parameter. This will ensure they do their job, no matter if the driver escapes percentage
     * signs in prepared statement parameters or not.</li>
     * <li><code>${PATTERN_1011}</code> is a <i>placeholder</i> without any meaning and not to be included in quotes here.</li>
     * <li>The best way to realize CONTAINS depends on your DB, and there might be multiple roads to success.</li>
     * </ul>
     * 
     * @param columnName this is the SQL column or alias to perform the CONTAINS/LIKE operation on, e.g. <code>COL123</code>
     * @param patternParameter placeholder (later argument to prepared statement) that will hold the pattern (e.g., <code>${PATTERN_1011}</code>
     */
    String createInstruction(String columnName, String patternParameter);

    /**
     * This method derives a new policy from the given one by replacing the preparation strategy with a new one.
     * 
     * @see #prepareSearchSnippet(String)
     * @param name identifier of the policy
     * @param prepareSearchSnippetFunction
     * @return new policy instance
     */
    default SqlContainsPolicy withPreparatorFunction(String name, PreparatorFunction prepareSearchSnippetFunction) {
        if (prepareSearchSnippetFunction == null) {
            throw new IllegalArgumentException("Cannot decorate with null-preparator, please use PreparatorFunction.none() instead.");
        }
        return new SqlContainsPolicyDecorator(name, this, prepareSearchSnippetFunction, null);
    }

    /**
     * This method derives a new policy from the given one by replacing the preparation strategy with a new one with an auto-generated name.
     * 
     * @see #prepareSearchSnippet(String)
     * @param prepareSearchSnippetFunction
     * @return new policy instance
     */
    default SqlContainsPolicy withPreparatorFunction(PreparatorFunction prepareSearchSnippetFunction) {
        if (prepareSearchSnippetFunction == null) {
            throw new IllegalArgumentException("Cannot decorate with null-preparator, please use PreparatorFunction.none() instead.");
        }
        return new SqlContainsPolicyDecorator(null, this, prepareSearchSnippetFunction, null);
    }

    /**
     * This method derives a new policy from the given one by replacing the creation strategy with a new one.
     * 
     * @see #createInstruction(String, String)
     * @param name identifier of the policy
     * @param createInstructionFunction
     * @return new policy instance
     */
    default SqlContainsPolicy withCreatorFunction(String name, CreatorFunction createInstructionFunction) {
        if (createInstructionFunction == null) {
            throw new IllegalArgumentException("Argument createInstructionFunction must not be null.");
        }
        return new SqlContainsPolicyDecorator(name, this, null, createInstructionFunction);
    }

    /**
     * This method derives a new policy from the given one by replacing the creation strategy with a new one with an auto-generated name.
     * 
     * @see #createInstruction(String, String)
     * @param createInstructionFunction
     * @return new policy instance
     */
    default SqlContainsPolicy withCreatorFunction(CreatorFunction createInstructionFunction) {
        if (createInstructionFunction == null) {
            throw new IllegalArgumentException("Argument createInstructionFunction must not be null.");
        }
        return new SqlContainsPolicyDecorator(null, this, null, createInstructionFunction);
    }

    /**
     * Tagging interface to ensure functions are serializable
     */
    @FunctionalInterface
    public interface PreparatorFunction extends UnaryOperator<String>, Serializable {

        /**
         * This function does not change the text snippet at all. Use this if your JDBC-driver correctly escapes additional wildcards entered by a user.
         * 
         * @return the input without any adjustments
         */
        public static PreparatorFunction none() {
            return s -> s;
        }

    }

    /**
     * Tagging interface to ensure functions are serializable
     */
    @FunctionalInterface
    public interface CreatorFunction extends BinaryOperator<String>, Serializable {
        // tagging
    }

}
