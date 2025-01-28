//@formatter:off
/*
 * QueryParameterApplicator
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

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;

/**
 * Instances encapsulate the knowledge how to apply a parameter to a statement.
 * <p>
 * Special custom instances can help adapting the values to very specific databases and their types.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface QueryParameterApplicator extends Serializable {

    /**
     * Sets the value of the given parameter on the statement at the given index.
     * <p>
     * This is the preferred (safe) method to process the parameters of an SQL-statement without the risk of SQL-injection.
     * 
     * @param stmt
     * @param parameter
     * @param parameterIndex
     * @throws SQLException if the set operation on the prepared statement failed
     */
    void apply(PreparedStatement stmt, QueryParameter parameter, int parameterIndex) throws SQLException;

    /**
     * Appends the value of the given parameter to an SQL script.
     * <p>
     * <b>Warning!</b> As the name of this method states, using this method to compose plain SQL-queries is <i>inherently unsafe and highly discouraged</i>.
     * <br>
     * Even with escaping, there is always a remaining risk of SQL-injection, users might somehow manage to enter text that slips through the string escaping by
     * exploiting the behavior of a particular database/driver combination.
     * <p>
     * <b>Thus, it is strongly recommended to work with {@link PreparedStatement}s and their safe parameters.</b>
     * <p>
     * However, there may be reasons (e.g., no JDBC-driver available, comparison etc.) to use this method besides debugging and testing.<br>
     * If so, please consider the following points:
     * <ul>
     * <li>The database user for querying should have <b>minimal permissions</b>, e.g., <i>read-only</i>.</li>
     * <li>Can you avoid strings and fully rely on values of type integer, decimal, date etc.? If so, you are safe because this method intermediately converts
     * values of these types to Java data types before composing the final string to be included in the query.</li>
     * <li>Double-check the documentation of your database driver related to escaping.</li>
     * <li>Related code should be subject to auditing. This is especially important for custom {@link AdlSqlType}s and custom {@link ArgValueFormatter}s.</li>
     * </ul>
     * 
     * @param sb
     * @param parameter
     * @param parameterIndex
     * @throws AdlFormattingException if the formatting failed
     */
    void applyUnsafe(StringBuilder sb, QueryParameter parameter, int parameterIndex);

}
