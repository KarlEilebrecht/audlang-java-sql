//@formatter:off
/*
 * QueryParameter
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
import de.calamanari.adl.irl.MatchOperator;

/**
 * A {@link QueryParameter} encapsulates the action to correctly set a parameter on a prepared statement.
 * <p>
 * The contained value is already pre-validated, so this class <i>timely decouples</i> the moment when we align types and convert values from the moment where
 * we actually set the parameters on a statement.
 * <p>
 * Each {@link QueryParameter} has a unique <b>{@link #id()}</b>. This was introduced to keep the parameters of the PreparedStatement independent from the
 * parameter <i>order</i>. The issue with relying on the order is that it is difficult to guarantee strict left-to-right production of parameters while building
 * a statement.<br>
 * And even worse: if you later put together larger text blocks to form the final statement, it may be impossible to keep the connection between the position of
 * the question marks and the actual parameter to set. Thus, in an intermediate step we include parameter id-references <code>${<b>id</b>}</code> in the
 * expression's SQL-text.<br>
 * Eventually (once all parameters are known), these temporary placeholders will be replaced with question marks to set the parameters safely on the the
 * resulting PreparedStatement.
 * 
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface QueryParameter extends Serializable {

    /**
     * @return technical identifier for this parameter, must not be blank or contain any whitespace or curly braces or dollar signs (see
     *         {@link Character#isLetterOrDigit(char)}), no whitespace
     */
    String id();

    /**
     * @return temporary placeholder to be used in an SQL-expression template: <code>${<b>id</b>}</code>, see {@link QueryParameter}
     */
    default String createReference() {
        return "${" + id() + "}";
    }

    /**
     * @return a description of the target column's type
     */
    AdlSqlType adlSqlType();

    /**
     * @return the value to be set on a prepared statement, concrete type depends on {@link QueryParameterCreator} and requires a matching
     *         {@link QueryParameterApplicator}
     */
    Serializable value();

    /**
     * Returns the operator currently being translated. This contextual information may influence the formatter in case of
     * {@link #applyUnsafe(StringBuilder, int)}.
     * 
     * @return the operator this parameter is used with, by default {@link MatchOperator#EQUALS}, not null
     */
    MatchOperator operator();

    /**
     * Sets the value of this parameter on the statement at the given index.
     * <p>
     * This is the preferred (safe) methods to process the parameters of an SQL-statement without the risk of SQL-injection.
     * 
     * @param stmt
     * @param parameterIndex
     * @throws SQLException if the set operation on the prepared statement failed
     */
    default void apply(PreparedStatement stmt, int parameterIndex) throws SQLException {
        adlSqlType().getQueryParameterApplicator().apply(stmt, this, parameterIndex);
    }

    /**
     * Appends the value of the given parameter to an SQL script.
     * <p>
     * <b>Warning!</b> As the name of this method states, using this method to compose plain SQL-queries is <i>inherently unsafe and highly discouraged</i>.
     * <p>
     * Please also read: {@link QueryParameterApplicator#applyUnsafe(StringBuilder, QueryParameter, int)}
     * 
     * @param sb to append the parameter value
     * @param parameterIndex
     * @throws AdlFormattingException if the formatting failed
     */
    default void applyUnsafe(StringBuilder sb, int parameterIndex) {
        adlSqlType().getQueryParameterApplicator().applyUnsafe(sb, this, parameterIndex);
    }

}
