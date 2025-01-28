//@formatter:off
/*
 * QueryParameterCreator
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

import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.irl.MatchOperator;

/**
 * A {@link QueryParameterCreator} creates a {@link QueryParameter} from an argument value of an expression.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface QueryParameterCreator extends Serializable {

    /**
     * Takes an argument from an expression with {@link AdlType} information as its input to create a {@link QueryParameter} to be applied to a
     * {@link PreparedStatement} at a later time.
     * 
     * @param id the temporary id of the new parameter, see {@link QueryParameter}, NOT NULL (use
     *            {@link #createParameter(ArgMetaInfo, String, MatchOperator, AdlSqlType)} instead)
     * @param argMetaInfo describes the source argument type
     * @param argValue value to be passed as parameter of a query
     * @param matchOperator context information: the operation the parameter is required for
     * @param adlSqlType requested type (target column)
     * @return parameter for PreparedStatement execution
     * @throws AdlFormattingException in case of incompatibilities and errors
     */
    QueryParameter createParameter(String id, ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType);

    /**
     * Takes an argument from an expression with {@link AdlType} information as its input to create a {@link QueryParameter} to be applied to a
     * {@link PreparedStatement} at a later time.
     * <p>
     * 
     * @param argMetaInfo describes the source argument type
     * @param argValue value to be passed as parameter of a query
     * @param matchOperator context information: the operation the parameter is required for
     * @param adlSqlType requested type (target column)
     * @return parameter for PreparedStatement execution
     * @throws AdlFormattingException in case of incompatibilities and errors
     */
    QueryParameter createParameter(ArgMetaInfo argMetaInfo, String argValue, MatchOperator matchOperator, AdlSqlType adlSqlType);

    /**
     * This method tells whether an argument of the given sourceType can be mapped to the targetType to create a valid SQL-parameter.<br>
     * It is meant for detecting configuration errors early.
     * <p>
     * <b>Important:</b> This method only tells that a conversion from the source type to the target type <i>might</i> be possible. It can still happen that a
     * particular value will be rejected at runtime.
     * <p>
     * Examples:
     * <ul>
     * <li>A string to integer conversion is only possible if the input value is the textual representation of an integer. This method should return
     * <b>true</b></li>
     * <li>It is principally impossible to turn a boolean into a date. This method should return <b>false</b>.</li>
     * </ul>
     * By definition, {@link AdlSqlType}s shall be <b>incompatible</b> (return false) to any {@link AdlSqlType} (even itself) to ensure SQL-types cannot be
     * abused for the logical data model.
     * 
     * @param sourceType argument type from the logical data model
     * @param targetType column type
     * @return true if the combination is supported by this creator, otherwise false
     */
    boolean isTypeCombinationSupported(AdlType sourceType, AdlSqlType targetType);

}
