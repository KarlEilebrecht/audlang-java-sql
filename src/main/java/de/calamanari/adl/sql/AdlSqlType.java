//@formatter:off
/*
 * AdlSqlType
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

import java.sql.Types;

import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.NativeTypeCaster;

/**
 * This sub-type of {@link AdlType} is especially made for SQL conversion.
 * <p>
 * If the given set of {@link DefaultAdlSqlType}s is insufficient or any special case handling is required, type behavior can be adjusted. By decorating types
 * (e.g., {@link #withQueryParameterApplicator(QueryParameterApplicator)}) implementors can adapt any type to any column type in the database.
 * <p>
 * Be careful when introducing entirely new types (besides {@link DefaultAdlSqlType} and their decorations) as the implementation of the other classes may
 * directly depend on concrete types. See also {@link DefaultQueryParameterCreator} and {@link DefaultQueryParameterApplicator}.
 * <p>
 * {@link AdlSqlType}s must not be used to configure the logical data model.
 * <p>
 * <b>Important:</b> The formatters ({@link #getFormatter()}) of these instances are actually <i>not used</i> to format the values of a query for execution
 * because for safety reasons we rely on PreparedStatements. Instead, the formatters of {@link AdlSqlType}s are meant for producing (functional) statements for
 * debugging and logging purposes. <br>
 * See also: {@link QueryParameterApplicator#applyUnsafe(StringBuilder, de.calamanari.adl.sql.QueryParameter, int)}.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface AdlSqlType extends AdlType {

    /**
     * @return this is the underlying SQL-type for this type, see {@link Types} (mainly for reference and debugging)
     */
    int getJavaSqlType();

    /**
     * @return the component that can convert a given expression argument of any given {@link AdlType} into an SQL-query parameter compliant to <b>this</b>
     *         type. NOT NULL.
     */
    QueryParameterCreator getQueryParameterCreator();

    /**
     * @return the component that can set a parameter (sql-value) on a statement, NOT NULL
     */
    QueryParameterApplicator getQueryParameterApplicator();

    @Override
    default AdlSqlType getBaseType() {
        return this;
    }

    @Override
    default AdlSqlType withNativeTypeCaster(String name, NativeTypeCaster nativeTypeCaster) {
        if (nativeTypeCaster == null) {
            return this;
        }
        return new AdlSqlTypeDecorator(name, this, null, nativeTypeCaster, null, null);
    }

    @Override
    default AdlSqlType withNativeTypeCaster(NativeTypeCaster nativeTypeCaster) {
        return withNativeTypeCaster(null, nativeTypeCaster);
    }

    @Override
    default AdlSqlType withFormatter(String name, ArgValueFormatter formatter) {
        if (formatter == null) {
            return this;
        }
        return new AdlSqlTypeDecorator(name, this, formatter, null, null, null);
    }

    @Override
    default AdlSqlType withFormatter(ArgValueFormatter formatter) {
        return withFormatter(null, formatter);
    }

    /**
     * Allows adding a new {@link QueryParameterCreator} to an existing type to refine the behavior of the composed {@link AdlSqlType}
     * <p>
     * Specifying a custom name may be useful if you know that the effectively <i>identical</i> type setup would otherwise occur multiple times with different
     * names (edge-case). Usually, the auto-generated wrapper names should be preferred.
     * 
     * @param name unique name (or null to auto-generate a unique one)
     * @param queryParameterCreator
     * @return composed type or <i>this instance</i> if the provided formatter was null
     */
    default AdlSqlType withQueryParameterCreator(String name, QueryParameterCreator queryParameterCreator) {
        if (queryParameterCreator == null) {
            return this;
        }
        return new AdlSqlTypeDecorator(name, this, null, null, queryParameterCreator, null);
    }

    /**
     * Allows adding a new {@link QueryParameterCreator} to an existing type to refine the behavior of the composed {@link AdlSqlType}
     * 
     * @param queryParameterCreator
     * @return composed type
     */
    default AdlSqlType withQueryParameterCreator(QueryParameterCreator queryParameterCreator) {
        return withQueryParameterCreator(null, queryParameterCreator);
    }

    /**
     * Allows adding a new {@link QueryParameterApplicator} to an existing type to refine the behavior of the composed {@link AdlSqlType}
     * <p>
     * Specifying a custom name may be useful if you know that the effectively <i>identical</i> type setup would otherwise occur multiple times with different
     * names (edge-case). Usually, the auto-generated wrapper names should be preferred.
     * 
     * @param name unique name (or null to auto-generate a unique one)
     * @param queryParameterApplicator
     * @return composed type or <i>this instance</i> if the provided formatter was null
     */
    default AdlSqlType withQueryParameterApplicator(String name, QueryParameterApplicator queryParameterApplicator) {
        if (queryParameterApplicator == null) {
            return this;
        }
        return new AdlSqlTypeDecorator(name, this, null, null, null, queryParameterApplicator);
    }

    /**
     * Allows adding a new {@link QueryParameterApplicator} to an existing type to refine the behavior of the composed {@link AdlSqlType}
     * 
     * @param queryParameterApplicator
     * @return composed type
     */
    default AdlSqlType withQueryParameterApplicator(QueryParameterApplicator queryParameterApplicator) {
        return withQueryParameterApplicator(null, queryParameterApplicator);
    }

    /**
     * Tells whether there is a chance that a value of the given argument type can be translated into an sql paramter of this type.
     * <p>
     * <b>Important:</b> This method only tells that a conversion from the given type to this type <i>might</i> be possible. It can still happen that a
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
     * @param type to be tested for compatibility with this type
     * @return true if there are any values of the given type that can be converted into this type
     */
    default boolean isCompatibleWith(AdlType type) {
        return this.getQueryParameterCreator().isTypeCombinationSupported(type, this);
    }

}
