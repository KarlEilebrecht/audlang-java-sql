//@formatter:off
/*
 * SqlConversionProcessContext
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

import java.util.List;
import java.util.Set;

import de.calamanari.adl.FormatStyle;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.sql.QueryParameter;
import de.calamanari.adl.sql.QueryType;
import de.calamanari.adl.sql.SqlFormatConstants;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DataTableConfig;
import de.calamanari.adl.sql.config.TableMetaInfo;

/**
 * This interface provides access to the variables, flags and the binding in the context of the current conversion.<br>
 * It also allows the registration of new parameters created during a conversion run.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface SqlConversionProcessContext extends ProcessContext {

    /**
     * @return the configured data binding of the converter
     */
    DataBinding getDataBinding();

    /**
     * Registers a query parameter of the current query.
     * <p>
     * Only registered parameters can later be applied to the final script.
     * 
     * @param parameter
     */
    void registerParameter(QueryParameter parameter);

    /**
     * @see #registerParameter(QueryParameter)
     * @return list of all parameters registered during the current conversion run
     */
    List<QueryParameter> getRegisteredParameters();

    /**
     * The main table is the table to start the query with. It is initialized with {@link DataTableConfig#primaryTable()}.
     * <p>
     * Depending on the conversion strategy it can be reasonable to override it with a different table, for example if only a single table is involved without
     * any complex joins.
     * 
     * @return the main table or null if not configured
     */
    TableMetaInfo getMainTable();

    /**
     * @return id-column name for joining and the returned result, defaults to {@link SqlFormatConstants#DEFAULT_ID_COLUMN_NAME}
     */
    String getIdColumnName();

    /**
     * Resolves the main id-column for joining further tables.
     * <p>
     * The effective name of the ID-column can vary if the tables have custom-IDs and there are aliases or a renamed base selection. <br>
     * This method simply tests if there is a main table (then the result is the column name) or not (then the ID is {@link #getIdColumnName()}.
     * <p>
     * <b>Note:</b> This is the raw name, of course finally the returned ID-column name is always {@link #getIdColumnName()} (renamed if required).
     * 
     * @return effective main id for joining
     */
    default String getMainIdColumnName() {
        String res = getIdColumnName();
        if (getMainTable() != null) {
            res = getMainTable().idColumnName();
        }
        return res;
    }

    /**
     * @return the converter's alias helper to create and maintain aliases
     */
    AliasHelper getAliasHelper();

    /**
     * @return the converter's expression helper to work with expressions and sub-expressions
     */
    CoreExpressionSqlHelper getExpressionHelper();

    /**
     * @return the factory for creating match conditions on target columns, set by the converter per conversion run
     */
    MatchConditionFactory getConditionFactory();

    /**
     * @return type of the query to be created, defaults to {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     */
    QueryType getQueryType();

    /**
     * @return configured formatting style (inline or multi-line)
     */
    FormatStyle getStyle();

    /**
     * @return WHERE-clause builder of the converter
     */
    StringBuilder getWhereClause();

    /**
     * @return list of all aliases referenced in the where-clause, mutable
     */
    Set<ExpressionAlias> getAliasesInWhereClause();

    /**
     * @return list of all tables referenced in the where-clause, mutable
     */
    Set<TableMetaInfo> getTablesInWhereClause();

}
