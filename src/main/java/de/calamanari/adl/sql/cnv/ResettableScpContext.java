//@formatter:off
/*
 * ResettableScpContext
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.calamanari.adl.Flag;
import de.calamanari.adl.FormatStyle;
import de.calamanari.adl.sql.QueryParameter;
import de.calamanari.adl.sql.QueryType;
import de.calamanari.adl.sql.SqlFormatConstants;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DataTableConfig;
import de.calamanari.adl.sql.config.TableMetaInfo;

/**
 * Implementation of a resettable {@link SqlConversionProcessContext}.
 * <p>
 * This class covers the state of an {@link AbstractSqlExpressionConverter} instance throughout its lifetime. <br>
 * It allows to reset this state to an initial configuration before each conversion run, so multiple runs won't interfere.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class ResettableScpContext implements SqlConversionProcessContext {

    /**
     * The data binding settings for the current conversion
     */
    private final DataBinding dataBinding;

    /**
     * Global variables to be set initially for each conversion run (construction time of converter)
     */
    private final Map<String, Serializable> globalVariablesTemplate;

    /**
     * Global variables of the current conversion run
     */
    private final Map<String, Serializable> globalVariables = new HashMap<>();

    /**
     * Flags to be set initially for each conversion run (construction time of converter)
     */
    private final Set<Flag> globalFlagsTemplate;

    /**
     * Flags for the current run
     */
    private final Set<Flag> globalFlags = new HashSet<>();

    /**
     * All parameters registered during the current conversion run
     */
    private final List<QueryParameter> registeredParameters = new ArrayList<>();

    /**
     * Alias helper, set by the converter upon initialization
     */
    private AliasHelper aliasHelper = null;

    /**
     * Expression helper, set by the converter during preparation
     */
    private CoreExpressionSqlHelper expressionHelper = null;

    /**
     * The id-column used in the current query to join aliases and to return ids, by default {@link SqlFormatConstants#DEFAULT_ID_COLUMN_NAME}
     */
    private String idColumnName = SqlFormatConstants.DEFAULT_ID_COLUMN_NAME;

    /**
     * Tells what kind of query is under construction
     */
    private QueryType queryType = QueryType.SELECT_DISTINCT_ID_ORDERED;

    /**
     * augmentation listener of the converter
     */
    private SqlAugmentationListener augmentationListener = SqlAugmentationListener.none();

    /**
     * Global WHERE-clause-builder, subsequently updated during construction
     */
    private final StringBuilder whereClause = new StringBuilder();

    /**
     * The factory creates and manages (tracks) all the conditions, avoiding duplicates
     */
    private MatchConditionFactory conditionFactory = null;

    /**
     * Decides whether the converter adds line breaks and indentation or not, by default we use pretty-printing for better readability
     */
    private FormatStyle style = FormatStyle.PRETTY_PRINT;

    /**
     * reference tracking for the global where clause, helps to understand which aliases are really used and which not (tables instead)
     */
    private final Set<ExpressionAlias> aliasesInWhereClause = new HashSet<>();

    /**
     * reference tracking for the global where clause, helps to understand which table names are in use, e.g., to decide whether the main query needs to be
     * renamed or not
     */
    private final Set<TableMetaInfo> tablesInWhereClause = new HashSet<>();

    /**
     * Main table, either the primary table or one of the tables referenced in the query
     */
    private TableMetaInfo mainTable = null;

    /**
     * @param dataBinding to be set initially for each conversion run
     * @param flagsTemplate to be set initially for each conversion run
     */
    public ResettableScpContext(DataBinding dataBinding, Map<String, Serializable> globalVariablesTemplate, Set<Flag> flagsTemplate) {
        this.globalVariablesTemplate = globalVariablesTemplate == null ? new HashMap<>() : globalVariablesTemplate;
        this.globalFlagsTemplate = flagsTemplate = flagsTemplate == null ? new HashSet<>() : flagsTemplate;
        this.dataBinding = dataBinding;
        this.reset();
    }

    /**
     * Initializes this context, so its state has the initially configured flags and variables again
     * <p>
     * Because we assume them to be rather stable once configured, the following properties won't be reset by this method:
     * <ul>
     * <li>{@link #setStyle(FormatStyle)}</li>
     * <li>{@link #idColumnName}</li>
     * <li>{@link #setQueryType(QueryType)}</li>
     * </ul>
     */
    public void reset() {
        this.globalVariables.clear();
        this.globalVariables.putAll(globalVariablesTemplate);
        this.globalFlags.clear();
        this.globalFlags.addAll(globalFlagsTemplate);
        this.registeredParameters.clear();
        this.mainTable = null;
        this.tablesInWhereClause.clear();
        this.aliasesInWhereClause.clear();
        this.conditionFactory = null;
        this.whereClause.setLength(0);
        this.expressionHelper = null;
        this.aliasHelper = null;
        this.augmentationListener.init();
    }

    @Override
    public Map<String, Serializable> getGlobalVariables() {
        return this.globalVariables;
    }

    @Override
    public Set<Flag> getGlobalFlags() {
        return this.globalFlags;
    }

    /**
     * @return the variables specified at construction time (this config acts as a template during {@link #reset()})
     */
    public Map<String, Serializable> getGlobalVariablesTemplate() {
        return globalVariablesTemplate;
    }

    /**
     * @return the flags specified at construction time (this config acts as a template during {@link #reset()})
     */
    public Set<Flag> getGlobalFlagsTemplate() {
        return globalFlagsTemplate;
    }

    @Override
    public DataBinding getDataBinding() {
        return this.dataBinding;
    }

    @Override
    public void registerParameter(QueryParameter parameter) {
        this.registeredParameters.add(parameter);
    }

    @Override
    public List<QueryParameter> getRegisteredParameters() {
        return this.registeredParameters;
    }

    @Override
    public AliasHelper getAliasHelper() {
        return aliasHelper;
    }

    /**
     * @param aliasHelper the converter's alias helper to create and maintain aliases
     */
    public void setAliasHelper(AliasHelper aliasHelper) {
        this.aliasHelper = aliasHelper;
    }

    @Override
    public CoreExpressionSqlHelper getExpressionHelper() {
        return expressionHelper;
    }

    /**
     * @param expressionHelper the converter's expression helper to work with expressions and sub-expressions
     */
    public void setExpressionHelper(CoreExpressionSqlHelper expressionHelper) {
        this.expressionHelper = expressionHelper;
    }

    /**
     * @return configured name of the main ID-column (when selecting IDs), defaults to {@link SqlFormatConstants#DEFAULT_ID_COLUMN_NAME}
     */
    @Override
    public String getIdColumnName() {
        return idColumnName;
    }

    /**
     * @param idColumnName name of the main ID-column (when selecting IDs), defaults to {@link SqlFormatConstants#DEFAULT_ID_COLUMN_NAME}
     */
    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    /**
     * @param queryType type of the query to be created, defaults to {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     */
    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    @Override
    public TableMetaInfo getMainTable() {
        return this.mainTable;
    }

    /**
     * The main table is the table to start the query with. It is initialized with {@link DataTableConfig#primaryTable()}.
     * <p>
     * Depending on the conversion strategy it can be reasonable to override it with a different table, for example if only a single table is involved without
     * any complex joins.
     * 
     * @param mainTable the table to prefer to start the query or null to enforce another query start strategy
     */
    public void setMainTable(TableMetaInfo mainTable) {
        this.mainTable = mainTable;
    }

    @Override
    public MatchConditionFactory getConditionFactory() {
        return conditionFactory;
    }

    /**
     * @param conditionFactory the factory for creating match conditions on target columns, <b>set by the converter per conversion run</b>
     */
    public void setConditionFactory(MatchConditionFactory conditionFactory) {
        this.conditionFactory = conditionFactory;
    }

    @Override
    public FormatStyle getStyle() {
        return style;
    }

    /**
     * @param style formatting style (inline or multi-line), default is {@link FormatStyle#PRETTY_PRINT}
     */
    public void setStyle(FormatStyle style) {
        this.style = style;
    }

    /**
     * @return the configured augmentation listener or {@link SqlAugmentationListener#none()}
     */
    public SqlAugmentationListener getAugmentationListener() {
        return augmentationListener;
    }

    /**
     * Sets the augmentation listener for this instance, default is {@link SqlAugmentationListener#none()}
     * 
     * @param augmentationListener
     */
    public void setAugmentationListener(SqlAugmentationListener augmentationListener) {
        this.augmentationListener = augmentationListener;
    }

    @Override
    public StringBuilder getWhereClause() {
        return whereClause;
    }

    @Override
    public Set<ExpressionAlias> getAliasesInWhereClause() {
        return aliasesInWhereClause;
    }

    @Override
    public Set<TableMetaInfo> getTablesInWhereClause() {
        return tablesInWhereClause;
    }

}