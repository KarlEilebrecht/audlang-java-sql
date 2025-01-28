//@formatter:off
/*
 * H2Mappings
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

import static de.calamanari.adl.cnv.tps.DefaultAdlType.BOOL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DATE;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DECIMAL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.INTEGER;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.STRING;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIGINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BOOLEAN;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DATE;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DECIMAL;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_FLOAT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_INTEGER;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_NUMERIC;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_SMALLINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TIMESTAMP;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TINYINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;

import java.util.ArrayList;
import java.util.List;

import de.calamanari.adl.cnv.tps.AdlType;
import de.calamanari.adl.cnv.tps.NativeTypeCaster;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DefaultSqlContainsPolicy;
import de.calamanari.adl.sql.config.MultiTableConfig;
import de.calamanari.adl.sql.config.SingleTableConfig;
import de.calamanari.adl.sql.config.TableNature;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class H2TestBindings {

    public static final NativeTypeCaster H2_VARCHAR_CASTER = new NativeTypeCaster() {

        private static final long serialVersionUID = 8734138751699714083L;

        @Override
        public String formatNativeTypeCast(String argName, String nativeFieldName, AdlType argType, AdlType requestedArgType) {
            if (requestedArgType.getBaseType() == INTEGER) {
                return "CAST(" + nativeFieldName + " AS INT)";
            }
            else if (requestedArgType.getBaseType() == DECIMAL) {
                return "CAST(" + nativeFieldName + " AS DECIMAL(10,2))";
            }
            else if (requestedArgType.getBaseType() == DATE) {
                // this covers both cases: '2024-09-23' and '2024-09-23 17:12:23'
                // alternatively the string comparison would also do the job
                return "CAST(CAST(" + nativeFieldName + " AS TIMESTAMP) AS DATE)";
            }
            else if (requestedArgType.getBaseType() == BOOL) {
                return "CASE WHEN " + nativeFieldName + " = 'Y' THEN TRUE WHEN " + nativeFieldName + " = 'N' THEN FALSE ELSE NULL END";
            }
            else {
                return nativeFieldName;
            }
        }
    };

    // @formatter:off
    public static final SingleTableConfig BASE_TABLE = SingleTableConfig.forTable("T_BASE")
                                                            .asPrimaryTable()
                                                            .withUniqueIds()
                                                            .idColumn("ID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("provider", STRING)
                                                            .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("home-country", STRING)
                                                            .dataColumn("CITY", SQL_VARCHAR).mappedToArgName("home-city", STRING)
                                                            .dataColumn("DEM_CODE", SQL_INTEGER).mappedToArgName("demCode", INTEGER)
                                                            .dataColumn("GENDER", SQL_VARCHAR).mappedToArgName("gender", STRING)
                                                            .dataColumn("OM_SCORE", SQL_FLOAT).mappedToArgName("omScore", DECIMAL)
                                                            .dataColumn("UPD_TIME", SQL_TIMESTAMP).mappedToArgName("upd1", DATE)
                                                            .dataColumn("UPD_DATE", SQL_DATE).mappedToArgName("upd2", DATE)
                                                            .dataColumn("TNT_CODE", SQL_TINYINT).mappedToArgName("tntCode", INTEGER)
                                                            .dataColumn("B_STATE", SQL_BOOLEAN).mappedToArgName("bState", BOOL)
                                                            .dataColumn("S_CODE", SQL_SMALLINT).mappedToArgName("sCode", INTEGER)
                                                            .dataColumn("BI_CODE", SQL_BIGINT).mappedToArgName("biCode", INTEGER)
                                                            .dataColumn("N_CODE", SQL_NUMERIC).mappedToArgName("nCode", DECIMAL)
                                                       .get();            

    public static final SingleTableConfig PROVIDER_ALWAYS_KNOWN_TABLE = SingleTableConfig.forTable("T_PROV")
                                                            .asPrimaryTable()
                                                            .withUniqueIds()
                                                            .idColumn("ID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("provider", STRING)
                                                                .alwaysKnown()
                                                       .get();            

    public static final SingleTableConfig FACT_TABLE = SingleTableConfig.forTable("T_FACTS") 
                                                            .idColumn("UID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("fact.provider", STRING)
                                                            .dataColumn("F_VALUE_STR", SQL_VARCHAR)
                                                                .autoMapped(s-> (s.startsWith("fact.") && s.endsWith(".str") && s.length() > 9) ? s.substring(5, s.length()-4) : null, STRING)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "${argName.local}")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "string")
                                                            .dataColumn("F_VALUE_INT", SQL_BIGINT)
                                                                .autoMapped(s-> (s.startsWith("fact.") && s.endsWith(".int") && s.length() > 9) ? s.substring(5, s.length()-4) : null, INTEGER)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "${argName.local}")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "int")
                                                            .dataColumn("F_VALUE_DEC", SQL_DECIMAL)
                                                                .autoMapped(s-> (s.startsWith("fact.") && s.endsWith(".dec") && s.length() > 9) ? s.substring(5, s.length()-4) : null, DECIMAL)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "${argName.local}")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "decimal")
                                                            .dataColumn("F_VALUE_FLG", SQL_BOOLEAN)
                                                                .autoMapped(s-> (s.startsWith("fact.") && s.endsWith(".flg") && s.length() > 9) ? s.substring(5, s.length()-4) : null, BOOL)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "${argName.local}")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "flag")
                                                            .dataColumn("F_VALUE_DT", SQL_DATE)
                                                                .autoMapped(s-> (s.startsWith("fact.") && s.endsWith(".dt") && s.length() > 8) ? s.substring(5, s.length()-3) : null, DATE)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "${argName.local}")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "date")
                                                            .dataColumn("F_VALUE_TS", SQL_TIMESTAMP)
                                                                .autoMapped(s-> (s.startsWith("fact.") && s.endsWith(".ts") && s.length() > 8) ? s.substring(5, s.length()-3) : null, DATE)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "${argName.local}")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "timestamp")
                                                       .get();
    
    public static final SingleTableConfig SURVEY_TABLE = SingleTableConfig.forTable("T_SURVEY").filteredBy("TENANT", SQL_INTEGER, "${tenant}") 
                                                            .idColumn("PID")
                                                            .dataColumn("A_STR", SQL_VARCHAR)
                                                                .autoMapped(s-> (s.startsWith("q.") && s.endsWith(".str") && s.length() > 6) ? s.substring(2, s.length()-4) : null, STRING)
                                                                .multiRow()
                                                                .filteredBy("Q_KEY", SQL_VARCHAR, "${argName.local}")
                                                            .dataColumn("A_INT", SQL_BIGINT)
                                                                .autoMapped(s-> (s.startsWith("q.") && s.endsWith(".int") && s.length() > 6) ? s.substring(2, s.length()-4) : null, INTEGER)
                                                                .multiRow()
                                                                .filteredBy("Q_KEY", SQL_VARCHAR, "${argName.local}")
                                                            .dataColumn("A_YESNO", SQL_BOOLEAN)
                                                                .autoMapped(s-> (s.startsWith("q.") && s.endsWith(".flg") && s.length() > 6) ? s.substring(2, s.length()-4) : null, BOOL)
                                                                .multiRow()
                                                                .filteredBy("Q_KEY", SQL_VARCHAR, "${argName.local}")
                                                       .get();
    // @formatter:on

    public static final DataBinding DEFAULT;
    static {
        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable(BASE_TABLE)
                                                  .withTable(FACT_TABLE)
                                                  .withTable(SURVEY_TABLE)
                                                  .withTable("T_POSDATA")
                                                           .idColumn("UID")
                                                           .dataColumn("INV_DATE", SQL_DATE).mappedToArgName("pos.date", DATE)
                                                           .dataColumn("DESCRIPTION", SQL_VARCHAR).mappedToArgName("pos.name", STRING)
                                                           .dataColumn("QUANTITY", SQL_INTEGER).mappedToArgName("pos.quantity", INTEGER)
                                                           .dataColumn("UNIT_PRICE", SQL_DECIMAL).mappedToArgName("pos.unitPrice", DECIMAL)
                                                           .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("pos.country", STRING)
                                                  .withTable("T_FLAT_TXT")
                                                           .idColumn("UID")
                                                           .dataColumn("C_VALUE", SQL_VARCHAR).mappedToArgName("sports", STRING)
                                                               .multiRow()
                                                               .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                           .dataColumn("C_VALUE", SQL_VARCHAR).mappedToArgName("hobbies", STRING)
                                                               .multiRow()
                                                               .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                           .dataColumn("C_VALUE", SQL_INTEGER.withNativeTypeCaster(H2_VARCHAR_CASTER))
                                                               .mappedToArgName("sizeCM", INTEGER)
                                                               .multiRow()
                                                               .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                           .dataColumn("C_VALUE", SQL_DECIMAL.withNativeTypeCaster(H2_VARCHAR_CASTER))
                                                               .mappedToArgName("bodyTempCelsius", DECIMAL)
                                                               .multiRow()
                                                               .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                           .dataColumn("C_VALUE", SQL_BOOLEAN.withNativeTypeCaster(H2_VARCHAR_CASTER))
                                                               .mappedToArgName("clubMember", BOOL)
                                                               .multiRow()
                                                               .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                           .dataColumn("C_VALUE", SQL_DATE.withNativeTypeCaster(H2_VARCHAR_CASTER))
                                                               .mappedToArgName("anniverseryDate", DATE)
                                                               .multiRow()
                                                               .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                  .get();
        // @formatter:on
        DEFAULT = new DataBinding(config, DefaultSqlContainsPolicy.SQL92);

    }

    public static final DataBinding DEFAULT_NO_PRIMARY_TABLE;
    static {
        MultiTableConfig dataTableConfig = (MultiTableConfig) DEFAULT.dataTableConfig();
        List<SingleTableConfig> tableConfigs = new ArrayList<>(dataTableConfig.tableConfigs());
        for (int idx = 0; idx < tableConfigs.size(); idx++) {
            SingleTableConfig tableConfig = tableConfigs.get(idx);
            if (tableConfig.tableNature().isPrimaryTable()) {
                TableNature replacementNature = null;
                switch (tableConfig.tableNature()) {
                case PRIMARY:
                    replacementNature = TableNature.ALL_IDS;
                    break;
                case PRIMARY_SPARSE:
                    replacementNature = TableNature.ALL_IDS_SPARSE;
                    break;
                case PRIMARY_UNIQUE:
                    replacementNature = TableNature.ALL_IDS_UNIQUE;
                    break;
                // $CASES-OMITTED$
                default:
                }

                SingleTableConfig replacement = new SingleTableConfig(tableConfig.tableName(), tableConfig.idColumnName(), replacementNature,
                        tableConfig.tableFilters(), tableConfig.argColumnMap(), tableConfig.autoMappingPolicy());
                tableConfigs.set(idx, replacement);
                break;
            }
        }
        DEFAULT_NO_PRIMARY_TABLE = new DataBinding(new MultiTableConfig(tableConfigs), DEFAULT.sqlContainsPolicy());
    }

    public static final DataBinding DEFAULT_NO_TABLE_WITH_ALL_IDS;
    static {
        MultiTableConfig dataTableConfig = (MultiTableConfig) DEFAULT.dataTableConfig();
        List<SingleTableConfig> tableConfigs = new ArrayList<>(dataTableConfig.tableConfigs());
        for (int idx = 0; idx < tableConfigs.size(); idx++) {
            SingleTableConfig tableConfig = tableConfigs.get(idx);
            if (tableConfig.tableNature().containsAllIds()) {
                SingleTableConfig replacement = new SingleTableConfig(tableConfig.tableName(), tableConfig.idColumnName(), TableNature.ID_SUBSET,
                        tableConfig.tableFilters(), tableConfig.argColumnMap(), tableConfig.autoMappingPolicy());
                tableConfigs.set(idx, replacement);
                break;
            }
        }
        DEFAULT_NO_TABLE_WITH_ALL_IDS = new DataBinding(new MultiTableConfig(tableConfigs), DEFAULT.sqlContainsPolicy());
    }

    public static final DataBinding FLAT_ONLY = extractFromDefault("T_FLAT_TXT", TableNature.PRIMARY);

    public static final DataBinding FACTS_ONLY = extractFromDefault("T_FACTS", TableNature.PRIMARY);

    public static final DataBinding SURVEY_ONLY = extractFromDefault("T_SURVEY", TableNature.PRIMARY);

    public static final DataBinding POSDATA_ONLY = extractFromDefault("T_POSDATA", TableNature.PRIMARY);

    public static final DataBinding PROVIDER_ALWAYS_KNOWN = new DataBinding(PROVIDER_ALWAYS_KNOWN_TABLE, DEFAULT.sqlContainsPolicy());

    /**
     * This setup combines T_BASE and T_POSDATA, but this time the INV_DATE is marked multi-row, which has consequences for queries with date conditions.
     * <p>
     * You can now query AND with multiple dates, but you also <i>lose the connection</i> to the remaining columns.
     */
    public static final DataBinding BASE_AND_POSDATA_MR;
    static {
        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable(BASE_TABLE)
                                                  .withTable("T_POSDATA")
                                                           .idColumn("UID")
                                                           .dataColumn("INV_DATE", SQL_DATE)
                                                               .mappedToArgName("pos.date", DATE)
                                                               .multiRow()
                                                           .dataColumn("DESCRIPTION", SQL_VARCHAR).mappedToArgName("pos.name", STRING)
                                                           .dataColumn("QUANTITY", SQL_INTEGER).mappedToArgName("pos.quantity", INTEGER)
                                                           .dataColumn("UNIT_PRICE", SQL_DECIMAL).mappedToArgName("pos.unitPrice", DECIMAL)
                                                           .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("pos.country", STRING)
                                                  .get();
        // @formatter:on
        BASE_AND_POSDATA_MR = new DataBinding(config, DefaultSqlContainsPolicy.SQL92);

    }

    /**
     * This setup combines T_BASE and T_POSDATA, this time - instead of marking the invoice date multi-row - we add a new attribute "anyDate"
     * <p>
     * You can now query AND with multiple dates, but still keep the connection between the attributes.
     */
    public static final DataBinding BASE_AND_POSDATA_MR_ANY;
    static {
        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable(BASE_TABLE)
                                                  .withTable("T_POSDATA")
                                                           .idColumn("UID")
                                                           .dataColumn("INV_DATE", SQL_DATE).mappedToArgName("pos.date", DATE)
                                                           .dataColumn("INV_DATE", SQL_DATE)
                                                               .mappedToArgName("pos.anyDate", DATE)
                                                               .multiRow()
                                                           .dataColumn("DESCRIPTION", SQL_VARCHAR).mappedToArgName("pos.name", STRING)
                                                           .dataColumn("QUANTITY", SQL_INTEGER).mappedToArgName("pos.quantity", INTEGER)
                                                           .dataColumn("UNIT_PRICE", SQL_DECIMAL).mappedToArgName("pos.unitPrice", DECIMAL)
                                                           .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("pos.country", STRING)
                                                  .get();
        // @formatter:on
        BASE_AND_POSDATA_MR_ANY = new DataBinding(config, DefaultSqlContainsPolicy.SQL92);

    }

    /**
     * This setup combines T_BASE and T_POSDATA, this time the whole T_POSDATA is marked sparse, effectively decoupling all the columns from each-other (all
     * multi-row attributes).
     * <p>
     * You can now query AND with multiple values, but there won't be any row connection between the conditions.
     */
    public static final DataBinding BASE_AND_POSDATA_SPARSE;
    static {
        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable(BASE_TABLE)
                                                  .withTable("T_POSDATA")
                                                           .withSparseData()
                                                           .idColumn("UID")
                                                           .dataColumn("INV_DATE", SQL_DATE).mappedToArgName("pos.date", DATE)
                                                           .dataColumn("INV_DATE", SQL_DATE).mappedToArgName("pos.anyDate", DATE)
                                                           .dataColumn("DESCRIPTION", SQL_VARCHAR).mappedToArgName("pos.name", STRING)
                                                           .dataColumn("QUANTITY", SQL_INTEGER).mappedToArgName("pos.quantity", INTEGER)
                                                           .dataColumn("UNIT_PRICE", SQL_DECIMAL).mappedToArgName("pos.unitPrice", DECIMAL)
                                                           .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("pos.country", STRING)
                                                  .get();
        // @formatter:on
        BASE_AND_POSDATA_SPARSE = new DataBinding(config, DefaultSqlContainsPolicy.SQL92);

    }

    /**
     * The survey table as primary table and the facts
     */
    public static final DataBinding SURVEY_AND_FACTS;
    static {

        SingleTableConfig surveyPrimaryTable = (SingleTableConfig) SURVEY_ONLY.dataTableConfig();

        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable(surveyPrimaryTable)
                                                  .withTable(FACT_TABLE)
                                                  .get();
        // @formatter:on
        SURVEY_AND_FACTS = new DataBinding(config, DefaultSqlContainsPolicy.SQL92);

    }

    private static DataBinding extractFromDefault(String tableName, TableNature tableNature) {
        MultiTableConfig dataTableConfig = (MultiTableConfig) DEFAULT.dataTableConfig();
        List<SingleTableConfig> tableConfigs = new ArrayList<>(dataTableConfig.tableConfigs());
        SingleTableConfig replacement = null;
        for (int idx = 0; idx < tableConfigs.size(); idx++) {
            SingleTableConfig tableConfig = tableConfigs.get(idx);
            if (tableConfig.tableName().equals(tableName)) {

                replacement = new SingleTableConfig(tableConfig.tableName(), tableConfig.idColumnName(), tableNature, tableConfig.tableFilters(),
                        tableConfig.argColumnMap(), tableConfig.autoMappingPolicy());
                break;
            }
        }
        return new DataBinding(replacement, DEFAULT.sqlContainsPolicy());
    }

    private H2TestBindings() {
        // constants
    }

}
