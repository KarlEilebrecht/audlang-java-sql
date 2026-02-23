//@formatter:off
/*
 * H2DocumentationTest
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DataTableConfig;
import de.calamanari.adl.sql.config.DefaultSqlContainsPolicy;
import de.calamanari.adl.sql.config.MultiTableConfig;
import de.calamanari.adl.sql.config.SingleTableConfig;

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
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TIMESTAMP;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.assertQueryResult;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.list;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class H2DocumentationTest {

    // @formatter:off
    public static final SingleTableConfig BASE_TABLE1 = SingleTableConfig.forTable("T_BASE")
                                                            .idColumn("ID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("provider", STRING)
                                                            .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("home-country", STRING)
                                                            .dataColumn("CITY", SQL_VARCHAR).mappedToArgName("home-city", STRING)
                                                            .dataColumn("DEM_CODE", SQL_INTEGER).mappedToArgName("demCode", INTEGER)
                                                            .dataColumn("GENDER", SQL_VARCHAR).mappedToArgName("gender", STRING)
                                                            .dataColumn("OM_SCORE", SQL_FLOAT).mappedToArgName("omScore", DECIMAL)
                                                       .get();            
    // @formatter:on    

    // @formatter:off
    public static final SingleTableConfig BASE_TABLE2 = SingleTableConfig.forTable("T_BASE")
                                                            .idColumn("ID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("provider", STRING)
                                                            .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("home-country", STRING)
                                                            .dataColumn("CITY", SQL_VARCHAR).mappedToArgName("home-city", STRING)
                                                            .dataColumn("DEM_CODE", SQL_INTEGER).mappedToArgName("demCode", INTEGER)
                                                            .dataColumn("GENDER", SQL_VARCHAR).mappedToArgName("gender", STRING)
                                                            .dataColumn("OM_SCORE", SQL_FLOAT).mappedToArgName("omScore", DECIMAL)
                                                            .dataColumn("UPD_TIME", SQL_TIMESTAMP).mappedToArgName("upd1", DATE)
                                                            .dataColumn("UPD_DATE", SQL_DATE).mappedToArgName("upd2", DATE)
                                                       .get();            
    // @formatter:on    

    // @formatter:off
    public static final SingleTableConfig BASE_TABLE2A = SingleTableConfig.forTable("T_BASE")
                                                            .idColumn("ID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("provider", STRING)
                                                            .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("home-country", STRING)
                                                            .dataColumn("CITY", SQL_VARCHAR).mappedToArgName("home-city", STRING)
                                                            .dataColumn("DEM_CODE", SQL_INTEGER).mappedToArgName("demCode", INTEGER)
                                                            .dataColumn("GENDER", SQL_VARCHAR).mappedToArgName("gender", STRING)
                                                            .dataColumn("OM_SCORE", SQL_FLOAT).mappedToArgName("omScore", DECIMAL)
                                                            .dataColumn("UPD_TIME", SQL_TIMESTAMP.withNativeTypeCaster(H2TestBindings.H2_VARCHAR_CASTER))
                                                                .mappedToArgName("upd1", DATE)
                                                            .dataColumn("UPD_DATE", SQL_DATE).mappedToArgName("upd2", DATE)
                                                       .get();            
    // @formatter:on    

    // @formatter:off
    public static final SingleTableConfig BASE_TABLE_P = SingleTableConfig.forTable("T_BASE")
                                                            .asPrimaryTable()
                                                            .withUniqueIds()
                                                            .idColumn("ID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR).mappedToArgName("provider", STRING).alwaysKnown()
                                                            .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("home-country", STRING)
                                                            .dataColumn("CITY", SQL_VARCHAR).mappedToArgName("home-city", STRING)
                                                            .dataColumn("DEM_CODE", SQL_INTEGER).mappedToArgName("demCode", INTEGER)
                                                            .dataColumn("GENDER", SQL_VARCHAR).mappedToArgName("gender", STRING)
                                                            .dataColumn("OM_SCORE", SQL_FLOAT).mappedToArgName("omScore", DECIMAL)
                                                       .get();            
    // @formatter:on    

    // @formatter:off
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

    // @formatter:on

    // @formatter:off
    public static final SingleTableConfig FACT_TABLE_BAD = SingleTableConfig.forTable("T_FACTS") 
                                                            .idColumn("UID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR)
                                                                 .mappedToArgName("fact.provider", STRING)
                                                            .dataColumn("F_VALUE_FLG", SQL_BOOLEAN)
                                                                .mappedToArgName("fact.hasCat.flg", BOOL)
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "hasCat")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "flag")
                                                            .dataColumn("F_VALUE_FLG", SQL_BOOLEAN)
                                                                .mappedToArgName("fact.hasBird.flg", BOOL)
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "hasBird")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "flag")
                                                       .get();

    // @formatter:on

    // @formatter:off
    public static final SingleTableConfig FACT_TABLE_MR = SingleTableConfig.forTable("T_FACTS") 
                                                            .idColumn("UID")
                                                            .dataColumn("PROVIDER", SQL_VARCHAR)
                                                                 .mappedToArgName("fact.provider", STRING)
                                                            .dataColumn("F_VALUE_FLG", SQL_BOOLEAN)
                                                                .mappedToArgName("fact.hasCat.flg", BOOL)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "hasCat")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "flag")
                                                            .dataColumn("F_VALUE_FLG", SQL_BOOLEAN)
                                                                .mappedToArgName("fact.hasBird.flg", BOOL)
                                                                .multiRow()
                                                                .filteredBy("F_KEY", SQL_VARCHAR, "hasBird")
                                                                .filteredBy("F_TYPE", SQL_VARCHAR, "flag")
                                                       .get();

    // @formatter:on

    // @formatter:off
    public static final SingleTableConfig POSDATA_TABLE = SingleTableConfig.forTable("T_POSDATA") 
                                                            .idColumn("UID")
                                                            .dataColumn("INV_DATE", SQL_DATE).mappedToArgName("pos.date", DATE)
                                                            .dataColumn("DESCRIPTION", SQL_VARCHAR).mappedToArgName("pos.name", STRING)
                                                            .dataColumn("QUANTITY", SQL_INTEGER).mappedToArgName("pos.quantity", INTEGER)
                                                            .dataColumn("UNIT_PRICE", SQL_DECIMAL).mappedToArgName("pos.unitPrice", DECIMAL)
                                                            .dataColumn("COUNTRY", SQL_VARCHAR).mappedToArgName("pos.country", STRING)
                                                       .get();

    // @formatter:on

    // @formatter:off
    public static final SingleTableConfig SURVEY_TABLE = SingleTableConfig.forTable("T_SURVEY")
                                                            .filteredBy("TENANT", SQL_INTEGER, "${tenant}") 
                                                            .idColumn("PID")
                                                            .dataColumn("A_STR", SQL_VARCHAR)
                                                                .autoMapped(s-> (s.startsWith("q.") && s.endsWith(".str") 
                                                                        && s.length() > 6) ? s.substring(2, s.length()-4) : null, STRING)
                                                                .multiRow()
                                                                .filteredBy("Q_KEY", SQL_VARCHAR, "${argName.local}")
                                                            .dataColumn("A_INT", SQL_BIGINT)
                                                                .autoMapped(s-> (s.startsWith("q.") && s.endsWith(".int") 
                                                                        && s.length() > 6) ? s.substring(2, s.length()-4) : null, INTEGER)
                                                                .multiRow()
                                                                .filteredBy("Q_KEY", SQL_VARCHAR, "${argName.local}")
                                                            .dataColumn("A_YESNO", SQL_BOOLEAN)
                                                                .autoMapped(s-> (s.startsWith("q.") && s.endsWith(".flg") 
                                                                        && s.length() > 6) ? s.substring(2, s.length()-4) : null, BOOL)
                                                                .multiRow()
                                                                .filteredBy("Q_KEY", SQL_VARCHAR, "${argName.local}")
                                                       .get();
    // @formatter:on

    // @formatter:off
    public static final SingleTableConfig FLAT_TXT_TABLE = SingleTableConfig.forTable("T_FLAT_TXT")
                                                                .idColumn("UID")
                                                                .dataColumn("C_VALUE", SQL_VARCHAR).mappedToArgName("sports", STRING)
                                                                    .multiRow()
                                                                    .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                                .dataColumn("C_VALUE", SQL_VARCHAR).mappedToArgName("hobbies", STRING)
                                                                    .multiRow()
                                                                    .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                                .dataColumn("C_VALUE", SQL_INTEGER.withNativeTypeCaster(H2TestBindings.H2_VARCHAR_CASTER))
                                                                    .mappedToArgName("sizeCM", INTEGER)
                                                                    .multiRow()
                                                                    .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                                .dataColumn("C_VALUE", SQL_DECIMAL.withNativeTypeCaster(H2TestBindings.H2_VARCHAR_CASTER))
                                                                    .mappedToArgName("bodyTempCelsius", DECIMAL)
                                                                    .multiRow()
                                                                    .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                                .dataColumn("C_VALUE", SQL_BOOLEAN.withNativeTypeCaster(H2TestBindings.H2_VARCHAR_CASTER))
                                                                    .mappedToArgName("clubMember", BOOL)
                                                                    .multiRow()
                                                                    .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                                .dataColumn("C_VALUE", SQL_DATE.withNativeTypeCaster(H2TestBindings.H2_VARCHAR_CASTER))
                                                                    .mappedToArgName("anniverseryDate", DATE)
                                                                    .multiRow()
                                                                    .filteredBy("C_KEY", SQL_VARCHAR, "${argName}")
                                                       .get();
    // @formatter:on

    private static final DataBinding binding(DataTableConfig... dataTableConfigs) {

        if (dataTableConfigs.length > 1) {

            return new DataBinding(new MultiTableConfig(Arrays.stream(dataTableConfigs).map(SingleTableConfig.class::cast).toList()),
                    DefaultSqlContainsPolicy.SQL92);

        }
        else {
            return new DataBinding(dataTableConfigs[0], DefaultSqlContainsPolicy.SQL92);
        }
    }

    @Test
    void testExample1() {

        assertQueryResult(list(19011, 19013), binding(BASE_TABLE1), "provider = LOGMOTH AND home-country = USA");
        assertQueryResult(list(19011, 19012, 19013, 19015, 19018, 19019, 19020, 19021), binding(BASE_TABLE1),
                "(provider = LOGMOTH AND home-country = USA) OR omScore > 5000");

    }

    @Test
    void testExample2() {

        assertQueryResult(list(19011, 19013), binding(BASE_TABLE2), "provider = LOGMOTH AND upd1 = 2024-09-24 AND upd2 = 2024-09-24");
        assertQueryResult(list(), binding(BASE_TABLE2), "provider = LOGMOTH AND upd1 = @upd2");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016), binding(BASE_TABLE2A), "provider = LOGMOTH AND upd1 = @upd2");

    }

    @Test
    void testExample3() {

        assertQueryResult(list(19014), binding(FACT_TABLE_BAD), "fact.provider = CLCPRO AND (fact.hasCat.flg=1 OR fact.hasBird.flg=1)");
        assertQueryResult(list(), binding(FACT_TABLE_BAD), "fact.provider = CLCPRO AND fact.hasCat.flg=1 AND fact.hasBird.flg=1");
        assertQueryResult(list(19014), binding(FACT_TABLE_MR), "fact.provider = CLCPRO AND fact.hasCat.flg=1 AND fact.hasBird.flg=1");

    }

    @Test
    void testExample4() {

        assertQueryResult(list(19020), binding(BASE_TABLE2, FACT_TABLE), "fact.provider = @provider AND fact.hasPet.flg=0");

        assertQueryResult(list(19015, 19018, 19019, 19020), binding(BASE_TABLE2, FACT_TABLE), "fact.provider = @provider OR fact.hasPet.flg=0");

        assertQueryResult(list(19020), binding(BASE_TABLE_P, FACT_TABLE), "fact.provider = @provider AND fact.hasPet.flg=0");

        assertQueryResult(list(19015, 19018, 19019, 19020), binding(BASE_TABLE_P, FACT_TABLE), "fact.provider = @provider OR fact.hasPet.flg=0");

    }

    @Test
    void testExample5() {

        assertQueryResult(list(19011, 19012), binding(BASE_TABLE_P, FACT_TABLE), "fact.contactTime.ts < 2024-09-01 AND fact.hasPet.flg=1");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19016, 19019), binding(BASE_TABLE_P, FACT_TABLE),
                "fact.contactTime.ts > 2024-11-13 OR fact.hasPet.flg=1");

    }

    @Test
    void testExample6() {

        assertQueryResult(list(19011, 19014, 19015), binding(BASE_TABLE_P, POSDATA_TABLE), "pos.country=@home-country and pos.date > 2024-04-01");

        assertQueryResult(list(19011, 19012, 19014, 19015), binding(BASE_TABLE_P, POSDATA_TABLE), "pos.date > 2024-04-01");

        assertQueryResult(list(19012, 19013, 19016, 19017, 19018, 19019, 19020, 19021), binding(BASE_TABLE_P, POSDATA_TABLE),
                "NOT (pos.country=@home-country AND pos.date > 2024-04-01)");

        assertQueryResult(list(19011, 19015), "pos.quantity > @pos.unitPrice AND pos.date > 2024-04-01");

        assertQueryResult(list(19012, 19013, 19014, 19016, 19017, 19018, 19019, 19020, 19021), "NOT (pos.quantity > @pos.unitPrice AND pos.date > 2024-04-01)");

        // 19016, 19018, 19019, 19020, 19021 don't have POS-data
        assertQueryResult(list(19012, 19013, 19014, 19017), "STRICT NOT (pos.quantity > @pos.unitPrice AND pos.date > 2024-04-01)");

    }

    @Test
    void testExample7() {

        Map<String, Serializable> globalVariables = new HashMap<>();
        globalVariables.put("tenant", 17);
        assertQueryResult(list(19012, 19013, 19018), binding(BASE_TABLE_P, SURVEY_TABLE), "q.monthlyIncome.int > 5000", globalVariables);

    }

    @Test
    void testExample8() {

        assertQueryResult(list(19014, 19021), binding(FLAT_TXT_TABLE), "sizeCM between (185, 195)");
        assertQueryResult(list(19011, 19013, 19021), binding(FLAT_TXT_TABLE), "bodyTempCelsius between (38, 39.1)");
        assertQueryResult(list(19011, 19013, 19015, 19017, 19021), binding(FLAT_TXT_TABLE), "clubMember=1");

    }

}
