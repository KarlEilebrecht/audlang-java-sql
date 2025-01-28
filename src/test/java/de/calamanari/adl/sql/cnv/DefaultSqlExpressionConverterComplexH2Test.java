//@formatter:off
/*
 * DefaultSqlExpressionConverterComplexH2Test
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

import static de.calamanari.adl.sql.cnv.H2TestBindings.BASE_AND_POSDATA_MR;
import static de.calamanari.adl.sql.cnv.H2TestBindings.BASE_AND_POSDATA_MR_ANY;
import static de.calamanari.adl.sql.cnv.H2TestBindings.BASE_AND_POSDATA_SPARSE;
import static de.calamanari.adl.sql.cnv.H2TestBindings.DEFAULT_NO_PRIMARY_TABLE;
import static de.calamanari.adl.sql.cnv.H2TestBindings.DEFAULT_NO_TABLE_WITH_ALL_IDS;
import static de.calamanari.adl.sql.cnv.H2TestBindings.FACTS_ONLY;
import static de.calamanari.adl.sql.cnv.H2TestBindings.SURVEY_AND_FACTS;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.TEST_AUGMENT;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.assertQueryResult;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.list;

import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultSqlExpressionConverterComplexH2Test {

    @Test
    void testMultiConditionSingleTable() {

        assertQueryResult(list(19011, 19013), "provider = LOGMOTH AND home-country = USA");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19019, 19020, 19021), "provider = LOGMOTH OR home-country = USA");
        assertQueryResult(list(19012, 19015, 19020), "(provider = LOGMOTH OR home-country = USA) AND gender=male");
        assertQueryResult(list(19011, 19013, 19014, 19016), "(provider = LOGMOTH OR home-country = USA) AND (sCode any of (11, 17) OR bState IS NOT UNKNOWN)");

        assertQueryResult(list(19011), "fact.hasDog.flg=1 AND fact.hasCat.flg=0");
        assertQueryResult(list(19011, 19016), "fact.hasDog.flg=1 AND fact.hasCat.flg != 1");
        assertQueryResult(list(19011, 19016), FACTS_ONLY, "fact.hasDog.flg=1 AND fact.hasCat.flg != 1");

        assertQueryResult(list(19012, 19013, 19018), "q.monthlyIncome.int > 4000 AND q.martialStatus.str != married");
        assertQueryResult(list(19012, 19013, 19017, 19018), "(q.monthlyIncome.int > 4000 AND q.martialStatus.str != married) OR q.children.int > 1");
        assertQueryResult(list(19011, 19012, 19013, 19018, 19020),
                "(q.monthlyIncome.int > 5000 AND q.martialStatus.str != married) OR (q.vegan.flg != 1 AND q.foodPref.str contains any of (fish, thai))");

        assertQueryResult(list(19012, 19013, 19017), "pos.name contains any of (MELON, PUMPKIN, CHEESE) AND pos.date > 2024-03-15");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19017),
                "(pos.name contains any of (MELON, PUMPKIN, CHEESE) AND pos.date > 2024-03-15) OR STRICT NOT pos.quantity > 2");

        assertQueryResult(list(19013, 19015, 19017, 19021), "(clubMember = 1 OR hobbies=origami) AND sports!=tennis");
        assertQueryResult(list(19017, 19021), "(clubMember = 1 OR hobbies=origami) AND STRICT sports!=tennis");

    }

    @Test
    void testMultiConditionMultiTable() {

        assertQueryResult(list(19011), "provider = LOGMOTH AND home-country = USA AND fact.hasDog.flg=1");

        assertQueryResult(list(19011, 19014, 19016),
                "(provider = LOGMOTH AND home-country = USA AND fact.hasDog.flg=1 AND q.monthlySpending.int >= 5000) OR clubMember=0");

        assertQueryResult(list(19011, 19012, 19014, 19016, 19018, 19019, 19020),
                "(provider = LOGMOTH AND home-country = USA AND fact.hasDog.flg=1 AND q.monthlySpending.int >= 5000) OR NOT clubMember=1");

        assertQueryResult(list(19012), """
                        ((provider = LOGMOTH
                               AND home-country = USA
                               AND fact.hasDog.flg=1
                               AND q.monthlySpending.int >= 5000)
                            OR NOT clubMember=1
                            )
                        AND pos.name contains any of (MELON, PUMPKIN, CHEESE) AND pos.date > 2024-03-15
                """);

        assertQueryResult(list(19015, 19016, 19019, 19020),
                "(fact.hasPet.flg=1 AND home-city strict not any of (Paris, Berlin)) OR (fact.hasPet.flg=0 AND STRICT home-city != Karlsruhe)");

        // prefers primary table over union
        assertQueryResult(list(19011, 19012, 19016), DEFAULT_NO_PRIMARY_TABLE, "fact.hasDog.flg=1 OR q.monthlySpending.int = 6000");

        // union base query
        assertQueryResult(list(19011, 19012, 19016), DEFAULT_NO_PRIMARY_TABLE, "fact.hasDog.flg=1 OR q.monthlySpending.int = 6000");

        // enforces first table with all IDs
        assertQueryResult(list(19011, 19012, 19016), DEFAULT_NO_PRIMARY_TABLE, "fact.hasDog.flg=1 OR q.monthlySpending.int = 6000",
                ConversionDirective.DISABLE_UNION);

    }

    @Test
    void testReferenceMatchSingleTable() {

        assertQueryResult(list(19013, 19017), "sCode > @tntCode AND nCode < @biCode");

        assertQueryResult(list(19011), "fact.homeCity.str != @fact.contactCode.str AND STRICT fact.hasDog.flg != @fact.hasBird.flg");

        assertQueryResult(list(19013, 19018), "NOT q.monthlySpending.int > @q.monthlyIncome.int AND q.carColor.str = @q.favColor.str");
        assertQueryResult(list(19011, 19013, 19018), "q.monthlySpending.int > @q.monthlyIncome.int OR q.carColor.str = @q.favColor.str");
        assertQueryResult(list(19013, 19018), "q.monthlySpending.int < @q.monthlyIncome.int AND q.carColor.str = @q.favColor.str");

        assertQueryResult(list(19011, 19013, 19015, 19017), "pos.quantity > @pos.unitPrice");
        assertQueryResult(list(19012, 19014, 19016, 19018, 19019, 19020, 19021), "NOT pos.quantity > @pos.unitPrice");
        assertQueryResult(list(19012, 19014), "STRICT NOT pos.quantity > @pos.unitPrice");

        assertQueryResult(list(19012, 19013), "bodyTempCelsius < @sizeCM and hobbies any of (origami, football, tennis)");

        assertQueryResult(list(19013, 19018), SURVEY_AND_FACTS, "q.carColor.str = @q.favColor.str");

        assertQueryResult(list(19018), "STRICT home-country != @provider and home-city=Paris");

    }

    @Test
    void testReferenceMatchMultiTable() {

        assertQueryResult(list(19018, 19019, 19020), "provider = @fact.provider");
        assertQueryResult(list(19019, 19020), "provider = @fact.provider AND fact.contactTime.ts > @upd1");
        assertQueryResult(list(19013, 19014), "STRICT NOT pos.date > @fact.contactTime.ts AND upd1 IS NOT UNKNOWN");
        assertQueryResult(list(19013, 19014, 19019, 19020),
                "(STRICT NOT pos.date > @fact.contactTime.ts AND upd1 IS NOT UNKNOWN) OR (provider = @fact.provider AND fact.contactTime.ts > @upd1)");

    }

    @Test
    void testMultiRowEffect() {

        assertQueryResult(list(19013), "pos.date = 2024-03-21 and pos.quantity > 1");

        // what we see here is that the multi-row property virtually decouples the invoice date from the row
        // So, the same query returns now all records that have ANY entry with the given date and ANY entry with the remaining attributes.
        assertQueryResult(list(19011, 19013, 19014), BASE_AND_POSDATA_MR, "pos.date = 2024-03-21 and pos.quantity > 1");

        // A table with multiple data columns should only be marked multi-row if the attributes are unrelated,
        // or you give a clear explanation to the user
        // The following query does not make any sense on regular data
        assertQueryResult(list(), "pos.date = 2024-03-21 and pos.date = 2024-03-17 and pos.quantity > 1");

        // When we mark the pos.date as multi-row, it returns a record
        // This record has indeed "any row" matching with the first pos.date and the second one, and there is "any row"
        // that matches the remaining condition "pos.quantity > 1 and pos.country=USA" - confusing!
        assertQueryResult(list(19011), BASE_AND_POSDATA_MR, "pos.date = 2024-03-21 and pos.date = 2024-03-17 and pos.quantity > 1 and pos.country=USA");

        // By mapping an explicit extra attribute (pos.anyDate) you can achieve more clarity here
        assertQueryResult(list(19013), BASE_AND_POSDATA_MR_ANY, "pos.anyDate = 2024-03-15 and pos.date = 2024-03-21 and pos.quantity > 1");

        // Pitfall: There is still no connection between the conditions because pos.anyDate is multi-row
        // The query below returns also rows where the (pos.quantity > 1 and pos.country=USA) is not met on that particular date but on any other!
        assertQueryResult(list(19013, 19017), BASE_AND_POSDATA_MR_ANY, "pos.anyDate = 2024-03-15 and pos.quantity > 1");

        assertQueryResult(list(19011, 19012, 19014, 19015, 19016, 19018, 19019, 19020, 19021), BASE_AND_POSDATA_MR_ANY,
                "NOT (pos.anyDate = 2024-03-15 and pos.quantity > 1)");

        assertQueryResult(list(19011, 19012, 19014, 19015), BASE_AND_POSDATA_MR_ANY, "STRICT NOT (pos.anyDate = 2024-03-15 and pos.quantity > 1)");

    }

    @Test
    void testSparseEffect() {

        assertQueryResult(list(19013), "pos.date = 2024-03-21 and pos.quantity > 1 and pos.country=USA");

        // The table is marked as sparse, so the columns do not depend on each other anymore
        // The query below returns all individuals
        // which have "any record with pos.date = 2024-03-21" AND "any record with pos.quantity = 1" AND "any record with pos.country=USA" in T_POS
        assertQueryResult(list(19011, 19012, 19013), BASE_AND_POSDATA_SPARSE, "pos.date = 2024-03-21 and pos.quantity = 1 and pos.country=USA");

        assertQueryResult(list(19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), BASE_AND_POSDATA_SPARSE,
                "NOT (pos.date = 2024-03-21 and pos.quantity = 1 and pos.country=USA)");

        assertQueryResult(list(19014, 19015, 19017), BASE_AND_POSDATA_SPARSE, "STRICT NOT (pos.date = 2024-03-21 and pos.quantity = 1 and pos.country=USA)");

    }

    @Test
    void testEnforcePrimaryTable() {

        assertQueryResult(list(19012), """
                        ((provider = LOGMOTH
                               AND home-country = USA
                               AND fact.hasDog.flg=1
                               AND q.monthlySpending.int >= 5000)
                            OR NOT clubMember=1
                            )
                        AND pos.name contains any of (MELON, PUMPKIN, CHEESE) AND pos.date > 2024-03-15
                """, ConversionDirective.ENFORCE_PRIMARY_TABLE);

        assertQueryResult(list(19011), "fact.hasDog.flg=1 AND fact.hasCat.flg=0", ConversionDirective.ENFORCE_PRIMARY_TABLE);

        assertQueryResult(list(19011), SURVEY_AND_FACTS, "fact.hasDog.flg=1 AND fact.hasCat.flg=0");

        assertQueryResult(list(19011), SURVEY_AND_FACTS, "fact.hasDog.flg=1 AND fact.hasCat.flg=0", ConversionDirective.ENFORCE_PRIMARY_TABLE);

        assertQueryResult(list(19011, 19016), SURVEY_AND_FACTS, "fact.hasDog.flg=1");

        assertQueryResult(list(19011), SURVEY_AND_FACTS, "fact.hasDog.flg=1", ConversionDirective.ENFORCE_PRIMARY_TABLE);

    }

    @Test
    void testNoTableWithAllIds() {

        assertQueryResult(list(19012), DEFAULT_NO_TABLE_WITH_ALL_IDS, """
                        ((provider = LOGMOTH
                               AND home-country != USA
                               AND fact.hasDog.flg=1
                               AND q.monthlySpending.int >= 5000)
                            OR NOT clubMember=1
                            )
                        AND pos.name contains any of (MELON, PUMPKIN, CHEESE) AND pos.date > 2024-03-15
                """);

        // ugly case: late primary alias promotion
        assertQueryResult(list(19016), DEFAULT_NO_TABLE_WITH_ALL_IDS, "fact.hasDog.flg=1 AND fact.hasCat.flg!=0");

        assertQueryResult(list(19012, 19013, 19014, 19015, 19017, 19018, 19019, 19020, 19021), "fact.hasDog.flg!=1");

        // force the same with a big UNION of all tables at the begin
        assertQueryResult(list(19012, 19013, 19014, 19015, 19017, 19018, 19019, 19020, 19021), DEFAULT_NO_TABLE_WITH_ALL_IDS, "fact.hasDog.flg!=1");

    }

    @Test
    void testAugmentedComments() {

        assertQueryResult(list(19012), """
                        ((provider = LOGMOTH
                               AND home-country = USA
                               AND fact.hasDog.flg=1
                               AND q.monthlySpending.int >= 5000)
                            OR NOT clubMember=1
                            )
                        AND pos.name contains any of (MELON, PUMPKIN, CHEESE) AND pos.date > 2024-03-15
                """, TEST_AUGMENT);

    }

}
