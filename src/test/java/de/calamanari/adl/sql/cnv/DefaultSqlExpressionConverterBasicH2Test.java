//@formatter:off
/*
 * DefaultSqlExpressionConverterBasicH2Test
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

import java.util.function.Supplier;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.calamanari.adl.AudlangUserMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ConversionException;

import static de.calamanari.adl.sql.cnv.H2TestBindings.FACTS_ONLY;
import static de.calamanari.adl.sql.cnv.H2TestBindings.FLAT_ONLY;
import static de.calamanari.adl.sql.cnv.H2TestBindings.POSDATA_ONLY;
import static de.calamanari.adl.sql.cnv.H2TestBindings.PROVIDER_ALWAYS_KNOWN;
import static de.calamanari.adl.sql.cnv.H2TestBindings.SURVEY_ONLY;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.assertQueryResult;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.list;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.selectCount;
import static de.calamanari.adl.sql.cnv.H2TestExecutionUtils.selectIds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultSqlExpressionConverterBasicH2Test {

    @Test
    void testBasicSingleAttributeSelect1() {

        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016), "provider = LOGMOTH");
        assertQueryResult(list(19017, 19018), "provider = ZOMBEE");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19019, 19020, 19021), "provider != ZOMBEE");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19019, 19020, 19021), "STRICT provider != ZOMBEE");

        assertThrowsErrorCode(CommonErrors.ERR_1002_ALWAYS_FALSE, () -> selectCount(PROVIDER_ALWAYS_KNOWN, "provider is unknown"));
        assertThrowsErrorCode(CommonErrors.ERR_1001_ALWAYS_TRUE, () -> selectCount(PROVIDER_ALWAYS_KNOWN, "provider is not unknown"));

        assertQueryResult(list(19011, 19013, 19019, 19020, 19021), "home-country = USA");
        assertQueryResult(list(19015, 19016), "home-country = UK");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018), "home-country != USA");
        assertQueryResult(list(19014, 19015, 19016, 19017, 19018), "STRICT home-country != USA");
        assertQueryResult(list(19012), "home-country is unknown");

        assertQueryResult(list(19011, 19019, 19021), "demCode = 7");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19021), "demCode > 5");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19021), "demCode != 5");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19021), "STRICT demCode != 5");
        assertQueryResult(list(), "demCode is unknown");

        assertQueryResult(list(19012, 19015, 19018), "omScore > 8000");
        assertQueryResult(list(19011, 19013, 19014, 19016, 19017, 19019, 19020, 19021), "omScore < 8000");
        assertQueryResult(list(), "omScore is unknown");

    }

    private static void assertThrowsErrorCode(AudlangUserMessage expectedError, Supplier<?> s) {
        ConversionException expected = null;
        try {
            s.get();
        }
        catch (ConversionException ex) {
            expected = ex;
        }

        assertNotNull(expected);
        assertEquals(expectedError.code(), expected.getUserMessage().code());

    }

    @Test
    void testBasicSingleAttributeSelect2() {

        // mapped to timestamp, requires range-queries
        assertQueryResult(list(19011, 19013), "upd1 = 2024-09-24");
        assertQueryResult(list(19019, 19020, 19021), "upd1 < 2024-09-24");
        assertQueryResult(list(19011, 19013, 19019, 19020, 19021), "upd1 <= 2024-09-24");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018), "upd1 >= 2024-09-25");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "upd1 != 2024-09-24");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "STRICT upd1 != 2024-09-24");
        assertQueryResult(list(), "upd1 is unknown");

        // mapped to date, no range queries
        assertQueryResult(list(19011, 19013), "upd2 = 2024-09-24");
        assertQueryResult(list(19019, 19020, 19021), "upd2 < 2024-09-24");
        assertQueryResult(list(19011, 19013, 19019, 19020, 19021), "upd2 <= 2024-09-24");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018), "upd2 >= 2024-09-25");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "upd2 != 2024-09-24");
        assertQueryResult(list(), "upd2 is unknown");

    }

    @Test
    void testBasicSingleAttributeSelect3() {

        assertQueryResult(list(19017, 19018), "tntCode = 6");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19019, 19020, 19021), "tntCode != 6");
        assertQueryResult(list(19011, 19013, 19014, 19015, 19016, 19019, 19020, 19021), "STRICT tntCode != 6");
        assertQueryResult(list(19015, 19017, 19018), "tntCode > 3");
        assertQueryResult(list(19011, 19016, 19019, 19020, 19021), "tntCode < 3");

        assertQueryResult(list(19013, 19016), "bState = 0");
        assertQueryResult(list(19011, 19012, 19014, 19015, 19017, 19018, 19019, 19020, 19021), "bState != 0");
        assertQueryResult(list(19011, 19014, 19017, 19018), "STRICT bState != 0");
        assertQueryResult(list(19011, 19014, 19017, 19018), "bState = 1");

        assertQueryResult(list(19014, 19017), "sCode = 17");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "sCode != 89");
        assertQueryResult(list(19013, 19014, 19016, 19017, 19018), "STRICT sCode != 89");
        assertQueryResult(list(19011, 19013, 19014, 19017), "sCode > 11");
        assertQueryResult(list(19016, 19018), "sCode < 17");

    }

    @Test
    void testBasicSingleAttributeSelect4() {

        assertQueryResult(list(19018), "biCode = 5512831");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19016, 19017, 19019, 19020, 19021), "biCode != 5512831");
        assertQueryResult(list(19011, 19013, 19015, 19016, 19017), "STRICT biCode != 5512831");
        assertQueryResult(list(19011, 19013, 19015, 19016, 19017), "biCode > 5512831");
        assertQueryResult(list(19018), "biCode < 6612732");

        assertQueryResult(list(19015, 19016, 19018), "nCode > 19273213");
        assertQueryResult(list(19012, 19013, 19017), "nCode < 19273213");

    }

    @Test
    void testBasicSingleAttributeSelect5() {

        assertQueryResult(list(19011, 19012, 19014, 19016, 19019), "fact.hasPet.flg = 1");
        assertQueryResult(list(19011, 19016), "fact.hasDog.flg = 1");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19017, 19018, 19019, 19020, 19021), "fact.hasDog.flg != 1");
        assertQueryResult(list(19012), "STRICT fact.hasDog.flg != 1");
        assertQueryResult(list(19012), "fact.hasDog.flg = 0");

        assertQueryResult(list(19011), "fact.dateOfBirth.dt = 2000-03-05");
        assertQueryResult(list(19014, 19019), "fact.dateOfBirth.dt > 2001-01-01");

        assertQueryResult(list(19011), "fact.contactTime.ts = 2023-12-24");
        assertQueryResult(list(19011), "fact.contactTime.ts < 2024-01-01");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19019, 19020), "fact.contactTime.ts > 2024-01-01");
        assertQueryResult(list(19012, 19013, 19014, 19019, 19020), "STRICT NOT fact.contactTime.ts < 2024-01-01");

        assertQueryResult(list(19011, 19013), "fact.contactCode.str = RX89");
        assertQueryResult(list(19012, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "fact.contactCode.str != RX89");
        assertQueryResult(list(19012, 19014, 19019, 19020), "STRICT fact.contactCode.str != RX89");
        assertQueryResult(list(19019, 19020), "fact.contactCode.str > RX89");
        assertQueryResult(list(19015, 19016, 19017, 19018, 19021), "fact.contactCode.str IS UNKNOWN");

    }

    @Test
    void testBasicSingleAttributeSelect6() {

        assertQueryResult(list(19011, 19012, 19015), "fact.yearOfBirth.int = 2000");
        assertQueryResult(list(19014, 19019), "fact.yearOfBirth.int > 2000");
        assertQueryResult(list(19011, 19012, 19013, 19015, 19016, 19017, 19018, 19020, 19021), "NOT fact.yearOfBirth.int > 2000");
        assertQueryResult(list(19011, 19012, 19015), "STRICT NOT fact.yearOfBirth.int > 2000");

        assertQueryResult(list(), "fact.xScore.dec > 1");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19016, 19018, 19019, 19020), "fact.xScore.dec < 1");

    }

    @Test
    void testBasicSingleAttributeSelect7() {

        assertQueryResult(list(19011), "q.monthlyIncome.int = 4600");
        assertQueryResult(list(19011, 19012, 19013, 19018), "q.monthlyIncome.int >= 4600");
        assertQueryResult(list(19011), "q.monthlyIncome.int < 5000");
        assertQueryResult(list(19014, 19015, 19016, 19017, 19019, 19020, 19021), "NOT q.monthlyIncome.int >= 4600");

        assertQueryResult(list(19011), "q.martialStatus.str = married");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "q.martialStatus.str != married");
        assertQueryResult(list(19012, 19013, 19017, 19018, 19021), "STRICT q.martialStatus.str != married");

        assertQueryResult(list(19012, 19021), "q.children.int = 0");
        assertQueryResult(list(19013, 19017), "q.children.int > 1");
        assertQueryResult(list(19011, 19012, 19014, 19015, 19016, 19018, 19019, 19020, 19021), "NOT q.children.int > 1");

        assertQueryResult(list(19011, 19013, 19018, 19021), "q.carOwner.flg = 1");
        assertQueryResult(list(19012), "q.carOwner.flg = 0");

    }

    @Test
    void testBasicSingleAttributeSelect8() {

        assertQueryResult(list(19011), "pos.date = 2024-01-13");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "pos.date != 2024-01-13");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19017), "STRICT pos.date != 2024-01-13");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19017), "pos.date <= 2024-06-20");

        assertQueryResult(list(19012), "pos.name = POPCORN");
        assertQueryResult(list(19013, 19017), "pos.name contains WATER");

        assertQueryResult(list(19011, 19013, 19014, 19015, 19017), "pos.quantity > 1");
        assertQueryResult(list(19012, 19016, 19018, 19019, 19020, 19021), "NOT pos.quantity > 1");
        assertQueryResult(list(19012), "STRICT NOT pos.quantity > 1");

        assertQueryResult(list(19012), "pos.unitPrice > 500");

        assertQueryResult(list(19011, 19012, 19013), "pos.country = USA");

    }

    @Test
    void testBasicSingleAttributeSelect9() {

        assertQueryResult(list(19011, 19012, 19016), "sports = tennis");
        assertQueryResult(list(19013, 19014, 19015, 19017, 19018, 19019, 19020, 19021), "sports != tennis");
        assertQueryResult(list(19014, 19017, 19021), "STRICT sports != tennis");

        // native type casters ensures we get a decimal comparison
        // although the column is text

        assertQueryResult(list(19011, 19013, 19014, 19016, 19021), "sizeCM > 175");
        assertQueryResult(list(19021), "sizeCM = 188");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19016, 19021), "sizeCM > 90");
        assertQueryResult(list(19015, 19017, 19018, 19019, 19020), "NOT sizeCM > 90");
        assertQueryResult(list(), "STRICT NOT sizeCM > 90");

        assertQueryResult(list(19011, 19012, 19013, 19016, 19021), "bodyTempCelsius > 4");

        assertQueryResult(list(19011, 19013, 19016, 19021), "anniverseryDate > 2024-01-01");
        assertQueryResult(list(19011), "anniverseryDate = 2024-09-20");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19016, 19017, 19018, 19019, 19020, 19021), "anniverseryDate != 2024-09-20");
        assertQueryResult(list(19012, 19013, 19016, 19021), "STRICT anniverseryDate != 2024-09-20");

        assertQueryResult(list(19011, 19013, 19015, 19017, 19021), "clubMember = 1");
        assertQueryResult(list(19014, 19016), "clubMember = 0");
        assertQueryResult(list(19012, 19014, 19016, 19018, 19019, 19020), "clubMember != 1");

    }

    @Test
    void testFlatOnlySingleAttributeSelect() {
        assertQueryResult(list(19013, 19014, 19015, 19017, 19021), FLAT_ONLY, "sports != tennis");
        assertQueryResult(list(19014, 19017, 19021), FLAT_ONLY, "STRICT sports != tennis");
    }

    @Test
    void testFactsOnlySingleAttributeSelect() {

        assertQueryResult(list(19011, 19012, 19015), FACTS_ONLY, "fact.yearOfBirth.int = 2000");
        assertQueryResult(list(19014, 19019), FACTS_ONLY, "fact.yearOfBirth.int > 2000");
        assertQueryResult(list(19011, 19012, 19013, 19015, 19016, 19018, 19020), FACTS_ONLY, "NOT fact.yearOfBirth.int > 2000");
        assertQueryResult(list(19011, 19012, 19015), FACTS_ONLY, "STRICT NOT fact.yearOfBirth.int > 2000");

    }

    @Test
    void testSurveyOnlySingleAttributeSelect() {

        assertQueryResult(list(19011), SURVEY_ONLY, "q.monthlyIncome.int = 4600");
        assertQueryResult(list(19011, 19012, 19013, 19018), SURVEY_ONLY, "q.monthlyIncome.int >= 4600");
        assertQueryResult(list(19017, 19020, 19021), SURVEY_ONLY, "NOT q.monthlyIncome.int >= 4600");
        assertQueryResult(list(), SURVEY_ONLY, "STRICT NOT q.monthlyIncome.int >= 4600");

    }

    @Test
    void testPosDataOnlySingleAttributeSelect() {

        assertQueryResult(list(19011), POSDATA_ONLY, "pos.date = 2024-01-13");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19017), POSDATA_ONLY, "pos.date != 2024-01-13");
        assertQueryResult(list(19012, 19013, 19014, 19015, 19017), POSDATA_ONLY, "STRICT pos.date != 2024-01-13");
        assertQueryResult(list(19011, 19012, 19013, 19014, 19015, 19017), POSDATA_ONLY, "pos.date <= 2024-06-20");

    }

    @Test
    @Disabled("These tests could be unreliable due to precision problems (for me they work ;-)")
    void testBasicSingleAttributeDecimalCount() {
        assertEquals(4, selectCount("omScore <= 1723.9"));
        assertEquals(7, selectCount("omScore >= 1723.9"));

        assertEquals(2, selectCount("nCode = 19273213.21"));
        assertEquals(9, selectCount("nCode != 19273213.21"));
        assertEquals(4, selectCount("STRICT nCode != 19273213.21"));
        assertEquals(1, selectCount("nCode > 19273213.21"));
        assertEquals(3, selectCount("nCode < 19273213.21"));

        assertEquals(2, selectCount("fact.xScore.dec = 0.9871621"));
        assertEquals(0, selectCount("fact.xScore.dec > 0.9871621"));
        assertEquals(6, selectCount("fact.xScore.dec < 0.9871621"));
        assertEquals(9, selectCount("fact.xScore.dec != 0.9871621"));
        assertEquals(6, selectCount("STRICT fact.xScore.dec != 0.9871621"));

        assertEquals(2, selectCount("pos.unitPrice = 499.99"));

    }

    @Test
    @Disabled("for analysis only")
    void testCheck() {
        assertEquals(2, selectCount("pos.unitPrice = 499.99"));
        selectIds("(home-country = Germany and q.monthlyIncome.int = 4600) OR (home-country = USA and STRICT q.monthlyIncome.int != 4600)",
                ConversionDirective.ENFORCE_PRIMARY_TABLE);

    }

}
