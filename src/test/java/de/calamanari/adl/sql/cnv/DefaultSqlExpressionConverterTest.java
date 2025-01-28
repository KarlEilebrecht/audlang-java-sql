//@formatter:off
/*
 * DefaultSqlExpressionConverterTest
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

package de.calamanari.adl.sql.cnv;

import static de.calamanari.adl.cnv.StandardConversions.parseCoreExpression;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.BOOL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DATE;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.DECIMAL;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.INTEGER;
import static de.calamanari.adl.cnv.tps.DefaultAdlType.STRING;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIGINT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BIT;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_BOOLEAN;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DATE;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_DECIMAL;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_INTEGER;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_TIMESTAMP;
import static de.calamanari.adl.sql.DefaultAdlSqlType.SQL_VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.ConversionException;
import de.calamanari.adl.Flag;
import de.calamanari.adl.FormatStyle;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.sql.QueryTemplateWithParameters;
import de.calamanari.adl.sql.config.DataBinding;
import de.calamanari.adl.sql.config.DefaultSqlContainsPolicy;
import de.calamanari.adl.sql.config.MultiTableConfig;
import de.calamanari.adl.sql.config.SingleTableConfig;
import de.calamanari.adl.sql.config.TableNature;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultSqlExpressionConverterTest {

    static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlExpressionConverterTest.class);

    private static DataBinding standardSingleTableBinding() {

        // @formatter:off
        SingleTableConfig config = SingleTableConfig.forTable("TBL_DATA")
                                                        .thatContainsAllIds()
                                                        .withUniqueIds()
                                                        .idColumn("ID")
                                                        .dataColumn("COLOR", SQL_VARCHAR).mappedToArgName("color", STRING)
                                                        .dataColumn("SHAPE", SQL_VARCHAR).mappedToArgName("shape", STRING)
                                                            .alwaysKnown()
                                                        .dataColumn("DATE_CREATED", SQL_DATE).mappedToArgName("dateCreated", DATE)
                                                        .dataColumn("UPDATE_TIME", SQL_TIMESTAMP).mappedToArgName("dateUpdated", DATE)
                                                        .dataColumn("MONTHLY_INCOME", SQL_INTEGER).mappedToArgName("monthlyIncome", INTEGER)
                                                        .dataColumn("MONTHLY_SPENDING", SQL_INTEGER).mappedToArgName("monthlySpending", INTEGER)
                                                        .dataColumn("IN_STOCK", SQL_BIGINT).mappedToArgName("inStock", INTEGER)
                                                        .dataColumn("PURCHASE_PRICE", SQL_DECIMAL).mappedToArgName("purchasePrice", DECIMAL)
                                                        .dataColumn("SALES_PRICE", SQL_DECIMAL).mappedToArgName("salesPrice", DECIMAL)
                                                        .dataColumn("ACTIVE", SQL_BOOLEAN).mappedToArgName("active", BOOL)
                                                    .get();
        // @formatter:on
        return new DataBinding(config, DefaultSqlContainsPolicy.SQL92);
    }

    private static DataBinding dummySingleTableBinding() {

        // @formatter:off
        SingleTableConfig config = SingleTableConfig.forTable("TBL_DATA")
                                                        .thatContainsAllIds()
                                                        .idColumn("ID")
                                                        .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                        .autoMapped(s-> s, STRING)
                                                        .multiRow()
                                                        .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                    .get();
        // @formatter:on
        return new DataBinding(config, DefaultSqlContainsPolicy.SQL92);
    }

    private static DataBinding dummySingleFilteredTableBinding() {

        // @formatter:off
        SingleTableConfig config = SingleTableConfig.forTable("TBL_DATA")
                                                        .thatContainsAllIds()
                                                        .filteredBy("FILTER1", SQL_VARCHAR, "xyz")
                                                        .idColumn("ID")
                                                        .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                        .autoMapped(s-> s, STRING)
                                                        .multiRow()
                                                        .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                    .get();
        // @formatter:on
        return new DataBinding(config, DefaultSqlContainsPolicy.SQL92);
    }

    private static DataBinding dummyMultiTableBinding() {

        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable("TBL_DATA1")
                                                       .thatContainsAllIds()
                                                       .idColumn("ID")
                                                       .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                           .autoMapped(s-> s.endsWith(".1") ? s.substring(0, s.length()-2) : null, STRING)
                                                           .multiRow()
                                                           .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                   .withTable("TBL_DATA2")
                                                       .asPrimaryTable()
                                                       .idColumn("ID2")
                                                       .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                           .autoMapped(s-> s.endsWith(".2") ? s.substring(0, s.length()-2) : null, STRING)
                                                           .multiRow()
                                                           .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                   .withTable("TBL_DATA3")
                                                       .idColumn("ID3")
                                                       .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                           .autoMapped(s-> s.endsWith(".3") ? s.substring(0, s.length()-2) : null, STRING)
                                                           .multiRow()
                                                           .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                   .withTable("TBL_DATA4")
                                                           .withUniqueIds()
                                                           .idColumn("ID4")
                                                           .dataColumn("PREMIUM", SQL_BOOLEAN)
                                                               .mappedToArgName("premium", BOOL)
                                                   .get();
        // @formatter:on
        return new DataBinding(config, DefaultSqlContainsPolicy.SQL92);
    }

    private static DataBinding dummyMultiTablePrimaryFilteredBinding() {

        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable("PRIM_TBL")
                                                       .asPrimaryTable()
                                                       .filteredBy("TENANT", SQL_VARCHAR, "FOOBAR")
                                                       .idColumn("IDP")
                                                       .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                           .autoMapped(s-> s.endsWith(".1") ? s.substring(0, s.length()-2) : null, STRING)
                                                           .multiRow()
                                                           .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                   .withTable("TBL_DATA")
                                                       .filteredBy("TENANT", SQL_VARCHAR, "FOOBAR")
                                                       .idColumn("ID1")
                                                       .dataColumn("DATA_COLUMN", SQL_VARCHAR)
                                                           .autoMapped(s-> s.endsWith(".2") ? s.substring(0, s.length()-2) : null, STRING)
                                                           .multiRow()
                                                           .filteredBy("FILTER_COLUMN", SQL_VARCHAR, "${argName.local}")
                                                   .get();
        // @formatter:on
        return new DataBinding(config, DefaultSqlContainsPolicy.SQL92);
    }

    private static DataBinding standardMultiTableBinding() {

        // @formatter:off
        MultiTableConfig config = MultiTableConfig.withTable("TBL_BASE")
                                                       .asPrimaryTable()
                                                       .idColumn("ID")
                                                       .dataColumn("C_COUNTRY", SQL_VARCHAR)
                                                           .mappedToArgName("country", STRING)
                                                           .alwaysKnown()
                                                       .dataColumn("C_COLOR", SQL_VARCHAR)
                                                           .mappedToArgName("color", STRING)
                                                       .dataColumn("C_SHAPE", SQL_VARCHAR)
                                                           .mappedToArgName("shape", STRING)
                                                       .dataColumn("C_DATE", SQL_DATE)
                                                           .mappedToArgName("date", DATE)
                                                       .dataColumn("C_CLICK", SQL_INTEGER)
                                                           .mappedToArgName("clicks", STRING)
                                                       .dataColumn("C_INCOME", SQL_DECIMAL)
                                                           .mappedToArgName("income", DECIMAL)
                                                   .withTable("TBL_FAV")
                                                       .idColumn("FID")
                                                       .dataColumn("FAV_SPORT", SQL_VARCHAR)
                                                           .mappedToArgName("fav.sport", STRING)
                                                           .multiRow()
                                                       .dataColumn("FAV_FOOD", SQL_VARCHAR)
                                                           .mappedToArgName("fav.food", STRING)
                                                           .multiRow()
                                                       .dataColumn("FAV_COLOR", SQL_VARCHAR)
                                                           .mappedToArgName("fav.color", STRING)
                                                           .multiRow()
                                                   .withTable("TBL_FLAGS")
                                                       .idColumn("ID")
                                                       .dataColumn("FLAG_STATE", SQL_BIT)
                                                           .autoMapped(s-> s.startsWith("flag.") ? s.substring(5) : null, DefaultAdlType.BOOL)
                                                           .multiRow()
                                                           .filteredBy("FLAG", SQL_INTEGER, "${argName.local}")
                                                   .withTable("TBL_FACTS")
                                                       .idColumn("ID")
                                                       .dataColumn("FACT_VALUE", SQL_VARCHAR)
                                                           .autoMapped(s-> s.startsWith("fact.") ? s.substring(5) : null, DefaultAdlType.STRING)
                                                           .multiRow()
                                                           .filteredBy("FACT", SQL_VARCHAR, "${argName.local}")
                                                           .filteredBy("TENENT", SQL_INTEGER, "${tenant}")
                                                   .withTable("TBL_SPECIAL")
                                                       .withSparseData()
                                                       .idColumn("ID")
                                                       .dataColumn("VISIT_TYPE", SQL_VARCHAR)
                                                           .mappedToArgName("vtype", STRING)
                                                       .dataColumn("VISIT_DURATION", SQL_INTEGER)
                                                           .mappedToArgName("vduration", INTEGER)
                                                       .dataColumn("VISIT_DATE", SQL_DATE)
                                                           .mappedToArgName("vdate", DATE)
                                                           .dataColumn("VISIT_DATE_FINE", SQL_TIMESTAMP)
                                                           .mappedToArgName("vdate2", DATE)
                                                       .dataColumn("VISIT_SPENT", SQL_DECIMAL)
                                                           .mappedToArgName("vspent", DECIMAL)
                                                   .withTable("TBL_SPECIAL2")
                                                       .withUniqueIds()
                                                       .filteredBy("KEC", SQL_VARCHAR, "FG")
                                                       .idColumn("ID")
                                                       .dataColumn("XNAME", SQL_VARCHAR)
                                                               .mappedToArgName("xname", STRING)
                                                   .get();
        // @formatter:on
        return new DataBinding(config, DefaultSqlContainsPolicy.SQL92);
    }

    private static DataBinding standardMultiTableBindingNoTableWithAllIds() {

        // @formatter:off
        SingleTableConfig baseTableReplacement = SingleTableConfig.forTable("TBL_BASE")
                                                       .idColumn("ID")
                                                       .dataColumn("C_COUNTRY", SQL_VARCHAR).mappedToArgName("country", STRING)
                                                       .dataColumn("C_COLOR", SQL_VARCHAR).mappedToArgName("color", STRING)
                                                       .dataColumn("C_SHAPE", SQL_VARCHAR).mappedToArgName("shape", STRING)
                                                       .dataColumn("C_DATE", SQL_DATE).mappedToArgName("date", DATE)
                                                       .dataColumn("C_CLICK", SQL_INTEGER).mappedToArgName("clicks", STRING)
                                                       .dataColumn("C_INCOME", SQL_DECIMAL).mappedToArgName("income", DECIMAL)
                                                   .get();
        // @formatter:on

        DataBinding template = standardMultiTableBinding();
        MultiTableConfig dataTableConfig = (MultiTableConfig) template.dataTableConfig();
        List<SingleTableConfig> tableConfigs = new ArrayList<>(dataTableConfig.tableConfigs());

        for (int idx = 0; idx < tableConfigs.size(); idx++) {
            SingleTableConfig tableConfig = tableConfigs.get(idx);
            if (tableConfig.tableName().equals("TBL_BASE")) {
                tableConfigs.set(idx, baseTableReplacement);
            }
            else if (tableConfig.tableNature().containsAllIds()) {
                SingleTableConfig replacement = new SingleTableConfig(tableConfig.tableName(), tableConfig.idColumnName(), TableNature.ID_SUBSET,
                        tableConfig.tableFilters(), tableConfig.argColumnMap(), tableConfig.autoMappingPolicy());
                tableConfigs.set(idx, replacement);
                break;
            }
        }
        return new DataBinding(new MultiTableConfig(tableConfigs), template.sqlContainsPolicy());

    }

    private static QueryTemplateWithParameters standardSingleConvert(String expression) {
        return new DefaultSqlExpressionConverter(standardSingleTableBinding()).convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters standardSingleAugConvert(String expression) {
        DefaultSqlExpressionConverter converter = new DefaultSqlExpressionConverter(standardSingleTableBinding());
        converter.setAugmentationListener(new TestCommentAugmentationListener());
        return converter.convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummySingleConvert(String expression) {
        return new DefaultSqlExpressionConverter(dummySingleTableBinding()).convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummySingleFilteredConvertWithAlternativeId(String expression) {
        DefaultSqlExpressionConverter converter = new DefaultSqlExpressionConverter(dummySingleFilteredTableBinding());

        converter.setIdColumnName("ALTERNATIVE_ID");

        return converter.convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummySingleInlineConvert(String expression) {
        DefaultSqlExpressionConverter converter = new DefaultSqlExpressionConverter(dummySingleTableBinding());
        converter.setStyle(FormatStyle.INLINE);
        return converter.convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummySingleAugConvert(String expression) {
        DefaultSqlExpressionConverter converter = new DefaultSqlExpressionConverter(dummySingleTableBinding());
        converter.setAugmentationListener(new TestCommentAugmentationListener());
        return converter.convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummySingleAugInlineConvert(String expression) {
        DefaultSqlExpressionConverter converter = new DefaultSqlExpressionConverter(dummySingleTableBinding());
        converter.setAugmentationListener(new TestCommentAugmentationListener());
        converter.setStyle(FormatStyle.INLINE);
        return converter.convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummyMultiConvert(String expression) {
        return new DefaultSqlExpressionConverter(dummyMultiTableBinding()).convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters dummyMultiPrimaryFilteredConvert(String expression, boolean enforcePrimaryTable) {
        if (enforcePrimaryTable) {
            return new DefaultSqlExpressionConverter(dummyMultiTablePrimaryFilteredBinding(), ConversionDirective.ENFORCE_PRIMARY_TABLE)
                    .convert(parseCoreExpression(expression));
        }
        else {
            return new DefaultSqlExpressionConverter(dummyMultiTablePrimaryFilteredBinding()).convert(parseCoreExpression(expression));
        }
    }

    private static QueryTemplateWithParameters standardMultiConvert(String expression, Flag... flags) {
        return new DefaultSqlExpressionConverter(standardMultiTableBinding(), flags).convert(parseCoreExpression(expression));
    }

    private static QueryTemplateWithParameters standardMultiConvertNoTableWithAllIds(String expression, Flag... flags) {
        return new DefaultSqlExpressionConverter(standardMultiTableBindingNoTableWithAllIds(), flags).convert(parseCoreExpression(expression));
    }

    @Test
    void testDummySingleTablePos() {

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color = red").toDebugString());

        assertEquals("""
                /* before script */
                /* before main statement (with=false) */
                SELECT
                /* after main SELECT */
                DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) )
                ORDER BY TBL_DATA.ID
                /* after script */""", dummySingleAugConvert("color = red").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT sq__001.ID
                FROM sq__001
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = sq__001.ID
                WHERE ( sq__001.ID IS NOT NULL
                        AND sq__002.ID IS NOT NULL
                        )
                ORDER BY sq__001.ID""", dummySingleConvert("color = red and shape = circle").toDebugString());

        assertEquals("""
                /* before script */
                WITH
                sq__001 AS ( SELECT
                /* after WITH-SELECT tables: [TBL_DATA] */
                DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT
                /* after WITH-SELECT tables: [TBL_DATA] */
                DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) )
                /* before main statement (with=true) */
                SELECT
                /* after main SELECT */
                DISTINCT sq__001.ID
                FROM sq__001
                /* join type not adjusted for from: sq__001 to: sq__002 */
                LEFT OUTER JOIN sq__002
                    /* before on clause with tables: sq__001, sq__002 */
                    ON
                    /* before on-conditions from: sq__001 to: sq__002 */
                    sq__002.ID = sq__001.ID
                    /* after on-conditions from: sq__001 to: sq__002 */
                WHERE ( sq__001.ID IS NOT NULL
                        AND sq__002.ID IS NOT NULL
                        )
                ORDER BY sq__001.ID
                /* after script */""", dummySingleAugConvert("color = red and shape = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) )
                        OR ( ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color = red or shape = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ( DATA_COLUMN IN ('black', 'blue', 'red') AND FILTER_COLUMN = 'color' ) )
                        OR ( ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color any of (red, blue, black) or shape = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( DATE_CREATED < UPDATE_TIME )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("dateUpdated > @dateCreated").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) )
                        OR ( ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("dateUpdated > 2024-12-13 OR dateCreated > 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) )
                        OR ( ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("dateUpdated = 2024-12-13 OR dateCreated = 2024-12-13").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '1' AND FILTER_COLUMN = 'active' ) ),
                        sq__002 AS ( SELECT DISTINCT TBL_DATA.ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'purchasePrice' AND sq__self.FILTER_COLUMN = 'salesPrice' ) )
                        SELECT DISTINCT sq__002.ID
                        FROM sq__002
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = sq__002.ID
                        WHERE ( sq__001.ID IS NOT NULL
                                AND sq__002.ID IS NOT NULL
                                )
                        ORDER BY sq__002.ID""",
                dummySingleConvert("active = 1 and purchasePrice < @salesPrice").toDebugString());

        assertEquals(
                """
                        /* before script */
                        WITH
                        sq__001 AS ( SELECT
                        /* after WITH-SELECT tables: [TBL_DATA] */
                        DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '1' AND FILTER_COLUMN = 'active' ) ),
                        sq__002 AS ( SELECT
                        /* after WITH-SELECT tables: [TBL_DATA, TBL_DATA] */
                        DISTINCT TBL_DATA.ID FROM TBL_DATA
                        /* join type not adjusted for from: TBL_DATA to: TBL_DATA */
                        INNER JOIN TBL_DATA sq__self
                            /* before on clause with tables: TBL_DATA, TBL_DATA */
                            ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'purchasePrice' AND sq__self.FILTER_COLUMN = 'salesPrice' ) )
                        /* before main statement (with=true) */
                        SELECT
                        /* after main SELECT */
                        DISTINCT sq__002.ID
                        FROM sq__002
                        /* join type not adjusted for from: sq__002 to: sq__001 */
                        LEFT OUTER JOIN sq__001
                            /* before on clause with tables: sq__002, sq__001 */
                            ON
                            /* before on-conditions from: sq__002 to: sq__001 */
                            sq__001.ID = sq__002.ID
                            /* after on-conditions from: sq__002 to: sq__001 */
                        WHERE ( sq__001.ID IS NOT NULL
                                AND sq__002.ID IS NOT NULL
                                )
                        ORDER BY sq__002.ID
                        /* after script */""",
                dummySingleAugConvert("active = 1 and purchasePrice < @salesPrice").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ( DATA_COLUMN IN ('blue', 'green', 'red') AND FILTER_COLUMN = 'color' ) )
                        OR ( ( DATA_COLUMN IN ('circle', 'square') AND FILTER_COLUMN = 'shape' ) )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color any of (red, blue, green) or shape any of (circle, square)").toDebugString());

    }

    @Test
    void testDummySingleTableNeg() {

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color != red").toDebugString());

        assertEquals("""
                /* before script */
                WITH
                sq__001 AS ( SELECT
                /* after WITH-SELECT tables: [TBL_DATA] */
                DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT
                /* after WITH-SELECT tables: [TBL_DATA] */
                DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                /* before main statement (with=true) */
                SELECT
                /* after main SELECT */
                DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                /* join type not adjusted for from: TBL_DATA to: sq__001 */
                LEFT OUTER JOIN sq__001
                    /* before on clause with tables: TBL_DATA, sq__001 */
                    ON
                    /* before on-conditions from: TBL_DATA to: sq__001 */
                    sq__001.ID = TBL_DATA.ID
                    /* after on-conditions from: TBL_DATA to: sq__001 */
                /* join type not adjusted for from: TBL_DATA to: sq__002 */
                LEFT OUTER JOIN sq__002
                    /* before on clause with tables: TBL_DATA, sq__002 */
                    ON
                    /* before on-conditions from: TBL_DATA to: sq__002 */
                    sq__002.ID = TBL_DATA.ID
                    /* after on-conditions from: TBL_DATA to: sq__002 */
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                        )
                ORDER BY TBL_DATA.ID
                /* after script */""", dummySingleAugConvert("color != red").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                            OR sq__002.ID IS NULL
                            )
                        AND ( ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                            OR sq__004.ID IS NULL
                            )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color != red and shape != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        OR sq__004.ID IS NULL
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color != red or shape != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IN ('black', 'blue', 'red') AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( sq__001.ID IS NULL
                    OR ( sq__003.ID IS NOT NULL AND sq__002.ID IS NULL )
                        OR sq__003.ID IS NULL
                        OR ( sq__001.ID IS NOT NULL AND sq__004.ID IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color not any of (red, blue, black) or shape != circle").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT TBL_DATA.ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'dateCreated' AND sq__self.FILTER_COLUMN = 'dateUpdated' ) ),
                        sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                        sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                        SELECT DISTINCT TBL_DATA.ID
                        FROM TBL_DATA
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = TBL_DATA.ID
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ID = TBL_DATA.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = TBL_DATA.ID
                        WHERE ( ( sq__002.ID IS NOT NULL AND sq__003.ID IS NOT NULL AND sq__001.ID IS NULL )
                                OR sq__002.ID IS NULL
                                OR sq__003.ID IS NULL
                                )
                        ORDER BY TBL_DATA.ID""",
                dummySingleConvert("NOT dateUpdated > @dateCreated").toDebugString());

        assertEquals(
                """
                        /* before script */
                        WITH
                        sq__001 AS ( SELECT
                        /* after WITH-SELECT tables: [TBL_DATA, TBL_DATA] */
                        DISTINCT TBL_DATA.ID FROM TBL_DATA
                        /* join type not adjusted for from: TBL_DATA to: TBL_DATA */
                        INNER JOIN TBL_DATA sq__self
                            /* before on clause with tables: TBL_DATA, TBL_DATA */
                            ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'dateCreated' AND sq__self.FILTER_COLUMN = 'dateUpdated' ) ),
                        sq__002 AS ( SELECT
                        /* after WITH-SELECT tables: [TBL_DATA] */
                        DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                        sq__003 AS ( SELECT
                        /* after WITH-SELECT tables: [TBL_DATA] */
                        DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                        /* before main statement (with=true) */
                        SELECT
                        /* after main SELECT */
                        DISTINCT TBL_DATA.ID
                        FROM TBL_DATA
                        /* join type not adjusted for from: TBL_DATA to: sq__001 */
                        LEFT OUTER JOIN sq__001
                            /* before on clause with tables: TBL_DATA, sq__001 */
                            ON
                            /* before on-conditions from: TBL_DATA to: sq__001 */
                            sq__001.ID = TBL_DATA.ID
                            /* after on-conditions from: TBL_DATA to: sq__001 */
                        /* join type not adjusted for from: TBL_DATA to: sq__002 */
                        LEFT OUTER JOIN sq__002
                            /* before on clause with tables: TBL_DATA, sq__002 */
                            ON
                            /* before on-conditions from: TBL_DATA to: sq__002 */
                            sq__002.ID = TBL_DATA.ID
                            /* after on-conditions from: TBL_DATA to: sq__002 */
                        /* join type not adjusted for from: TBL_DATA to: sq__003 */
                        LEFT OUTER JOIN sq__003
                            /* before on clause with tables: TBL_DATA, sq__003 */
                            ON
                            /* before on-conditions from: TBL_DATA to: sq__003 */
                            sq__003.ID = TBL_DATA.ID
                            /* after on-conditions from: TBL_DATA to: sq__003 */
                        WHERE ( ( sq__002.ID IS NOT NULL AND sq__003.ID IS NOT NULL AND sq__001.ID IS NULL )
                                OR sq__002.ID IS NULL
                                OR sq__003.ID IS NULL
                                )
                        ORDER BY TBL_DATA.ID
                        /* after script */""",
                dummySingleAugConvert("NOT dateUpdated > @dateCreated").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        OR sq__004.ID IS NULL
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("NOT dateUpdated > 2024-12-13 OR NOT dateCreated > 2024-12-13").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        OR sq__004.ID IS NULL
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("dateUpdated != 2024-12-13 OR dateCreated != 2024-12-13").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '1' AND FILTER_COLUMN = 'active' ) ),
                        sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'active' ) ),
                        sq__003 AS ( SELECT DISTINCT TBL_DATA.ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'purchasePrice' AND sq__self.FILTER_COLUMN = 'salesPrice' ) ),
                        sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'purchasePrice' ) ),
                        sq__005 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'salesPrice' ) )
                        SELECT DISTINCT TBL_DATA.ID
                        FROM TBL_DATA
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = TBL_DATA.ID
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ID = TBL_DATA.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = TBL_DATA.ID
                        LEFT OUTER JOIN sq__004
                            ON sq__004.ID = TBL_DATA.ID
                        LEFT OUTER JOIN sq__005
                            ON sq__005.ID = TBL_DATA.ID
                        WHERE ( ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                                    OR sq__002.ID IS NULL
                                    )
                                AND ( ( sq__004.ID IS NOT NULL AND sq__005.ID IS NOT NULL AND sq__003.ID IS NULL )
                                    OR sq__004.ID IS NULL
                                    OR sq__005.ID IS NULL
                                    )
                                )
                        ORDER BY TBL_DATA.ID""",
                dummySingleConvert("active != 1 and NOT purchasePrice < @salesPrice").toDebugString());

    }

    @Test
    void testDummySingleTableNegStrict() {

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT sq__002.ID
                FROM sq__002
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = sq__002.ID
                WHERE ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                ORDER BY sq__002.ID""", dummySingleConvert("STRICT color != red").toDebugString());

        assertEquals("""
                /* before script */
                WITH
                sq__001 AS ( SELECT
                /* after WITH-SELECT tables: [TBL_DATA] */
                DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT
                /* after WITH-SELECT tables: [TBL_DATA] */
                DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                /* before main statement (with=true) */
                SELECT
                /* after main SELECT */
                DISTINCT sq__002.ID
                FROM sq__002
                /* join type not adjusted for from: sq__002 to: sq__001 */
                LEFT OUTER JOIN sq__001
                    /* before on clause with tables: sq__002, sq__001 */
                    ON
                    /* before on-conditions from: sq__002 to: sq__001 */
                    sq__001.ID = sq__002.ID
                    /* after on-conditions from: sq__002 to: sq__001 */
                WHERE ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                ORDER BY sq__002.ID
                /* after script */""", dummySingleAugConvert("STRICT color != red").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT sq__002.ID
                FROM sq__002
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = sq__002.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = sq__002.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = sq__002.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    AND ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY sq__002.ID""", dummySingleConvert("STRICT color != red and STRICT shape != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("STRICT color != red or STRICT shape != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IN ('black', 'blue', 'red') AND FILTER_COLUMN = 'color' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("color STRICT not any of (red, blue, black) or STRICT shape != circle").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT TBL_DATA.ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'dateCreated' AND sq__self.FILTER_COLUMN = 'dateUpdated' ) ),
                        sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                        sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                        SELECT DISTINCT sq__002.ID
                        FROM sq__002
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = sq__002.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = sq__002.ID
                        WHERE ( sq__002.ID IS NOT NULL AND sq__003.ID IS NOT NULL AND sq__001.ID IS NULL )
                        ORDER BY sq__002.ID""",
                dummySingleConvert("STRICT NOT dateUpdated > @dateCreated").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("STRICT NOT dateUpdated > 2024-12-13 OR STRICT NOT dateCreated > 2024-12-13").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA.ID
                FROM TBL_DATA
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", dummySingleConvert("STRICT dateUpdated != 2024-12-13 OR STRICT dateCreated != 2024-12-13").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = '1' AND FILTER_COLUMN = 'active' ) ),
                        sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'active' ) ),
                        sq__003 AS ( SELECT DISTINCT TBL_DATA.ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'purchasePrice' AND sq__self.FILTER_COLUMN = 'salesPrice' ) ),
                        sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'purchasePrice' ) ),
                        sq__005 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'salesPrice' ) )
                        SELECT DISTINCT sq__002.ID
                        FROM sq__002
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = sq__002.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = sq__002.ID
                        LEFT OUTER JOIN sq__004
                            ON sq__004.ID = sq__002.ID
                        LEFT OUTER JOIN sq__005
                            ON sq__005.ID = sq__002.ID
                        WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                            AND ( sq__004.ID IS NOT NULL AND sq__005.ID IS NOT NULL AND sq__003.ID IS NULL )
                                )
                        ORDER BY sq__002.ID""",
                dummySingleConvert("STRICT active != 1 and STRICT NOT purchasePrice < @salesPrice").toDebugString());

    }

    @Test
    void testDummySingleTableInline() {

        assertEquals("""
                SELECT DISTINCT ID FROM TBL_DATA WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ) ORDER BY TBL_DATA.ID""",
                dummySingleInlineConvert("color = red").toDebugString());

        assertEquals("/* before script */ /* before main statement (with=false) */ "
                + "SELECT /* after main SELECT */ DISTINCT ID FROM TBL_DATA WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ) "
                + "ORDER BY TBL_DATA.ID /* after script */", dummySingleAugInlineConvert("color = red").toDebugString());

        assertEquals("SELECT DISTINCT ID FROM TBL_DATA WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ) ORDER BY TBL_DATA.ID",
                dummySingleInlineConvert("color = red").toDebugString());

        assertEquals("/* before script */ /* before main statement (with=false) */ "
                + "SELECT /* after main SELECT */ DISTINCT ID FROM TBL_DATA WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ) "
                + "ORDER BY TBL_DATA.ID /* after script */", dummySingleAugInlineConvert("color = red").toDebugString());

        assertEquals(
                "WITH sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ), "
                        + "sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ) "
                        + "SELECT DISTINCT sq__001.ID FROM sq__001 LEFT OUTER JOIN sq__002 ON sq__002.ID = sq__001.ID "
                        + "WHERE ( sq__001.ID IS NOT NULL AND sq__002.ID IS NOT NULL ) ORDER BY sq__001.ID",
                dummySingleInlineConvert("color = red and shape = circle").toDebugString());

        assertEquals("/* before script */ WITH sq__001 AS ( SELECT /* after WITH-SELECT tables: [TBL_DATA] */ DISTINCT ID "
                + "FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ), "
                + "sq__002 AS ( SELECT /* after WITH-SELECT tables: [TBL_DATA] */ DISTINCT ID FROM TBL_DATA "
                + "WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ) "
                + "/* before main statement (with=true) */ SELECT /* after main SELECT */ DISTINCT sq__001.ID "
                + "FROM sq__001 /* join type not adjusted for from: sq__001 to: sq__002 */ "
                + "LEFT OUTER JOIN sq__002 /* before on clause with tables: sq__001, sq__002 */ ON /* before on-conditions from: sq__001 to: sq__002 */ "
                + "sq__002.ID = sq__001.ID /* after on-conditions from: sq__001 to: sq__002 */ "
                + "WHERE ( sq__001.ID IS NOT NULL AND sq__002.ID IS NOT NULL ) ORDER BY sq__001.ID /* after script */",
                dummySingleAugInlineConvert("color = red and shape = circle").toDebugString());

        assertEquals(
                "WITH sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ), "
                        + "sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ) "
                        + "SELECT DISTINCT sq__001.ID FROM sq__001 LEFT OUTER JOIN sq__002 ON sq__002.ID = sq__001.ID "
                        + "WHERE ( sq__001.ID IS NOT NULL AND sq__002.ID IS NOT NULL ) ORDER BY sq__001.ID",
                dummySingleInlineConvert("color = red and shape = circle").toDebugString());

        assertEquals(
                "/* before script */ WITH sq__001 AS ( SELECT /* after WITH-SELECT tables: [TBL_DATA] */ DISTINCT ID FROM TBL_DATA "
                        + "WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ), "
                        + "sq__002 AS ( SELECT /* after WITH-SELECT tables: [TBL_DATA] */ DISTINCT ID FROM TBL_DATA "
                        + "WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ) /* before main statement (with=true) */ "
                        + "SELECT /* after main SELECT */ DISTINCT sq__001.ID FROM sq__001 /* join type not adjusted for from: sq__001 to: sq__002 */ "
                        + "LEFT OUTER JOIN sq__002 /* before on clause with tables: sq__001, sq__002 */ "
                        + "ON /* before on-conditions from: sq__001 to: sq__002 */ sq__002.ID = sq__001.ID /* after on-conditions from: sq__001 to: sq__002 */ "
                        + "WHERE ( sq__001.ID IS NOT NULL AND sq__002.ID IS NOT NULL ) ORDER BY sq__001.ID /* after script */",
                dummySingleAugInlineConvert("color = red and shape = circle").toDebugString());

    }

    @Test
    void testStandardSingleTablePos() {

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( COLOR = 'red' )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color = red").toDebugString());

        assertEquals("""
                /* before script */
                /* before main statement (with=false) */
                SELECT
                /* after main SELECT */
                DISTINCT ID
                FROM TBL_DATA
                WHERE ( COLOR = 'red' )
                ORDER BY TBL_DATA.ID
                /* after script */""", standardSingleAugConvert("color = red").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR = 'red' )
                        AND ( SHAPE = 'circle' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color = red and shape = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR = 'red' )
                        OR ( SHAPE = 'circle' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color = red or shape = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR IN ('black', 'blue', 'red') )
                        OR ( SHAPE = 'circle' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color any of (red, blue, black) or shape = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( DATE_CREATED < UPDATE_TIME )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("dateUpdated > @dateCreated").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( DATE_CREATED > DATE '2024-12-13' )
                        OR ( UPDATE_TIME >= TIMESTAMP '2024-12-14 00:00:00' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("dateUpdated > 2024-12-13 OR dateCreated > 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( DATE_CREATED = DATE '2024-12-13' )
                        OR ( (UPDATE_TIME >= TIMESTAMP '2024-12-13 00:00:00' AND UPDATE_TIME < TIMESTAMP '2024-12-14 00:00:00') )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("dateUpdated = 2024-12-13 OR dateCreated = 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ACTIVE = TRUE )
                        AND ( PURCHASE_PRICE < SALES_PRICE )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("active = 1 and purchasePrice < @salesPrice").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( COLOR IS NULL )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color IS UNKNOWN").toDebugString());

    }

    @Test
    void testStandardSingleTableNeg() {

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR <> 'red' )
                        OR ( COLOR IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color != red").toDebugString());

        assertEquals("""
                /* before script */
                /* before main statement (with=false) */
                SELECT
                /* after main SELECT */
                DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR <> 'red' )
                        OR ( COLOR IS NULL )
                        )
                ORDER BY TBL_DATA.ID
                /* after script */""", standardSingleAugConvert("color != red").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( SHAPE <> 'circle' )
                        AND ( ( COLOR <> 'red' )
                            OR ( COLOR IS NULL )
                            )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color != red and shape != circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR <> 'red' )
                        OR ( COLOR IS NULL )
                    OR ( SHAPE <> 'circle' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color != red or shape != circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR IS NULL )
                    OR ( SHAPE <> 'circle' )
                        OR ( COLOR NOT IN ('black', 'blue', 'red') )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color not any of (red, blue, black) or shape != circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( NOT DATE_CREATED < UPDATE_TIME )
                        OR ( DATE_CREATED IS NULL )
                        OR ( UPDATE_TIME IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("NOT dateUpdated > @dateCreated").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( NOT DATE_CREATED > DATE '2024-12-13' )
                        OR ( DATE_CREATED IS NULL )
                    OR ( NOT UPDATE_TIME >= TIMESTAMP '2024-12-14 00:00:00' )
                        OR ( UPDATE_TIME IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("NOT dateUpdated > 2024-12-13 OR NOT dateCreated > 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( DATE_CREATED <> DATE '2024-12-13' )
                        OR ( DATE_CREATED IS NULL )
                    OR ( (UPDATE_TIME < TIMESTAMP '2024-12-13 00:00:00' OR UPDATE_TIME >= TIMESTAMP '2024-12-14 00:00:00') )
                        OR ( UPDATE_TIME IS NULL )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("dateUpdated != 2024-12-13 OR dateCreated != 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ( ACTIVE <> TRUE )
                            OR ( ACTIVE IS NULL )
                            )
                        AND ( ( NOT PURCHASE_PRICE < SALES_PRICE )
                            OR ( PURCHASE_PRICE IS NULL )
                            OR ( SALES_PRICE IS NULL )
                            )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("active != 1 and NOT purchasePrice < @salesPrice").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( COLOR IS NOT NULL )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color IS NOT UNKNOWN").toDebugString());

    }

    @Test
    void testStandardSingleTableNegStrict() {

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( COLOR <> 'red' )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT color != red").toDebugString());

        assertEquals("""
                /* before script */
                /* before main statement (with=false) */
                SELECT
                /* after main SELECT */
                DISTINCT ID
                FROM TBL_DATA
                WHERE ( COLOR <> 'red' )
                ORDER BY TBL_DATA.ID
                /* after script */""", standardSingleAugConvert("STRICT color != red").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR <> 'red' )
                    AND ( SHAPE <> 'circle' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT color != red and STRICT shape != circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( COLOR <> 'red' )
                    OR ( SHAPE <> 'circle' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT color != red or STRICT shape != circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( SHAPE <> 'circle' )
                        OR ( COLOR NOT IN ('black', 'blue', 'red') )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("color STRICT not any of (red, blue, black) or STRICT shape != circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( NOT DATE_CREATED < UPDATE_TIME )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT NOT dateUpdated > @dateCreated").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( NOT DATE_CREATED > DATE '2024-12-13' )
                    OR ( NOT UPDATE_TIME >= TIMESTAMP '2024-12-14 00:00:00' )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT NOT dateUpdated > 2024-12-13 OR STRICT NOT dateCreated > 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( DATE_CREATED <> DATE '2024-12-13' )
                    OR ( (UPDATE_TIME < TIMESTAMP '2024-12-13 00:00:00' OR UPDATE_TIME >= TIMESTAMP '2024-12-14 00:00:00') )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT dateUpdated != 2024-12-13 OR STRICT dateCreated != 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA
                WHERE ( ( ACTIVE <> TRUE )
                    AND ( NOT PURCHASE_PRICE < SALES_PRICE )
                        )
                ORDER BY TBL_DATA.ID""", standardSingleConvert("STRICT active != 1 and STRICT NOT purchasePrice < @salesPrice").toDebugString());

    }

    @Test
    void testDummyMultiTablePos() {

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_DATA1
                WHERE ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) )
                ORDER BY TBL_DATA1.ID""", dummyMultiConvert("color.1 = red").toDebugString());

        assertEquals("""
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                INNER JOIN TBL_DATA1
                    ON TBL_DATA1.ID = TBL_DATA2.ID2
                    AND ( ( TBL_DATA1.DATA_COLUMN = 'red' AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                WHERE ( ( ( TBL_DATA1.DATA_COLUMN = 'red' AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                        AND ( ( TBL_DATA2.DATA_COLUMN = 'circle' AND TBL_DATA2.FILTER_COLUMN = 'shape' ) )
                        )
                ORDER BY ID""", dummyMultiConvert("color.1 = red and shape.2 = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN TBL_DATA3
                    ON TBL_DATA3.ID3 = TBL_DATA2.ID2
                    AND ( ( TBL_DATA3.DATA_COLUMN = 'circle' AND TBL_DATA3.FILTER_COLUMN = 'shape' ) )
                WHERE ( ( ( TBL_DATA2.DATA_COLUMN = 'red' AND TBL_DATA2.FILTER_COLUMN = 'color' ) )
                        OR ( ( TBL_DATA3.DATA_COLUMN = 'circle' AND TBL_DATA3.FILTER_COLUMN = 'shape' ) )
                        )
                ORDER BY ID""", dummyMultiConvert("color.2 = red or shape.3 = circle").toDebugString());

        assertEquals("""
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN TBL_DATA1
                    ON TBL_DATA1.ID = TBL_DATA2.ID2
                    AND ( ( TBL_DATA1.DATA_COLUMN IN ('black', 'blue', 'red') AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                LEFT OUTER JOIN TBL_DATA3
                    ON TBL_DATA3.ID3 = TBL_DATA2.ID2
                    AND ( ( TBL_DATA3.DATA_COLUMN = 'circle' AND TBL_DATA3.FILTER_COLUMN = 'shape' ) )
                WHERE ( ( ( TBL_DATA1.DATA_COLUMN IN ('black', 'blue', 'red') AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                        OR ( ( TBL_DATA3.DATA_COLUMN = 'circle' AND TBL_DATA3.FILTER_COLUMN = 'shape' ) )
                        )
                ORDER BY ID""", dummyMultiConvert("color.1 any of (red, blue, black) or shape.3 = circle").toDebugString());

        assertEquals(
                """
                        SELECT DISTINCT TBL_DATA2.ID2 AS ID
                        FROM TBL_DATA2
                        INNER JOIN TBL_DATA1
                            ON TBL_DATA1.ID = TBL_DATA2.ID2
                            AND ( ( TBL_DATA1.DATA_COLUMN IS NOT NULL AND TBL_DATA1.FILTER_COLUMN = 'dateUpdated' ) )
                        WHERE ( ( TBL_DATA2.DATA_COLUMN < TBL_DATA1.DATA_COLUMN AND TBL_DATA2.FILTER_COLUMN = 'dateCreated' AND TBL_DATA1.FILTER_COLUMN = 'dateUpdated' ) )
                        ORDER BY ID""",
                dummyMultiConvert("dateUpdated.1 > @dateCreated.2").toDebugString());

        assertEquals("""
                SELECT DISTINCT TBL_DATA1.ID
                FROM TBL_DATA1
                INNER JOIN TBL_DATA3
                    ON TBL_DATA3.ID3 = TBL_DATA1.ID
                    AND ( ( TBL_DATA3.DATA_COLUMN > '2024-12-13' AND TBL_DATA3.FILTER_COLUMN = 'dateUpdated' ) )
                WHERE ( ( ( TBL_DATA1.DATA_COLUMN > '2024-12-13' AND TBL_DATA1.FILTER_COLUMN = 'dateCreated' ) )
                        AND ( ( TBL_DATA3.DATA_COLUMN > '2024-12-13' AND TBL_DATA3.FILTER_COLUMN = 'dateUpdated' ) )
                        )
                ORDER BY TBL_DATA1.ID""", dummyMultiConvert("dateUpdated.3 > 2024-12-13 AND dateCreated.1 > 2024-12-13").toDebugString());

        assertEquals("""
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN TBL_DATA1
                    ON TBL_DATA1.ID = TBL_DATA2.ID2
                    AND ( ( TBL_DATA1.DATA_COLUMN = '2024-12-13' AND TBL_DATA1.FILTER_COLUMN = 'dateCreated' ) )
                LEFT OUTER JOIN TBL_DATA3
                    ON TBL_DATA3.ID3 = TBL_DATA2.ID2
                    AND ( ( TBL_DATA3.DATA_COLUMN = '2024-12-13' AND TBL_DATA3.FILTER_COLUMN = 'dateUpdated' ) )
                WHERE ( ( ( TBL_DATA1.DATA_COLUMN = '2024-12-13' AND TBL_DATA1.FILTER_COLUMN = 'dateCreated' ) )
                        OR ( ( TBL_DATA3.DATA_COLUMN = '2024-12-13' AND TBL_DATA3.FILTER_COLUMN = 'dateUpdated' ) )
                        )
                ORDER BY ID""", dummyMultiConvert("dateUpdated.3 = 2024-12-13 OR dateCreated.1 = 2024-12-13").toDebugString());

        assertEquals(
                """
                        SELECT DISTINCT TBL_DATA2.ID2 AS ID
                        FROM TBL_DATA2
                        INNER JOIN TBL_DATA1
                            ON TBL_DATA1.ID = TBL_DATA2.ID2
                            AND ( ( TBL_DATA1.DATA_COLUMN IS NOT NULL AND TBL_DATA1.FILTER_COLUMN = 'purchasePrice' ) )
                        LEFT OUTER JOIN TBL_DATA4
                            ON TBL_DATA4.ID4 = TBL_DATA2.ID2
                            AND ( TBL_DATA4.PREMIUM = TRUE )
                        WHERE ( ( TBL_DATA4.PREMIUM = TRUE )
                                AND ( ( TBL_DATA1.DATA_COLUMN < TBL_DATA2.DATA_COLUMN AND TBL_DATA1.FILTER_COLUMN = 'purchasePrice' AND TBL_DATA2.FILTER_COLUMN = 'salesPrice' ) )
                                )
                        ORDER BY ID""",
                dummyMultiConvert("premium = 1 and purchasePrice.1 < @salesPrice.2").toDebugString());

    }

    @Test
    void testDummyMultiTableNeg() {

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                        )
                ORDER BY ID""", dummyMultiConvert("color.1 != red").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                            OR sq__002.ID IS NULL
                            )
                        AND ( ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                            OR sq__004.ID IS NULL
                            )
                        )
                ORDER BY ID""", dummyMultiConvert("color.1 != red and shape.2 != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        OR sq__004.ID IS NULL
                        )
                ORDER BY ID""", dummyMultiConvert("color.2 != red or shape.3 != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IN ('black', 'blue', 'red') AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( sq__001.ID IS NULL
                    OR ( sq__003.ID IS NOT NULL AND sq__002.ID IS NULL )
                        OR sq__003.ID IS NULL
                        OR ( sq__001.ID IS NOT NULL AND sq__004.ID IS NULL )
                        )
                ORDER BY ID""", dummyMultiConvert("color.1 not any of (red, blue, black) or shape.3 != circle").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT TBL_DATA2.ID2 AS ID FROM TBL_DATA2 INNER JOIN TBL_DATA1 ON TBL_DATA2.ID2 = TBL_DATA1.ID WHERE ( TBL_DATA2.DATA_COLUMN < TBL_DATA1.DATA_COLUMN AND TBL_DATA2.FILTER_COLUMN = 'dateCreated' AND TBL_DATA1.FILTER_COLUMN = 'dateUpdated' ) ),
                        sq__002 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                        sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                        SELECT DISTINCT TBL_DATA2.ID2 AS ID
                        FROM TBL_DATA2
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = TBL_DATA2.ID2
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ID = TBL_DATA2.ID2
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = TBL_DATA2.ID2
                        WHERE ( ( sq__002.ID IS NOT NULL AND sq__003.ID IS NOT NULL AND sq__001.ID IS NULL )
                                OR sq__002.ID IS NULL
                                OR sq__003.ID IS NULL
                                )
                        ORDER BY ID""",
                dummyMultiConvert("NOT dateUpdated.1 > @dateCreated.2").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                            OR sq__002.ID IS NULL
                            )
                        AND ( ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                            OR sq__004.ID IS NULL
                            )
                        )
                ORDER BY ID""", dummyMultiConvert("NOT dateUpdated.3 > 2024-12-13 AND NOT dateCreated.1 > 2024-12-13").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR sq__002.ID IS NULL
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        OR sq__004.ID IS NULL
                        )
                ORDER BY ID""", dummyMultiConvert("dateUpdated.3 != 2024-12-13 OR dateCreated.1 != 2024-12-13").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__002 AS ( SELECT ID4 AS ID FROM TBL_DATA4 WHERE PREMIUM IS NOT NULL ),
                        sq__003 AS ( SELECT DISTINCT TBL_DATA1.ID FROM TBL_DATA1 INNER JOIN TBL_DATA2 ON TBL_DATA1.ID = TBL_DATA2.ID2 WHERE ( TBL_DATA1.DATA_COLUMN < TBL_DATA2.DATA_COLUMN AND TBL_DATA1.FILTER_COLUMN = 'purchasePrice' AND TBL_DATA2.FILTER_COLUMN = 'salesPrice' ) ),
                        sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'purchasePrice' ) ),
                        sq__005 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'salesPrice' ) )
                        SELECT DISTINCT TBL_DATA2.ID2 AS ID
                        FROM TBL_DATA2
                        LEFT OUTER JOIN TBL_DATA4
                            ON TBL_DATA4.ID4 = TBL_DATA2.ID2
                            AND ( TBL_DATA4.PREMIUM <> TRUE )
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ID = TBL_DATA2.ID2
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = TBL_DATA2.ID2
                        LEFT OUTER JOIN sq__004
                            ON sq__004.ID = TBL_DATA2.ID2
                        LEFT OUTER JOIN sq__005
                            ON sq__005.ID = TBL_DATA2.ID2
                        WHERE ( ( ( TBL_DATA4.PREMIUM <> TRUE )
                                    OR ( sq__002.ID IS NULL )
                                    )
                                AND ( ( sq__004.ID IS NOT NULL AND sq__005.ID IS NOT NULL AND sq__003.ID IS NULL )
                                    OR sq__004.ID IS NULL
                                    OR sq__005.ID IS NULL
                                    )
                                )
                        ORDER BY ID""",
                dummyMultiConvert("premium != 1 and NOT purchasePrice.1 < @salesPrice.2").toDebugString());

    }

    @Test
    void testDummyMultiTableNegStrict() {

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT sq__002.ID
                FROM sq__002
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = sq__002.ID
                WHERE ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                ORDER BY sq__002.ID""", dummyMultiConvert("STRICT color.1 != red").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT sq__004.ID
                FROM sq__004
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = sq__004.ID
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = sq__004.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = sq__004.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    AND ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY sq__004.ID""", dummyMultiConvert("STRICT color.1 != red and STRICT shape.2 != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' ) ),
                sq__002 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__004 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY ID""", dummyMultiConvert("STRICT color.2 != red or STRICT shape.3 != circle").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' ) ),
                sq__002 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'shape' ) ),
                sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IN ('black', 'blue', 'red') AND FILTER_COLUMN = 'color' ) ),
                sq__004 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                        OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY ID""", dummyMultiConvert("color.1 STRICT not any of (red, blue, black) or STRICT shape.3 != circle").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT TBL_DATA2.ID2 AS ID FROM TBL_DATA2 INNER JOIN TBL_DATA1 ON TBL_DATA2.ID2 = TBL_DATA1.ID WHERE ( TBL_DATA2.DATA_COLUMN < TBL_DATA1.DATA_COLUMN AND TBL_DATA2.FILTER_COLUMN = 'dateCreated' AND TBL_DATA1.FILTER_COLUMN = 'dateUpdated' ) ),
                        sq__002 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                        sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                        SELECT DISTINCT sq__002.ID
                        FROM sq__002
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = sq__002.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = sq__002.ID
                        WHERE ( sq__002.ID IS NOT NULL AND sq__003.ID IS NOT NULL AND sq__001.ID IS NULL )
                        ORDER BY sq__002.ID""",
                dummyMultiConvert("STRICT NOT dateUpdated.1 > @dateCreated.2").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN > '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT sq__002.ID
                FROM sq__002
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = sq__002.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = sq__002.ID
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = sq__002.ID
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    AND ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY sq__002.ID""", dummyMultiConvert("STRICT NOT dateUpdated.3 > 2024-12-13 AND STRICT NOT dateCreated.1 > 2024-12-13").toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__002 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateCreated' ) ),
                sq__003 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN = '2024-12-13' AND FILTER_COLUMN = 'dateUpdated' ) ),
                sq__004 AS ( SELECT DISTINCT ID3 AS ID FROM TBL_DATA3 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'dateUpdated' ) )
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_DATA2.ID2
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_DATA2.ID2
                WHERE ( ( sq__002.ID IS NOT NULL AND sq__001.ID IS NULL )
                    OR ( sq__004.ID IS NOT NULL AND sq__003.ID IS NULL )
                        )
                ORDER BY ID""", dummyMultiConvert("STRICT dateUpdated.3 != 2024-12-13 OR STRICT dateCreated.1 != 2024-12-13").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__002 AS ( SELECT DISTINCT TBL_DATA1.ID FROM TBL_DATA1 INNER JOIN TBL_DATA2 ON TBL_DATA1.ID = TBL_DATA2.ID2 WHERE ( TBL_DATA1.DATA_COLUMN < TBL_DATA2.DATA_COLUMN AND TBL_DATA1.FILTER_COLUMN = 'purchasePrice' AND TBL_DATA2.FILTER_COLUMN = 'salesPrice' ) ),
                        sq__003 AS ( SELECT DISTINCT ID FROM TBL_DATA1 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'purchasePrice' ) ),
                        sq__004 AS ( SELECT DISTINCT ID2 AS ID FROM TBL_DATA2 WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'salesPrice' ) )
                        SELECT DISTINCT sq__004.ID
                        FROM sq__004
                        LEFT OUTER JOIN TBL_DATA4
                            ON TBL_DATA4.ID4 = sq__004.ID
                            AND ( TBL_DATA4.PREMIUM <> TRUE )
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ID = sq__004.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = sq__004.ID
                        WHERE ( ( TBL_DATA4.PREMIUM <> TRUE )
                            AND ( sq__003.ID IS NOT NULL AND sq__004.ID IS NOT NULL AND sq__002.ID IS NULL )
                                )
                        ORDER BY sq__004.ID""",
                dummyMultiConvert("STRICT premium != 1 and STRICT NOT purchasePrice.1 < @salesPrice.2").toDebugString());

    }

    @Test
    void testStandardMultiTable() {

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_BASE
                WHERE ( ( C_COLOR = 'red' )
                        AND ( C_COUNTRY = 'Germany' )
                        AND ( C_SHAPE = 'circle' )
                        )
                ORDER BY TBL_BASE.ID""", standardMultiConvert("(color=red and shape=circle) and country=Germany").toDebugString());

        assertEquals("""
                WITH
                sq__002 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD = 'burger' ),
                sq__003 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD IS NOT NULL )
                SELECT DISTINCT TBL_BASE.ID
                FROM TBL_BASE
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_BASE.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_BASE.ID
                WHERE ( ( TBL_BASE.C_COUNTRY = 'Germany' )
                        AND ( ( sq__003.ID IS NOT NULL AND sq__002.ID IS NULL )
                            OR sq__003.ID IS NULL
                            )
                        )
                ORDER BY TBL_BASE.ID""", standardMultiConvert("country=Germany and fav.food != burger").toDebugString());

        assertEquals("""
                WITH
                sq__002 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD = 'burger' ),
                sq__003 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD IS NOT NULL )
                SELECT DISTINCT TBL_BASE.ID
                FROM TBL_BASE
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = TBL_BASE.ID
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = TBL_BASE.ID
                WHERE ( ( TBL_BASE.C_COUNTRY = 'Germany' )
                    AND ( sq__003.ID IS NOT NULL AND sq__002.ID IS NULL )
                        )
                ORDER BY TBL_BASE.ID""", standardMultiConvert("country=Germany and STRICT fav.food != burger").toDebugString());

        assertEquals("""
                WITH
                sq__004 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD = 'burger' ),
                sq__005 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD IS NOT NULL )
                SELECT DISTINCT TBL_BASE.ID
                FROM TBL_BASE
                LEFT OUTER JOIN TBL_SPECIAL
                    ON TBL_SPECIAL.ID = TBL_BASE.ID
                    AND ( ( TBL_SPECIAL.VISIT_DURATION > 1000 )
                        OR ( TBL_SPECIAL.VISIT_SPENT IS NOT NULL )
                    )
                LEFT OUTER JOIN sq__004
                    ON sq__004.ID = TBL_BASE.ID
                LEFT OUTER JOIN sq__005
                    ON sq__005.ID = TBL_BASE.ID
                WHERE ( ( TBL_BASE.C_INCOME < TBL_SPECIAL.VISIT_SPENT )
                        OR ( TBL_SPECIAL.VISIT_DURATION > 1000 )
                        OR ( ( TBL_BASE.C_COUNTRY = 'Germany' )
                            AND ( ( sq__005.ID IS NOT NULL AND sq__004.ID IS NULL )
                                OR sq__005.ID IS NULL
                                )
                            )
                        )
                ORDER BY TBL_BASE.ID""",
                standardMultiConvert("(country=Germany and fav.food != burger) or vduration > 1000 or vspent > @income").toDebugString());

        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT TBL_BASE.ID FROM TBL_BASE INNER JOIN TBL_SPECIAL ON TBL_BASE.ID = TBL_SPECIAL.ID WHERE TBL_BASE.C_INCOME < TBL_SPECIAL.VISIT_SPENT ),
                        sq__002 AS ( SELECT ID FROM TBL_BASE WHERE C_INCOME IS NOT NULL ),
                        sq__003 AS ( SELECT DISTINCT ID FROM TBL_SPECIAL WHERE VISIT_SPENT IS NOT NULL ),
                        sq__006 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD = 'burger' ),
                        sq__007 AS ( SELECT DISTINCT FID AS ID FROM TBL_FAV WHERE FAV_FOOD IS NOT NULL )
                        SELECT DISTINCT TBL_BASE.ID
                        FROM TBL_BASE
                        LEFT OUTER JOIN TBL_SPECIAL
                            ON TBL_SPECIAL.ID = TBL_BASE.ID
                            AND ( TBL_SPECIAL.VISIT_DURATION > 1000 )
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ID = TBL_BASE.ID
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ID = TBL_BASE.ID
                        LEFT OUTER JOIN sq__003
                            ON sq__003.ID = TBL_BASE.ID
                        LEFT OUTER JOIN sq__006
                            ON sq__006.ID = TBL_BASE.ID
                        LEFT OUTER JOIN sq__007
                            ON sq__007.ID = TBL_BASE.ID
                        WHERE ( ( sq__002.ID IS NOT NULL AND sq__003.ID IS NOT NULL AND sq__001.ID IS NULL )
                                OR ( TBL_SPECIAL.VISIT_DURATION > 1000 )
                                OR ( ( TBL_BASE.C_COUNTRY = 'Germany' )
                                AND ( sq__007.ID IS NOT NULL AND sq__006.ID IS NULL )
                                    )
                                )
                        ORDER BY TBL_BASE.ID""",
                standardMultiConvert("(country=Germany and STRICT fav.food != burger) or vduration > 1000 or STRICT NOT (vspent > @income)").toDebugString());

    }

    @Test
    void testDummyMultiTableEnforcePrimary() {

        assertEquals("""
                SELECT DISTINCT ID1 AS ID
                FROM TBL_DATA
                WHERE ( ( ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' AND TENANT = 'FOOBAR' ) )
                        OR ( ( DATA_COLUMN = 'circle' AND FILTER_COLUMN = 'shape' AND TENANT = 'FOOBAR' ) )
                        )
                ORDER BY ID""", dummyMultiPrimaryFilteredConvert("color.2 = red or shape.2 = circle", false).toDebugString());

        assertEquals("""
                SELECT DISTINCT PRIM_TBL.IDP AS ID
                FROM PRIM_TBL
                LEFT OUTER JOIN TBL_DATA
                    ON TBL_DATA.ID1 = PRIM_TBL.IDP AND PRIM_TBL.TENANT = 'FOOBAR'
                    AND ( ( ( TBL_DATA.DATA_COLUMN = 'red' AND TBL_DATA.FILTER_COLUMN = 'color' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                        OR ( ( TBL_DATA.DATA_COLUMN = 'circle' AND TBL_DATA.FILTER_COLUMN = 'shape' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                    )
                WHERE ( ( ( ( TBL_DATA.DATA_COLUMN = 'red' AND TBL_DATA.FILTER_COLUMN = 'color' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                        OR ( ( TBL_DATA.DATA_COLUMN = 'circle' AND TBL_DATA.FILTER_COLUMN = 'shape' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                        ) ) AND PRIM_TBL.TENANT = 'FOOBAR'
                ORDER BY ID""", dummyMultiPrimaryFilteredConvert("color.2 = red or shape.2 = circle", true).toDebugString());

    }

    @Test
    void testTableConditionOnInnerJoin() {
        assertEquals(
                """
                        WITH
                        sq__001 AS ( SELECT DISTINCT ID AS ALTERNATIVE_ID FROM TBL_DATA WHERE ( DATA_COLUMN = '1' AND FILTER_COLUMN = 'active' AND FILTER1 = 'xyz' ) ),
                        sq__002 AS ( SELECT DISTINCT TBL_DATA.ID AS ALTERNATIVE_ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER1 = 'xyz' WHERE ( TBL_DATA.DATA_COLUMN < sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'purchasePrice' AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER_COLUMN = 'salesPrice' AND sq__self.FILTER1 = 'xyz' ) )
                        SELECT DISTINCT sq__002.ALTERNATIVE_ID
                        FROM sq__002
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ALTERNATIVE_ID = sq__002.ALTERNATIVE_ID
                        WHERE ( sq__001.ALTERNATIVE_ID IS NOT NULL
                                AND sq__002.ALTERNATIVE_ID IS NOT NULL
                                )
                        ORDER BY sq__002.ALTERNATIVE_ID""",
                dummySingleFilteredConvertWithAlternativeId("active = 1 and purchasePrice < @salesPrice").toDebugString());

        assertEquals("""
                SELECT DISTINCT PRIM_TBL.IDP AS ID
                FROM PRIM_TBL
                LEFT OUTER JOIN TBL_DATA
                    ON TBL_DATA.ID1 = PRIM_TBL.IDP AND PRIM_TBL.TENANT = 'FOOBAR'
                    AND ( ( ( TBL_DATA.DATA_COLUMN = 'red' AND TBL_DATA.FILTER_COLUMN = 'color' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                        OR ( ( TBL_DATA.DATA_COLUMN = 'circle' AND TBL_DATA.FILTER_COLUMN = 'shape' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                    )
                WHERE ( ( ( ( TBL_DATA.DATA_COLUMN = 'red' AND TBL_DATA.FILTER_COLUMN = 'color' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                        OR ( ( TBL_DATA.DATA_COLUMN = 'circle' AND TBL_DATA.FILTER_COLUMN = 'shape' AND TBL_DATA.TENANT = 'FOOBAR' ) )
                        ) ) AND PRIM_TBL.TENANT = 'FOOBAR'
                ORDER BY ID""", dummyMultiPrimaryFilteredConvert("color.2 = red or shape.2 = circle", true).toDebugString());

        assertEquals("""
                WITH
                sq__001 AS ( SELECT DISTINCT ID1 AS ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'red' AND FILTER_COLUMN = 'color' AND TENANT = 'FOOBAR' ) ),
                sq__002 AS ( SELECT DISTINCT ID1 AS ID FROM TBL_DATA WHERE ( DATA_COLUMN IS NOT NULL AND FILTER_COLUMN = 'color' AND TENANT = 'FOOBAR' ) ),
                sq__003 AS ( SELECT DISTINCT ID1 AS ID FROM TBL_DATA WHERE ( DATA_COLUMN = 'blue' AND FILTER_COLUMN = 'color' AND TENANT = 'FOOBAR' ) )
                SELECT DISTINCT PRIM_TBL.IDP AS ID
                FROM PRIM_TBL
                LEFT OUTER JOIN sq__001
                    ON sq__001.ID = PRIM_TBL.IDP AND PRIM_TBL.TENANT = 'FOOBAR'
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = PRIM_TBL.IDP AND PRIM_TBL.TENANT = 'FOOBAR'
                LEFT OUTER JOIN sq__003
                    ON sq__003.ID = PRIM_TBL.IDP AND PRIM_TBL.TENANT = 'FOOBAR'
                WHERE ( sq__001.ID IS NOT NULL
                        OR sq__002.ID IS NULL
                        OR ( sq__003.ID IS NOT NULL
                            AND ( ( PRIM_TBL.DATA_COLUMN = 'circle' AND PRIM_TBL.FILTER_COLUMN = 'shape' AND PRIM_TBL.TENANT = 'FOOBAR' ) )
                            )
                        )
                ORDER BY ID""",
                dummyMultiPrimaryFilteredConvert("color.2 = red or (shape.1 = circle and color.2 = blue) OR color.2 IS UNKNOWN", true).toDebugString());

    }

    @Test
    void testRefJoin() {
        assertEquals(
                """
                        WITH
                        bq__start AS ( SELECT TBL_DATA.ID AS ALTERNATIVE_ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER1 = 'xyz' WHERE ( ( TBL_DATA.DATA_COLUMN = sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'color' AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER_COLUMN = 'favColor' AND sq__self.FILTER1 = 'xyz' ) ) OR ( ( TBL_DATA.DATA_COLUMN = sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'favSport' AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER_COLUMN = 'sport' AND sq__self.FILTER1 = 'xyz' ) ) ),
                        sq__001 AS ( SELECT DISTINCT TBL_DATA.ID AS ALTERNATIVE_ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER1 = 'xyz' WHERE ( TBL_DATA.DATA_COLUMN = sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'color' AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER_COLUMN = 'favColor' AND sq__self.FILTER1 = 'xyz' ) ),
                        sq__002 AS ( SELECT DISTINCT TBL_DATA.ID AS ALTERNATIVE_ID FROM TBL_DATA INNER JOIN TBL_DATA sq__self ON TBL_DATA.ID = sq__self.ID AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER1 = 'xyz' WHERE ( TBL_DATA.DATA_COLUMN = sq__self.DATA_COLUMN AND TBL_DATA.FILTER_COLUMN = 'favSport' AND TBL_DATA.FILTER1 = 'xyz' AND sq__self.FILTER_COLUMN = 'sport' AND sq__self.FILTER1 = 'xyz' ) )
                        SELECT DISTINCT bq__start.ALTERNATIVE_ID
                        FROM bq__start
                        LEFT OUTER JOIN sq__001
                            ON sq__001.ALTERNATIVE_ID = bq__start.ALTERNATIVE_ID
                        LEFT OUTER JOIN sq__002
                            ON sq__002.ALTERNATIVE_ID = bq__start.ALTERNATIVE_ID
                        WHERE ( sq__001.ALTERNATIVE_ID IS NOT NULL
                                OR sq__002.ALTERNATIVE_ID IS NOT NULL
                                )
                        ORDER BY bq__start.ALTERNATIVE_ID""",
                dummySingleFilteredConvertWithAlternativeId("color = @favColor OR sport = @favSport").toDebugString());

    }

    @Test
    void testMergedInClauseJoin() {
        assertEquals("""
                SELECT DISTINCT TBL_DATA2.ID2 AS ID
                FROM TBL_DATA2
                LEFT OUTER JOIN TBL_DATA1
                    ON TBL_DATA1.ID = TBL_DATA2.ID2
                    AND ( ( TBL_DATA1.DATA_COLUMN IN ('red', 'yellow') AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                WHERE ( ( ( ( TBL_DATA1.DATA_COLUMN = 'red' AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                            AND ( ( TBL_DATA2.DATA_COLUMN IN ('riding', 'tennis') AND TBL_DATA2.FILTER_COLUMN = 'sport' ) )
                            )
                        OR ( ( ( TBL_DATA1.DATA_COLUMN = 'yellow' AND TBL_DATA1.FILTER_COLUMN = 'color' ) )
                            AND ( ( TBL_DATA2.DATA_COLUMN IN ('boxing', 'football') AND TBL_DATA2.FILTER_COLUMN = 'sport' ) )
                            )
                        )
                ORDER BY ID""",
                dummyMultiConvert("(color.1 = red and sport.2 any of (riding, tennis)) or (color.1 = yellow and sport.2 any of (boxing, football))")
                        .toDebugString());

    }

    @Test
    void testStandardEnforced() {
        assertEquals("""
                SELECT DISTINCT TBL_BASE.ID
                FROM TBL_BASE
                LEFT OUTER JOIN TBL_SPECIAL2
                    ON TBL_SPECIAL2.ID = TBL_BASE.ID
                    AND ( ( TBL_SPECIAL2.XNAME IS NOT NULL AND TBL_SPECIAL2.KEC = 'FG' ) )
                WHERE ( ( ( TBL_BASE.C_COUNTRY = 'Germany' )
                            AND ( ( TBL_SPECIAL2.XNAME = 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                            )
                        OR ( ( TBL_BASE.C_COUNTRY = 'USA' )
                        AND ( ( TBL_SPECIAL2.XNAME <> 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                            )
                        )
                ORDER BY TBL_BASE.ID""", standardMultiConvert("(country=Germany and xname=Karl) OR (country=USA and STRICT xname!=Karl)").toDebugString());

        assertEquals("""
                SELECT DISTINCT TBL_BASE.ID
                FROM TBL_BASE
                INNER JOIN TBL_SPECIAL2
                    ON TBL_SPECIAL2.ID = TBL_BASE.ID
                    AND ( ( TBL_SPECIAL2.XNAME = 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                WHERE ( ( TBL_SPECIAL2.XNAME = 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                ORDER BY TBL_BASE.ID""", standardMultiConvert("xname=Karl", ConversionDirective.ENFORCE_PRIMARY_TABLE).toDebugString());

    }

    @Test
    void testNoTableWithAllIds() {
        assertEquals("""
                WITH
                bq__start AS ( SELECT ID FROM TBL_BASE
                        UNION SELECT ID FROM TBL_FACTS
                        UNION SELECT FID AS ID FROM TBL_FAV
                        UNION SELECT ID FROM TBL_FLAGS
                        UNION SELECT ID FROM TBL_SPECIAL
                        UNION SELECT ID FROM TBL_SPECIAL2 WHERE TBL_SPECIAL2.KEC = 'FG' )
                SELECT DISTINCT bq__start.ID
                FROM bq__start
                LEFT OUTER JOIN TBL_BASE
                    ON TBL_BASE.ID = bq__start.ID
                    AND ( TBL_BASE.C_COUNTRY IN ('Germany', 'USA') )
                LEFT OUTER JOIN TBL_SPECIAL2
                    ON TBL_SPECIAL2.ID = bq__start.ID
                    AND ( ( TBL_SPECIAL2.XNAME IS NOT NULL AND TBL_SPECIAL2.KEC = 'FG' ) )
                WHERE ( ( ( TBL_BASE.C_COUNTRY = 'Germany' )
                            AND ( ( TBL_SPECIAL2.XNAME = 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                            )
                        OR ( ( TBL_BASE.C_COUNTRY = 'USA' )
                        AND ( ( TBL_SPECIAL2.XNAME <> 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                            )
                        )
                ORDER BY bq__start.ID""",
                standardMultiConvertNoTableWithAllIds("(country=Germany and xname=Karl) OR (country=USA and STRICT xname!=Karl)").toDebugString());

        assertThrows(ConversionException.class, () -> standardMultiConvertNoTableWithAllIds("xname=Karl", ConversionDirective.ENFORCE_PRIMARY_TABLE));

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_SPECIAL2
                WHERE ( ( XNAME = 'Karl' AND KEC = 'FG' ) )
                ORDER BY TBL_SPECIAL2.ID""", standardMultiConvertNoTableWithAllIds("xname=Karl").toDebugString());

        assertEquals("""
                WITH
                bq__start AS ( SELECT ID FROM TBL_BASE
                        UNION SELECT ID FROM TBL_FACTS
                        UNION SELECT FID AS ID FROM TBL_FAV
                        UNION SELECT ID FROM TBL_FLAGS
                        UNION SELECT ID FROM TBL_SPECIAL
                        UNION SELECT ID FROM TBL_SPECIAL2 WHERE TBL_SPECIAL2.KEC = 'FG' ),
                sq__002 AS ( SELECT ID FROM TBL_SPECIAL2 WHERE ( XNAME IS NOT NULL AND KEC = 'FG' ) )
                SELECT DISTINCT bq__start.ID
                FROM bq__start
                LEFT OUTER JOIN TBL_SPECIAL2
                    ON TBL_SPECIAL2.ID = bq__start.ID
                    AND ( ( TBL_SPECIAL2.XNAME <> 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                LEFT OUTER JOIN sq__002
                    ON sq__002.ID = bq__start.ID
                WHERE ( ( ( TBL_SPECIAL2.XNAME <> 'Karl' AND TBL_SPECIAL2.KEC = 'FG' ) )
                        OR ( sq__002.ID IS NULL )
                        )
                ORDER BY bq__start.ID""", standardMultiConvertNoTableWithAllIds("xname!=Karl").toDebugString());

        assertEquals("""
                SELECT DISTINCT ID
                FROM TBL_SPECIAL2
                WHERE ( ( XNAME <> 'Karl' AND KEC = 'FG' ) )
                ORDER BY TBL_SPECIAL2.ID""", standardMultiConvertNoTableWithAllIds("STRICT xname!=Karl").toDebugString());

    }

}
