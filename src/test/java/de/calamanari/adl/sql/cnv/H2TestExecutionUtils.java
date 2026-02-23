//@formatter:off
/*
 * H2TestExecutionUtils
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.Flag;
import de.calamanari.adl.FormatStyle;
import de.calamanari.adl.sql.QueryTemplateWithParameters;
import de.calamanari.adl.sql.QueryType;
import de.calamanari.adl.sql.config.DataBinding;

import static de.calamanari.adl.cnv.StandardConversions.parseCoreExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Utility to wrap the boiler-plate code to run a prepared statement via JDBC against the in-memory H2-database.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class H2TestExecutionUtils {

    static final Logger LOGGER = LoggerFactory.getLogger(H2TestExecutionUtils.class);

    /**
     * Attaches the {@link TestCommentAugmentationListener} to the converter
     */
    public static final Flag TEST_AUGMENT = new Flag() {

        private static final long serialVersionUID = 2973922161515979928L;
    };

    /**
     * @param <T>
     * @param template
     * @param countQuery if true this is a count query, Integer will be the return value, otherwise a list of Integer
     * @return either count or list of IDs
     */
    private static <T extends Object> T selectInternal(QueryTemplateWithParameters template, boolean countQuery) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("\n{}", template.toDebugString());
        }

        List<Integer> res = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:adl;INIT=RUNSCRIPT FROM 'src/test/resources/h2init.sql';", "admin", "password");
                PreparedStatement stmt = template.createPreparedStatement(conn);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                if (countQuery) {
                    Integer countValue = rs.getInt(1);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.info("\n-> {}", countValue);
                    }

                    @SuppressWarnings("unchecked")
                    T retValue = (T) countValue;
                    return retValue;
                }
                res.add(rs.getInt("ID"));
            }
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error during SQL-execution", ex);
        }
        if (countQuery) {
            throw new RuntimeException("Unexpected empty result set.");
        }
        else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.info("\n-> {}", res);
            }
            @SuppressWarnings("unchecked")
            T retValue = (T) res;
            return retValue;
        }
    }

    /**
     * @param template should be of type {@link QueryType#SELECT_DISTINCT_ID} or {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     * @return list with all IDs matching the query criteria
     */
    public static List<Integer> selectIds(QueryTemplateWithParameters template) {
        return selectInternal(template, false);
    }

    /**
     * @param template should be a query of type {@link QueryType#SELECT_DISTINCT_COUNT}
     * @return number of IDs matching the query criteria
     */
    public static int selectCount(QueryTemplateWithParameters template) {
        return selectInternal(template, true);
    }

    /**
     * @param queryType if null: {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     * @param dataBinding if null: {@link H2TestBindings#DEFAULT}
     * @param expression
     * @param globalVariables if null: sets the variable <b><code>tenent=17</code></b>
     * @param flags (optional)
     * @return list with all the IDs matching the given expression
     */
    public static List<Integer> selectIds(QueryType queryType, DataBinding dataBinding, String expression, Map<String, Serializable> globalVariables,
            Flag... flags) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("Selecting IDs for: \n{}", expression);
        }

        QueryTemplateWithParameters template = createQueryTemplate(queryType, true, dataBinding, expression, globalVariables, flags);

        List<Integer> res1 = selectIds(template);

        template = createQueryTemplate(queryType, false, dataBinding, expression, globalVariables, flags);

        List<Integer> res2 = selectIds(template);

        assertEquals(res1, res2);

        return res1;

    }

    /**
     * @param dataBinding if null: {@link H2TestBindings#DEFAULT}
     * @param expression
     * @param globalVariables if null: sets the variable <b><code>tenent=17</code></b>
     * @param flags (optional)
     * @return list with all the IDs matching the given expression
     */
    public static List<Integer> selectIds(DataBinding dataBinding, String expression, Map<String, Serializable> globalVariables, Flag... flags) {
        return selectIds(null, dataBinding, expression, globalVariables, flags);
    }

    /**
     * Applies default mapping {@link H2TestBindings#DEFAULT}, sets the variable <b><code>tenent=17</code></b>
     * 
     * @param expression
     * @param globalVariables
     * @param flags
     * @return list with all the IDs matching the given expression
     */
    public static List<Integer> selectIds(String expression, Map<String, Serializable> globalVariables, Flag... flags) {
        return selectIds(null, expression, globalVariables, flags);
    }

    /**
     * Applies default mapping {@link H2TestBindings#DEFAULT}, sets the variable <b><code>tenent=17</code></b>
     * 
     * @param expression
     * @param flags
     * @return list with all the IDs matching the given expression
     */
    public static List<Integer> selectIds(String expression, Flag... flags) {
        return selectIds(null, expression, null, flags);
    }

    /**
     * Sets the variable <b><code>tenent=17</code></b>
     * 
     * @param dataBinding
     * @param expression
     * @param flags
     * @return list with all the IDs matching the given expression
     */
    public static List<Integer> selectIds(DataBinding dataBinding, String expression, Flag... flags) {
        return selectIds(dataBinding, expression, null, flags);
    }

    /**
     * @param dataBinding
     * @param expression
     * @param globalVariables
     * @param flags
     * @return number of IDs matching the given expression
     */
    public static int selectCount(DataBinding dataBinding, String expression, Map<String, Serializable> globalVariables, Flag... flags) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("Selecting count for: \n{}", expression);
        }

        QueryTemplateWithParameters template = createQueryTemplate(QueryType.SELECT_DISTINCT_COUNT, true, dataBinding, expression, globalVariables, flags);

        int count1 = selectCount(template);

        template = createQueryTemplate(QueryType.SELECT_DISTINCT_COUNT, false, dataBinding, expression, globalVariables, flags);

        int count2 = selectCount(template);

        assertEquals(count1, count2);

        return count1;

    }

    /**
     * Applies default mapping {@link H2TestBindings#DEFAULT}, sets the variable <b><code>tenent=17</code></b>
     * 
     * @param expression
     * @param globalVariables
     * @param flags
     * @return number of IDs matching the given expression
     */
    public static int selectCount(String expression, Map<String, Serializable> globalVariables, Flag... flags) {
        return selectCount(null, expression, globalVariables, flags);
    }

    /**
     * Applies default mapping {@link H2TestBindings#DEFAULT}, sets the variable <b><code>tenent=17</code></b>
     * 
     * @param expression
     * @param flags
     * @return number of IDs matching the given expression
     */
    public static int selectCount(String expression, Flag... flags) {
        return selectCount(null, expression, null, flags);
    }

    /**
     * Sets the variable <b><code>tenent=17</code></b>
     * 
     * @param dataBinding
     * @param expression
     * @param flags
     * @return number of IDs matching the given expression
     */
    public static int selectCount(DataBinding dataBinding, String expression, Flag... flags) {
        return selectCount(dataBinding, expression, null, flags);
    }

    /**
     * @param queryType if null: {@link QueryType#SELECT_DISTINCT_ID_ORDERED}
     * @param inline to create the statement in one line
     * @param dataBinding
     * @param expression
     * @param globalVariables
     * @param flags
     * @return query template to be executed
     */
    private static QueryTemplateWithParameters createQueryTemplate(QueryType queryType, boolean inline, DataBinding dataBinding, String expression,
            Map<String, Serializable> globalVariables, Flag... flags) {

        dataBinding = dataBinding == null ? H2TestBindings.DEFAULT : dataBinding;

        DefaultSqlExpressionConverter converter = new DefaultSqlExpressionConverter(dataBinding);

        converter.setQueryType(queryType == null ? QueryType.SELECT_DISTINCT_ID_ORDERED : queryType);

        if (inline) {
            converter.setStyle(FormatStyle.INLINE);
        }

        if (TEST_AUGMENT.check(flags)) {
            converter.setAugmentationListener(new TestCommentAugmentationListener());
        }

        if (flags != null) {
            converter.getInitialFlags().addAll(Arrays.asList(flags));
        }

        if (globalVariables != null) {
            converter.getInitialVariables().putAll(globalVariables);
        }
        else {
            converter.getInitialVariables().put("tenant", "17");
        }

        if (expression == null) {
            throw new IllegalArgumentException("expression must not be null");
        }

        return converter.convert(parseCoreExpression(expression));
    }

    public static void assertQueryResult(List<Integer> expectedIds, DataBinding dataBinding, String expression, Map<String, Serializable> globalVariables,
            Flag... flags) {
        assertEquals(expectedIds, selectIds(QueryType.SELECT_DISTINCT_ID, dataBinding, expression, globalVariables, flags));
        assertEquals(expectedIds.size(), selectCount(dataBinding, expression, globalVariables, flags));

        List<Integer> res2 = selectIds(dataBinding, expression, globalVariables, flags);
        Collections.sort(res2);
        assertEquals(expectedIds, res2);

    }

    public static void assertQueryResult(List<Integer> expectedIds, String expression, Map<String, Serializable> globalVariables, Flag... flags) {
        assertQueryResult(expectedIds, null, expression, globalVariables, flags);
    }

    public static void assertQueryResult(List<Integer> expectedIds, String expression, Flag... flags) {
        assertQueryResult(expectedIds, null, expression, null, flags);
    }

    public static void assertQueryResult(List<Integer> expectedIds, DataBinding dataBinding, String expression, Flag... flags) {
        assertQueryResult(expectedIds, dataBinding, expression, null, flags);
    }

    /**
     * Creates a list of Integers (for expected values)
     * 
     * @param values
     * @return Integer list
     */
    public static List<Integer> list(int... values) {
        return Arrays.stream(values).boxed().toList();
    }

    private H2TestExecutionUtils() {
        // static utilities
    }

}
