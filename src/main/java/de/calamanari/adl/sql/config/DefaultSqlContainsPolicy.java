//@formatter:off
/*
 * DefaultSqlContainsPolicy
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

package de.calamanari.adl.sql.config;

import java.sql.PreparedStatement;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.ContainsNotSupportedException;
import de.calamanari.adl.irl.MatchOperator;
import de.calamanari.adl.sql.AdlSqlType;
import de.calamanari.adl.sql.QueryParameterCreator;

/**
 * The implementations in this enumerations cover the CONTAINS translation (typically SQL LIKE) for a couple of databases.
 * <p>
 * <b>Notes:</b><br>
 * Be aware that the default behavior for most of the policies below is to <i>remove</i> any potentially conflicting pattern matching characters (such as '%',
 * '_') if they are part of a given search text.<br>
 * This creates the limitation that users won't be able to search for them. Should you have the need to support these characters it depends on the behavior of
 * your JDBC-driver what you can do. First check what happens if you decorate your current policy with {@link PreparatorFunction#none()} to pass the full
 * (unfiltered) search snippet as the parameter of the {@link PreparedStatement}. Test this with different user-inputs.<br>
 * Should the result not look as expected, try pattern escaping with database-specific SQL-syntax, e.g. <code>LIKE ... ESCAPE ...</code> using characters that
 * won't be handled by the driver. An interesting discussion regarding this topic can be found
 * <a href="https://stackoverflow.com/questions/8247970/using-like-wildcard-in-prepared-statement">here</a>.<br>
 * If the result still does not look as desired, you might solve this issue by configuring a custom {@link AdlSqlType} that should deal with the problem in a
 * custom {@link QueryParameterCreator} in combination with a custom {@link ArgValueFormatter}, where you have the information {@link MatchOperator#CONTAINS}
 * available indicating that the current parameter value should be a pattern for a LIKE. However, because of the complexity this should be the last option.
 * 
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum DefaultSqlContainsPolicy implements SqlContainsPolicy {

    /**
     * Creates a standard like with pattern concatenation using CONCAT.
     */
    MYSQL(
            s -> DefaultSqlContainsPolicy.removePatternCharacters(s, "%_"),
            (columnName, patternParameter) -> columnName + " LIKE CONCAT('%', " + patternParameter + ", '%')"),

    /**
     * Creates a standard LIKE with pattern concatenation using ANSI-'||', should work for many databases such as Oracle, SQLite, PostgreSQL
     */
    SQL92(
            s -> DefaultSqlContainsPolicy.removePatternCharacters(s, "%_"),
            (columnName, patternParameter) -> columnName + " LIKE '%' || " + patternParameter + " || '%'"),

    /**
     * Creates a standard LIKE with pattern concatenation using '+', e.g., Microsoft SQL-Server, also removes any '['-bracket from the search snippet.
     */
    SQL_SERVER(
            s -> DefaultSqlContainsPolicy.removePatternCharacters(s, "%_["),
            (columnName, patternParameter) -> columnName + " LIKE '%' + " + patternParameter + " + '%'"),

    /**
     * Creates a CHARINDEX-based CONTAINS, only Microsoft SQL-Server, accepts <i>any</i> snippet as there is no wildcard removal.
     */
    SQL_SERVER2(PreparatorFunction.none(), (columnName, patternParameter) -> "CHARINDEX(" + patternParameter + ", " + columnName + ", 0) > 0"),

    /**
     * Special case policy: will reject any attempt to translate a CONTAINS and throw {@link ContainsNotSupportedException}
     */
    UNSUPPORTED(PreparatorFunction.none(), (columnName, patternParameter) -> {
        AudlangMessage userMessage = AudlangMessage.msg(CommonErrors.ERR_2200_CONTAINS_NOT_SUPPORTED);
        throw new ContainsNotSupportedException("Error: Underlying data store does not support translating CONTAINS conditions, column: " + columnName,
                userMessage);
    });

    private final PreparatorFunction prepareSearchSnippetFunction;

    private final CreatorFunction createInstructionFunction;

    private DefaultSqlContainsPolicy(PreparatorFunction prepareSearchSnippetFunction, CreatorFunction createInstructionFunction) {
        this.prepareSearchSnippetFunction = prepareSearchSnippetFunction;
        this.createInstructionFunction = createInstructionFunction;
    }

    /**
     * Removes the given characters from the snippet
     * 
     * @param value
     * @param patternCharacters
     * @return
     */
    private static String removePatternCharacters(String value, String patternCharacters) {
        StringBuilder sb = null;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            boolean illegal = patternCharacters.indexOf(ch) > -1;
            if (illegal && sb == null) {
                sb = new StringBuilder();
                sb.append(value.substring(0, i));
            }
            else if (!illegal && sb != null) {
                sb.append(ch);
            }
        }
        return sb != null ? sb.toString() : value;
    }

    @Override
    public String prepareSearchSnippet(String value) {
        return prepareSearchSnippetFunction.apply(value);
    }

    @Override
    public String createInstruction(String columnName, String patternParameter) {
        return createInstructionFunction.apply(columnName, patternParameter);
    }

}
