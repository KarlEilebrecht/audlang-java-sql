//@formatter:off
/*
 * ConfigUtils
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

import de.calamanari.adl.ProcessContext;

/**
 * Set of utilities, mainly for validation to avoid duplication.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class ConfigUtils {

    /**
     * @param argName
     * @throws IllegalArgumentException if the argName is null or blank
     */
    public static void assertValidArgName(String argName) {
        if (!isValidArgName(argName)) {
            throw new IllegalArgumentException("Parameter argName must not be null or empty.");
        }
    }

    /**
     * @param ctx object
     * @throws IllegalArgumentException if the settings is null
     */
    public static void assertContextNotNull(ProcessContext ctx) {
        if (ctx == null) {
            throw new IllegalArgumentException(
                    "The process context must not not be null, use ProcessContext.empty() instead to call this method without any settings.");
        }
    }

    /**
     * @param argName
     * @return true if the argName is acceptable (not null, not blank)
     */
    public static boolean isValidArgName(String argName) {
        return (argName != null && !argName.isBlank());
    }

    /**
     * Validates the given table name.
     * <p>
     * This is a best-guess implementation (applicable to many databases) to help detecting configuration errors early.
     * <p>
     * <ul>
     * <li>In unquoted form, table names and column names may contain: <code>[0-9,a-z,A-Z$_]</code> (ciphers 0-9, basic Latin letters (lowercase and uppercase),
     * dollar sign, underscore)</li>
     * <li>Quoted table and column names are accepted within <i>backticks</i>, e.g. <code>&#96;name&#96;</code>.</li>
     * </ul>
     * 
     * @param name
     * @return true if the given table name is invalid
     */
    public static boolean isValidTableName(String name) {
        return isValidSqlName(name, false);
    }

    /**
     * Validates the given column name.
     * <p>
     * This is a best-guess implementation (applicable to many databases) to help detecting configuration errors early.
     * <p>
     * <ul>
     * <li>In unquoted form, table names and column names may contain: <code>[0-9,a-z,A-Z$_]</code> (ciphers 0-9, basic Latin letters (lowercase and uppercase),
     * dollar sign, underscore)</li>
     * <li>Quoted table and column names are accepted within <i>backticks</i>, e.g. <code>&#96;name&#96;</code>.</li>
     * </ul>
     * 
     * @param name
     * @return true if the given column name is invalid
     */
    public static boolean isValidColumnName(String name) {
        return isValidSqlName(name, true);
    }

    /**
     * @param name
     * @param isColumn if true we don't allow the dot in unquoted names
     * @return true if the given name is invalid
     */
    private static boolean isValidSqlName(String name, boolean isColumn) {
        if (name == null || name.isBlank()) {
            return false;
        }
        if (name.startsWith("`") && name.endsWith("`")) {
            return isValidSqlNameBT(name);
        }
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (!(isPlainIdChar(ch) || (ch == '.' && !isColumn && i > 0 && i < name.length() - 1))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param ch
     * @return true if ch in <code>[0-9,a-z,A-Z$_]</code> (ciphers 0-9, basic Latin letters (lowercase and uppercase), dollar sign, underscore)
     */
    private static boolean isPlainIdChar(char ch) {
        return ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '$' || ch == '_');
    }

    /**
     * @param name
     * @return true if the name inside the backticks is not blank and does not contain backticks
     */
    private static boolean isValidSqlNameBT(String name) {
        String innerName = name.substring(1, name.length() - 1);
        return !innerName.isBlank() && innerName.indexOf('`') < 0;
    }

    private ConfigUtils() {
        // utilities
    }

}
