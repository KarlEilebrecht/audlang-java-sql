//@formatter:off
/*
 * ConfigUtilsTest
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

package de.calamanari.adl.sql.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class ConfigUtilsTest {

    @Test
    void testAssertValidArgName() {

        assertTrue(ConfigUtils.isValidArgName("a"));
        assertTrue(ConfigUtils.isValidArgName("a.b"));
        assertTrue(ConfigUtils.isValidArgName("*"));

        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.assertValidArgName(null));
        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.assertValidArgName(""));
        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.assertValidArgName("  "));
        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.assertValidArgName(" \n "));

    }

    @Test
    void testAssertValidTableName() {

        assertTrue(ConfigUtils.isValidTableName("a"));
        assertTrue(ConfigUtils.isValidTableName("a.b"));
        assertTrue(ConfigUtils.isValidTableName("`a.`"));
        assertTrue(ConfigUtils.isValidTableName("A_B"));
        assertTrue(ConfigUtils.isValidTableName("A$5"));

        assertFalse(ConfigUtils.isValidTableName(null));
        assertFalse(ConfigUtils.isValidTableName(""));
        assertFalse(ConfigUtils.isValidTableName("  "));
        assertFalse(ConfigUtils.isValidTableName(" \n "));
        assertFalse(ConfigUtils.isValidTableName("a."));
        assertFalse(ConfigUtils.isValidTableName(".a"));
        assertFalse(ConfigUtils.isValidTableName("``"));
        assertFalse(ConfigUtils.isValidTableName("A&B"));

    }

    @Test
    void testAssertValidColumnName() {

        assertTrue(ConfigUtils.isValidColumnName("a"));
        assertTrue(ConfigUtils.isValidColumnName("`a.`"));
        assertTrue(ConfigUtils.isValidColumnName("A_B"));
        assertTrue(ConfigUtils.isValidColumnName("A$5"));

        assertFalse(ConfigUtils.isValidColumnName(null));
        assertFalse(ConfigUtils.isValidColumnName(""));
        assertFalse(ConfigUtils.isValidColumnName("  "));
        assertFalse(ConfigUtils.isValidColumnName(" \n "));
        assertFalse(ConfigUtils.isValidColumnName("a."));
        assertFalse(ConfigUtils.isValidColumnName(".a"));
        assertFalse(ConfigUtils.isValidColumnName("a.b"));
        assertFalse(ConfigUtils.isValidColumnName("``"));
        assertFalse(ConfigUtils.isValidColumnName("A&B"));

    }

}
