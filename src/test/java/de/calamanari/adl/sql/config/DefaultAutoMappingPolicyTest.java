//@formatter:off
/*
 * DefaultAutoMappingPolicyTest
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import de.calamanari.adl.DeepCopyUtils;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ConfigException;
import de.calamanari.adl.cnv.tps.DefaultAdlType;
import de.calamanari.adl.sql.DefaultAdlSqlType;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
class DefaultAutoMappingPolicyTest {

    @Test
    void testBasics() {

        DataColumn col1 = new DataColumn("TBL1", "COL1", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        ArgMetaInfo metaInfo1 = new ArgMetaInfo("arg", DefaultAdlType.STRING, false, false);

        ArgColumnAssignment aca = new ArgColumnAssignment(metaInfo1, col1);

        DefaultAutoMappingPolicy policy = new DefaultAutoMappingPolicy(s -> s.startsWith("Foo") && s.endsWith("Bar") ? "foo" : null, aca);

        assertEquals(aca, policy.assignmentTemplate);

        assertTrue(policy.isApplicable("FooBar"));

        DataColumn col2 = new DataColumn("TBL1", "COL2", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        ArgMetaInfo metaInfo2 = new ArgMetaInfo("arg2", DefaultAdlType.STRING, false, false);

        ArgColumnAssignment aca2 = new ArgColumnAssignment(metaInfo2, col2);

        DefaultAutoMappingPolicy policy2 = new DefaultAutoMappingPolicy(s -> s.startsWith("Foo") ? "foo2" : null, aca2);

        assertTrue(policy2.isApplicable("FooBar"));

        CompositeAutoMappingPolicy copo = new CompositeAutoMappingPolicy(Arrays.asList(policy, policy2));

        assertTrue(copo.isApplicable("FooBar"));

        assertFalse(copo.isApplicable("OxMox"));

        ProcessContext ctx = mock(ProcessContext.class);

        assertThrows(ConfigException.class, () -> copo.map("OxMox", ctx));

        DataColumn colDerived = new DataColumn("TBL1", "COL1", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        ArgMetaInfo metaInfoDerived = new ArgMetaInfo("FooBar", DefaultAdlType.STRING, false, false);

        ArgColumnAssignment expected1 = new ArgColumnAssignment(metaInfoDerived, colDerived);

        assertEquals(expected1, copo.map("FooBar", ProcessContext.empty()));

        assertTrue(copo.isApplicable("FooWang"));

        colDerived = new DataColumn("TBL1", "COL2", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        metaInfoDerived = new ArgMetaInfo("FooWang", DefaultAdlType.STRING, false, false);

        ArgColumnAssignment expected2 = new ArgColumnAssignment(metaInfoDerived, colDerived);

        assertEquals(expected2, copo.map("FooWang", ProcessContext.empty()));

        List<AutoMappingPolicy> policiesBad1 = Arrays.asList((AutoMappingPolicy) null);

        assertThrows(IllegalArgumentException.class, () -> new CompositeAutoMappingPolicy(policiesBad1));

        List<AutoMappingPolicy> policiesBad2 = Arrays.asList((AutoMappingPolicy) null, policy);

        assertThrows(IllegalArgumentException.class, () -> new CompositeAutoMappingPolicy(policiesBad2));

    }

    @Test
    void testSpecial() {

        DataColumn col1 = new DataColumn("TBL1", "COL1", DefaultAdlSqlType.SQL_VARCHAR, false, false, null);

        ArgMetaInfo metaInfo1 = new ArgMetaInfo("arg", DefaultAdlType.STRING, false, false);

        ArgColumnAssignment aca = new ArgColumnAssignment(metaInfo1, col1);

        assertThrows(IllegalArgumentException.class, () -> new DefaultAutoMappingPolicy(null, aca));

        assertThrows(IllegalArgumentException.class, () -> new DefaultAutoMappingPolicy(s -> s.startsWith("Foo") ? "foo2" : null, null));

        AutoMappingPolicy policyEmpty = new CompositeAutoMappingPolicy(null);

        assertFalse(policyEmpty.isApplicable("FooWang"));

        AutoMappingPolicy policy = DefaultAutoMappingPolicy.NONE;

        AutoMappingPolicy policy2 = DeepCopyUtils.deepCopy(policy);

        assertSame(policy, policy2);

        assertFalse(policy.isApplicable("hugo"));

        ProcessContext ctx = mock(ProcessContext.class);

        assertThrows(ConfigException.class, () -> policy.map("hugo", ctx));

    }

}
