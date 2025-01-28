//@formatter:off
/*
 * AutoMappingPolicy
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

import java.io.Serializable;

import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ConfigException;

/**
 * An {@link AutoMappingPolicy} derives the target column assignment for a given argName by parsing or mapping the given argName.
 * <p>
 * Instances should be immutable because they are part of the static configuration.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public interface AutoMappingPolicy extends Serializable {

    /**
     * Returns whether this mapping policy can map the given argument to a column
     * 
     * @param argName
     * @return true if the policy is applicable
     */
    boolean isApplicable(String argName);

    /**
     * Returns the column assignment for the given argName
     * 
     * @param argName
     * @param ctx
     * @return column assignment
     * @throws ConfigException if not applicable or if the underlying resolution failed
     */
    ArgColumnAssignment map(String argName, ProcessContext ctx);

}
