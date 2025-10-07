//@formatter:off
/*
 * DefaultAutoMappingPolicy
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

import static de.calamanari.adl.sql.config.ConfigUtils.assertContextNotNull;

import java.io.Serializable;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.ProcessContext;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ConfigException;

/**
 * The {@link DefaultAutoMappingPolicy} maps arguments to columns with a template-based approach and an extractor operator.
 * <p>
 * If the extractor returns a non-null value (policy applies to this argName), then a copy of the template with the adjusted argName will be returned.
 * <p>
 * <b>Example:</b> You want to map any structured argName <code>varName.int</code> to column <code>XYZ</code> of type SQL INTEGER with the qualifier column Q17
 * set to the argName. Then you can configure a predicate like <code>s -> s.endsWith(".int") ? s.substring(0, s.length()-4 : null</code> and configure the
 * target column with the filter column value set to <code>${argName.local}</code>, see {@value #ARG_NAME_LOCAL_PLACEHOLDER}.
 * <p>
 * The convenience variable <code>${argName.local}</code> covers the very common demand to <i>decode</i> a single identifier from the argName.<br>
 * However, if you need advanced extraction (e.g., you want to extract multiple variables from an argName like "org7.section9.dep34.int"), then you can subclass
 * {@link DefaultAutoMappingPolicy} and overwrite {@link #prepareVariables(String, ConversionSettings)}.
 * <p>
 * This way you can map a whole category of variables to columns without knowing and listing them beforehand.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class DefaultAutoMappingPolicy implements AutoMappingPolicy {

    private static final long serialVersionUID = 7860446717172333819L;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAutoMappingPolicy.class);

    /**
     * Dynamic variable that can be specified as a filterValue to tell the system to replace it at runtime with the value of the <i>global variable</i>
     * <code>argName.local</code> (if present). By default this value will be set by this policy right before processing the filter condition setup (parameter
     * creation), so the value will be the result of this instances extractor function.
     */
    public static final String VAR_ARG_NAME_LOCAL = "argName.local";

    /**
     * Reference to the variable {@value #VAR_ARG_NAME_LOCAL}
     */
    public static final String ARG_NAME_LOCAL_PLACEHOLDER = "${" + VAR_ARG_NAME_LOCAL + "}";

    /**
     * Configuration element if no policy is defined to avoid null
     */
    public static final AutoMappingPolicy NONE = new AutoMappingPolicy() {

        private static final long serialVersionUID = 2854932785875792470L;

        @Override
        public ArgColumnAssignment map(String argName, ProcessContext settings) {
            throw new ConfigException("Auto-mapping error: NONE-mapper called for argName=" + argName,
                    AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
        }

        @Override
        public boolean isApplicable(String argName) {
            return false;
        }

        /**
         * @return singleton instance in JVM
         */
        Object readResolve() {
            return NONE;
        }
    };

    /**
     * At runtime we return the same mapping for all matching argNames, argMetaInfo updated with the <i>current</i> argName
     */
    protected final ArgColumnAssignment assignmentTemplate;

    /**
     * The function that extracts the {@value #ARG_NAME_LOCAL_PLACEHOLDER} from the argName
     */
    protected final LocalArgNameExtractor extractorFunction;

    /**
     * 
     * @see LocalArgNameExtractor
     * @param extractorFunction function that extracts the {@value #ARG_NAME_LOCAL_PLACEHOLDER} from the argName
     * @param assignmentTemplate configured assignment with a placeholder name
     * 
     */
    public DefaultAutoMappingPolicy(LocalArgNameExtractor extractorFunction, ArgColumnAssignment assignmentTemplate) {
        if (extractorFunction == null) {
            throw new IllegalArgumentException(
                    "The extractorFunction for extracting argName.local from the argName must not be null, assignmentTemplate=" + assignmentTemplate);
        }
        if (assignmentTemplate == null) {
            throw new IllegalArgumentException("The assignmentTemplate must not be null.");
        }
        this.extractorFunction = extractorFunction;
        this.assignmentTemplate = assignmentTemplate;
    }

    @Override
    public boolean isApplicable(String argName) {

        String localArgName = extractorFunction.apply(argName);

        boolean valid = ConfigUtils.isValidArgName(localArgName);
        if (localArgName != null && !valid) {
            LOGGER.debug("An auto-mapping policy defined for table={} applied to argName={} returned an invalid localArgName={} - SKIPPED!",
                    assignmentTemplate.column().tableName(), argName, localArgName);
        }
        return valid;
    }

    /**
     * This TEMPLATE METHOD allows sub-classes to compute and set further global variables which can then be referenced inside the mapping process.
     * <p>
     * <b>Example:</b> Let the argName be structured <code>org.section.<b>department</b></code> and you want to pass the <i>department</i> as a filter column
     * value.<br>
     * Simply extract (parse) the qualifier from the argName in <i>this method</i> and put it as a global variable <i>department</i> into the settings.<br>
     * Now you can reference it in a filter column value, e.g., <code>SEC/${department}</code> like any other variable.
     * <p>
     * By default this method calls the extractor function and sets the global variable {@value #ARG_NAME_LOCAL_PLACEHOLDER} to the value extracted from the
     * argName.
     * 
     * @param argName
     * @param ctx to obtain and set variables
     */
    protected void prepareVariables(String argName, ProcessContext ctx) {
        assertContextNotNull(ctx);
        String argNameLocal = extractorFunction.apply(argName);
        ctx.getGlobalVariables().put(VAR_ARG_NAME_LOCAL, argNameLocal);
    }

    @Override
    public ArgColumnAssignment map(String argName, ProcessContext ctx) {
        if (!isApplicable(argName)) {
            throw new ConfigException("Auto-mapping error: this mapper is not applicable to argName=" + argName,
                    AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, argName));
        }
        this.prepareVariables(argName, ctx);
        ArgMetaInfo argTemplate = assignmentTemplate.arg();
        ArgMetaInfo arg = new ArgMetaInfo(argName, argTemplate.type(), argTemplate.isAlwaysKnown(), argTemplate.isCollection());
        return new ArgColumnAssignment(arg, assignmentTemplate.column());
    }

    /**
     * Functional interface to ensure this part of the configuration remains serializable
     * <p>
     * E.g., <code>s -> s.substring(0, 5)</code>
     */
    public interface LocalArgNameExtractor extends UnaryOperator<String>, Serializable {
        // tagging
    }

}
