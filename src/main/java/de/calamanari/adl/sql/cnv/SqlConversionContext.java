//@formatter:off
/*
 * SqlConversionContext
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

import java.util.ArrayList;
import java.util.List;

import de.calamanari.adl.CombinedExpressionType;
import de.calamanari.adl.cnv.ConversionContext;
import de.calamanari.adl.irl.CoreExpression;

/**
 * The {@link SqlConversionContext} is a container holding the information for a particular <i>level</i> of the expression to be converted while visiting the
 * {@link CoreExpression}'s DAG.
 * <p>
 * <b>Important:</b> Each {@link SqlConversionContext} instance has a local (level) state and a <b>global state</b> (process state of the converter). The
 * {@link #clear()}-command does not affect the instance returned by {@link #getProcessContext()}.<br>
 * The global state gets assigned by the converter (precisely the {@link SqlConversionProcessContext} <i>injects itself</i> upon creation of the level context
 * object. This ensures that the global process is common for all level context objects created during the same conversion run.
 * <p>
 * The concept allows parts of the conversion logic not only to access configuration data and the isolated data of a level and the parent level but also to
 * <i>exchange</i> information. This is for example required when we automatically map arguments to columns with derived filter conditions.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class SqlConversionContext implements ConversionContext {

    /**
     * Global process context, not covered by {@link #clear()}
     */
    private SqlConversionProcessContext processContext;

    /**
     * List of expressions handled by the parent conversion implicitly, so they must be ignored below to avoid duplicate processing
     */
    private final List<CoreExpression> skippedChildExpressions = new ArrayList<>();

    /**
     * If true, we are processing a negated match
     */
    private boolean negation = false;

    /**
     * Combination type of this level, AND by default
     */
    private CombinedExpressionType combiType = CombinedExpressionType.AND;

    /**
     * Flag that indicates that we don't need a closing brace because a formerly combined expression (OR/AND) has turned into a simple IN or NOT IN SQL
     * condition.
     */
    private boolean closingBraceSuppressed = false;

    /**
     * Sets the given expression on the skip-list to prevent duplicate processing, if a child has been processed implicitly by the parent
     * 
     * @param expression to be marked as skipped
     */
    public void skipChildExpression(CoreExpression expression) {
        skippedChildExpressions.add(expression);
    }

    /**
     * @param expression
     * @return true if the given element has been skipped and should not be processed again
     */
    public boolean isSkipped(CoreExpression expression) {
        return skippedChildExpressions.contains(expression);
    }

    /**
     * @return true if we are currently processing a negated match
     */
    public boolean isNegation() {
        return negation;
    }

    /**
     * Marks this level to be a negated match
     */
    public void markNegation() {
        this.negation = true;
    }

    /**
     * @return combination type of this level, AND by default
     */
    public CombinedExpressionType getCombiType() {
        return combiType;
    }

    /**
     * @param combiType combination type to be set for the current level
     */
    public void setCombiType(CombinedExpressionType combiType) {
        this.combiType = combiType;
    }

    /**
     * Resets this context to its defaults
     */
    @Override
    public void clear() {
        skippedChildExpressions.clear();
        negation = false;
        closingBraceSuppressed = false;
        combiType = CombinedExpressionType.AND;
    }

    /**
     * Method to be called initially by the converter to ensure all level contexts created during a conversion share the same global process context.
     * 
     * @param processContext the converters global state
     */
    public void setProcessContext(SqlConversionProcessContext processContext) {
        this.processContext = processContext;
    }

    /**
     * Sets a flag that we don't need a closing brace because a formerly combined expression (OR/AND)<br>
     * has turned into a simple IN or NOT IN SQL condition.
     */
    public void suppressClosingBrace() {
        this.closingBraceSuppressed = true;
    }

    /**
     * @return true if an OR/AND should omit the closing brace
     */
    public boolean isClosingBraceSuppressed() {
        return this.closingBraceSuppressed;
    }

    /**
     * @return list with skipped child expressions
     */
    public List<CoreExpression> getSkippedChildExpressions() {
        return skippedChildExpressions;
    }

    /**
     * @return the global part of the context (related to the conversion process independent from the levels)
     */
    public SqlConversionProcessContext getProcessContext() {
        return processContext;
    }

}
