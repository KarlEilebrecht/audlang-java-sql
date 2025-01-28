//@formatter:off
/*
 * SqlContainsPolicyDecorator
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link SqlContainsPolicyDecorator} allows to quickly create a new policy by decorating a given one with a different behavioral aspect.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class SqlContainsPolicyDecorator implements SqlContainsPolicy {

    private static final long serialVersionUID = 7131812266291596818L;

    /**
     * Static counter to ensure we get unique names for the decorators
     */
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

    /**
     * The decorated policy
     */
    private final SqlContainsPolicy delegate;

    /**
     * either a decorated function or null
     */
    private final PreparatorFunction prepareSearchSnippetFunction;

    /**
     * either a decorated function or null
     */
    private final CreatorFunction createInstructionFunction;

    /**
     * We append a number to the original name, this keeps the identifiers short and still informative regarding the base policy
     */
    private final String decoratorName;

    /**
     * @param name if null, the wrapper gets a unique id assigned as its name
     * @param delegate to be decorated NOT NULL
     * @param prepareSearchSnippetFunction a different preparation function or null (use delegate)
     * @param createInstructionFunction a different creation function or null (use delegate)
     */
    SqlContainsPolicyDecorator(String name, SqlContainsPolicy delegate, PreparatorFunction prepareSearchSnippetFunction,
            CreatorFunction createInstructionFunction) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate must not be null.");
        }
        this.delegate = delegate;
        this.prepareSearchSnippetFunction = prepareSearchSnippetFunction;
        this.createInstructionFunction = createInstructionFunction;
        if (name != null) {
            this.decoratorName = name;
        }
        else if (delegate instanceof SqlContainsPolicyDecorator dec) {
            this.decoratorName = dec.getBaseName() + "-" + INSTANCE_COUNTER.incrementAndGet();
        }
        else {
            this.decoratorName = delegate.name() + "-" + INSTANCE_COUNTER.incrementAndGet();
        }

    }

    /**
     * @return the name of the inner type (center of the "type onion")
     */
    private String getBaseName() {
        if (delegate instanceof SqlContainsPolicyDecorator dec) {
            return dec.getBaseName();
        }
        return delegate.name();
    }

    @Override
    public String prepareSearchSnippet(String value) {
        if (prepareSearchSnippetFunction != null) {
            return prepareSearchSnippetFunction.apply(value);
        }
        else {
            return delegate.prepareSearchSnippet(value);
        }
    }

    @Override
    public String createInstruction(String columnName, String patternParameter) {
        if (createInstructionFunction != null) {
            return createInstructionFunction.apply(columnName, patternParameter);
        }
        else {
            return delegate.createInstruction(columnName, patternParameter);
        }
    }

    @Override
    public String name() {
        return decoratorName;
    }

    @Override
    public String toString() {
        return this.decoratorName;
    }
}
