//@formatter:off
/*
 * AdlSqlTypeDecorator
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

package de.calamanari.adl.sql;

import java.util.concurrent.atomic.AtomicInteger;

import de.calamanari.adl.cnv.tps.ArgValueFormatter;
import de.calamanari.adl.cnv.tps.NativeTypeCaster;

/**
 * Often an {@link AdlSqlType}s behavior will be common resp. applicable in many scenarios but we want to change the formatter or add a type caster. To avoid
 * creating boiler-plate code the {@link AdlSqlTypeDecorator} provides an easy solution by composition. A given type gets wrapped to adapt its behavior.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class AdlSqlTypeDecorator implements AdlSqlType {

    private static final long serialVersionUID = 2654441110457026318L;

    /**
     * Static counter to ensure we get unique names for the decorators
     */
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

    /**
     * the decorated type
     */
    private final AdlSqlType delegate;

    /**
     * decorated formatter or null (use the one of the delegate)
     */
    private final ArgValueFormatter formatter;

    /**
     * decorated type caster or null (use the one of the delegate)
     */
    private final NativeTypeCaster nativeTypeCaster;

    /**
     * decorated parameter creator or null (use the one of the delegate)
     */
    private final QueryParameterCreator queryParameterCreator;

    /**
     * decorated parameter applicator or null (use the one of the delegate)
     */
    private final QueryParameterApplicator queryParameterApplicator;

    /**
     * We append a number to the original name, this keeps the identifiers short and still informative regarding the base type
     */
    private final String decoratorName;

    @Override
    public AdlSqlType getBaseType() {
        return delegate.getBaseType();
    }

    /**
     * @param name if null, the wrapper gets a unique id assigned as its name
     * @param delegate NOT NULL
     * @param formatter
     * @param nativeTypeCaster
     */
    AdlSqlTypeDecorator(String name, AdlSqlType delegate, ArgValueFormatter formatter, NativeTypeCaster nativeTypeCaster,
            QueryParameterCreator queryParameterCreator, QueryParameterApplicator queryParameterApplicator) {
        this.delegate = delegate;
        this.formatter = formatter;
        this.nativeTypeCaster = nativeTypeCaster;
        this.queryParameterCreator = queryParameterCreator;
        this.queryParameterApplicator = queryParameterApplicator;
        if (name != null) {
            this.decoratorName = name;
        }
        else if (delegate instanceof AdlSqlTypeDecorator dec) {
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
        if (delegate instanceof AdlSqlTypeDecorator dec) {
            return dec.getBaseName();
        }
        return delegate.name();
    }

    @Override
    public String name() {
        return this.decoratorName;
    }

    @Override
    public ArgValueFormatter getFormatter() {
        return this.formatter == null ? delegate.getFormatter() : this.formatter;
    }

    @Override
    public NativeTypeCaster getNativeTypeCaster() {
        return this.nativeTypeCaster == null ? delegate.getNativeTypeCaster() : this.nativeTypeCaster;
    }

    @Override
    public boolean supportsContains() {
        return delegate.supportsContains();
    }

    @Override
    public boolean supportsLessThanGreaterThan() {
        return delegate.supportsLessThanGreaterThan();
    }

    @Override
    public int getJavaSqlType() {
        return delegate.getJavaSqlType();
    }

    @Override
    public QueryParameterCreator getQueryParameterCreator() {
        return this.queryParameterCreator == null ? delegate.getQueryParameterCreator() : this.queryParameterCreator;
    }

    @Override
    public QueryParameterApplicator getQueryParameterApplicator() {
        return this.queryParameterApplicator == null ? delegate.getQueryParameterApplicator() : this.queryParameterApplicator;
    }

    @Override
    public String toString() {
        return this.decoratorName;
    }

}