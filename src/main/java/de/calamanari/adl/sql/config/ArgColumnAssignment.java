//@formatter:off
/*
 * ArgColumnMapping
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.AudlangMessage;
import de.calamanari.adl.CommonErrors;
import de.calamanari.adl.cnv.tps.ArgMetaInfo;
import de.calamanari.adl.cnv.tps.ConfigException;

/**
 * {@link ArgColumnAssignment}s assign arguments to columns.
 * <p>
 * <b>Important:</b> the properties {@link ArgMetaInfo#isCollection()} and {@link ArgMetaInfo#isAlwaysKnown()} will always be overridden by
 * {@link DataColumn#isMultiRow()} and {@link DataColumn#isAlwaysKnown()}.
 * 
 * @param arg argument meta data, <b>see also {@link ArgColumnAssignment})</b>
 * @param column data column the argument is mapped to
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record ArgColumnAssignment(ArgMetaInfo arg, DataColumn column) implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArgColumnAssignment.class);

    /**
     * @param arg argument meta data, <b>see also {@link ArgColumnAssignment})</b>
     * @param column data column the argument is mapped to
     */
    public ArgColumnAssignment(ArgMetaInfo arg, DataColumn column) {
        if (arg == null || column == null) {
            throw new IllegalArgumentException(String.format("Arguments must not be null, given: arg=%s, column=%s", arg, column));
        }
        if (!column.columnType().isCompatibleWith(arg.type())) {
            throw new ConfigException(String.format("""
                    Incompatible types: cannot map this attribute to the given column, given: arg=%s, column=%s
                    Either choose a different type or configure a custom QueryParameterCreator to bridge the gap.
                    See also AdlSqlType.withQueryParameterCreator(...)
                    """, arg, column), AudlangMessage.argMsg(CommonErrors.ERR_3000_MAPPING_FAILED, arg.argName()));
        }
        if (arg.isAlwaysKnown() != column.isAlwaysKnown() || arg.isCollection() != column.isMultiRow()) {
            // effective setting comes from the column
            this.arg = new ArgMetaInfo(arg.argName(), arg.type(), column.isAlwaysKnown(), column.isMultiRow());
            LOGGER.trace("ArgMetaInfo auto-adjustment from column: {} -> {} (reason: {})", arg, this.arg, column);
        }
        else {
            this.arg = arg;
        }
        this.column = column;
    }

}
