//@formatter:off
/*
 * TableNature
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

import java.io.Serializable;

import de.calamanari.adl.sql.cnv.ConversionDirective;

/**
 * The {@link TableNature} is a crucial information for the query building process. Based on this information the system will decide if and how joins must be
 * created or if additional existence checks are required or not.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public enum TableNature implements Serializable {

    /**
     * Table contains all IDs and is marked as primary table (to start selections)
     * 
     * @see ConversionDirective#ENFORCE_PRIMARY_TABLE
     */
    PRIMARY,

    /**
     * Table contains all IDs, and it contains sparse data (treat columns independently), and it is marked as primary table (to start selections)
     * 
     * @see ConversionDirective#ENFORCE_PRIMARY_TABLE
     */
    PRIMARY_SPARSE,

    /**
     * Table contains all IDs, each ID appears once and once-only. The table is marked as primary table (to start selections).
     * 
     * @see ConversionDirective#ENFORCE_PRIMARY_TABLE
     */
    PRIMARY_UNIQUE,

    /**
     * Table contains all IDs.
     */
    ALL_IDS,

    /**
     * Table contains all IDs, and it contains sparse data (treat columns independently).
     */
    ALL_IDS_SPARSE,

    /**
     * Table contains all IDs, each ID appears once and once-only.
     */
    ALL_IDS_UNIQUE,

    /**
     * Table does usually <b>not</b> contain all IDs but only a subset (this is the default).
     */
    ID_SUBSET,

    /**
     * Table does usually <b>not</b> contain all IDs but only a subset, and it contains sparse data (treat columns independently).
     */
    ID_SUBSET_SPARSE,

    /**
     * Table does usually <b>not</b> contain all IDs but only a subset, each ID appears only once (if at all).
     */
    ID_SUBSET_UNIQUE;

    /**
     * Tells the system that every record is listed in this table. This allows the query builder to start a query on this table before joining any other tables.
     * In other words: it cannot happen that any valid record has no entry in this table.
     * 
     * @return true if this table has an entry for <i>every</i> record in the database, so it can be used to start a selection
     */
    public boolean containsAllIds() {
        return this == PRIMARY || this == PRIMARY_SPARSE || this == PRIMARY_UNIQUE || this == ALL_IDS || this == ALL_IDS_SPARSE || this == ALL_IDS_UNIQUE;
    }

    /**
     * Tells that this table should be used as the primary table of a setup, means the table to start the base selection on before joining any other table. This
     * only works if all possible records are present in this table.
     * <p>
     * If this method returns true, then {@link #containsAllIds()} also must return true.
     * 
     * @return true if this table has been configured as the primary table, preferred to start the selection with
     */
    public boolean isPrimaryTable() {
        return this == PRIMARY || this == PRIMARY_SPARSE || this == PRIMARY_UNIQUE;
    }

    /**
     * @see SingleTableConfig
     * @return true if the table contains sparse data
     */
    public boolean isSparse() {
        return this == PRIMARY_SPARSE || this == ALL_IDS_SPARSE || this == ID_SUBSET_SPARSE;
    }

    /**
     * Tells the system that in this table there cannot exist multiple rows for the same record (ID)
     * 
     * @return true if the same record (ID) can only appear once in this table
     */
    public boolean isIdUnique() {
        return this == PRIMARY_UNIQUE || this == ALL_IDS_UNIQUE || this == ID_SUBSET_UNIQUE;
    }

}
