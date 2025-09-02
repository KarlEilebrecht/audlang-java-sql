#### [Project Overview](../README.md)
----

# About the nature of tables and columns

It is crucial to tell the query builder as much as possible about the nature of the tables and columns you want to map attributes to. This allows the converter to make the right decisions to generate correct queries.

## Table characteristics

The [TableNature](../src/main/java/de/calamanari/adl/sql/config/TableNature.java) can be described with the following properties:
 * **Primary table**: This table contains all IDs of your base audience and should be preferred when starting a selection if none of the existing sub-selections (aliases) work. In case of a primary table the query builder will never create any UNION-base-queries.

   **:warning: Important:**  The primary table property does not guarantee that every query will start with this table. It only tells the converter not to create any complex unions and instead use this table if there is no viable base selection. If the current query has a valid base selection and the primary table is unrelated to this query, then the query builder will omit selecting from the primary table not to further increase the query's complexity. If you want or must ensure that *all* queries start with a specific table, then you should set the flag **[ENFORCE_PRIMARY_TABLE](../src/main/java/de/calamanari/adl/sql/cnv/ConversionDirective.java)** for the conversion. This would be the case if only the primary table contains a crucial information like `IS_ACTIVE` or `TENANT` that should be considered in *every query*. 
 * **Unique IDs**: This informs the query-builder that there is a physical or logical *unique constraint* on the ID-column, so the same record of your base audience cannot have more than one row in that table.
 * **Contains all IDs**: This property tells the query builder that the table contains all IDs of your base audience. Thus, any query could start with this table.
 * **Sparse**: See [Multi-row and sparse data](./multi-row-concept.md)

 Additionally, a table can carry **table filters**, additional conditions in form of [FilterColumns](../src/main/java/de/calamanari/adl/sql/config/FilterColumn.java). Whenever a query involves this table, the table filters narrow the scope. Table filters are meant for scenarions where queries on a table must be limited independently from the conditions of a given expression (e.g., an `IS_ACTIVE`-column).

## Column characteristics

 * **Always known**: If a column is marked as *always known*, then we know that this information is available for *every record of the base audience*. Be aware that this is *stronger* than a NOT NULL constraint! Only set this property if it is guaranteed that there is an entry in the corresponding table for every record of the base audience. Besides characterizing the column, this is an indirect table information. If any column in a table is marked as *always known* then this table will implicitly be marked as **containing all IDs**.
 * **Multi-row**: See [Multi-row and sparse data](./multi-row-concept.md).
 * **Column filters**: Every data column can carry additional conditions in the form of [FilterColumns](../src/main/java/de/calamanari/adl/sql/config/FilterColumn.java). Whenever a query involves this column, the column filters narrow the scope. Column filter conditions are meant for scenarios where the data of multiple attributes must be mapped to the same data column (e.g., key-value).

## NOT always translates to NOT ANY

This rule applies to every field assignment no matter if it is marked *multi-row* or not. This is sometimes inconvenient for the user, but it is an **essential requirement**.

Given a condition `color = red` Audlang requires the two sets defined by `color=red` and `color != red` to be **disjoint**.

Would we allow testing a negative condition on a particular *row* rather than *any*, the sets above could *overlap*. Let's assume the column `color` sits on a table *layout* and for the individual `4711` exist two layout-rows, one with `color=red` and the other with `color=blue`. Testing on row level would mean that the query `color=red` returns `4711` but also `NOT color=red`. This is not only a cosmetic issue, it potentially compromises the whole expression logic behind the scenes.

