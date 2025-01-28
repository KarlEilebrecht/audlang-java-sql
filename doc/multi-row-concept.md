#### [Project Overview](../README.md) | [About the nature of tables and columns](./table-column-nature.md)
----

# About "multi-row" and sparse data

## Definition of "multi-row"

The multi-row-concept addresses an issue you see when mapping attributes to columns in a table. When queried in the wrong way the result set can be empty or incomplete.

It is easier to explain this with examples:

### Example 1: A "collection" attribute

Let's assume a scenario with an attribute *favorite color* `favColor`. An individual (record in the base audience) can have multiple `favColor`s. Therefor the data was onboarded to the table T_FAV_COLOR with the layout below.

| ID   | COLOR  |
|------|--------|
| 2145 | red    |
| 2234 | blue   |
| 2234 | yellow |
| 2551 | red    |

The user wants to see all guys that have the two favorite colors blue and yellow (`favColor=blue AND favColor=yellow`) *at the same time*. The attribute `favColor` is a collection, so this should return `2234`.

Look at the query below:

```sql
SELECT DISTINCT ID
FROM T_FAV_COLOR
WHERE COLOR = 'red' AND COLOR = 'yellow'
```

*Do you see the problem?* - The first condition `COLOR = 'red'` fixes the row, so the second condition cannot be met anymore. This effect is called *accidental row-pinning*. Just like in this simple example similar problems can occur in more complicated ways.

This is an example where we must inform the query builder that the mapping is **multi-row**, means: the data for the same individual in this table may sit on more than one row. In pure SQL (no magic data types involved) **every collection attribute must be a multi-row attribute**.

### Example 2: Lazy onboarding

| ID   | ID_OWNER | COLOR |
|------|----------|-------|
| 2145 | GANYMED  | red   |
| 2234 | JOKER    | blue  |
| 2234 | JOKER    | red   |
| 2234 |          | green |
| 2551 | KNOLL    | black |

Here, the attribute in column `ID_OWNER` is not a collection. There cannot be multiple owners for the same ID. But unfortunately, for whatever reason it is not always present.

This causes two issues:

First, the query
```sql
SELECT DISTINCT ID
FROM T_FAV_COLOR
WHERE ID_OWNER = 'JOKER' AND COLOR = 'green'
```
does **not** return the record with ID 2234 although the ID-owner is available.

Second, the query
```sql
SELECT DISTINCT ID
FROM T_FAV_COLOR
WHERE ID_OWNER IS NULL AND COLOR = 'green'
```
**returns** the record with ID 2234 although the ID-owner is available.

So, there are cases where we must tell the query-builder that a mapping is **multi-row** even if the related attribute is not a collection.

## Sparse data

By marking a table to carry "sparse data" you basically tell the query-builder that *every* column in this column should be treated as *multi-row*.

This can happen if there is a single table with each column representing an attribute, but the data has been *unioned* from multiple sources without actually *merging* it.

| ID   |  A1  |  A2  |  A3  |
|------|------|------|------|
| 2145 |  17  | NULL | NULL |
| 2234 |  11  | NULL | NULL |
| 2234 | NULL | X172 | NULL |
| 2234 | NULL | NULL | K612 |
| 2551 | NULL | NULL | K612 |

This again causes the two known issues:

First, the query
```sql
SELECT DISTINCT ID
FROM T_FAV_COLOR
WHERE A1 = 11 AND A2 = 'X172' AND A3 = 'K612'
```
does **not** return the record with ID 2234.

Second, the query
```sql
SELECT DISTINCT ID
FROM T_FAV_COLOR
WHERE A1 IS NULL OR A2 IS NULL OR A3 IS NULL
```
**returns** the record with ID 2234 although there is information for 2234 and all requested attributes.

Here, we should mark the table as *sparse*. Then the query-builder will create existence checks rather than simply combining conditions.

## Disadvantages

First of all, marking an attribute mapping *multi-row* most of the time complicates the query. Instead of running a plain select, it must be ensured that there is (not) any row matching a single condition. This is **expensive**. However, in case of key-value tables or collections you can't avoid this complexity.

But there is another potential problem: In a table with multiple columns a "multi-row" attribute gets *logically detached* from its row-context.

Check the `T_POSDATA`-data table below:

| UID | INV_NO | INV_DATE | CODE | DESCRIPTION | QUANTITY |UNIT_PRICE | COUNTRY |
|-------|-------|-------|-------|------------|---|-------|-----|
| 19011 | 72163 | 2024-01-13 | 145AB | DROP LIGHT | 2 |198.78 | USA |
| 19017 | 41191 | 2024-03-14 | 115BC | RED WINE 0.75L | 6 |5.75 | FRANCE |
| 19017 | 39967 | 2024-03-15 | 22039 | CORNFLAKES | 2 |4.25 | FRANCE |
| 19017 | 23721 | 2024-03-15 | H778A | CHEESE 36M 100GR | 1 |8.99 | FRANCE |
| 19017 | 64785 | 2024-03-31 | 5521C | WATERMELON EXTRACT 0.118L | 1 |199.99 | FRANCE |

You decided to mark the *invoiceDate* attribute as a collection resp. the mapping as "multi-row",
so the user can easily find out if *anything* was bought at a given date.

The issue with this approach is that the *invoiceDate* is now logically decoupled from the remaining columns.

For record `19017` we know this guy bought *anything* (namely this super-expensive watermelon extract :smile: ) on `2024-03-31`. And we know the same guy bought cornflakes on `2024-03-15`.
So, assuming sparse data, the query `invoiceDate=2024-03-31 AND description=CORNFLAKES` would return record `19017`. This may be what you want or not ...

A simple tweak would be mapping the `INV_DATE` column twice, first regularly to *invoiceDate* and a second time to *anyInvoiceDate*, this time marked *multi-row*.

Whatever you decide, keep in mind that *multi-row* or *sparse* don't play well with tables that carry *related attributes* of a record. Especially, *sparse* should only be used temporarily. Better streamline the table content or layout instead.

The table above is part of the **[H2](https://www.h2database.com/html/main.html)** unit test setup. See [h2init.sql](../src/test/resources/h2init.sql), [H2TestBindings](../src/test/java/de/calamanari/adl/sql/cnv/H2TestBindings.java) and [DefaultSqlExpressionConverterComplexH2Test](../src/test/java/de/calamanari/adl/sql/cnv/DefaultSqlExpressionConverterComplexH2Test.java).