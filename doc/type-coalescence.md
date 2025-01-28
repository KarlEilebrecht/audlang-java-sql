#### [Project Overview](../README.md) | [Mapping a table landscape](./mapping.md)
----

# About type-coalescence

The Audience Definition Language (ADL) itself is *type-agnostic* but comes with a set of **[type conventions](https://github.com/KarlEilebrecht/audlang-spec/blob/main/doc/AudienceDefinitionLanguageSpecification.md#2-type-conventions)**. SQL on the other hand has a few standard types and many additional types, potentially with vendor-specific behavior.

## Implementation goals

 * Mapping should be made simple. If a type combination logically works, then the framework should just do the job, roughly following the idea of [duck typing](https://en.wikipedia.org/wiki/Duck_typing).
 * If there is no logical model (means all user-entered information is string), then the ADL's type conventions should be used to make the values compatible to the mapped database column's type if possible.

## Basic value adjustments

The auto-adjustment supports many common scenarios, for example:
 * STRINGs are compatible to *every* underlying data type as long as their format meets the [§2 Type conventions](https://github.com/KarlEilebrecht/audlang-spec/blob/main/doc/AudienceDefinitionLanguageSpecification.md#2-type-conventions)).
 * An INTEGER is compatible to SQL INTEGER, TINYINT but also to DECIMAL, VARCHAR and some other.
 * A DECIMAL is compatible to DECIMAL but also to SQL INTEGER (ignoring the digits after the dot), STRING and some more types.
 * A BOOL (0,1) is compatible to SQL BOOLEAN, BIT but also to String and Integer.

 The full list of adjustments can be found in [DefaultQueryParameterCreator#**isTypeCombinationSupported(...)**](../src/main/java/de/calamanari/adl/sql/DefaultQueryParameterCreator.java). In this class you can also find the exact implementation.

 ## Special case: Date

 By intention the ADL does not deal with *time* (see [§2.3 Date Values](https://github.com/KarlEilebrecht/audlang-spec/blob/main/doc/AudienceDefinitionLanguageSpecification.md#23-date-values) and [Dealing with date values](https://github.com/KarlEilebrecht/audlang-spec/blob/main/doc/AudienceDefinitionLanguageSpecification.md#dealing-with-date-values)).

 This leads to problems when it comes to mapping a database. Of course, best would be requesting any mappable column carrying a date to be of type SQL DATE. But this would be inconvenient. In fact, we must be prepared for date information occurring as DATE column or as TIMESTAMP (with 00:00:00 time portion) or even as TIMESTAMP with arbitrary time portion. Or maybe the date is stored as an [EPOCH value](https://en.wikipedia.org/wiki/Epoch_(computing)).

 The advanced type coalescence automatically detects if an ADL-date is being mapped onto a column with a *finer resolution*, e.g., TIMESTAMP and performs special adjustments.

 *Examples:*
  * Let's assume the column `COL_A` maped to attribute `a` contains a TIMESTAMP `2024-07-31 17:29:21` and the query would be `a = 2024-07-31`. Obviously, any direct equals-comparison would fail. Thus, the query generator will turn the query into a proper range query (everything between `2024-07-31 00:00:00` (incl.) and `2024-08-01 00:00:00` (excl.)).
  * A similar adjust will be made for *greater than* queries. If a user queries `a > 2024-07-31` then it would be surprising to *include* `2024-07-31 17:29:21`. Instead the query builder will adjust the condition to `>= 2024-08-01 00:00:00`.

Automatic date alignment happens for TIMESTAMP, INTEGER and some other column types. The details can be found in [MatchCondition#**shouldAlignDate(...)**](../src/main/java/de/calamanari/adl/sql/cnv/MatchCondition.java).

:bulb: To disable the advanced date handling you can configure the flag **[DISABLE_DATE_TIME_ALIGNMENT](../src/main/java/de/calamanari/adl/sql/cnv/ConversionDirective.java)** for the conversion run.

