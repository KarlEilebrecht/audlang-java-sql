# Audlang Java SQL

This repository is part of project [Audlang](https://github.com/users/KarlEilebrecht/projects/1/views/1?pane=info) and provides an SQL-converter implementation for the **[Audience Definition Language Specification (Audlang)](https://github.com/KarlEilebrecht/audlang-spec/blob/main/doc/AudienceDefinitionLanguageSpecification.md#audience-definition-language-specification)** based
on the [Audlang Java Core Project](https://github.com/KarlEilebrecht/audlang-java-core). Latest build artifacts can be found on [Maven Central](https://central.sonatype.com/namespace/de.calamanari.adl).

```xml
		<dependency>
			<groupId>de.calamanari.adl</groupId>
			<artifactId>audlang-java-sql</artifactId>
			<version>1.1.0</version>
		</dependency>
```

There is a small set of further dependencies (e.g., SLF4j for logging, JUnit), please refer to this project's POM for details.

Primarily, this project includes a functional **ready-to-use implementation** to map various table landscapes and generate SQLs for these setups from [CoreExpressions](https://github.com/KarlEilebrecht/audlang-java-core/blob/main/src/main/java/de/calamanari/adl/irl/README.md). Additionally, it aims to be an **extensible framework** for easy adaption to the needs of an individual database environment.

From the beginning it was clear that a general purpose strategy to structure SQLs cannot match all possible scenarios or meet all individual standards and preferences. Thus, there are many points where you can **easily adjust aspects** of the generation process, by configuration, inheritance or a mix. For example, the [SqlAugmentationListener](./src/main/java/de/calamanari/adl/sql/cnv/SqlAugmentationListener.java) interface allows for the application of vendor-specific **database hints** to any generated SQL without touching the core implementation.

The framework encourages the use of JDBC and **PreparedStatements to avoid any [SQL-injection vulnerability](https://en.wikipedia.org/wiki/SQL_injection)**. Consequently, the conversion result is a [QueryTemplateWithParameters](./src/main/java/de/calamanari/adl/sql/QueryTemplateWithParameters.java) for immediate execution as a PreparedStatement on a database connection. However, for the sake of easy logging and debugging, resp. to support edge cases (e.g., no JDBC-driver available), every statement can also be printed in readable form.

Significant parts of this project are dedicated to the definition and mapping of a physical table setup. A major goal was to **make as little assumptions** about the table landscape as possible. The approach should be rather ***"describe what you have"*** than *"make your db-layout compatible"*. The only concession I had to make was that the connection between any two tables must happen through a single key, and the type of this key must be the same for all involved tables. Composite primary/foreign keys are not supported. Besides this restriction most table landscapes should be mappable. A **fluent self-explaining API** helps simplify the setup and keep the configuration short and readable. Special aspects like **multi-tenancy** are covered by the built-in support for **table and column filters**. This way data columns can be kept separate from technical columns to limit or scope the data access. Although, best practice should be first setting up a logical data model in form of an [ArgMetaInfoLookup](https://github.com/KarlEilebrecht/audlang-java-core/tree/main/src/main/java/de/calamanari/adl/cnv/tps#readme) and mapping it to the tables, the framework also provides **rule-based auto-mapping**. This is especially interesting for initial experimenting and testing.

Bridging the gap between a logical data model that conforms to the [Audlang Type Conventions](https://github.com/KarlEilebrecht/audlang-spec/blob/main/doc/AudienceDefinitionLanguageSpecification.md#2-type-conventions) (resp. an untyped model) and a physical data model that uses SQL-types was a main challenge during the development. The framework addresses the problem most of the time with **[automatic type coalescence](./doc/type-coalescence.md)** for many JDBC-types. Special cases can be handled with **type decoration** or **native type casting**.

Assuming a functional JDBC-connection to your database, in less than than an hour you should be able to run the first queries against your database. Alternatively, you can use or extend the [example data](./src/test/resources/h2init.sql) for the [H2-database](https://www.h2database.com/html/main.html).

*Give it a try, have fun!*

Karl Eilebrecht, January 2025

***Read next:***
 * **[Mapping a table landscape](./doc/mapping.md)**
 * [Package documentation](./src/main/java/de/calamanari/adl/sql/README.md)
 
----
<img align="right" src="https://sonarcloud.io/api/project_badges/measure?project=KarlEilebrecht_audlang-java-sql&metric=alert_status" />

[![SonarQube Cloud](https://sonarcloud.io/images/project_badges/sonarcloud-light.svg)](https://sonarcloud.io/summary/new_code?id=KarlEilebrecht_audlang-java-sql)


