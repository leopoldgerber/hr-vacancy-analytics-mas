
# Dataset Description

This README provides a comprehensive description of the database structure, including all tables, their fields, data types, and relationships.

The database stores information related to clients, geographic references (countries, regions, cities), analytical entities (profiles), and vacancy activity data collected from job platforms. It is designed to support multi-tenant data isolation, geographic normalization, lifecycle tracking, and analytical processing.


## Table of Contents

- [vacancy_activity](#vacancy_activity)
- [cities](#cities)
- [clients](#clients)
- [countries](#countries)
- [profiles](#profiles)
- [regions](#regions)
- [vacancies](#vacancies)

## Tables and Fields Description

### vacancy_activity

`vacancy_activity` - Vacancy activity table
Stores published vacancy snapshots, including attributes, compensation
details, and response metrics collected from job platforms, enabling
time-based analytics, performance tracking, and cross-segmentation by
client, geography, and profile.

`client_id` (**INTEGER**)
Unique identifier of the client who posted the vacancy.

`source` (**TEXT**)
Platform where the vacancy is published.

`vacancy_id` (**BIGINT**)
Unique identifier of the vacancy on the platform.

`publication_date` (**TIMESTAMP**)
Date when the vacancy was published on the platform.

`tariff` (**TEXT**)
Publication tariff or plan used on the platform.

`responses` (**INTEGER**)
Number of responses to the vacancy during a fixed observation period
(cannot be strictly determined).

`total_responses` (**INTEGER**)
Total number of responses to the vacancy over its entire publication
period.

`company_name` (**TEXT**)
Name of the employer company that posted the vacancy.

`salary_from_recalculated` (**INTEGER**)
Lower bound of the salary after conversion to a unified format or
currency.

`salary_to_recalculated` (**INTEGER**)
Upper bound of the salary after conversion to a unified format or
currency.

`tax` (**NUMERIC(12, 4)**)
Indicator specifying how taxation is accounted for in the stated salary.

`salary_indication` (**TEXT**)
Flag indicating the presence or method of salary specification in the
vacancy.

`city` (**TEXT**)
City where the job is located.

`profile` (**TEXT**)
Professional domain or profile of the vacancy.

`region` (**TEXT**)
Administrative region where the vacancy is located.

`employment_type` (**TEXT**)
Type of employment offered in the vacancy.

`work_experience` (**TEXT**)
Required level or duration of professional experience.

`work_schedule` (**TEXT**)
Work schedule format.

`date` (**DATE**)
Technical date capturing the vacancy state in the dataset.

`vacancy_title` (**TEXT**)
Title of the vacancy.

`salary_from` (**INTEGER**)
Original lower salary bound specified in the vacancy.

`salary_to` (**INTEGER**)
Original upper salary bound specified in the vacancy.

`payment_type` (**TEXT**)
Type of salary payment.

`specialization` (**TEXT**)
Professional specialization within a broader profile.

`skills` (**TEXT**)
List of required skills and competencies for the vacancy.

`metro_stations` (**TEXT**)
Metro stations located near the workplace.

`vacancy_description` (**TEXT**)
Text description of the vacancy, including responsibilities,
requirements, and conditions.

`config_id` (**TEXT**)
Identifier of the configuration or parameter set used to generate the
dataset sample.

------------------------------------------------------------------------

### cities

`cities` - Cities reference table
Stores normalized city entities to ensure unambiguous geographic
filtering and consistent interpretation of user queries and analytics.

`id` (SERIAL)
Unique city identifier (primary key).

`country_id` (INTEGER)
Reference to the country (`FK -> countries.id`).

`region_id` (INTEGER)
Reference to the region (`FK -> regions.id`).

`name` (TEXT)
Official city name used for display.

`is_active` (BOOLEAN)
Flag indicating whether the city is active in the system.

`created_at` (TIMESTAMP)
Record creation timestamp.

`updated_at` (TIMESTAMP)
Last update timestamp.

`population` (INTEGER)
City population used for analytical segmentation and metric
normalization.

------------------------------------------------------------------------

### clients

`clients` - Client account
Stores client entities and isolates their data across the system.

`id` (SERIAL)
Primary key.

`uuid` (UUID)
Public client identifier.

`name` (TEXT)
Company or client name.

`slug` (VARCHAR(100))
Human-readable unique identifier used in URLs.

`is_active` (INTEGER)
Reference to client status (`FK -> client_statuses.id`).

`country_id` (INTEGER)
Reference to country (`FK -> countries.id`).

`timezone` (INTEGER)
Timezone offset relative to Helsinki time.

`plan_id` (INTEGER)
Reference to subscription plan (`FK -> plans.id`).

`created_at` (TIMESTAMP)
Creation timestamp.

`updated_at` (TIMESTAMP)
Last update timestamp.

------------------------------------------------------------------------

### countries

`countries` - Countries reference table
Stores supported countries for geographic normalization.

`id` (SERIAL)
Primary key.

`name` (TEXT)
Country name.

`iso2_code` (CHAR(2))
Two-letter ISO code.

`iso3_code` (CHAR(3))
Three-letter ISO code.

`language_code` (VARCHAR(5))
Default country language.

`created_at` (TIMESTAMP)
Creation timestamp.

`updated_at` (TIMESTAMP)
Last update timestamp.

`is_active` (BOOLEAN)
Active flag.

------------------------------------------------------------------------

### profiles

`profiles` - Client profiles
Stores analytical entities for metric tracking within a client.

`id` (SERIAL)
Primary key.

`client_id` (INTEGER)
Reference to client (`FK -> clients.id`).

`uuid` (UUID)
Public profile identifier.

`name` (TEXT)
Profile name.

`is_active` (BOOLEAN)
Active flag.

`created_at` (TIMESTAMP)
Creation timestamp.

`updated_at` (TIMESTAMP)
Last modification timestamp.

------------------------------------------------------------------------

### regions

`regions` - Regions reference table
Stores administrative units within a country.

`id` (SERIAL)
Primary key.

`country_id` (INTEGER)
Reference to country (`FK -> countries.id`).

`name` (TEXT)
Region name.

`code` (TEXT)
Short region code.

`is_active` (BOOLEAN)
Active flag.

`created_at` (TIMESTAMP)
Creation timestamp.

`updated_at` (TIMESTAMP)
Last update timestamp.

------------------------------------------------------------------------

### vacancies

`vacancy_activity` - Vacancy activity snapshot
Stores platform-level snapshots of client vacancies used for analytics
and monitoring.

`id` (SERIAL)
Primary key.

`created_at` (TIMESTAMP)
Record creation timestamp.

(Structure fully matches the `vacancy_activity` table, including all
fields such as client_id, source, vacancy_id, publication_date, etc.)
