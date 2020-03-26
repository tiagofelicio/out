-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : tables.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @tables.sql
-- Last Modified : 2020/03/26
-- ----------------------------------------------------------------------------------------------------------------------------

-- metadata

create table metadata.parameters (
    code varchar2(50) not null,
    description varchar2(250) not null,
    value varchar2(500) not null,
    constraint parameters_pk primary key (code)
)
pctfree 20 nologging compress noparallel;

insert into metadata.parameters (code, description, value) values ('reference_date', 'ETL Reference Date', '20200101');
commit;

-- data hub

create table dhub.dh_covid_19 (
    reference_date date,
    country varchar2(250),
    confirmed number,
    recovered number,
    deaths number,
    resource_name varchar2(250)
)
pctfree 0 nologging compress parallel;

create table dhub.dh_us_euro_foreign_exchange_rate (
    reference_date date,
    country varchar2(250),
    exchange_rate number,
    resource_name varchar2(250)
)
pctfree 0 nologging compress parallel;

create table dhub.dh_currency_codes (
    entity varchar2(250),
    currency varchar2(250),
    alphabeticcode varchar2(3),
    numericcode number(3),
    minorunit varchar2(250),
    withdrawaldate varchar2(250),
    resource_name varchar2(250)
)
pctfree 0 nologging compress parallel;

create table dhub.dh_country_codes (
    official_name_ar varchar2(250),
    official_name_cn varchar2(250),
    official_name_en varchar2(250),
    official_name_es varchar2(250),
    official_name_fr varchar2(250),
    official_name_ru varchar2(250),
    iso3166_1_alpha_2 varchar2(2),
    iso3166_1_alpha_3 varchar2(3),
    iso3166_1_numeric varchar2(3),
    iso4217_currency_alphabetic_code varchar2(250),
    iso4217_currency_country_name varchar2(250),
    iso4217_currency_minor_unit varchar2(250),
    iso4217_currency_name varchar2(250),
    iso4217_currency_numeric_code varchar2(250),
    m49 number,
    unterm_arabic_formal varchar2(250),
    unterm_arabic_short varchar2(250),
    unterm_chinese_formal varchar2(250),
    unterm_chinese_short varchar2(250),
    unterm_english_formal varchar2(250),
    unterm_english_short varchar2(250),
    unterm_french_formal varchar2(250),
    unterm_french_short varchar2(250),
    unterm_russian_formal varchar2(250),
    unterm_russian_short varchar2(250),
    unterm_spanish_formal varchar2(250),
    unterm_spanish_short varchar2(250),
    cldr_display_name varchar2(250),
    capital varchar2(250),
    continent varchar2(250),
    ds varchar2(250),
    developed_developing_countries varchar2(250),
    dial varchar2(250),
    edgar varchar2(250),
    fifa varchar2(250),
    fips varchar2(250),
    gaul varchar2(250),
    geoname_id number,
    global_code varchar2(250),
    global_name varchar2(250),
    ioc varchar2(250),
    itu varchar2(250),
    intermediate_region_code varchar2(250),
    intermediate_region_name varchar2(250),
    land_locked_developing_countries_lldc varchar2(250),
    languages varchar2(250),
    least_developed_countries_ldc varchar2(250),
    marc varchar2(250),
    region_code varchar2(250),
    region_name varchar2(250),
    small_island_developing_states_sids varchar2(250),
    sub_region_code varchar2(250),
    sub_region_name varchar2(250),
    tld varchar2(250),
    wmo varchar2(250),
    is_independent varchar2(250),
    resource_name varchar2(250)
)
pctfree 0 nologging compress parallel;

-- data warehouse

create table dwh.currencies (
    code varchar2(50),
    num varchar2(50),
    name varchar2(250),
    minor_unit number,
    id number
)
pctfree 0 nologging compress parallel;

insert into dwh.currencies (code, num, name, minor_unit, id) values (' ', null, null, null, 0);
commit;

create table dwh.regions (
    code varchar2(50),
    name varchar2(250),
    id number
)
pctfree 0 nologging compress parallel;

insert into dwh.regions (code, name, id) values (' ', null, 0);
commit;

create table dwh.countries (
    code varchar2(50),
    name varchar2(250),
    region_code varchar2(50),
    id number,
    region_id number
)
pctfree 0 nologging compress parallel;

insert into dwh.countries (code, name, region_code, id, region_id) values (' ', null, null, 0, 0);
commit;

create table dwh.exchange_rate_types (
    code varchar2(50),
    description varchar2(250),
    id number
)
pctfree 0 nologging compress parallel;

insert into dwh.exchange_rate_types (code, description, id) values (' ', null, 0);
insert into dwh.exchange_rate_types (code, description, id) values ('A', 'Average', 1);
commit;

create table dwh.exchange_rates (
    reference_date date,
    from_currency_code varchar2(50),
    to_currency_code varchar2(50),
    exchange_rate_type_code varchar2(50),
    value number,
    from_currency_id number,
    to_currency_id number,
    exchange_rate_type_id number
)
pctfree 0 nologging compress
partition by list (reference_date) (
    partition day_20200101 values (to_date('20200101', 'yyyymmdd'))
)
parallel;

create table dwh.covid_19_history (
    reference_date date,
    country_code varchar2(50),
    daily_cases number,
    daily_deaths number,
    daily_recovered number,
    total_cases number,
    total_deaths number,
    total_recovered number,
    country_id number,
    region_id number
)
pctfree 0 nologging compress
partition by list (reference_date) (
    partition day_20200101 values (to_date('20200101', 'yyyymmdd'))
)
parallel;
