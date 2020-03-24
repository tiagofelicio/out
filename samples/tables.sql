-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : tables.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @tables.sql
-- Last Modified : 2020/03/24
-- ----------------------------------------------------------------------------------------------------------------------------

-- hub

create table hub.dh_world_cities (
    name varchar2(250),
    country varchar2(250),
    subcountry varchar2(250),
    geonameid number,
    resource_name varchar2(250)
);

create table hub.dh_covid_19 (
    reference_date date,
    country varchar2(250),
    confirmed number,
    recovered number,
    deaths number,
    resource_name varchar2(250)
);

create table hub.dh_us_euro_foreign_exchange_rate (
    reference_date date,
    country varchar2(250),
    exchange_rate number,
    resource_name varchar2(250)
);

create table hub.dh_currency_codes (
    entity varchar2(250),
    currency varchar2(250),
    alphabeticcode varchar2(3),
    numericcode number(3),
    minorunit varchar2(250),
    withdrawaldate varchar2(250),
    resource_name varchar2(250)
);

-- dw
