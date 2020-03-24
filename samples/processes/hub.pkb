create or replace package body metadata.hub is

    subtype text is varchar2(100);

    -- data hub
    dh_api_url constant text := 'https://datahub.io';

    -- landing zone
    landing_zone constant text := '/home/oracle';

    procedure dh_covid_19 is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'covid-19');
        out.bind('landing_zone', macro => landing_zone);
        out.internet.http_get('#landing_zone/#dh_resource.csv', '#dh_api_url/#dh_library/#dh_resource/r/2.csv');
        out.files.load('dh_covid_19_01', '#landing_zone/#dh_resource.csv', q'[
            reference_date date mask "yyyy-mm-dd"
            country varchar2(250)
            confirmed number
            recovered number
            deaths number
        ]', q'[
            file format => delimited
            field separator => ,
            heading => 1
            record separator => \r\n
            text delimiter => "
        ]');
        out.data_integration.create_table('dh_covid_19', q'[
            select
                reference_date,
                country,
                confirmed,
                recovered,
                deaths,
                '#dh_library/#dh_resource' resource_name
            from dh_covid_19_01
        ]');
        out.data_integration.check_primary_key('dh_covid_19', 'reference_date, country');
        out.data_integration.incremental_update('data.dh_covid_19', 'dh_covid_19', q'[
            natural key => reference_date, country
        ]');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_covid_19_01');
        out.data_integration.drop_table('dh_covid_19');
        out.process('done');
    exception
        when others then
            out.process('error');
    end dh_covid_19;

    procedure dh_currency_codes is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'currency-codes');
        out.bind('landing_zone', macro => landing_zone);
        out.internet.http_get('#landing_zone/#dh_resource.csv', '#dh_api_url/#dh_library/#dh_resource/r/0.csv');
        out.files.load('dh_currency_codes_01', '#landing_zone/#dh_resource.csv', q'[
            entity varchar2(250)
            currency varchar2(250)
            alphabeticcode varchar2(3)
            numericcode number(3)
            minorunit varchar2(250)
            withdrawaldate varchar2(250)
        ]', q'[
            file format => delimited
            field separator => ,
            heading => 1
            record separator => \r\n
            text delimiter => "
        ]');
        out.data_integration.create_table('dh_currency_codes', q'[
            select
                entity,
                currency,
                alphabeticcode,
                numericcode,
                minorunit,
                withdrawaldate,
                '#dh_library/#dh_resource' resource_name
            from dh_currency_codes_01
        ]');
        out.data_integration.control_append('data.dh_currency_codes', 'dh_currency_codes');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_currency_codes_01');
        out.data_integration.drop_table('dh_currency_codes');
        out.process('done');
    exception
        when others then
            out.process('error');
    end dh_currency_codes;

    procedure dh_us_euro_foreign_exchange_rate is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'us-euro-foreign-exchange-rate');
        out.bind('landing_zone', macro => landing_zone);
        out.internet.http_get('#landing_zone/#dh_resource.csv', '#dh_api_url/#dh_library/#dh_resource/r/0.csv');
        out.files.load('dh_us_euro_foreign_exchange_rate_01', '#landing_zone/#dh_resource.csv', q'[
            reference_date date mask "yyyy-mm-dd"
            country varchar2(250)
            exchange_rate number
        ]', q'[
            file format => delimited
            field separator => ,
            heading => 1
            record separator => \r\n
            text delimiter => "
        ]');
        out.data_integration.create_table('dh_us_euro_foreign_exchange_rate', q'[
            select
                reference_date,
                country,
                exchange_rate,
                '#dh_library/#dh_resource' resource_name
            from dh_us_euro_foreign_exchange_rate_01
        ]');
        out.data_integration.check_primary_key('dh_us_euro_foreign_exchange_rate', 'reference_date, country');
        out.data_integration.incremental_update('data.dh_us_euro_foreign_exchange_rate', 'dh_us_euro_foreign_exchange_rate', q'[
            natural key => reference_date, country
        ]');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_us_euro_foreign_exchange_rate_01');
        out.data_integration.drop_table('dh_us_euro_foreign_exchange_rate');
        out.process('done');
    exception
        when others then
            out.process('error');
    end dh_us_euro_foreign_exchange_rate;

    procedure dh_world_cities is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'world-cities');
        out.bind('landing_zone', macro => landing_zone);
        out.internet.http_get('#landing_zone/#dh_resource.csv', '#dh_api_url/#dh_library/#dh_resource/r/0.csv');
        out.files.load('dh_world_cities_01', '#landing_zone/#dh_resource.csv', q'[
            name varchar2(250)
            country varchar2(250)
            subcountry varchar2(250)
            geonameid number
        ]', q'[
            file format => delimited
            field separator => ,
            heading => 1
            record separator => \r\n
            text delimiter => "
        ]');
        out.data_integration.create_table('dh_world_cities', q'[
            select
                name,
                country,
                subcountry,
                geonameid,
                '#dh_library/#dh_resource' resource_name
            from dh_world_cities_01
        ]');
        out.data_integration.check_primary_key('dh_world_cities', 'geonameid');
        out.data_integration.incremental_update('data.dh_world_cities', 'dh_world_cities', q'[
            natural key => geonameid
        ]');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_world_cities_01');
        out.data_integration.drop_table('dh_world_cities');
        out.process('done');
    exception
        when others then
            out.process('error');
    end dh_world_cities;

end hub;
/