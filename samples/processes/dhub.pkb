create or replace package body metadata.dhub is

    subtype text is varchar2(100);

    -- data hub
    dh_api_url constant text := 'https://datahub.io';

    -- landing zone
    landing_zone constant text := '/home/oracle';

    procedure dh_country_codes is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'country-codes');
        out.bind('landing_zone', macro => landing_zone);
        out.internet.http_get('#landing_zone/#dh_resource.csv', '#dh_api_url/#dh_library/#dh_resource/r/0.csv');
        out.files.load('dh_country_codes_01', '#landing_zone/#dh_resource.csv', q'[
            official_name_ar varchar2(250)
            official_name_cn varchar2(250)
            official_name_en varchar2(250)
            official_name_es varchar2(250)
            official_name_fr varchar2(250)
            official_name_ru varchar2(250)
            iso3166_1_alpha_2 varchar2(2)
            iso3166_1_alpha_3 varchar2(3)
            iso3166_1_numeric varchar2(3)
            iso4217_currency_alphabetic_code varchar2(250)
            iso4217_currency_country_name varchar2(250)
            iso4217_currency_minor_unit varchar2(250)
            iso4217_currency_name varchar2(250)
            iso4217_currency_numeric_code varchar2(250)
            m49 number
            unterm_arabic_formal varchar2(250)
            unterm_arabic_short varchar2(250)
            unterm_chinese_formal varchar2(250)
            unterm_chinese_short varchar2(250)
            unterm_english_formal varchar2(250)
            unterm_english_short varchar2(250)
            unterm_french_formal varchar2(250)
            unterm_french_short varchar2(250)
            unterm_russian_formal varchar2(250)
            unterm_russian_short varchar2(250)
            unterm_spanish_formal varchar2(250)
            unterm_spanish_short varchar2(250)
            cldr_display_name varchar2(250)
            capital varchar2(250)
            continent varchar2(250)
            ds varchar2(250)
            developed_developing_countries varchar2(250)
            dial varchar2(250)
            edgar varchar2(250)
            fifa varchar2(250)
            fips varchar2(250)
            gaul varchar2(250)
            geoname_id number
            global_code varchar2(250)
            global_name varchar2(250)
            ioc varchar2(250)
            itu varchar2(250)
            intermediate_region_code varchar2(250)
            intermediate_region_name varchar2(250)
            land_locked_developing_countries_lldc varchar2(250)
            languages varchar2(250)
            least_developed_countries_ldc varchar2(250)
            marc varchar2(250)
            region_code varchar2(250)
            region_name varchar2(250)
            small_island_developing_states_sids varchar2(250)
            sub_region_code varchar2(250)
            sub_region_name varchar2(250)
            tld varchar2(250)
            wmo varchar2(250)
            is_independent varchar2(250)
        ]', q'[
            file format => delimited
            field separator => ,
            heading => 1
            record separator => \r\n
            text delimiter => "
        ]');
        out.data_integration.create_table('dh_country_codes', q'[
            select
                official_name_ar,
                official_name_cn,
                official_name_en,
                official_name_es,
                official_name_fr,
                official_name_ru,
                iso3166_1_alpha_2,
                iso3166_1_alpha_3,
                iso3166_1_numeric,
                iso4217_currency_alphabetic_code,
                iso4217_currency_country_name,
                iso4217_currency_minor_unit,
                iso4217_currency_name,
                iso4217_currency_numeric_code,
                m49,
                unterm_arabic_formal,
                unterm_arabic_short,
                unterm_chinese_formal,
                unterm_chinese_short,
                unterm_english_formal,
                unterm_english_short,
                unterm_french_formal,
                unterm_french_short,
                unterm_russian_formal,
                unterm_russian_short,
                unterm_spanish_formal,
                unterm_spanish_short,
                cldr_display_name,
                capital,
                continent,
                ds,
                developed_developing_countries,
                dial,
                edgar,
                fifa,
                fips,
                gaul,
                geoname_id,
                global_code,
                global_name,
                ioc,
                itu,
                intermediate_region_code,
                intermediate_region_name,
                land_locked_developing_countries_lldc,
                languages,
                least_developed_countries_ldc,
                marc,
                region_code,
                region_name,
                small_island_developing_states_sids,
                sub_region_code,
                sub_region_name,
                tld,
                wmo,
                is_independent,
                '#dh_library/#dh_resource' resource_name
            from dh_country_codes_01
        ]');
        out.data_integration.control_append('dhub.dh_country_codes', 'dh_country_codes', q'[
            truncate table => true
        ]');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_country_codes_01');
        out.data_integration.drop_table('dh_country_codes');
        out.process('done');
    exception
        when others then
            out.process('error');
    end dh_country_codes;

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
        out.data_integration.control_append('dhub.dh_covid_19', 'dh_covid_19', q'[
            truncate table => true
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
        out.data_integration.control_append('dhub.dh_currency_codes', 'dh_currency_codes', q'[
            truncate table => true
        ]');
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
        out.data_integration.control_append('dhub.dh_us_euro_foreign_exchange_rate', 'dh_us_euro_foreign_exchange_rate', q'[
            truncate table => true
        ]');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_us_euro_foreign_exchange_rate_01');
        out.data_integration.drop_table('dh_us_euro_foreign_exchange_rate');
        out.process('done');
    exception
        when others then
            out.process('error');
    end dh_us_euro_foreign_exchange_rate;

end dhub;
/