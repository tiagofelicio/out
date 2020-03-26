create or replace package body metadata.dwh is

    procedure countries is
    begin
        out.process('start');
        out.data_integration.create_table('countries_01', q'[
            select
                iso3166_1_alpha_2 code,
                cldr_display_name name,
                case iso3166_1_alpha_2 when 'TW' then '30' else coalesce(intermediate_region_code, sub_region_code, iso3166_1_alpha_2) end region_code
            from dhub.dh_country_codes
            where
                iso3166_1_alpha_2 is not null
        ]');
        out.data_integration.create_table('countries', q'[
            select
                countries_01.code,
                countries_01.name,
                countries_01.region_code,
                nvl(regions.id, 0) region_id
            from countries_01
            left join dwh.regions on
                countries_01.region_code = regions.code
        ]');
        out.data_integration.check_primary_key('countries', 'code');
        out.data_integration.incremental_update('dwh.countries', 'countries', q'[
            natural key => code
            surrogate key => id
        ]');
        out.data_integration.drop_table('countries_01');
        out.data_integration.drop_table('countries');
        out.process('done');
    exception
        when others then
            out.process('error');
    end countries;

    procedure covid_19_history is
    begin
        out.process('start');
        out.bind('reference_date', date_value => out.utilities.plsql(into_date => q'[select to_date(value, 'yyyymmdd') from metadata.parameters where code = 'reference_date']'));
        out.data_integration.create_table('covid_19_history_01', q'[
            select
                reference_date,
                country,
                confirmed,
                recovered,
                deaths
            from dhub.dh_covid_19
            where
                reference_date = #reference_date
        ]');
        out.data_integration.create_table('covid_19_history_02', q'{
            select
                covid_19_history_01.country,
                case covid_19_history_01.country
                    when 'Cote d''Ivoire' then 'CI'
                    when 'Eswatini' then 'SZ'
                    when 'Korea, South' then 'KR'
                    when 'North Macedonia' then 'MK'
                    when 'United Kingdom' then 'GB'
                    else dh_country_codes.iso3166_1_alpha_2
                end country_code
            from covid_19_history_01
            left join dhub.dh_country_codes on
                regexp_replace(covid_19_history_01.country, '[^a-zA-z]') = regexp_replace(dh_country_codes.cldr_display_name, '[^a-zA-z]') or
                regexp_replace(covid_19_history_01.country, '[^a-zA-z]') = regexp_replace(dh_country_codes.official_name_en, '[^a-zA-z]')
        }');
        out.data_integration.check_primary_key('covid_19_history_02', 'country');
        out.data_integration.create_table('covid_19_history_03', q'[
            select
                covid_19_history_01.reference_date,
                covid_19_history_02.country_code,
                sum(covid_19_history_01.confirmed) total_cases,
                sum(covid_19_history_01.deaths) total_deaths,
                sum(covid_19_history_01.recovered) total_recovered
            from covid_19_history_01
            left join covid_19_history_02 on
                covid_19_history_01.country = covid_19_history_02.country
            group by
                covid_19_history_01.reference_date,
                covid_19_history_02.country_code
        ]');
        out.data_integration.create_table('covid_19_history_04', q'[
            select
                covid_19_history_03.reference_date,
                covid_19_history_03.country_code,
                covid_19_history_03.total_cases,
                covid_19_history_03.total_deaths,
                covid_19_history_03.total_recovered,
                nvl(countries.id, 0) country_id,
                nvl(countries.region_id, 0) region_id
            from covid_19_history_03
            left join dwh.countries on
                covid_19_history_03.country_code = countries.code
        ]');
        out.data_integration.check_primary_key('covid_19_history_04', 'country_id');
        out.data_integration.create_table('covid_19_history_05', q'[
            select
                total_cases,
                total_deaths,
                total_recovered,
                country_id
            from dwh.covid_19_history
            where
                reference_date = #reference_date - 1
        ]');
        out.data_integration.create_table('covid_19_history', q'[
            select
                covid_19_history_04.reference_date,
                covid_19_history_04.country_code,
                coalesce(covid_19_history_04.total_cases, covid_19_history_05.total_cases, 0) - nvl(covid_19_history_05.total_cases, 0) daily_cases,
                coalesce(covid_19_history_04.total_deaths, covid_19_history_05.total_deaths, 0) - nvl(covid_19_history_05.total_deaths, 0) daily_deaths,
                coalesce(covid_19_history_04.total_recovered, covid_19_history_05.total_recovered, 0) - nvl(covid_19_history_05.total_recovered, 0) daily_recovered,
                coalesce(covid_19_history_04.total_cases, covid_19_history_05.total_cases, 0) total_cases,
                coalesce(covid_19_history_04.total_deaths, covid_19_history_05.total_deaths, 0) total_deaths,
                coalesce(covid_19_history_04.total_recovered, covid_19_history_05.total_recovered, 0) total_recovered,
                covid_19_history_04.country_id,
                covid_19_history_04.region_id
            from covid_19_history_04
            left join covid_19_history_05 on
                covid_19_history_04.country_id = covid_19_history_05.country_id
        ]');
        out.data_integration.check_primary_key('covid_19_history', 'country_id');
        out.data_integration.control_append('dwh.covid_19_history', 'covid_19_history', q'[
            partition value => #reference_date
            truncate partition => true
        ]');
        out.data_integration.drop_table('covid_19_history_01');
        out.data_integration.drop_table('covid_19_history_02');
        out.data_integration.drop_table('covid_19_history_03');
        out.data_integration.drop_table('covid_19_history_04');
        out.data_integration.drop_table('covid_19_history_05');
        out.data_integration.drop_table('covid_19_history');
        out.process('done');
    exception
        when others then
            out.process('error');
    end covid_19_history;

    procedure currencies is
    begin
        out.process('start');
        out.data_integration.create_table('currencies', q'[
            select distinct
                alphabeticcode code,
                lpad(to_char(numericcode), 3, '0') num,
                currency name,
                to_number(minorunit) minor_unit
            from dhub.dh_currency_codes
            where
                alphabeticcode is not null and
                replace(minorunit, '-') is not null
        ]');
        out.data_integration.check_primary_key('currencies', 'code');
        out.data_integration.incremental_update('dwh.currencies', 'currencies', q'[
            natural key => code
            surrogate key => id
        ]');
        out.data_integration.drop_table('currencies');
        out.process('done');
    exception
        when others then
            out.process('error');
    end currencies;

    procedure exchange_rates is
    begin
        out.process('start');
        out.bind('reference_date', date_value => out.utilities.plsql(into_date => q'[select to_date(value, 'yyyymmdd') from metadata.parameters where code = 'reference_date']'));
        out.data_integration.create_table('exchange_rates_01', q'[
            select
                max(reference_date) reference_date,
                country
            from dhub.dh_us_euro_foreign_exchange_rate
            where
                reference_date <= #reference_date and
                exchange_rate is not null
            group by
                country
        ]');
        out.data_integration.create_table('exchange_rates_02', q'[
            select
                dh_us_euro_foreign_exchange_rate.reference_date,
                dh_us_euro_foreign_exchange_rate.country,
                dh_us_euro_foreign_exchange_rate.exchange_rate
            from dhub.dh_us_euro_foreign_exchange_rate
            inner join exchange_rates_01 on
                dh_us_euro_foreign_exchange_rate.reference_date = exchange_rates_01.reference_date and
                dh_us_euro_foreign_exchange_rate.country = exchange_rates_01.country
        ]');
        out.data_integration.create_table('exchange_rates_03', q'[
            select
                exchange_rates_02.country,
                case exchange_rates_02.country
                    when 'Euro' then 'EUR' when 'United Kingdom' then 'GBP'
                    when 'Taiwan' then 'TWD'
                    else dh_country_codes.iso4217_currency_alphabetic_code
                end currency_code
            from exchange_rates_02
            left join dhub.dh_country_codes on
                exchange_rates_02.country = dh_country_codes.cldr_display_name
        ]');
        out.data_integration.check_primary_key('exchange_rates_03', 'country');
        out.data_integration.create_table('exchange_rates_04', q'[
            select
                #reference_date reference_date,
                'USD' from_currency_code,
                exchange_rates_03.currency_code to_currency_code,
                'A' exchange_rate_type_code,
                exchange_rates_02.exchange_rate value
            from exchange_rates_02
            left join exchange_rates_03 on
                exchange_rates_02.country = exchange_rates_03.country
        ]');
        out.data_integration.create_table('exchange_rates', q'[
            select
                exchange_rates_04.reference_date,
                exchange_rates_04.from_currency_code,
                exchange_rates_04.to_currency_code,
                exchange_rates_04.exchange_rate_type_code,
                exchange_rates_04.value,
                nvl(from_currencies.id, 0) from_currency_id,
                nvl(to_currencies.id, 0) to_currency_id,
                exchange_rate_types.id exchange_rate_type_id
            from exchange_rates_04
            left join dwh.currencies from_currencies on
                exchange_rates_04.from_currency_code = from_currencies.code
            left join dwh.currencies to_currencies on
                exchange_rates_04.to_currency_code = to_currencies.code
            left join dwh.exchange_rate_types on
                exchange_rates_04.exchange_rate_type_code = exchange_rate_types.code
        ]');
        out.data_integration.check_primary_key('exchange_rates', 'from_currency_code, to_currency_code, exchange_rate_type_code');
        out.data_integration.control_append('dwh.exchange_rates', 'exchange_rates', q'[
            partition value => #reference_date
            truncate partition => true
        ]');
        out.data_integration.drop_table('exchange_rates_01');
        out.data_integration.drop_table('exchange_rates_02');
        out.data_integration.drop_table('exchange_rates_03');
        out.data_integration.drop_table('exchange_rates_04');
        out.data_integration.drop_table('exchange_rates');
        out.process('done');
    exception
        when others then
            out.process('error');
    end exchange_rates;

    procedure regions is
    begin
        out.process('start');
        out.data_integration.create_table('regions', q'[
            select distinct
                coalesce(intermediate_region_code, sub_region_code, iso3166_1_alpha_2) code,
                coalesce(intermediate_region_name, sub_region_name, cldr_display_name) name
            from dhub.dh_country_codes
            where
                sub_region_code is not null or
                iso3166_1_alpha_2 = 'AQ'
        ]');
        out.data_integration.check_primary_key('regions', 'code');
        out.data_integration.incremental_update('dwh.regions', 'regions', q'[
            natural key => code
            surrogate key => id
        ]');
        out.data_integration.drop_table('regions');
        out.process('done');
    exception
        when others then
            out.process('error');
    end regions;

end dwh;
/