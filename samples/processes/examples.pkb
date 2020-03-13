create or replace package body metadata.examples is

    subtype text is varchar2(100);

    -- landing zone
    landing_zone constant text := '/home/oracle';

    -- data hub
    dh_api_url constant text := 'https://datahub.io';

    procedure dh_country_codes is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('landing_zone', macro => landing_zone);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'country-codes');
        out.tools.shell('curl -L "#dh_api_url/#dh_library/#dh_resource/r/0.csv" > #landing_zone/#dh_resource.csv');
        out.data_integration.create_table('stage.dh_country_codes_01', q'[
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
                is_independent
            from #landing_zone/#dh_resource.csv
        ]', q'[
            type => delimited file
            heading => 1
            field delimiter => "
            field separator => ,
        ]');
        out.data_integration.check_unique_key('stage.dh_country_codes_01', 'iso3166_1_alpha_3');
        /*
        out.data_integration.create_table('rstage.dh_country_codes', q'[
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
                to_number(m49) m49,
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
                to_number(geoname_id) geoname_id,
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
            from rstage.dh_country_codes_01
            where
                iso3166_1_alpha_3 is not null
        ]');
        out.data_integration.control_append('rhub.dh_country_codes', 'rstage.dh_country_codes', q'[
            truncate partition => true
            partition name => delta
        ]');
        out.data_integration.incremental_update('rhub.dh_country_codes', 'rstage.dh_country_codes', q'[
            natural key => iso3166_1_alpha_3
            partition name => snapshot
            staging area => rstage
        ]');
        out.tools.remove_file('#landing_zone/csv/#dh_resource.csv');
        out.data_integration.drop_table('rstage.dh_country_codes_01');
        out.data_integration.drop_table('rstage.dh_country_codes');
        */
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end dh_country_codes;

end examples;
/