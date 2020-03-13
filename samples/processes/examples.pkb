create or replace package body metadata.examples is

    subtype text is varchar2(100);

    -- landing zone
    landing_zone constant text := '/home/oracle';

    -- data hub
    dh_api_url constant text := 'https://datahub.io';

    procedure data_integration_test is
    begin
        null;
    end data_integration_test;

    procedure files_test is
    begin
        null;
    end files_test;

    procedure dh_country_codes is
    begin
        out.process('start');
        out.bind('dh_api_url', macro => dh_api_url);
        out.bind('landing_zone', macro => landing_zone);
        out.bind('dh_library', macro => 'core');
        out.bind('dh_resource', macro => 'country-codes');
        out.tools.shell('curl -L "#dh_api_url/#dh_library/#dh_resource/r/0.csv" > #landing_zone/#dh_resource.csv');
        out.files.copy('#landing_zone/#dh_resource_1.csv', '#landing_zone/#dh_resource.csv');
        out.files.copy('#landing_zone/test_2', '#landing_zone/test', q'[
            directory => true
        ]');
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end dh_country_codes;

end examples;
/