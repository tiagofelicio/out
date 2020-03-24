create or replace package body metadata.examples is

    subtype text is varchar2(100);

    -- data hub
    dh_api_url constant text := 'https://datahub.io';

    -- landing zone
    landing_zone constant text := '/home/oracle';

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
        out.data_integration.check_primary_key('dh_world_cities', 'geonameid, subcountry');
        out.data_integration.check_not_null('dh_world_cities', 'subcountry');
        out.data_integration.check_unique_key('dh_world_cities', 'geonameid');
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

end examples;
/