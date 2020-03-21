create or replace package body metadata.examples is

    subtype text is varchar2(100);

    -- data hub
    dh_api_url constant text := 'https://datahub.io';

    -- landing zone
    landing_zone constant text := '/home/oracle';

    procedure data_integration_test is
    begin
        out.process('start');
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end data_integration_test;

    procedure files_test is
    begin
        out.process('start');
        out.bind('landing_zone', macro => landing_zone);
        out.utilities.shell('rm -rf #landing_zone/*');
        out.utilities.shell('touch #landing_zone/file_1.txt');
        out.files.copy('#landing_zone/file_1_1.txt', '#landing_zone/file_1.txt');
        out.utilities.shell('mkdir -p #landing_zone/folder1');
        out.files.copy('#landing_zone/folder1_1', '#landing_zone/folder1', q'[
            recursive => true
        ]');
        out.utilities.shell('mkdir -p #landing_zone/folder2/folder2/folder2');
        out.files.copy('#landing_zone/folder2_1', '#landing_zone/folder2', q'[
            recursive => true
        ]');
        out.files.move('#landing_zone/folder', '#landing_zone/folder1');
        out.files.move('#landing_zone/folder/folder2_1', '#landing_zone/folder2_1');
        out.files.move('#landing_zone/folder/folder2', '#landing_zone/folder2');
        out.files.move('#landing_zone/file.txt', '#landing_zone/file_1_1.txt');
        out.files.remove('#landing_zone/file_1.txt', q'[
            force => true
        ]');
        out.files.remove('#landing_zone/folder1_1', q'[
            recursive => true
        ]');
        out.utilities.shell('(sleep 10; touch #landing_zone/trigger) &');
        out.files.wait('#landing_zone/trigger');
        out.files.wait('#landing_zone/trigger', q'[
            polling interval => 10
        ]');
        out.files.zip('#landing_zone/file.zip', '#landing_zone/file.txt');
        out.files.zip('#landing_zone/folder.zip', '#landing_zone/folder', q'[
            recursive => true
        ]');
        out.files.zip('#landing_zone/test.zip', '#landing_zone/*', q'[
            compress level => 9
            password => test
        ]');
        out.files.unzip('/', '#landing_zone/test.zip', q'[
            password => test
        ]');
        out.files.unzip('/', '#landing_zone/file.zip');
        out.files.unzip('/', '#landing_zone/folder.zip', q'[
            keep input files => true
        ]');
        out.files.unload('/home/oracle/all_users.csv', 'sys.all_users', q'[
            date format => yyyymmddhh24miss
        ]');
        out.files.load('stage.file_all_users', '/home/oracle/all_users.csv', q'[
            username varchar2(128)
            user_id number
            created date mask "yyyymmddhh24miss"
            common varchar2(3)
            oracle_maintained varchar2(1)
            inherited varchar2(3)
            default_collation varchar2(100)
            implicit varchar2(3)
            all_shard varchar2(3)
        ]');
        out.files.load('stage.file_lob', '/home/oracle/all_users.csv', null, q'[
            file format => large object
        ]');
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end files_test;

    procedure utilities_test is
    begin
        out.process('start');
        out.bind('landing_zone', macro => landing_zone);
        out.utilities.shell('rm -rf #landing_zone/*');
        out.utilities.shell('touch #landing_zone/file_1.txt');
        out.utilities.shell('echo "line 1" > #landing_zone/file_2.txt');
        out.utilities.shell('echo "line 2" >> #landing_zone/file_2.txt');
        out.utilities.shell('echo "line 3" >> #landing_zone/file_2.txt');
        out.utilities.shell('echo "line 4" >> #landing_zone/file_2.txt');
        out.utilities.shell('mkdir -p #landing_zone/folder1/folder2/folder3');
        dbms_output.put_line('ls #landing_zone ->' || out.utilities.shell('ls #landing_zone'));
        dbms_output.put_line('cat #landing_zone/file_1.txt ->' || out.utilities.shell('cat #landing_zone/file_1.txt'));
        dbms_output.put_line('cat #landing_zone/file_2.txt ->' || out.utilities.shell('cat #landing_zone/file_2.txt'));
        dbms_output.put_line('ls #landing_zone/folder1/folder2 ->' || out.utilities.shell('ls #landing_zone/folder1/folder2/folder3'));
        dbms_output.put_line('cat #landing_zone/aaa.txt (ignore errors) ->' || out.utilities.shell('cat #landing_zone/aaa.txt', q'[
            ignore errors => true
        ]'));
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end utilities_test;

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
        out.data_integration.check_unique_key('dh_world_cities', 'geonameid');
        out.data_integration.control_append('data.dh_world_cities', 'dh_world_cities', q'[
            truncate table => true
        ]');
        out.data_integration.control_append('data.dh_world_cities_4', 'dh_world_cities', q'[
            partition name => p_all
            truncate partition => true
        ]');
        out.data_integration.incremental_update('data.dh_world_cities_2', 'dh_world_cities', q'[
            natural key => geonameid
            surrogate key => sk
        ]');
        out.data_integration.incremental_update('data.dh_world_cities_3', 'dh_world_cities', q'[
            natural key => geonameid, name
        ]');
        out.data_integration.incremental_update('data.dh_world_cities_4', 'dh_world_cities', q'[
            natural key => geonameid
            surrogate key => sk
            partition name => p_all
        ]');
        out.data_integration.incremental_update('data.dh_world_cities_5', 'dh_world_cities', q'[
            method => merge
            natural key => geonameid
        ]');
        out.data_integration.incremental_update('data.dh_world_cities_4', 'dh_world_cities', q'[
            natural key => geonameid
            surrogate key => sk
            partition name => p_all
            method => merge
        ]');
        out.files.remove('#landing_zone/#dh_resource.csv');
        out.data_integration.drop_table('dh_world_cities_01');
        out.data_integration.drop_table('dh_world_cities');
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end dh_world_cities;

end examples;
/