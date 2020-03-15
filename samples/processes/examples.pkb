create or replace package body metadata.examples is

    subtype text is varchar2(100);

    -- landing zone
    landing_zone constant text := '/home/oracle';

    procedure data_integration_test is
    begin
        out.process('start');
        out.data_integration.create_table('stage.t1', q'[
            select *
            from all_users
        ]');
        out.data_integration.check_unique_key('stage.t1', 'username');
        out.data_integration.create_table('stage.t2', q'[
            select *
            from all_users
            where 1 = 0
        ]');
        out.data_integration.incremental_update('stage.t2', 'stage.t1', q'[
            natural key => username
        ]');
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
        out.files.remove('#landing_zone/file_1.txt');
        out.files.remove('#landing_zone/folder1_1', q'[
            recursive => true
        ]');
        out.utilities.shell('(sleep 10; touch #landing_zone/trigger) &');
        out.files.wait('#landing_zone/trigger');
        out.files.wait('#landing_zone/trigger', q'[
            polling interval => 10
        ]');
        out.files.zip('#landing_zone/file.zip', '#landing_zone/file.txt', q'[
            keep => true
        ]');
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
            keep => true
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

end examples;
/