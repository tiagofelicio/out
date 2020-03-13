create or replace package body metadata.examples is

    subtype text is varchar2(100);

    -- landing zone
    landing_zone constant text := '/home/oracle';

    procedure data_integration_test is
    begin
        out.process('start');
        out.bind('landing_zone', macro => landing_zone);
        
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end data_integration_test;

    procedure files_test is
    begin
        null;
    end files_test;

    procedure utilities_test is
    begin
        out.process('start');
        out.bind('landing_zone', macro => landing_zone);
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
        out.utilities.shell('rm -rf #landing_zone/*');
        out.process('done');
    exception
        when others then
            out.process('error', sqlerrm);
    end utilities_test;

end examples;
/