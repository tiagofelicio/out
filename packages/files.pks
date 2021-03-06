create or replace package out.files authid current_user is

    procedure copy(target_filename varchar2, source_filename varchar2, options varchar2 default null);

    procedure move(target_filename varchar2, source_filename varchar2, options varchar2 default null);

    procedure remove(filename varchar2, options varchar2 default null);

    procedure wait(filename varchar2, options varchar2 default null);

    procedure load(work_table_name varchar2, filename varchar2, attributes varchar2, options varchar2 default null);

    procedure unload(filename varchar2, work_table_name varchar2, options varchar2 default null);

    procedure zip(archive_name varchar2, filename varchar2, options varchar2 default null);

    procedure unzip(directory_name varchar2, archive_name varchar2, options varchar2 default null);

end files;
/