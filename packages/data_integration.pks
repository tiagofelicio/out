create or replace package out.data_integration authid current_user is

    procedure check_unique_key(work_table_name varchar2, columns_name varchar2);

    procedure create_table(work_table_name varchar2, statement varchar2);

    procedure drop_table(work_table_name varchar2);

    procedure control_append(target_table_name varchar2, work_table_name varchar2, options varchar2 default null);

    procedure incremental_update(target_table_name varchar2, work_table_name varchar2, options varchar2);

end data_integration;
/