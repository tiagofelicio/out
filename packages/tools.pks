create or replace package out.tools authid current_user is

    procedure remove_file(filename varchar2, options varchar2 default null);

    function shell(command varchar2, options varchar2 default null) return clob;

    procedure shell(command varchar2);

end tools;
/