create or replace package out.utilities authid current_user is

    function shell(command varchar2, options varchar2 default null) return clob;

    procedure shell(command varchar2);

end utilities;
/