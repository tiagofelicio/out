create or replace package out.utilities authid current_user is

    function bash(statement varchar2, options varchar2 default null) return varchar2;

    procedure bash(statement varchar2, options varchar2 default null);

    function plsql(into_date varchar2, options varchar2 default null) return date;

    function plsql(into_number varchar2, options varchar2 default null) return number;

    function plsql(into_varchar2 varchar2, options varchar2 default null) return varchar2;

    procedure plsql(statement varchar2, options varchar2 default null);

end utilities;
/