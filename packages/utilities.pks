create or replace package out.utilities authid current_user is

    procedure plsql(statement varchar2, options varchar2 default null);

    procedure plsql(statement varchar2, result out date, options varchar2 default null);

    procedure plsql(statement varchar2, result out number, options varchar2 default null);

    procedure plsql(statement varchar2, result out varchar2, options varchar2 default null);

    function shell(statement varchar2, options varchar2 default null) return clob;

    procedure shell(statement varchar2, options varchar2 default null);

end utilities;
/