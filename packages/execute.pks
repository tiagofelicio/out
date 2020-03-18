create or replace package out.execute authid definer is

    function plsql(statement types.statement) return anydata;

    procedure plsql(statements types.statements);

    function shell(statement types.statement) return varchar2;

    procedure shell(statement types.statement);

end execute;
/