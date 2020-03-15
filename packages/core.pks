create or replace package out.core authid current_user is

    debug boolean;

    subtype text_t is varchar2(32767);

    type statement_t is record (
        code text_t,
        execute boolean default true,
        ignore_error number
    );

    type statements_t is table of statement_t index by pls_integer;

    procedure bind(variable_type varchar2, variable_name varchar2, variable_value varchar2);

    procedure unbind(variable_type varchar2, variable_name varchar2 default null);

    procedure dump;

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean;

    procedure plsql(statements statements_t);

    procedure plsql(statement statement_t, result out date);

    procedure plsql(statement statement_t, result out number);

    procedure plsql(statement statement_t, result out varchar2);

    function shell(statement varchar2, log boolean default true) return varchar2;

    function solve(text varchar2) return varchar2;

end core;
/