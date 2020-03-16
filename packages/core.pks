create or replace package out.core authid current_user is

    debug boolean;

    subtype text_t is varchar2(32767);

    type statement_t is record (
        code text_t,
        execute boolean default true,
        ignore_error number,
        log boolean default true
    );

    type statements_t is table of statement_t index by pls_integer;

    procedure bind(variable_type varchar2, variable_name varchar2, variable_value varchar2);

    procedure unbind(variable_type varchar2, variable_name varchar2 default null);

    procedure dump;

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean;

    function plsql(into_date statement_t) return date;

    function plsql(into_number statement_t) return number;

    function plsql(into_varchar2 statement_t) return varchar2;

    procedure plsql(statements statements_t);

    function shell(statement statement_t) return varchar2;

    function solve(text varchar2) return varchar2;

end core;
/