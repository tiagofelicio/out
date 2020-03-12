create or replace package out.core authid current_user is

    debug boolean;

    subtype string_t is varchar2(32767);

    procedure bind(variable_type varchar2, variable_name varchar2, variable_value varchar2);

    procedure unbind(variable_type varchar2, variable_name varchar2 default null);

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean;

    function solve(text varchar2) return varchar2;

    procedure dump;

end core;
/