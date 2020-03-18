create or replace package out.core authid current_user is

    debug boolean;

    procedure bind(variable_type varchar2, variable_name varchar2, variable_value varchar2);

    procedure unbind(variable_type varchar2, variable_name varchar2 default null);

    procedure dump;

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean;

    function get_property_value(property_name varchar2, argument varchar2) return varchar2;

    function solve(text varchar2) return varchar2;

end core;
/