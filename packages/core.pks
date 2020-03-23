create or replace package out.core authid definer is

    procedure bind(variable_name varchar2, variable_value varchar2);

    procedure unbind(variable_name varchar2 default null);

    function get(property_name varchar2) return varchar2;

    procedure set(property_name varchar2, arg1 varchar2 default null, arg2 varchar2 default null, arg3 varchar2 default null);

    function isset(property_name varchar2) return boolean;

    function execute(statement in out nocopy types.statement, unset boolean default true) return anydata;

    procedure execute(statements in out nocopy types.statements, unset boolean default true);

end core;
/