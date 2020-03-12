create or replace package body out.core is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    type binds_t is table of string_t index by varchar2(255);

    internal_binds binds_t;
    user_binds binds_t;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure bind(variable_type varchar2, variable_name varchar2, variable_value varchar2) is
    begin
        case variable_type
            when '$' then
                internal_binds('$' || variable_name) := variable_value;
            when '#' then
                user_binds('#' || variable_name) := variable_value;
            else
                raise_application_error(-20000, 'Unsupported varible type ' || variable_type || ';');
        end case;
    end bind;

    procedure unbind(variable_type varchar2, variable_name varchar2 default null) is
    begin
        case variable_type
            when '$' then
                if variable_name is null then
                    internal_binds.delete;
                else
                    internal_binds.delete('$' || variable_name);
                end if;
            when '#' then
                if variable_name is null then
                    user_binds.delete;
                else
                    user_binds.delete('#' || variable_name);
                end if;
            else
                raise_application_error(-20000, 'Unsupported varible type ' || variable_type || ';');
        end case;
    end unbind;

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2 is
        option_value string_t;
    begin
        option_value := lower(regexp_substr(solve(options), replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1));
        return nvl(option_value, default_value);
    end get_option;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean is
        option_value string_t;
        option_boolean_value boolean;
    begin
        option_value := get_option(replace(option_name, ' set ?'), options);
        if option_value is null or length(option_value) = 0 then
            option_boolean_value := defaul_value;
        elsif option_name like '% set ?' then
            option_boolean_value := true;
        else
            case option_value
                when 'false' then
                    option_boolean_value := false;
                when 'true' then
                    option_boolean_value := true;
                else
                    raise_application_error(-20000, 'Invalid boolean option value (' || option_value || ').');
            end case;
        end if;
        return option_boolean_value;
    end get_option;

    function solve(text varchar2) return varchar2 is
        solved_text string_t;
        internal_variable_name string_t;
        internal_variable_value string_t;
        user_variable_name string_t;
        user_variable_value string_t;
    begin
        solved_text := text;
        internal_variable_name := internal_binds.first;
        while internal_variable_name is not null loop
            internal_variable_value := internal_binds(internal_variable_name);
            solved_text := replace(solved_text, internal_variable_name, internal_variable_value);
            internal_variable_name := internal_binds.next(internal_variable_name);
        end loop;
        user_variable_name := user_binds.first;
        while user_variable_name is not null loop
            user_variable_value := user_binds(user_variable_name);
            solved_text := replace(solved_text, user_variable_name, user_variable_value);
            user_variable_name := user_binds.next(user_variable_name);
        end loop;
        return solved_text;
    end solve;

    procedure dump is
        internal_variable_name string_t;
        internal_variable_value string_t;
        user_variable_name string_t;
        user_variable_value string_t;
    begin
        dbms_output.put_line('');
        dbms_output.put_line('debug is ' || case when debug then 'on' else 'off' end || '.');
        dbms_output.put_line('');
        dbms_output.put_line('============================== binds ==============================');
        internal_variable_name := internal_binds.first;
        while internal_variable_name is not null loop
            internal_variable_value := internal_binds(internal_variable_name);
            dbms_output.put_line('    ' || internal_variable_name || ' : ' || internal_variable_value);
            internal_variable_name := internal_binds.next(internal_variable_name);
        end loop;
        user_variable_name := user_binds.first;
        while user_variable_name is not null loop
            user_variable_value := user_binds(user_variable_name);
            dbms_output.put_line('    ' || user_variable_name || ' : ' || user_variable_value);
            user_variable_name := user_binds.next(user_variable_name);
        end loop;
    end dump;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end core;
/