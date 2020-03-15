create or replace package body out.core is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    type binds_t is table of text_t index by varchar2(255);

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

    procedure dump is
        internal_variable_name text_t;
        internal_variable_value text_t;
        user_variable_name text_t;
        user_variable_value text_t;
    begin
        dbms_output.put_line('=============================== out ===============================');
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

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2 is
        option_value text_t;
    begin
        option_value := lower(regexp_substr(solve(options), replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1));
        return nvl(option_value, default_value);
    end get_option;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean is
        option_value text_t;
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

    procedure plsql(statements statements_t) is
        solved_statement text_t;
        statement statement_t;
        work number;
    begin
        for i in statements.first .. statements.last loop
            statement := statements(i);
            if statement.execute then
                solved_statement := solve(statement.code);
                begin
                    internal.log_session_step_task('start', solved_statement);
                    execute immediate solved_statement;
                    work := sql%rowcount;
                    commit;
                    internal.log_session_step_task('done', work => work);
                exception
                    when others then
                        rollback;
                        if statement.ignore_error is null or statement.ignore_error <> sqlcode then
                            internal.log_session_step_task('error', sqlerrm);
                        else
                            internal.log_session_step_task('warning', sqlerrm);
                        end if;
                end;
            end if;
        end loop;
    end plsql;

    procedure plsql(statement statement_t, result out date) is
        solved_statement text_t;
    begin
        if statement.execute then
            solved_statement := solve(statement.code);
            internal.log_session_step_task('start', solved_statement);
            execute immediate solved_statement into result;
            internal.log_session_step_task('done');
        end if;
    exception
        when others then
            internal.log_session_step_task('error', sqlerrm);
    end plsql;

    procedure plsql(statement statement_t, result out number) is
        solved_statement text_t;
    begin
        if statement.execute then
            solved_statement := solve(statement.code);
            internal.log_session_step_task('start', solved_statement);
            execute immediate solved_statement into result;
            internal.log_session_step_task('done');
        end if;
    exception
        when others then
            internal.log_session_step_task('error', sqlerrm);
    end plsql;

    procedure plsql(statement statement_t, result out varchar2) is
        solved_statement text_t;
    begin
        if statement.execute then
            solved_statement := solve(statement.code);
            internal.log_session_step_task('start', solved_statement);
            execute immediate solved_statement into result;
            internal.log_session_step_task('done');
        end if;
    exception
        when others then
            internal.log_session_step_task('error', sqlerrm);
    end plsql;

    function shell(statement varchar2, log boolean default true) return varchar2 is
        exit_value pls_integer;
        output text_t;
        solved_statement text_t;
        stderr text_t;
        stdout text_t;
    begin
        solved_statement := solve(statement);
        if log then
            internal.log_session_step_task('start', solved_statement);
        end if;
        output := internal.shell(solved_statement);
        exit_value := to_number(regexp_substr(output, '(^|' || internal.shell_output_separator || ')([^' || internal.shell_output_separator || ']*)', 1, 1, null, 2));
        stderr := regexp_substr(output, '(^|' || internal.shell_output_separator || ')([^' || internal.shell_output_separator || ']*)', 1, 3, null, 2);
        stdout := regexp_substr(output, '(^|' || internal.shell_output_separator || ')([^' || internal.shell_output_separator || ']*)', 1, 2, null, 2);
        if exit_value <> 0 then
            raise_application_error(-20000, stderr);
        end if;
        if log then
            internal.log_session_step_task('done');
        end if;
        return stdout;
    exception
        when others then
            if log then
                internal.log_session_step_task('error', sqlerrm);
            else
                raise;
            end if;
    end shell;

    function solve(text varchar2) return varchar2 is
        solved_text text_t;
        internal_variable_name text_t;
        internal_variable_value text_t;
        user_variable_name text_t;
        user_variable_value text_t;
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

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end core;
/