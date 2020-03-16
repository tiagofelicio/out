create or replace package body out.utilities is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function plsql(into_date varchar2, options varchar2 default null) return date is
        internal_statement core.statement_t;
        result date;
    begin
        internal.log_session_step('start');
        internal_statement.code := into_date;
        result := core.plsql(into_date => internal_statement);
        internal.log_session_step('done');
        return result;
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    function plsql(into_number varchar2, options varchar2 default null) return number is
        internal_statement core.statement_t;
        result number;
    begin
        internal.log_session_step('start');
        internal_statement.code := into_number;
        result := core.plsql(into_number => internal_statement);
        internal.log_session_step('done');
        return result;
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    function plsql(into_varchar2 varchar2, options varchar2 default null) return varchar2 is
        internal_statement core.statement_t;
        result varchar2(4000);
    begin
        internal.log_session_step('start');
        internal_statement.code := into_varchar2;
        result := core.plsql(into_varchar2 => internal_statement);
        internal.log_session_step('done');
        return result;
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    procedure plsql(statement varchar2, options varchar2 default null) is
        internal_statements core.statements_t;
    begin
        internal.log_session_step('start');
        internal_statements(1).code := statement;
        core.plsql(internal_statements);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    function shell(statement varchar2, options varchar2 default null) return clob is
        internal_statement core.statement_t;
        output core.text_t;
    begin
        internal_statement.code := statement;
        internal_statement.log := false;
        output := core.shell(internal_statement);
        return output;
    exception
        when others then
            if core.get_option('ignore errors', options, false) then
                return null;
            else
                raise;
            end if;
    end shell;

    procedure shell(statement varchar2, options varchar2 default null) is
        internal_statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        internal_statement.code := statement;
        output := core.shell(internal_statement);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end shell;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end utilities;
/