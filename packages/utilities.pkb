create or replace package body out.utilities is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure plsql(statement varchar2, options varchar2 default null) is
        statements core.statements_t;
    begin
        internal.log_session_step('start');
        statements(1).code := statement;
        core.plsql(statements);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    procedure plsql(statement varchar2, result out date, options varchar2 default null) is
        i_statement core.statement_t;
    begin
        internal.log_session_step('start');
        i_statement.code := statement;
        core.plsql(i_statement, result);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    procedure plsql(statement varchar2, result out number, options varchar2 default null) is
        i_statement core.statement_t;
    begin
        internal.log_session_step('start');
        i_statement.code := statement;
        core.plsql(i_statement, result);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    procedure plsql(statement varchar2, result out varchar2, options varchar2 default null) is
        i_statement core.statement_t;
    begin
        internal.log_session_step('start');
        i_statement.code := statement;
        core.plsql(i_statement, result);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end plsql;

    function shell(statement varchar2, options varchar2 default null) return clob is
        output core.text_t;
    begin
        output := core.shell(statement, false);
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
        output core.text_t;
    begin
        internal.log_session_step('start');
        output := core.shell(statement);
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