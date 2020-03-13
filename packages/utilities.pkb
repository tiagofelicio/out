create or replace package body out.utilities is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function shell(command varchar2, options varchar2 default null) return clob is
        output core.string_t;
    begin
        output := core.shell(command, log => false);
        return output;
    exception
        when others then
            if core.get_option('ignore errors', options, false) then
                return null;
            else
                raise;
            end if;
    end shell;

    procedure shell(command varchar2) is
        output core.string_t;
    begin
        internal.log_session_step('start');
        output := core.shell(command);
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