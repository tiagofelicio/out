create or replace package out.internal authid definer is

    procedure log_session(name varchar2, status varchar2, options varchar2 default null);

    procedure log_session_step(status varchar2, options varchar2 default null);

    procedure log_session_step_task(status varchar2, options varchar2 default null, work number default 0);

    function tools_shell(command varchar2) return varchar2;

    function tools_shell_output_separator return varchar2;

end internal;
/