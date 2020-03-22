create or replace package out.logger authid definer is

    procedure session(name varchar2, status varchar2);

    procedure session_step(status varchar2, error varchar2 default null);

    procedure session_step_task(status varchar2, work number default 0, code varchar2 default null, error varchar2 default null);

end logger;
/