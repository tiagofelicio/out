create or replace package out.logger authid definer is

    procedure session(name varchar2, status varchar2, options varchar2 default null);

    procedure session_step(status varchar2, options varchar2 default null);

    procedure session_step_task(status varchar2, options varchar2 default null, work number default 0);

end logger;
/