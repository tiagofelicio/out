create or replace package body out.internal is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    log_session_no number;
    log_session_step_no number;
    log_session_step_task_no number;
    log_session_step_work number;
    log_session_work number;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function mutex_get_handle(lock_name varchar2) return varchar2 is
        pragma autonomous_transaction;
        lock_handle varchar2(128);
    begin
        dbms_lock.allocate_unique(lock_name, lock_handle, 864000);
        return lock_handle;
    end mutex_get_handle;

    procedure mutex_lock(lock_name varchar2) is
        lock_status number;
    begin
        lock_status := dbms_lock.request(mutex_get_handle(lock_name), dbms_lock.x_mode, 86400, false);
        if lock_status <> 0 then
            raise_application_error(-20000, 'Unable to request lock ' || lock_name || ' (status = ' || to_char(lock_status) || ').');
        end if;
    end mutex_lock;

    procedure mutex_unlock(lock_name varchar2) is
        lock_status number;
    begin
        lock_status := dbms_lock.release(mutex_get_handle(lock_name));
        if lock_status <> 0 then
            raise_application_error(-20000, 'Unable to release lock ' || lock_name || ' (status = ' || to_char(lock_status) || ').');
        end if;
    end mutex_unlock;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure log_session(name varchar2, status varchar2, options varchar2) is
        pragma autonomous_transaction;
    begin
        case status
            when 'start' then
                if log_session_no is not null then
                    raise_application_error(-20000, 'Session ' || log_session_no || ' is executing.');
                end if;
                mutex_lock('out$log_session');
                select nvl(max(no), 0) + 1 into log_session_no from sessions;
                insert into sessions (no, name, begin, status, username) values (log_session_no, name, sysdate, 'r', lower(sys_context('userenv', 'session_user')));
                commit;
                mutex_unlock('out$log_session');
                dbms_application_info.set_module(name, null);
                dbms_application_info.set_client_info('Oracle Unified Toolkit');
                log_session_work := 0;
            when 'done' then
                if log_session_no is null then
                    raise_application_error(-20000, 'Session is null. Start a process first.');
                end if;
                update sessions set end = sysdate, status = 'd', work = log_session_work where no = log_session_no;
                commit;
                dbms_application_info.set_module(null, null);
                dbms_application_info.set_client_info(null);
                log_session_no := null;
                log_session_work := null;
            when 'warning' then
                if log_session_no is null then
                    raise_application_error(-20000, 'Session is null. Start a process first.');
                end if;
                update sessions set end = sysdate, status = 'w', work = log_session_work, error = substr(options, 1, 4000) where no = log_session_no;
                commit;
                dbms_application_info.set_module(null, null);
                dbms_application_info.set_client_info(null);
                log_session_no := null;
                log_session_work := null;
            when 'error' then
                if log_session_no is null then
                    raise_application_error(-20000, 'Session is null. Start a process first.');
                end if;
                update sessions set end = sysdate, status = 'e', work = log_session_work, error = substr(options, 1, 4000) where no = log_session_no;
                commit;
                dbms_application_info.set_module(null, null);
                dbms_application_info.set_client_info(null);
                log_session_no := null;
                log_session_work := null;
                raise_application_error(-20000, substr(options, 1, 4000));
            else
                raise_application_error(-20000, 'Unknown session status ' || status || '.');
        end case;
    end log_session;

    procedure log_session_step(status varchar2, options varchar2 default null) is
        pragma autonomous_transaction;
        who_called_me_owner varchar2(100);
        who_called_me_name varchar2(100);
        who_called_me_lineno number;
        who_called_me_caller_t varchar2(100);
        step_name varchar2(250);
    begin
        if log_session_no is null then
            if status = 'error' then
                raise_application_error(-20000, substr(options, 1, 4000));
            end if;
            return;
        end if;
        case status
            when 'start' then
                owa_util.who_called_me(who_called_me_owner, who_called_me_name, who_called_me_lineno, who_called_me_caller_t);
                step_name := lower(who_called_me_owner || '.' || who_called_me_name);
                mutex_lock('out$log_session_step');
                select nvl(max(no), 0) + 1 into log_session_step_no from session_steps where session_no = log_session_no;
                insert into session_steps (session_no, no, name, begin, status) values (log_session_no, log_session_step_no, step_name, sysdate, 'r');
                commit;
                mutex_unlock('out$log_session_step');
                dbms_application_info.set_action(step_name || ' : ' || to_char(who_called_me_lineno));
                log_session_step_work := 0;
            when 'done' then
                update session_steps set end = sysdate, status = 'd', work = log_session_step_work where session_no = log_session_no and no = log_session_step_no;
                commit;
                dbms_application_info.set_action(null);
                log_session_work := log_session_work + log_session_step_work;
                log_session_step_no := null;
                log_session_step_work := null;
            when 'warning' then
                update session_steps set end = sysdate, status = 'w', work = log_session_step_work, error = substr(options, 1, 4000) where session_no = log_session_no and no = log_session_step_no;
                commit;
                dbms_application_info.set_action(null);
                log_session_work := log_session_work + log_session_step_work;
                log_session_step_no := null;
                log_session_step_work := null;
            when 'error' then
                update session_steps set end = sysdate, status = 'e', work = log_session_step_work, error = substr(options, 1, 4000) where session_no = log_session_no and no = log_session_step_no;
                commit;
                dbms_application_info.set_action(null);
                log_session_work := log_session_work + log_session_step_work;
                log_session_step_no := null;
                log_session_step_work := null;
                raise_application_error(-20000, substr(options, 1, 4000));
            else
                raise_application_error(-20000, 'Unknown session step status ' || status || '.');
        end case;
    end log_session_step;

    procedure log_session_step_task(status varchar2, options varchar2 default null, work number default 0) is
        pragma autonomous_transaction;
    begin
        if log_session_no is null then
            if status = 'start' and core.debug then
                dbms_output.put_line('---> ' || options);
            elsif status = 'error' then
                raise_application_error(-20000, substr(options, 1, 4000));
            end if;
            return;
        end if;
        case status
            when 'start' then
                mutex_lock('out$log_session_step_task');
                select nvl(max(no), 0) + 1 into log_session_step_task_no from session_step_tasks where session_no = log_session_no and session_step_no = log_session_step_no;
                insert into session_step_tasks (session_no, session_step_no, no, begin, status, text) values (log_session_no, log_session_step_no, log_session_step_task_no, sysdate, 'r', substr(options, 1, 4000));
                commit;
                mutex_unlock('out$log_session_step_task');
            when 'done' then
                update session_step_tasks set end = sysdate, status = 'd', work = log_session_step_task.work where session_no = log_session_no and session_step_no = log_session_step_no and no = log_session_step_task_no;
                commit;
                log_session_step_work := log_session_step_work + log_session_step_task.work;
                log_session_step_task_no := null;
            when 'warning' then
                update session_step_tasks set end = sysdate, status = 'w', work = log_session_step_task.work, error = substr(options, 1, 4000) where session_no = log_session_no and session_step_no = log_session_step_no and no = log_session_step_task_no;
                commit;
                log_session_step_work := log_session_step_work + log_session_step_task.work;
                log_session_step_task_no := null;
            when 'error' then
                update session_step_tasks set end = sysdate, status = 'e', work = log_session_step_task.work, error = substr(options, 1, 4000) where session_no = log_session_no and session_step_no = log_session_step_no and no = log_session_step_task_no;
                commit;
                log_session_step_work := log_session_step_work + log_session_step_task.work;
                log_session_step_task_no := null;
                raise_application_error(-20000, substr(options, 1, 4000));
            else
                raise_application_error(-20000, 'Unknown session step status ' || status || '.');
        end case;
    end log_session_step_task;

    function shell(command varchar2) return varchar2 is
    language java
    name 'OUTTools.shell(java.lang.String) return java.lang.String';

    function shell_output_separator return varchar2 is
    language java
    name 'OUTTools.SHELL_OUTPUT_SEPARATOR() return java.lang.String';

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end internal;
/