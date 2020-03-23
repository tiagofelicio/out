create or replace package body out.logger is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    session_no number;
    session_step_no number;
    session_step_task_no number;
    session_step_work number;
    session_work number;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function format_error_trace return varchar2 is
    begin
        return regexp_substr(dbms_utility.format_error_stack, '^.*$', 1, 1, 'm') || chr(10) || dbms_utility.format_error_backtrace;
    end format_error_trace;

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

    procedure session(name varchar2, status varchar2) is
        pragma autonomous_transaction;
        error clob;
    begin
        case status
            when 'start' then
                if logger.session_no is not null then
                    raise_application_error(-20000, 'Session ' || logger.session_no || ' is executing.');
                end if;
                mutex_lock('out$log_session');
                select nvl(max(no), 0) + 1 into logger.session_no from sessions;
                insert into sessions (no, name, begin, status, username, error) values (logger.session_no, name, sysdate, 'r', lower(sys_context('userenv', 'session_user')), empty_clob());
                commit;
                mutex_unlock('out$log_session');
                dbms_application_info.set_module(name, null);
                dbms_application_info.set_client_info('Unified Toolkit for Oracle');
                logger.session_work := 0;
            when 'done' then
                if logger.session_no is null then
                    raise_application_error(-20000, 'Session is null. Start a process first.');
                end if;
                update sessions set end = sysdate, status = 'd', work = logger.session_work where no = logger.session_no;
                commit;
                dbms_application_info.set_module(null, null);
                dbms_application_info.set_client_info(null);
                logger.session_no := null;
                logger.session_work := null;
            when 'warning' then
                if logger.session_no is null then
                    raise_application_error(-20000, 'Session is null. Start a process first.');
                end if;
                session.error := format_error_trace();
                update sessions set end = sysdate, status = 'w', work = logger.session_work, error = session.error where no = logger.session_no;
                commit;
                dbms_application_info.set_module(null, null);
                dbms_application_info.set_client_info(null);
                logger.session_no := null;
                logger.session_work := null;
            when 'error' then
                if logger.session_no is null then
                    raise_application_error(-20000, 'Session is null. Start a process first.');
                end if;
                session.error := format_error_trace();
                update sessions set end = sysdate, status = 'e', work = logger.session_work, error = session.error where no = logger.session_no;
                commit;
                dbms_application_info.set_module(null, null);
                dbms_application_info.set_client_info(null);
                logger.session_no := null;
                logger.session_work := null;
                raise_application_error(-20000, session.error);
            else
                raise_application_error(-20000, 'Unknown session status ' || status || '.');
        end case;
    end session;

    procedure session_step(status varchar2, error varchar2 default null) is
        pragma autonomous_transaction;
        who_called_me_owner varchar2(250);
        who_called_me_name varchar2(250);
        who_called_me_lineno number;
        who_called_me_caller_t varchar2(100);
        step_name varchar2(500);
    begin
        case status
            when 'start' then
                if logger.session_no is not null then
                    owa_util.who_called_me(who_called_me_owner, who_called_me_name, who_called_me_lineno, who_called_me_caller_t);
                    step_name := lower(who_called_me_owner || '.' || who_called_me_name);
                    mutex_lock('out$log_session_step');
                    select nvl(max(no), 0) + 1 into logger.session_step_no from session_steps where session_no = logger.session_no;
                    insert into session_steps (session_no, no, name, begin, status, error) values (logger.session_no, logger.session_step_no, step_name, sysdate, 'r', empty_clob());
                    commit;
                    mutex_unlock('out$log_session_step');
                    dbms_application_info.set_action(step_name || ' : ' || to_char(who_called_me_lineno));
                    logger.session_step_work := 0;
                end if;
            when 'done' then
                if logger.session_no is not null then
                    update session_steps set end = sysdate, status = 'd', work = logger.session_step_work where session_no = logger.session_no and no = logger.session_step_no;
                    commit;
                    dbms_application_info.set_action(null);
                    logger.session_work := logger.session_work + logger.session_step_work;
                    logger.session_step_no := null;
                    logger.session_step_work := null;
                end if;
            when 'warning' then
                if logger.session_no is not null then
                    update session_steps set end = sysdate, status = 'w', work = logger.session_step_work, error = session_step.error where session_no = logger.session_no and no = logger.session_step_no;
                    commit;
                    dbms_application_info.set_action(null);
                    logger.session_work := logger.session_work + logger.session_step_work;
                    logger.session_step_no := null;
                    logger.session_step_work := null;
                end if;
            when 'error' then
                if logger.session_no is not null then
                    update session_steps set end = sysdate, status = 'e', work = logger.session_step_work, error = session_step.error where session_no = logger.session_no and no = logger.session_step_no;
                    commit;
                    dbms_application_info.set_action(null);
                    logger.session_work := logger.session_work + logger.session_step_work;
                    logger.session_step_no := null;
                    logger.session_step_work := null;
                end if;
                raise_application_error(-20000, session_step.error);
            else
                raise_application_error(-20000, 'Unknown session step status ' || status || '.');
        end case;
    end session_step;

    procedure session_step_task(status varchar2, work number default 0, code varchar2 default null, error varchar2 default null) is
        pragma autonomous_transaction;
    begin
        case status
            when 'start' then
                if logger.session_no is not null then
                    mutex_lock('out$log_session_step_task');
                    select nvl(max(no), 0) + 1 into logger.session_step_task_no from session_step_tasks where session_no = logger.session_no and session_step_no = logger.session_step_no;
                    insert into session_step_tasks (session_no, session_step_no, no, begin, status, code, error) values (logger.session_no, logger.session_step_no, logger.session_step_task_no, sysdate, 'r', session_step_task.code, empty_clob());
                    commit;
                    mutex_unlock('out$log_session_step_task');
                else
                    dbms_output.put_line(to_char(systimestamp, 'yyyy-mm-dd hh24:mi:ss.ff6') || ' : ' || session_step_task.code);
                end if;
            when 'done' then
                if logger.session_no is not null then
                    update session_step_tasks set end = sysdate, status = 'd', work = session_step_task.work where session_no = logger.session_no and session_step_no = logger.session_step_no and no = logger.session_step_task_no;
                    commit;
                    logger.session_step_work := logger.session_step_work + session_step_task.work;
                    logger.session_step_task_no := null;
                end if;
            when 'warning' then
                if logger.session_no is not null then
                    update session_step_tasks set end = sysdate, status = 'w', work = session_step_task.work, error = session_step_task.error where session_no = logger.session_no and session_step_no = logger.session_step_no and no = logger.session_step_task_no;
                    commit;
                    logger.session_step_work := logger.session_step_work + session_step_task.work;
                    logger.session_step_task_no := null;
                end if;
            when 'error' then
                if logger.session_no is not null then
                    update session_step_tasks set end = sysdate, status = 'e', work = session_step_task.work, error = session_step_task.error where session_no = logger.session_no and session_step_no = logger.session_step_no and no = logger.session_step_task_no;
                    commit;
                    logger.session_step_work := logger.session_step_work + session_step_task.work;
                    logger.session_step_task_no := null;
                end if;
                raise_application_error(-20000, session_step_task.error);
            else
                raise_application_error(-20000, 'Unknown session step status ' || status || '.');
        end case;
    end session_step_task;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end logger;
/