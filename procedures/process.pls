create or replace procedure out.process(status varchar2, options varchar2 default null) authid current_user is
    process_name varchar2(250);
    who_called_me_owner varchar2(100);
    who_called_me_name varchar2(100);
    who_called_me_lineno number;
    who_called_me_caller_t varchar2(100);
begin
    owa_util.who_called_me(who_called_me_owner, who_called_me_name, who_called_me_lineno, who_called_me_caller_t);
    process_name := lower(who_called_me_owner || '.' || who_called_me_name);
    internal.log_session(process_name, lower(status), options);
end process;