create or replace procedure out.process(status varchar2) authid current_user is
    process_name types.text;
    who_called_me_owner types.text;
    who_called_me_name types.text;
    who_called_me_lineno number;
    who_called_me_caller_t types.text;
begin
    owa_util.who_called_me(who_called_me_owner, who_called_me_name, who_called_me_lineno, who_called_me_caller_t);
    process_name := lower(who_called_me_owner || '.' || who_called_me_name);
    logger.session(process_name, lower(status));
end process;
/