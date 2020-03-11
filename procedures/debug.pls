create or replace procedure out.debug(state boolean) authid current_user is
begin
    core.debug := state;
end debug;