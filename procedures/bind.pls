create or replace procedure out.bind(variable_name varchar2, date_value date default null, number_value number default null, text_value varchar2 default null, macro varchar2 default null) authid current_user is
begin
    if date_value is not null then
        core.bind('#', variable_name, 'to_date(''' || to_char(date_value, 'yyyymmdd') || ''', ''yyyymmdd'')');
    elsif number_value is not null then
        core.bind('#', variable_name, to_char(number_value));
    elsif text_value is not null then
        core.bind('#', variable_name, '''' || text_value || '''');
    elsif macro is not null then
        core.bind('#', variable_name, macro);
    else
        raise_application_error(-20000, 'Unsupported bind.');
    end if;
end bind;