create or replace package out.internet authid current_user is

    procedure http_get(filename varchar2, url varchar2, options varchar2 default null);

end internet;
/