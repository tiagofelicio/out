create or replace package out.internet authid current_user is

    procedure http_get;

    procedure http_put;

end internet;
/