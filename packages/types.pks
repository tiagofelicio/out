create or replace package out.types authid current_user is

    subtype text is varchar2(32767);

    type binds is table of text index by varchar2(255);

    type statement is record (
        code text,
        execute boolean default true,
        ignore_error number,
        log boolean default true
    );

    type statements is table of statement index by pls_integer;

end types;
/