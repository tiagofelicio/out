create or replace package out.types authid definer is

    subtype text is varchar2(32767);

    type map is table of text index by varchar2(255);

    type bash_lang is record (
        code text
    );

    type plsql_lang is record (
        code text,
        to_fetch boolean default false
    );

    type statement is record (
        bash bash_lang,
        plsql plsql_lang,
        execute boolean default true,
        ignore_error number default 0,
        log boolean default true
    );

    type statements is table of statement index by pls_integer;

    function to_boolean(arg text) return boolean;

end types;
/