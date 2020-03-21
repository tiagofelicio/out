create or replace package out.types authid definer is

    subtype text is varchar2(32767);

    type map is table of text index by varchar2(255);

    type statement is record (
        code text,
        execute boolean default true,
        ignore_error number,
        log boolean default true
    );

    type statements is table of statement index by pls_integer;

    function to_boolean(arg text) return boolean;

end types;
/