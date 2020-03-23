create or replace package body out.internet is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure http_get(filename varchar2, url varchar2, options varchar2 default null) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).bash.code := q'[
            curl --request GET --fail --silent --show-error --location "$internet.http_get.url" --output "$internet.http_get.filename"
        ]';
        core.set('internet.http_get.filename', filename);
        core.set('internet.http_get.url', url);
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end http_get;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end internet;
/