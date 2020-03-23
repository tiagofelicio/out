create or replace package body out.utilities is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function bash(statement varchar2, options varchar2 default null) return varchar2 is
        i_statement types.statement;
        output anydata := anydata.ConvertVarchar2(null);
    begin
        core.set('utilities.bash.(options).ignore_errors', options);
        if types.to_boolean(core.get('utilities.bash.(options).ignore_errors')) then
            i_statement.ignore_error := null;
        end if;
        i_statement.log := false;
        i_statement.bash.code := statement;
        output := core.execute(i_statement);
        return output.AccessVarchar2;
    end bash;

    procedure bash(statement varchar2, options varchar2 default null) is
        i_statement types.statement;
        output anydata;
    begin
        logger.session_step('start');
        i_statement.bash.code := statement;
        output := core.execute(i_statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end bash;

    function plsql(into_date varchar2, options varchar2 default null) return date is
        i_statement types.statement;
        result anydata;
    begin
        i_statement.log := false;
        i_statement.plsql.code := into_date;
        i_statement.plsql.to_fetch := true;
        result := core.execute(i_statement);
        case result.getTypeName
            when 'SYS.DATE' then
                return result.AccessDate;
            else
                raise_application_error(-20000, 'Statement does not returns a date value.');
        end case;
    end plsql;

    function plsql(into_number varchar2, options varchar2 default null) return number is
        i_statement types.statement;
        result anydata;
    begin
        i_statement.log := false;
        i_statement.plsql.code := into_number;
        i_statement.plsql.to_fetch := true;
        result := core.execute(i_statement);
        case result.getTypeName
            when 'SYS.BINARY_DOUBLE' then
                return result.AccessBDouble;
            when 'SYS.BINARY_FLOAT' then
                return result.AccessBFloat;
            when 'SYS.NUMBER' then
                return result.AccessNumber;
            else
                raise_application_error(-20000, 'Statement does not returns a number like value.');
        end case;
    end plsql;

    function plsql(into_varchar2 varchar2, options varchar2 default null) return varchar2 is
        i_statement types.statement;
        result anydata;
    begin
        i_statement.log := false;
        i_statement.plsql.code := into_varchar2;
        i_statement.plsql.to_fetch := true;
        result := core.execute(i_statement);
        case result.getTypeName
            when 'SYS.CHAR' then
                return result.AccessChar;
            when 'SYS.CLOB' then
                return result.AccessClob;
            when 'SYS.NCHAR' then
                return result.AccessNChar;
            when 'SYS.NCLOB' then
                return result.AccessNClob;
            when 'SYS.NVARCHAR2' then
                return result.AccessNVarchar2;
            when 'SYS.VARCHAR' then
                return result.AccessVarchar;
            when 'SYS.VARCHAR2' then
                return result.AccessVarchar2;
            else
                raise_application_error(-20000, 'Statement does not returns a varchar2 like value.');
        end case;
    end plsql;

    procedure plsql(statement varchar2, options varchar2 default null) is
        i_statement types.statement;
        output anydata;
    begin
        logger.session_step('start');
        i_statement.plsql.code := statement;
        output := core.execute(i_statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end plsql;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end utilities;
/