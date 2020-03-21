create or replace package body out.execute is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function shell(command varchar2) return varchar2 is
    language java
    name 'OUTTools.shell(java.lang.String) return java.lang.String';

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function plsql(statement types.statement) return anydata is
        solved_statement types.text;
        solved_statement_cursor pls_integer;
        solved_statement_column_count pls_integer;
        solved_statement_column_description dbms_sql.desc_tab2;
        result anydata := anydata.ConvertRaw(null);
        result_bfile bfile;
        result_binary_double binary_double;
        result_binary_float binary_float;
        result_blob blob;
        result_char char(2000);
        result_clob clob;
        result_date date;
        result_inverval_day_to_second interval day to second;
        result_inverval_year_to_month interval year to month;
        result_nchar nchar(2000);
        result_nclob nclob;
        result_number number;
        result_nvarchar2 nvarchar2(4000);
        result_raw raw(2000);
        result_timestamp timestamp;
        result_timestamp_with_local_time_zone timestamp with local time zone;
        result_timestamp_with_time_zone timestamp with time zone;
        result_urowid urowid;
        result_varchar varchar(4000);
        result_varchar2 varchar2(4000);
    begin
        if statement.execute then
            solved_statement := core.solve(statement.code);
            if statement.log then
                logger.session_step_task('start', solved_statement);
            end if;
            solved_statement_cursor := dbms_sql.open_cursor;
            dbms_sql.parse(solved_statement_cursor, solved_statement, dbms_sql.native);
            dbms_sql.describe_columns2(solved_statement_cursor, solved_statement_column_count, solved_statement_column_description);
            dbms_sql.close_cursor(solved_statement_cursor);
            case solved_statement_column_description(1).col_type
                when dbms_types.typecode_bfile then
                    execute immediate solved_statement into result_bfile;
                    result := anydata.ConvertBfile(result_bfile);
                when dbms_types.typecode_bdouble then
                    execute immediate solved_statement into result_binary_double;
                    result := anydata.ConvertBDouble(result_binary_double);
                when dbms_types.typecode_bfloat then
                    execute immediate solved_statement into result_binary_float;
                    result := anydata.ConvertBFloat(result_binary_float);
                when dbms_types.typecode_blob then
                    execute immediate solved_statement into result_blob;
                    result := anydata.ConvertBlob(result_blob);
                when dbms_types.typecode_char then
                    execute immediate solved_statement into result_char;
                    result := anydata.ConvertChar(result_char);
                when dbms_types.typecode_clob then
                    execute immediate solved_statement into result_clob;
                    result := anydata.ConvertClob(result_clob);
                when dbms_types.typecode_date then
                    execute immediate solved_statement into result_date;
                    result := anydata.ConvertDate(result_date);
                when dbms_types.typecode_interval_ds then
                    execute immediate solved_statement into result_inverval_day_to_second;
                    result := anydata.ConvertIntervalDS(result_inverval_day_to_second);
                when dbms_types.typecode_interval_ym then
                    execute immediate solved_statement into result_inverval_year_to_month;
                    result := anydata.ConvertIntervalYM(result_inverval_year_to_month);
                when dbms_types.typecode_nchar then
                    execute immediate solved_statement into result_nchar;
                    result := anydata.ConvertNchar(result_nchar);
                when dbms_types.typecode_nclob then
                    execute immediate solved_statement into result_nclob;
                    result := anydata.ConvertNClob(result_nclob);
                when dbms_types.typecode_number then
                    execute immediate solved_statement into result_number;
                    result := anydata.ConvertNumber(result_number);
                when dbms_types.typecode_nvarchar2 then
                    execute immediate solved_statement into result_nvarchar2;
                    result := anydata.ConvertNVarchar2(result_nvarchar2);
                when dbms_types.typecode_raw then
                    execute immediate solved_statement into result_raw;
                    result := anydata.ConvertRaw(result_raw);
                when dbms_types.typecode_timestamp then
                    execute immediate solved_statement into result_timestamp;
                    result := anydata.ConvertTimestamp(result_timestamp);
                when dbms_types.typecode_timestamp_ltz then
                    execute immediate solved_statement into result_timestamp_with_local_time_zone;
                    result := anydata.ConvertTimestampLTZ(result_timestamp_with_local_time_zone);
                when dbms_types.typecode_timestamp_tz then
                    execute immediate solved_statement into result_timestamp_with_time_zone;
                    result := anydata.ConvertTimestampTZ(result_timestamp_with_time_zone);
                when dbms_types.typecode_urowid then
                    execute immediate solved_statement into result_urowid;
                    result := anydata.ConvertURowid(result_urowid);
                when dbms_types.typecode_varchar then
                    execute immediate solved_statement into result_varchar;
                    result := anydata.ConvertVarchar2(result_varchar);
                when dbms_types.typecode_varchar2 then
                    execute immediate solved_statement into result_varchar2;
                    result := anydata.ConvertVarchar2(result_varchar2);
            end case;
            if statement.log then
                logger.session_step_task('done', work => 1);
            end if;
        end if;
        core.unset;
        return result;
    exception
        when others then
            if dbms_sql.is_open(solved_statement_cursor) then
                dbms_sql.close_cursor(solved_statement_cursor);
            end if;
            if statement.log then
                logger.session_step_task('error', sqlerrm);
            else
                raise;
            end if;
    end plsql;

    procedure plsql(statements types.statements) is
        solved_statement types.text;
        statement types.statement;
        work number;
    begin
        for i in statements.first .. statements.last loop
            statement := statements(i);
            if statement.execute then
                solved_statement := core.solve(statement.code);
                begin
                    logger.session_step_task('start', solved_statement);
                    execute immediate solved_statement;
                    work := sql%rowcount;
                    commit;
                    logger.session_step_task('done', work => work);
                exception
                    when others then
                        rollback;
                        if statement.ignore_error is null or statement.ignore_error <> sqlcode then
                            logger.session_step_task('error', sqlerrm);
                        else
                            logger.session_step_task('warning', sqlerrm);
                        end if;
                end;
            end if;
        end loop;
        core.unset;
    end plsql;

    function shell(statement types.statement) return varchar2 is
        exit_value pls_integer;
        output types.text;
        solved_statement types.text;
        stderr types.text;
        stdout types.text;
    begin
        if statement.execute then
            solved_statement := core.solve(statement.code);
            if statement.log then
                logger.session_step_task('start', solved_statement);
            end if;
            output := shell(solved_statement);
            exit_value := to_number(regexp_substr(output, '(^|~)([^~]*)', 1, 1, null, 2));
            stderr := regexp_substr(output, '(^|~)([^~]*)', 1, 3, null, 2);
            stdout := regexp_substr(output, '(^|~)([^~]*)', 1, 2, null, 2);
            if exit_value <> 0 then
                raise_application_error(-20000, stderr);
            end if;
            if statement.log then
                logger.session_step_task('done');
            end if;
            return stdout;
        end if;
        return null;
    exception
        when others then
            if statement.log then
                logger.session_step_task('error', sqlerrm);
            else
                raise;
            end if;
    end shell;

    procedure shell(statement types.statement) is
        output types.text;
    begin
        output := execute.shell(statement);
        core.unset;
    end shell;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end execute;
/