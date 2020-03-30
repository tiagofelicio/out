create or replace package body out.core is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    properties types.map;
    variables types.map;

    ----------------------------------------------------------------------------------------------------------------------------
    -- .parse properties and variables -----------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure parse(text in out nocopy varchar2) is
        property_name types.text;
        property_value types.text;
        variable_name types.text;
        variable_value types.text;
    begin
        property_name := properties.first;
        while property_name is not null loop
            property_value := properties(property_name);
            text := replace(text, property_name, property_value);
            property_name := properties.next(property_name);
        end loop;
        variable_name := variables.first;
        while variable_name is not null loop
            variable_value := variables(variable_name);
            text := replace(text, variable_name, variable_value);
            variable_name := variables.next(variable_name);
        end loop;
    end parse;

    function parse_variables(text varchar2) return varchar2 is
        parsed_text types.text;
        variable_name types.text;
        variable_value types.text;
    begin
        parsed_text := text;
        variable_name := variables.first;
        while variable_name is not null loop
            variable_value := variables(variable_name);
            parsed_text := replace(parsed_text, variable_name, variable_value);
            variable_name := variables.next(variable_name);
        end loop;
        return parsed_text;
    end parse_variables;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure check_work_table(table_name varchar2) is
    begin
        if regexp_count(table_name, '\.') <> 0 then
            raise_application_error(-20000, 'Invalid work table "' || table_name || '".');
        end if;
    end check_work_table;

    function get_column_list(table_name varchar2, start_text varchar2 default '', pattern varchar2, separator varchar2, end_text varchar2 default '', column_list_in varchar2 default null, column_list_not_in varchar2 default null) return varchar2 is
        column_list types.text := '';
        column_name all_tab_columns.column_name%type;
        columns_cursor sys_refcursor;
        plsql types.text;
        function cleansing(column_list varchar2) return varchar2 is
        begin
            return '''' || replace(regexp_replace(regexp_replace(upper(column_list), '[[:space:]]+', ' '), '([[:space:]]{0,},[[:space:]]{0,})', ','), ',', ''',''') || '''';
        end cleansing;
    begin
        plsql := q'[
            select lower(column_name)
            from all_tab_columns
            where
                (owner || '.' || table_name) = upper(:table_name)
            ]' || case when column_list_in is not null then 'and column_name in (' || cleansing(column_list_in) || ')' end || q'[
            ]' || case when column_list_not_in is not null then 'and column_name not in (' || cleansing(column_list_not_in) || ')' end || q'[
            order by column_id
        ]';
        open columns_cursor for plsql using parse_variables(table_name);
        loop
            fetch columns_cursor into column_name;
            exit when columns_cursor%notfound;
            column_list := column_list || separator || regexp_replace(pattern, 'column_name', column_name, 1, 0, 'i');
        end loop;
        close columns_cursor;
        if column_list is null then
            return null;
        else
            return replace(replace(start_text || substr(column_list, length(separator) + 1) || end_text, '\n', chr(10)), '\t', '    ');
        end if;
    end get_column_list;

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2 is
        option_value types.text;
    begin
        option_value := lower(trim(regexp_substr(options, replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1)));
        return nvl(option_value, default_value);
    end get_option;

    ----------------------------------------------------------------------------------------------------------------------------
    -- .languages --------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function bash(statement varchar2) return varchar2 is
    language java
    name 'OUTTools.bash(java.lang.String) return java.lang.String';

    function bash(statement in out nocopy types.bash_lang, work in out number) return anydata is
        exit_value pls_integer;
        output types.text;
        stderr types.text;
        stdout types.text;
    begin
        output := bash(statement.code);
        exit_value := to_number(regexp_substr(output, '(^|~)([^~]*)', 1, 1, null, 2));
        stderr := regexp_substr(output, '(^|~)([^~]*)', 1, 3, null, 2);
        stdout := regexp_substr(output, '(^|~)([^~]*)', 1, 2, null, 2);
        if exit_value <> 0 then
            raise_application_error(-20000, stderr);
        end if;
        work := 0;
        return anydata.ConvertVarchar2(stdout);
    end bash;

    function plsql(statement in out nocopy types.plsql_lang, work in out number) return anydata is
        pragma autonomous_transaction;
        statement_cursor pls_integer;
        statement_column_count pls_integer;
        statement_column_description dbms_sql.desc_tab2;
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
        case statement.to_fetch
            when false then
                execute immediate statement.code;
                work := sql%rowcount;
            when true then
                statement_cursor := dbms_sql.open_cursor;
                dbms_sql.parse(statement_cursor, statement.code, dbms_sql.native);
                dbms_sql.describe_columns2(statement_cursor, statement_column_count, statement_column_description);
                dbms_sql.close_cursor(statement_cursor);
                case statement_column_description(1).col_type
                    when dbms_types.typecode_bfile then
                        execute immediate statement.code into result_bfile;
                        result := anydata.ConvertBfile(result_bfile);
                    when dbms_types.typecode_bdouble then
                        execute immediate statement.code into result_binary_double;
                        result := anydata.ConvertBDouble(result_binary_double);
                    when dbms_types.typecode_bfloat then
                        execute immediate statement.code into result_binary_float;
                        result := anydata.ConvertBFloat(result_binary_float);
                    when dbms_types.typecode_blob then
                        execute immediate statement.code into result_blob;
                        result := anydata.ConvertBlob(result_blob);
                    when dbms_types.typecode_char then
                        execute immediate statement.code into result_char;
                        result := anydata.ConvertChar(result_char);
                    when dbms_types.typecode_clob then
                        execute immediate statement.code into result_clob;
                        result := anydata.ConvertClob(result_clob);
                    when dbms_types.typecode_date then
                        execute immediate statement.code into result_date;
                        result := anydata.ConvertDate(result_date);
                    when dbms_types.typecode_interval_ds then
                        execute immediate statement.code into result_inverval_day_to_second;
                        result := anydata.ConvertIntervalDS(result_inverval_day_to_second);
                    when dbms_types.typecode_interval_ym then
                        execute immediate statement.code into result_inverval_year_to_month;
                        result := anydata.ConvertIntervalYM(result_inverval_year_to_month);
                    when dbms_types.typecode_nchar then
                        execute immediate statement.code into result_nchar;
                        result := anydata.ConvertNChar(result_nchar);
                    when dbms_types.typecode_nclob then
                        execute immediate statement.code into result_nclob;
                        result := anydata.ConvertNClob(result_nclob);
                    when dbms_types.typecode_number then
                        execute immediate statement.code into result_number;
                        result := anydata.ConvertNumber(result_number);
                    when dbms_types.typecode_nvarchar2 then
                        execute immediate statement.code into result_nvarchar2;
                        result := anydata.ConvertNVarchar2(result_nvarchar2);
                    when dbms_types.typecode_raw then
                        execute immediate statement.code into result_raw;
                        result := anydata.ConvertRaw(result_raw);
                    when dbms_types.typecode_timestamp then
                        execute immediate statement.code into result_timestamp;
                        result := anydata.ConvertTimestamp(result_timestamp);
                    when dbms_types.typecode_timestamp_ltz then
                        execute immediate statement.code into result_timestamp_with_local_time_zone;
                        result := anydata.ConvertTimestampLTZ(result_timestamp_with_local_time_zone);
                    when dbms_types.typecode_timestamp_tz then
                        execute immediate statement.code into result_timestamp_with_time_zone;
                        result := anydata.ConvertTimestampTZ(result_timestamp_with_time_zone);
                    when dbms_types.typecode_urowid then
                        execute immediate statement.code into result_urowid;
                        result := anydata.ConvertURowid(result_urowid);
                    when dbms_types.typecode_varchar then
                        execute immediate statement.code into result_varchar;
                        result := anydata.ConvertVarchar2(result_varchar);
                    when dbms_types.typecode_varchar2 then
                        execute immediate statement.code into result_varchar2;
                        result := anydata.ConvertVarchar2(result_varchar2);
                end case;
                work := 0;
        end case;
        commit;
        return result;
    exception
        when others then
            rollback;
            if dbms_sql.is_open(statement_cursor) then
                dbms_sql.close_cursor(statement_cursor);
            end if;
            raise;
    end plsql;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure bind(variable_name varchar2, variable_value varchar2) is
    begin
        variables('#' || variable_name) := variable_value;
    end bind;

    procedure unbind(variable_name varchar2 default null) is
    begin
        if variable_name is null then
            variables.delete;
        else
            variables.delete('#' || variable_name);
        end if;
    end unbind;

    function get(property_name varchar2) return varchar2 is
    begin
        return properties('$' || property_name);
    end get;

    procedure set(property_name varchar2, arg1 varchar2 default null) is
        option_value types.text;
        property_value types.text;
        i_arg1 types.text := case when arg1 is not null then parse_variables(arg1) end;
    begin
        case property_name
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.append
            when 'data_integration.append.target_table_name' then
                property_value := i_arg1;
            when 'data_integration.append.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'data_integration.append.(options).partition_name' then
                option_value := get_option('partition name', i_arg1);
                property_value := option_value;
            when 'data_integration.append.(options).partition_value' then
                option_value := get_option('partition value', i_arg1);
                property_value := option_value;
            when 'data_integration.append.(options).truncate_partition' then
                option_value := get_option('truncate partition', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
            when 'data_integration.append.(options).truncate_table' then
                option_value := get_option('truncate table', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
            when 'data_integration.append.{analyze_partition_clause}' then
                if isset('data_integration.append.<partition_name>') then
                    property_value := 'partname => ''' || get('data_integration.append.<partition_name>') || ''',';
                end if;
            when 'data_integration.append.<partition_name>' then
                if isset('data_integration.append.(options).partition_name') then
                    property_value := get('data_integration.append.(options).partition_name');
                else
                    if isset('data_integration.append.(options).partition_value') then
                        declare
                            exists_partition number;
                            partition_name all_tab_partitions.partition_name%type;
                            partition_value all_tab_partitions.high_value%type;
                        begin
                            select 'out$_' || to_char(systimestamp, 'yyyymmddhh24missff6') || '_' || to_char(ora_hash(get('data_integration.append.(options).partition_value'))) into partition_name from dual;
                            for c in (select partition_name, high_value from all_tab_partitions where lower(table_owner || '.' || table_name) = get('data_integration.append.target_table_name') order by partition_position) loop
                                partition_value := c.high_value;
                                execute immediate 'select case when ' || get('data_integration.append.(options).partition_value') || ' = ' || partition_value || ' then 1 else 0 end from dual' into exists_partition;
                                if exists_partition = 1 then
                                    partition_name := c.partition_name;
                                    exit;
                                end if;
                            end loop;
                            property_value := lower(partition_name);
                        end;
                    end if;
                end if;
            when 'data_integration.append.{partition_clause}' then
                if isset('data_integration.append.<partition_name>') then
                    property_value := 'partition (' || get('data_integration.append.<partition_name>') || ')';
                end if;
            when 'data_integration.append.{target_table_owner_name}' then
                property_value := regexp_substr(get('data_integration.append.target_table_name'), '[^\.]+', 1, 1);
            when 'data_integration.append.{target_table_short_name}' then
                property_value := regexp_substr(get('data_integration.append.target_table_name'), '[^\.]+', 1, 2);
            when 'data_integration.append.{work_table_columns}' then
                property_value := get_column_list(
                                      table_name => 'out.' || get('data_integration.append.work_table_name'),
                                      pattern => 'column_name',
                                      separator => ',\n\t\t\t\t'
                                  );
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.check_not_null
            when 'data_integration.check_not_null.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'data_integration.check_not_null.column_name' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.check_primary_key
            when 'data_integration.check_primary_key.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'data_integration.check_primary_key.columns_name' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.check_unique_key
            when 'data_integration.check_unique_key.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'data_integration.check_unique_key.columns_name' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.create_table
            when 'data_integration.create_table.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'data_integration.create_table.statement' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.drop_table
            when 'data_integration.drop_table.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.incremental_update
            when 'data_integration.incremental_update.target_table_name' then
                property_value := i_arg1;
            when 'data_integration.incremental_update.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'data_integration.incremental_update.(options).method' then
                option_value := get_option('method', i_arg1, 'full join');
                case option_value
                    when 'full join' then
                        property_value := 'full join';
                    when 'merge' then
                        property_value := 'merge';
                end case;
            when 'data_integration.incremental_update.(options).natural_key' then
                option_value := get_option('natural key', i_arg1);
                i_arg1 := null;
                case length(option_value) = 0
                    when false then
                        property_value := option_value;
                end case;
            when 'data_integration.incremental_update.(options).partition_name' then
                option_value := get_option('partition name', i_arg1);
                property_value := option_value;
            when 'data_integration.incremental_update.(options).surrogate_key' then
                option_value := get_option('surrogate key', i_arg1);
                property_value := case when option_value is not null then option_value end;
            when 'data_integration.incremental_update.{analyze_partition_clause}' then
                if isset('data_integration.incremental_update.(options).partition_name') then
                    property_value := 'partname => ''' || get('data_integration.incremental_update.(options).partition_name') || ''',';
                end if;
            when 'data_integration.incremental_update.{interation_01_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'w.column_name',
                                      separator => ',\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_01_join}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'w.column_name = t.column_name',
                                      separator => ' and\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_02_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'nvl(i01.column_name, t.column_name) column_name',
                                      separator => ',\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_02_join}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'i01.column_name = t.column_name',
                                      separator => ' and\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_03_01_columns}' then
                property_value := get_column_list(
                                      table_name => 'out.' || get('data_integration.incremental_update.work_table_name'),
                                      pattern => 'nvl(w.column_name, t.column_name) column_name',
                                      separator => ',\n\t\t\t\t',
                                      column_list_not_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_03_01_join_01}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'i02.column_name = w.column_name',
                                      separator => ' and\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_03_01_join_02}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'i02.column_name = t.column_name',
                                      separator => ' and\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_03_01_natural_key_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'i02.column_name',
                                      separator => ',\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_03_02_columns}' then
                property_value := get_column_list(
                                      table_name => 'out.' || get('data_integration.incremental_update.work_table_name'),
                                      pattern => 'nvl(w.column_name, t.column_name) column_name',
                                      separator => ',\n\t\t\t\t'
                                  );
            when 'data_integration.incremental_update.{interation_03_02_join}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'w.column_name = t.column_name',
                                      separator => ' and\n\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{interation_03_target_only_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      start_text => ', ',
                                      pattern => 't.column_name',
                                      separator => ',\n\t\t\t\t',
                                      column_list_not_in => get_column_list(
                                                                table_name => 'out.' || get('data_integration.incremental_update.work_table_name'),
                                                                pattern => 'column_name',
                                                                separator => ',',
                                                                end_text => ',' || nvl(get('data_integration.incremental_update.(options).surrogate_key'), '#')
                                                            )
                                  );
            when 'data_integration.incremental_update.{interation_table_base_name}' then
                property_value := 'out$_' || regexp_substr(get('data_integration.incremental_update.target_table_name'), '[^\.]+', 1, 2);
            when 'data_integration.incremental_update.{merge_condition}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 't.column_name = i03.column_name',
                                      separator => ' and\n\t\t\t\t\t',
                                      column_list_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{merge_insert_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'i03.column_name',
                                      separator => ',\n\t\t\t\t\t\t'
                                  );
            when 'data_integration.incremental_update.{merge_target_table_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 't.column_name',
                                      separator => ',\n\t\t\t\t\t\t'
                                  );
            when 'data_integration.incremental_update.{merge_update_clause}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      start_text => 'when matched then\n\t\t\t\t\tupdate set\n\t\t\t\t\t\t',
                                      pattern => 't.column_name = i03.column_name',
                                      separator => ',\n\t\t\t\t\t\t',
                                      column_list_not_in => get('data_integration.incremental_update.(options).natural_key')
                                  );
            when 'data_integration.incremental_update.{partition_clause}' then
                if isset('data_integration.incremental_update.(options).partition_name') then
                    property_value := 'partition (' || get('data_integration.incremental_update.(options).partition_name') || ')';
                end if;
            when 'data_integration.incremental_update.{target_table_columns}' then
                property_value := get_column_list(
                                      table_name => get('data_integration.incremental_update.target_table_name'),
                                      pattern => 'column_name',
                                      separator => ',\n\t\t\t\t'
                                  );
            when 'data_integration.incremental_update.{target_table_owner_name}' then
                property_value := regexp_substr(get('data_integration.incremental_update.target_table_name'), '[^\.]+', 1, 1);
            when 'data_integration.incremental_update.{target_table_short_name}' then
                property_value := regexp_substr(get('data_integration.incremental_update.target_table_name'), '[^\.]+', 1, 2);
        ------------------------------------------------------------------------------------------------------------------------ < files.copy
            when 'files.copy.target_filename' then
                property_value := i_arg1;
            when 'files.copy.source_filename' then
                property_value := i_arg1;
            when 'files.copy.(options).recursive' then
                option_value := get_option('recursive', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-R';
                end case;
        ------------------------------------------------------------------------------------------------------------------------ < files.load
            when 'files.load.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'files.load.filename' then
                property_value := i_arg1;
            when 'files.load.attributes' then
                property_value := i_arg1;
            when 'files.load.(options).file_format' then
                option_value := get_option('file format', i_arg1, 'delimited');
                case option_value
                    when 'delimited' then
                        property_value := 'delimited';
                    when 'large object' then
                        property_value := 'large object';
                end case;
            when 'files.load.(options).field_separator' then
                option_value := get_option('field separator', i_arg1, ',');
                property_value := option_value;
            when 'files.load.(options).heading' then
                option_value := get_option('heading', i_arg1, '1');
                property_value := option_value;
            when 'files.load.(options).record_separator' then
                option_value := get_option('record separator', i_arg1, '\n');
                property_value := option_value;
            when 'files.load.(options).text_delimiter' then
                option_value := get_option('text delimiter', i_arg1);
                property_value := case when option_value is not null then 'optionally enclosed by ''' || option_value || '''' end;
            when 'files.load.{directory_name}' then
                property_value := 'out$_' || get('files.load.work_table_name');
            when 'files.load.{directory_path}' then
                property_value := replace(get('files.load.filename'), '/' || regexp_substr(get('files.load.filename'), '[^/]+$'));
            when 'files.load.{external_table_name}' then
                property_value := 'out$_' || get('files.load.work_table_name');
            when 'files.load.{external_table_columns}' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(get('files.load.attributes') || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(get('files.load.attributes'), line_start, line_end - 1));
                        if lower(line) like '% date %' then
                            line := trim(substr(line, 1, instr(lower(line), ' mask ') - 1));
                        end if;
                        if  length(line) > 0 then
                            property_value := property_value || chr(10) || lpad(' ', 24) || line || ',';
                        end if;
                        line_start := line_start + line_end + 1;
                    end loop;
                    property_value := trim(rtrim(ltrim(property_value, chr(10)), ','));
                end;
            when 'files.load.{external_table_field_list_clause}' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(get('files.load.attributes') || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(get('files.load.attributes'), line_start, line_end - 1));
                        if lower(line) like '% date %' then
                            line := trim(replace(line, ' date ', ' char date_format date '));
                        else
                            line := trim(substr(line, 1, instr(line, ' ') - 1));
                        end if;
                        if  length(line) > 0 then
                            property_value := property_value || chr(10) || lpad(' ', 32) || line || ',';
                        end if;
                        line_start := line_start + line_end + 1;
                    end loop;
                    property_value := trim(rtrim(ltrim(property_value, chr(10)), ','));
                end;
            when 'files.load.{file_basename}' then
                property_value := regexp_substr(get('files.load.filename'), '[^/]+$');
            when 'files.load.{work_table_columns}' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(get('files.load.attributes') || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(get('files.load.attributes'), line_start, line_end - 1));
                        line := trim(substr(line, 1, instr(line, ' ') - 1));
                        if  length(line) > 0 then
                            property_value := property_value || chr(10) || lpad(' ', 24) || line || ',';
                        end if;
                        line_start := line_start + line_end + 1;
                    end loop;
                    property_value := trim(rtrim(ltrim(property_value, chr(10)), ','));
                end;
        ------------------------------------------------------------------------------------------------------------------------ < files.move
            when 'files.move.target_filename' then
                property_value := i_arg1;
            when 'files.move.source_filename' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.remove
            when 'files.remove.filename' then
                property_value := i_arg1;
            when 'files.remove.(options).force' then
                option_value := get_option('force', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-f';
                end case;
            when 'files.remove.(options).recursive' then
                option_value := get_option('recursive', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-r';
                end case;
        ------------------------------------------------------------------------------------------------------------------------ < files.unload
            when 'files.unload.filename' then
                property_value := i_arg1;
            when 'files.unload.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
            when 'files.unload.(options).date_format' then
                option_value := get_option('date format', i_arg1, 'yyyy/mm/dd hh24:mi:ss');
                property_value := option_value;
            when 'files.unload.(options).file_format' then
                option_value := get_option('file format', i_arg1, 'delimited');
                case option_value
                    when 'delimited' then
                        property_value := 'delimited';
                end case;
            when 'files.unload.(options).field_separator' then
                option_value := get_option('field separator', i_arg1, ',');
                property_value := option_value;
            when 'files.unload.(options).generate_header' then
                option_value := get_option('generate header', i_arg1, 'true');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
            when 'files.unload.(options).record_separator' then
                option_value := get_option('record separator', i_arg1, '\n');
                property_value := option_value;
            when 'files.unload.(options).text_delimiter' then
                option_value := get_option('text delimiter', i_arg1);
                property_value := option_value;
            when 'files.unload.{directory_name}' then
                property_value := substr('out$_' || regexp_replace(regexp_substr(regexp_substr(get('files.unload.filename'), '[^/]+$'), '[^\.]+', 1, 1), '[^a-zA-Z0-9]+'), 1, 128);
            when 'files.unload.{directory_path}' then
                property_value := replace(get('files.unload.filename'), '/' || regexp_substr(get('files.unload.filename'), '[^/]+$'));
            when 'files.unload.{file_basename}' then
                property_value := regexp_substr(get('files.unload.filename'), '[^/]+$');
            when 'files.unload.{nls_date_format}' then
                property_value := sys_context('userenv', 'nls_date_format');
        ------------------------------------------------------------------------------------------------------------------------ < files.unzip
            when 'files.unzip.directory_name' then
                property_value := i_arg1;
            when 'files.unzip.archive_name' then
                property_value := i_arg1;
            when 'files.unzip.(options).keep_input_files' then
                option_value := get_option('keep input files', i_arg1, 'false');
                property_value := option_value;
            when 'files.unzip.(options).password' then
                option_value := get_option('password', i_arg1);
                property_value := case when option_value is not null then '-P ' || option_value end;
        ------------------------------------------------------------------------------------------------------------------------ < files.wait
            when 'files.wait.filename' then
                property_value := i_arg1;
            when 'files.wait.(options).polling_interval' then
                option_value := get_option('polling interval', i_arg1, '60');
                property_value := option_value;
        ------------------------------------------------------------------------------------------------------------------------ < files.zip
            when 'files.zip.archive_name' then
                property_value := i_arg1;
            when 'files.zip.filename' then
                property_value := i_arg1;
            when 'files.zip.(options).compress_level' then
                option_value := get_option('compress level', i_arg1, '6');
                property_value := '-' || option_value;
            when 'files.zip.(options).keep_input_files' then
                option_value := get_option('keep input files', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '-m';
                    when 'true' then
                        property_value := '';
                end case;
            when 'files.zip.(options).password' then
                option_value := get_option('password', i_arg1);
                property_value := case when option_value is not null then '-P ' || option_value end;
            when 'files.zip.(options).recursive' then
                option_value := get_option('recursive', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-r';
                end case;
        ------------------------------------------------------------------------------------------------------------------------ < internet.http_get
            when 'internet.http_get.filename' then
                property_value := i_arg1;
            when 'internet.http_get.url' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < utilities.bash
            when 'utilities.bash.(options).ignore_errors' then
                option_value := get_option('ignore errors', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
        end case;
        properties('$' || property_name) := property_value;
    exception
        when others then
            if sqlcode = -06592 then
                raise_application_error(-20000, 'Unexpected value "' || coalesce(option_value, i_arg1) || '" for property ' || property_name || ';');
            else
                raise;
            end if;
    end set;

    function isset(property_name varchar2) return boolean is
    begin
        return properties('$' || property_name) is not null;
    exception
        when no_data_found then
            return false;
    end isset;

    function execute(statement in out nocopy types.statement, unset boolean default true) return anydata is
        output anydata := anydata.ConvertRaw(null);
        work number := 0;
    begin
        parse(statement.plsql.code);
        parse(statement.bash.code);
        if statement.log then
            logger.session_step_task('start', code => coalesce(statement.plsql.code, statement.bash.code));
        end if;
        case
            when statement.plsql.code is not null then
                output := plsql(statement.plsql, work);
            when statement.bash.code is not null then
                output := bash(statement.bash, work);
            else
                raise_application_error(-20000, 'Unrecognized language.');
        end case;
        if unset then
            properties.delete;
        end if;
        if statement.log then
            logger.session_step_task('done', work => work);
        end if;
        return output;
    exception
        when others then
            if unset then
                properties.delete;
            end if;
            if statement.ignore_error = sqlcode or statement.ignore_error is null then
                if statement.log then
                    logger.session_step_task('warning', error => sqlerrm);
                end if;
                return output;
            else
                if statement.log then
                    logger.session_step_task('error', error => sqlerrm);
                else
                    raise;
                end if;
            end if;
    end execute;

    procedure execute(statements in out nocopy types.statements, unset boolean default true) is
        output anydata;
    begin
        for i in statements.first .. statements.last loop
            if statements(i).execute then
                output := execute(statements(i), unset => false);
            end if;
        end loop;
        properties.delete;
    exception
        when others then
            properties.delete;
            raise;
    end execute;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end core;
/