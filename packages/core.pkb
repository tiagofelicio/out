create or replace package body out.core is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    properties types.map;
    variables types.map;

    function get_column_list(table_name varchar2, start_text varchar2, pattern varchar2, separator varchar2, end_text varchar2, column_list_in varchar2 default null, column_list_not_in varchar2 default null) return varchar2 is
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
        open columns_cursor for plsql using solve(table_name);
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

    procedure set(property_name varchar2, arg1 varchar2 default null, arg2 varchar2 default null, arg3 varchar2 default null) is
        option_value types.text;
        property_value types.text;
        i_arg1 types.text := case when arg1 is not null then solve(arg1) end;
        i_arg2 types.text := case when arg2 is not null then solve(arg2) end;
        i_arg3 types.text := case when arg3 is not null then solve(arg3) end;
        procedure check_work_table(table_name varchar2) is
        begin
            if regexp_count(table_name, '\.') <> 0 then
                raise_application_error(-20000, 'Invalid work table "' || table_name || '".');
            end if;
        end check_work_table;
        function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2 is
            option_value types.text;
        begin
            option_value := lower(trim(regexp_substr(options, replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1)));
            return nvl(option_value, default_value);
        end get_option;
    begin
        case property_name
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.check_unique_key
            when 'data_integration.check_unique_key.columns_name' then
                property_value := i_arg1;
            when 'data_integration.check_unique_key.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.control_append
            when 'data_integration.control_append.{analyze_partition_clause}' then
                option_value := get_option('partition name', i_arg1);
                property_value := case when option_value is not null then 'partname => ''' || option_value || ''',' end;
            when 'data_integration.control_append.{partition_clause}' then
                option_value := get_option('partition name', i_arg1);
                property_value := case when option_value is not null then 'partition (' || option_value || ')' end;
            when 'data_integration.control_append.{target_table_owner_name}' then
                property_value := regexp_substr(i_arg1, '[^\.]+', 1, 1);
            when 'data_integration.control_append.{target_table_short_name}' then
                property_value := regexp_substr(i_arg1, '[^\.]+', 1, 2);
            when 'data_integration.control_append.{work_table_columns}' then
                property_value := get_column_list('out.' || i_arg1, '', 'column_name', ',\n\t\t\t\t', '');
            when 'data_integration.control_append.(options).partition_name' then
                option_value := get_option('partition name', i_arg1);
                property_value := option_value;
            when 'data_integration.control_append.(options).truncate_partition' then
                option_value := get_option('truncate partition', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
            when 'data_integration.control_append.(options).truncate_table' then
                option_value := get_option('truncate table', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
            when 'data_integration.control_append.target_table_name' then
                property_value := i_arg1;
            when 'data_integration.control_append.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.create_table
            when 'data_integration.create_table.statement' then
                property_value := i_arg1;
            when 'data_integration.create_table.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.drop_table
            when 'data_integration.drop_table.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < data_integration.incremental_update
            when 'data_integration.incremental_update.{analyze_partition_clause}' then
                option_value := get_option('partition name', i_arg1);
                property_value := case when option_value is not null then 'partname => ''' || option_value || ''',' end;
            when 'data_integration.incremental_update.{interation_01_columns}' then
                property_value := get_column_list(i_arg2, '', 'w.column_name', ',\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_01_join}' then
                property_value := get_column_list(i_arg2, '', 'w.column_name = t.column_name', ' and\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_02_columns}' then
                property_value := get_column_list(i_arg2, '', 'nvl(i01.column_name, t.column_name) column_name', ',\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_02_join}' then
                property_value := get_column_list(i_arg2, '', 'i01.column_name = t.column_name', ' and\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_03_01_columns}' then
                property_value := get_column_list('out.' || i_arg2, '', 'nvl(w.column_name, t.column_name) column_name', ',\n\t\t\t\t', '', null, get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_03_01_join_01}' then
                property_value := get_column_list(i_arg2, '', 'i02.column_name = w.column_name', ' and\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_03_01_join_02}' then
                property_value := get_column_list(i_arg2, '', 'i02.column_name = t.column_name', ' and\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_03_01_natural_key_columns}' then
                property_value := get_column_list(i_arg2, '', 'i02.column_name', ',\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_03_02_columns}' then
                property_value := get_column_list('out.' || i_arg1, '', 'nvl(w.column_name, t.column_name) column_name', ',\n\t\t\t\t', '');
            when 'data_integration.incremental_update.{interation_03_02_join}' then
                property_value := get_column_list(i_arg2, '', 'w.column_name = t.column_name', ' and\n\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{interation_03_target_only_columns}' then
                property_value := get_column_list(i_arg2, ', ', 't.column_name', ',\n\t\t\t\t', '', null, get_column_list('out.' || i_arg3, '', 'column_name', ',', ',') || get_option('surrogate key', i_arg1, '#'));
            when 'data_integration.incremental_update.{interation_table_base_name}' then
                property_value := substr('out$_' || regexp_substr(i_arg1, '[^\.]+', 1, 2), 1, 128 - 3);
            when 'data_integration.incremental_update.{merge_condition}' then
                property_value := get_column_list(i_arg2, '', 't.column_name = i03.column_name', ' and\n\t\t\t\t\t', '', get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{merge_insert_columns}' then
                property_value := get_column_list('out.' || i_arg1, '', 'i03.column_name', ',\n\t\t\t\t\t\t', '');
            when 'data_integration.incremental_update.{merge_target_table_columns}' then
                property_value := get_column_list('out.' || i_arg1, '', 't.column_name', ',\n\t\t\t\t\t\t', '');
            when 'data_integration.incremental_update.{merge_update_clause}' then
                property_value := get_column_list('out.' || i_arg2, 'when matched then\n\t\t\t\t\tupdate set\n\t\t\t\t\t\t', 't.column_name = i03.column_name', ',\n\t\t\t\t\t\t', '', null, get_option('natural key', i_arg1));
            when 'data_integration.incremental_update.{partition_clause}' then
                option_value := get_option('partition name', i_arg1);
                property_value := case when option_value is not null then 'partition (' || option_value || ')' end;
            when 'data_integration.incremental_update.{target_table_columns}' then
                property_value := get_column_list(i_arg1, '', 'column_name', ',\n\t\t\t\t', '');
            when 'data_integration.incremental_update.{target_table_owner_name}' then
                property_value := regexp_substr(i_arg1, '[^\.]+', 1, 1);
            when 'data_integration.incremental_update.{target_table_short_name}' then
                property_value := regexp_substr(i_arg1, '[^\.]+', 1, 2);
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
            when 'data_integration.incremental_update.target_table_name' then
                property_value := i_arg1;
            when 'data_integration.incremental_update.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.copy
            when 'files.copy.(options).recursive' then
                option_value := get_option('recursive', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-R';
                end case;
            when 'files.copy.source_filename' then
                property_value := i_arg1;
            when 'files.copy.target_filename' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.load
            when 'files.load.{directory_name}' then
                property_value := substr('out$_' || i_arg1, 1, 128);
            when 'files.load.{directory_path}' then
                property_value := replace(i_arg1, '/' || regexp_substr(i_arg1, '[^/]+$'));
            when 'files.load.{external_table_name}' then
                property_value := 'out$_' || i_arg1;
            when 'files.load.{external_table_columns}' then
                
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(i_arg1 || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(i_arg1, line_start, line_end - 1));
                        if lower(line) like '%date%' then
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
                        line_end := nvl(instr(substr(i_arg1 || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(i_arg1, line_start, line_end - 1));
                        if lower(line) like '%date%' then
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
            when 'files.load.{work_table_columns}' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(i_arg1 || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(i_arg1, line_start, line_end - 1));
                        line := trim(substr(line, 1, instr(line, ' ') - 1));
                        if  length(line) > 0 then
                            property_value := property_value || chr(10) || lpad(' ', 24) || line || ',';
                        end if;
                        line_start := line_start + line_end + 1;
                    end loop;
                    property_value := trim(rtrim(ltrim(property_value, chr(10)), ','));
                end;
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
            when 'files.load.filename' then
                property_value := regexp_substr(i_arg1, '[^/]+$');
            when 'files.load.work_table_name' then
                check_work_table(i_arg1);
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.move
            when 'files.move.source_filename' then
                property_value := i_arg1;
            when 'files.move.target_filename' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.remove
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
            when 'files.remove.filename' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.unload
            when 'files.unload.{directory_name}' then
                property_value := substr('out$_' || regexp_substr(regexp_substr(i_arg1, '[^/]+$'), '[^\.]+', 1, 1), 1, 128);
            when 'files.unload.{directory_path}' then
                property_value := replace(i_arg1, '/' || regexp_substr(i_arg1, '[^/]+$'));
            when 'files.unload.{nls_date_format}' then
                property_value := sys_context('userenv', 'nls_date_format');
            when 'files.unload.(options).date_format' then
                option_value := get_option('date format', i_arg1, 'yyyy/mm/dd hh24:mi:ss');
                property_value := option_value;
            when 'files.unload.(options).field_separator' then
                option_value := get_option('field separator', i_arg1, ',');
                property_value := option_value;
            when 'files.unload.(options).file_format' then
                option_value := get_option('file format', i_arg1, 'delimited');
                case option_value
                    when 'delimited' then
                        property_value := 'delimited';
                end case;
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
            when 'files.unload.filename' then
                property_value := i_arg1;
            when 'files.unload.table_name' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.unzip
            when 'files.unzip.(options).keep_input_files' then
                option_value := get_option('keep input files', i_arg1, 'false');
                property_value := option_value;
            when 'files.unzip.(options).password' then
                option_value := get_option('password', i_arg1);
                property_value := case when option_value is not null then '-P ' || option_value end;
            when 'files.unzip.archive_name' then
                property_value := i_arg1;
            when 'files.unzip.directory_name' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.wait
            when 'files.wait.(options).polling_interval' then
                option_value := get_option('polling interval', i_arg1, '60');
                property_value := option_value;
            when 'files.wait.filename' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < files.zip
            when 'files.zip.(options).compress_level' then
                option_value := get_option('compress level', i_arg1, '6');
                property_value := '-' || option_value;
            when 'files.zip.(options).keep_input_files' then
                option_value := get_option('keep input files', i_arg1, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-m';
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
            when 'files.zip.archive_name' then
                property_value := i_arg1;
            when 'files.zip.filename' then
                property_value := i_arg1;
        ------------------------------------------------------------------------------------------------------------------------ < internet.http_get
            when 'internet.http_get.filename' then
                property_value := i_arg1;
            when 'internet.http_get.url' then
                property_value := i_arg1;
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
    end isset;

    procedure unset(property_name varchar2 default null) is
    begin
        if property_name is null then
            properties.delete;
        else
            properties.delete('#' || property_name);
        end if;
    end unset;

    function solve(text varchar2) return varchar2 is
        solved_text types.text;
        property_name types.text;
        property_value types.text;
        variable_name types.text;
        variable_value types.text;
    begin
        solved_text := text;
        property_name := properties.first;
        while property_name is not null loop
            property_value := properties(property_name);
            solved_text := replace(solved_text, property_name, property_value);
            property_name := properties.next(property_name);
        end loop;
        variable_name := variables.first;
        while variable_name is not null loop
            variable_value := variables(variable_name);
            solved_text := replace(solved_text, variable_name, variable_value);
            variable_name := variables.next(variable_name);
        end loop;
        return solved_text;
    end solve;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end core;
/