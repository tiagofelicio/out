create or replace package body out.data_integration is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function get_columns(table_name varchar2, start_text varchar2 default null, pattern varchar2 default null, separator varchar2 default null, columns_in varchar2 default null, columns_not_in varchar2 default null, remove_last_occurence varchar2 default null) return varchar2 is
        statement core.text_t;
        columns_list core.text_t;
        columns_name_cursor sys_refcursor;
        column_name all_tab_columns.column_name%type;
    begin
        statement := q'[
            select column_name
            from all_tab_columns
            where
                (owner || '.' || table_name) = upper(:table_name)
                $columns_in
                $columns_not_in
            order by column_id
        ]';
        columns_list := '';
        core.bind('$', 'columns_in', case when columns_in is not null then 'and (column_name in (''' || replace(regexp_replace(regexp_replace(upper(columns_in), '[[:space:]]+', ' '), '([[:space:]]{0,},[[:space:]]{0,})', ','), ',', ''',''') || '''))' end);
        core.bind('$', 'columns_not_in', case when columns_not_in is not null then 'and (column_name not in (''' || replace(regexp_replace(regexp_replace(upper(columns_not_in), '[[:space:]]+', ' '), '([[:space:]]{0,},[[:space:]]{0,})', ','), ',', ''',''') || '''))' end);
        open columns_name_cursor for core.solve(statement) using core.solve(table_name);
        core.unbind('$', 'columns_in');
        core.unbind('$', 'columns_not_in');
        loop
            fetch columns_name_cursor into column_name;
            exit when columns_name_cursor%notfound;
            if pattern is not null and separator is null then
                columns_list := columns_list || regexp_replace(pattern, 'column_name', column_name, 1, 0, 'i');
            elsif pattern is null and separator is not null then
                columns_list := columns_list || separator || column_name;
            else
                close columns_name_cursor;
                raise_application_error(-20000, 'Invalid call using pattern "' || pattern || '" and separator "' || separator || '".');
            end if;
        end loop;
        close columns_name_cursor;
        if pattern is not null and remove_last_occurence is not null then
            columns_list := rtrim(columns_list, remove_last_occurence);
        elsif separator is not null then
            columns_list := substr(columns_list, length(separator) + 1);
        end if;
        if start_text is not null and columns_list is not null then
            columns_list := start_text || columns_list;
        end if;
        return columns_list;
    end get_columns;

    function get_property(property_name varchar2, text varchar2) return varchar2 is
        property_value core.text_t;
        solved_text core.text_t;
        analyze_partition_clause core.text_t;
        directory_name all_directories.directory_name%type;
        directory_path all_directories.directory_path%type;
        external_table_columns_definition core.text_t;
        external_table_name core.text_t;
        field_delimiter_clause core.text_t;
        filename core.text_t;
        filename_with_path core.text_t;
        integration_table_01_name core.text_t;
        integration_table_02_name core.text_t;
        integration_table_03_name core.text_t;
        lob_column_name all_tab_columns.column_name%type;
        partition_clause core.text_t;
        table_owner_name all_tables.owner%type;
        table_short_name all_tables.table_name%type;
    begin
        solved_text := core.solve(text);
        case lower(property_name)
            when 'analyze partition clause' then
                analyze_partition_clause := case when solved_text is null or length(solved_text) = 0 then '' else 'partname => ''' || solved_text || ''',' end;
                property_value := analyze_partition_clause;
            when 'directory name' then
                directory_name := substr('o_' || regexp_substr(solved_text, '[^\.]+', 1, 2), 1, 128);
                property_value := directory_name;
            when 'directory path' then
                filename_with_path := regexp_substr(solved_text, 'from[[:space:]]+([^[:space:]]+)', 1, 1, 'mix', 1);
                filename := regexp_substr(filename_with_path, '[^/]+$');
                directory_path := replace(filename_with_path, '/' || filename);
                property_value := directory_path;
            when 'external table columns definition' then
                external_table_columns_definition := replace(regexp_substr(solved_text, 'select[[:space:]]+(.*?)[[:space:]]+from', 1, 1, 'minx', 1), ',', ' varchar2(4000),') || ' varchar2(4000)';
                property_value := external_table_columns_definition;
            when 'external table name' then
                table_owner_name := regexp_substr(solved_text, '[^\.]+', 1, 1);
                table_short_name := regexp_substr(solved_text, '[^\.]+', 1, 2);
                external_table_name := table_owner_name || '.' || substr('o_' || table_short_name, 1, 128);
                property_value := external_table_name;
            when 'field delimiter clause' then
                field_delimiter_clause := case when solved_text is null or length(solved_text) = 0 then '' else 'optionally enclosed by ''' || solved_text || '''' end;
                property_value := field_delimiter_clause;
            when 'filename' then
                filename_with_path := regexp_substr(solved_text, 'from[[:space:]]+([^[:space:]]+)', 1, 1, 'mix', 1);
                filename := regexp_substr(filename_with_path, '[^/]+$');
                property_value := filename;
            when 'filename with path' then
                filename_with_path := regexp_substr(solved_text, 'from[[:space:]]+([^[:space:]]+)', 1, 1, 'mix', 1);
                property_value := filename_with_path;
            when 'integration table 01 name' then
                table_owner_name := regexp_substr(solved_text, '[^\.]+', 1, 1);
                table_short_name := regexp_substr(solved_text, '[^\.]+', 1, 2);
                integration_table_01_name := table_owner_name || '.' || 'o_' || substr(table_short_name, 1, 128 -  4) || '_01';
                property_value := integration_table_01_name;
            when 'integration table 02 name' then
                table_owner_name := regexp_substr(solved_text, '[^\.]+', 1, 1);
                table_short_name := regexp_substr(solved_text, '[^\.]+', 1, 2);
                integration_table_02_name := table_owner_name || '.' || 'o_' || substr(table_short_name, 1, 128 -  4) || '_02';
                property_value := integration_table_02_name;
            when 'integration table 03 name' then
                table_owner_name := regexp_substr(solved_text, '[^\.]+', 1, 1);
                table_short_name := regexp_substr(solved_text, '[^\.]+', 1, 2);
                integration_table_03_name := table_owner_name || '.' || 'o_' || substr(table_short_name, 1, 128 -  4) || '_03';
                property_value := integration_table_03_name;
            when 'lob column name' then
                lob_column_name := regexp_substr(solved_text, 'select[[:space:]]+(.*?)[[:space:]]+from', 1, 1, 'minx', 1);
                property_value := lob_column_name;
            when 'partition clause' then
                partition_clause := case when solved_text is null or length(solved_text) = 0 then '' else 'partition (' || solved_text || ')' end;
                property_value := partition_clause;
            when 'table owner name' then
                table_owner_name := regexp_substr(solved_text, '[^\.]+', 1, 1);
                property_value := table_owner_name;
            when 'table short name' then
                table_short_name := regexp_substr(solved_text, '[^\.]+', 1, 2);
                property_value := table_short_name;
            else
                raise_application_error(-20000, 'Invalid property name (' || property_name || ').');
        end case;
        return property_value;
    end get_property;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure check_unique_key(table_name varchar2, columns_name varchar2) is
        statement core.statement_t;
        duplicated_keys number;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            select count(1)
            from (
                select $columns_name
                from $table_name
                group by $columns_name
                having count(1) > 1
            )
        ]';
        core.bind('$', 'columns_name', columns_name);
        core.bind('$', 'table_name', table_name);
        core.plsql(statement, duplicated_keys);
        if duplicated_keys <> 0 then
            raise_application_error(-20000, core.solve('Found ' || to_char(duplicated_keys) || ' duplicated keys in table $table_name using ' || columns_name || ' as key.'));
        end if;
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end check_unique_key;

    procedure create_table(table_name varchar2, statement varchar2) is
        statements core.statements_t;
    begin
        internal.log_session_step('start');
        statements(1).code := q'[
            drop table $table_name purge
        ]';
        statements(1).ignore_error := -00942;
        statements(2).code := q'[
            create table $table_name nologging pctfree 0 compress parallel as
            $statement
        ]';
        statements(3).code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$table_owner_name',
                    tabname => '$table_short_name',
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'all',
                    cascade => false,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        core.bind('$', 'statement', statement);
        core.bind('$', 'table_name', table_name);
        core.bind('$', 'table_owner_name', get_property('table owner name', table_name));
        core.bind('$', 'table_short_name', get_property('table short name', table_name));
        core.plsql(statements);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end create_table;

    procedure create_table(table_name varchar2, statement varchar2, options varchar2) is
        statements core.statements_t;
    begin
        internal.log_session_step('start');
        case core.get_option('type', options)
            when 'delimited file' then
                statements(1).code := q'[
                    drop directory $directory_name
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $directory_name as '$directory_path'
                ]';
                statements(3).code := q'[
                    drop table $external_table_name purge
                ]';
                statements(3).ignore_error := -00942;
                statements(4).code := q'[
                    create table $external_table_name (
                        $external_table_columns_definition
                    )
                    organization external (
                        type oracle_loader
                        default directory $directory_name
                        access parameters (
                            records delimited by newline
                            skip $heading
                            nobadfile
                            nodiscardfile
                            nologfile
                            fields terminated by '$field_separator' $field_delimiter_clause
                            missing field values are null
                        )
                        location ('$filename')
                    )
                    nomonitoring
                    parallel
                    reject limit 0
                ]';
                statements(5).code := q'[
                    drop table $table_name purge
                ]';
                statements(5).ignore_error := -00942;
                statements(6).code := q'[
                    create table $table_name nologging pctfree 0 compress parallel as
                    $statement
                ]';
                statements(7).code := q'[
                    begin
                        dbms_stats.gather_table_stats(
                            ownname => '$table_owner_name',
                            tabname => '$table_short_name',
                            estimate_percent => dbms_stats.auto_sample_size,
                            method_opt => 'for all columns size auto',
                            degree => dbms_stats.auto_degree,
                            granularity => 'all',
                            cascade => true,
                            no_invalidate => dbms_stats.auto_invalidate
                        );
                    end;
                ]';
                statements(8).code := q'[
                    drop table $external_table_name purge
                ]';
                statements(9).code := q'[
                    drop directory $directory_name
                ]';
                core.bind('$', 'directory_name', get_property('directory name', table_name));
                core.bind('$', 'directory_path', get_property('directory path', statement));
                core.bind('$', 'external_table_columns_definition', get_property('external table columns definition', statement));
                core.bind('$', 'external_table_name', get_property('external table name', table_name));
                core.bind('$', 'field_delimiter_clause', get_property('field delimiter clause', core.get_option('field delimiter', options)));
                core.bind('$', 'field_separator', core.get_option('field separator', options));
                core.bind('$', 'filename', get_property('filename', statement));
                core.bind('$', 'heading', core.get_option('heading', options));
                core.bind('$', 'statement', replace(core.solve(statement), get_property('filename with path', statement), get_property('external table name', table_name)));
                core.bind('$', 'table_name', table_name);
                core.bind('$', 'table_owner_name', get_property('table owner name', table_name));
                core.bind('$', 'table_short_name', get_property('table short name', table_name));
                core.plsql(statements);
            when 'file 2 lob' then
                statements(1).code := q'[
                    drop directory $directory_name
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $directory_name as '$directory_path'
                ]';
                statements(3).code := q'[
                    drop table $table_name purge
                ]';
                statements(3).ignore_error := -00942;
                statements(4).code := q'[
                    create table $table_name (
                        $lob_column_name clob
                    )
                    nologging
                    pctfree 0
                    compress
                    parallel
                ]';
                statements(5).code := q'[
                    declare
                        l_bfile bfile;
                        l_clob clob;
                        l_dest_offset integer := 1;
                        l_src_offset integer := 1;
                        l_lang_context integer := 0;
                        l_warning integer := 0;
                    begin
                        insert into $table_name (data) values (empty_clob()) return data into l_clob;
                        l_bfile := bfilename(upper('$directory_name'), '$filename');
                        dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
                        dbms_lob.trim(l_clob, 0);
                        dbms_lob.loadclobfromfile(
                            dest_lob => l_clob,
                            src_bfile => l_bfile,
                            amount => dbms_lob.lobmaxsize,
                            dest_offset => l_dest_offset,
                            src_offset => l_src_offset,
                            bfile_csid => 0,
                            lang_context => l_lang_context,
                            warning => l_warning
                        );
                        dbms_lob.fileclose(l_bfile);
                    end;
                ]';
                statements(6).code := q'[
                    begin
                        dbms_stats.gather_table_stats(
                            ownname => '$table_owner_name',
                            tabname => '$table_short_name',
                            estimate_percent => dbms_stats.auto_sample_size,
                            method_opt => 'for all columns size auto',
                            degree => dbms_stats.auto_degree,
                            granularity => 'all',
                            cascade => true,
                            no_invalidate => dbms_stats.auto_invalidate
                        );
                    end;
                ]';
                statements(7).code := q'[
                    drop directory $directory_name
                ]';
                core.bind('$', 'directory_name', get_property('directory name', table_name));
                core.bind('$', 'directory_path', get_property('directory path', statement));
                core.bind('$', 'filename', get_property('filename', statement));
                core.bind('$', 'lob_column_name', get_property('lob column name', statement));
                core.bind('$', 'table_name', table_name);
                core.bind('$', 'table_owner_name', get_property('table owner name', table_name));
                core.bind('$', 'table_short_name', get_property('table short name', table_name));
                core.plsql(statements);
            else
                raise_application_error(-20000, 'Unsupported type ' || core.get_option('type', options) || '.');
        end case;
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end create_table;

    procedure drop_table(table_name varchar2) is
        statements core.statements_t;
    begin
        internal.log_session_step('start');
        statements(1).code := q'[
            drop table $table_name purge
        ]';
        core.bind('$', 'table_name', table_name);
        core.plsql(statements);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end drop_table;

    procedure control_append(target_table_name varchar2, source_table_name varchar2, options varchar2) is
        statements core.statements_t;
    begin
        internal.log_session_step('start');
        statements(1).code := q'[
            truncate table $target_table_owner_name.$target_table_short_name drop storage
        ]';
        statements(2).code := q'[
            alter table $target_table_owner_name.$target_table_short_name truncate partition $partition_name drop storage
        ]';
        statements(3).code := q'[
            insert /*+ append parallel */ into $target_table_owner_name.$target_table_short_name $partition_clause nologging (
                $source_columns_name
            )
            select
                $source_columns_name
            from $source_table_name
        ]';
        statements(4).code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$target_table_owner_name',
                    tabname => '$target_table_short_name',
                    $analyze_partition_clause
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'all',
                    cascade => false,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        statements(1).execute := core.get_option('truncate table', options, false);
        statements(2).execute := core.get_option('truncate partition', options, false);
        core.bind('$', 'analyze_partition_clause', get_property('analyze partition clause', core.get_option('partition name', options)));
        core.bind('$', 'partition_clause', get_property('partition clause', core.get_option('partition name', options)));
        core.bind('$', 'partition_name', core.get_option('partition name', options));
        core.bind('$', 'source_columns_name', get_columns(source_table_name, separator => ', '));
        core.bind('$', 'source_table_name', source_table_name);
        core.bind('$', 'target_table_owner_name', get_property('table owner name', target_table_name));
        core.bind('$', 'target_table_short_name', get_property('table short name', target_table_name));
        core.plsql(statements);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end control_append;

    procedure incremental_update(target_table_name varchar2, source_table_name varchar2, options varchar2) is
        statements core.statements_t;
    begin
        internal.log_session_step('start');
        statements(1).code := q'[
            drop table $staging_area.$integration_table_01_short_name purge
        ]';
        statements(1).ignore_error := -00942;
        statements(2).code := q'[
            create table $staging_area.$integration_table_01_short_name nologging pctfree 0 compress parallel as
            select
                $integration_01_natural_key,
                nvl((select max($surrogate_key) from $target_table_owner_name.$target_table_short_name), 0) + rownum $surrogate_key
            from $source_table_name s
            left join $target_table_owner_name.$target_table_short_name $partition_clause t on
                $integration_01_join
            where
                t.$surrogate_key is null
        ]';
        statements(3).code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$staging_area',
                    tabname => '$integration_table_01_short_name',
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'all',
                    cascade => false,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        statements(4).code := q'[
            drop table $staging_area.$integration_table_02_short_name purge
        ]';
        statements(4).ignore_error := -00942;
        statements(5).code := q'[
            create table $staging_area.$integration_table_02_short_name nologging pctfree 0 compress parallel as
            select
                $integration_02_columns_expression
            from $staging_area.$integration_table_01_short_name i01
            full join $target_table_owner_name.$target_table_short_name $partition_clause t on
                $integration_02_join
        ]';
        statements(6).code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$staging_area',
                    tabname => '$integration_table_02_short_name',
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'all',
                    cascade => false,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        statements(7).code := q'[
            drop table $staging_area.$integration_table_03_short_name purge
        ]';
        statements(7).ignore_error := -00942;
        statements(8).code := q'[
            create table $staging_area.$integration_table_03_short_name nologging pctfree 0 compress parallel as
            select
                i02.$surrogate_key,
                $integration_03_01_natural_key,
                $integration_03_01_columns_expression
                $integration_03_target_only_columns
            from $staging_area.$integration_table_02_short_name i02
            left join $source_table_name s on
                $integration_03_01_join_01
            left join $target_table_owner_name.$target_table_short_name $partition_clause t on
                $integration_03_01_join_02
        ]';
        statements(9).code := q'[
            create table $staging_area.$integration_table_03_short_name nologging pctfree 0 compress parallel as
            select
                $integration_03_02_columns_expression
                $integration_03_target_only_columns
            from $source_table_name s
            full join $target_table_owner_name.$target_table_short_name $partition_clause t on
                $integration_03_02_join_01
        ]';
        statements(10).code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$staging_area',
                    tabname => '$integration_table_03_short_name',
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'all',
                    cascade => false,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        statements(11).code := q'[
            truncate table $target_table_owner_name.$target_table_short_name drop storage
        ]';
        statements(12).code := q'[
            alter table $target_table_owner_name.$target_table_short_name truncate partition $partition_name drop storage
        ]';
        statements(13).code := q'[
            insert /*+ append parallel */ into $target_table_owner_name.$target_table_short_name $partition_clause nologging (
                $target_columns
            )
            select
                $target_columns
            from $staging_area.$integration_table_03_short_name
        ]';
        statements(14).code := q'[
            merge /*+ append parallel */ into $target_table_owner_name.$target_table_short_name t
                using $staging_area.$integration_table_03_short_name i03 on
                    ($merge_condition)
                $merge_update_clause
                when not matched then
                    insert ($target_columns) values ($merge_insert_columns)
        ]';
        statements(15).code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$target_table_owner_name',
                    tabname => '$target_table_short_name',
                    $analyze_partition_clause
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'all',
                    cascade => true,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        statements(16).code := q'[
            drop table $staging_area.$integration_table_01_short_name purge
        ]';
        statements(17).code := q'[
            drop table $staging_area.$integration_table_02_short_name purge
        ]';
        statements(18).code := q'[
            drop table $staging_area.$integration_table_03_short_name purge
        ]';
        statements(1).execute := core.get_option('surrogate key set ?', options, false);
        statements(2).execute := core.get_option('surrogate key set ?', options, false);
        statements(3).execute := core.get_option('surrogate key set ?', options, false);
        statements(4).execute := core.get_option('surrogate key set ?', options, false);
        statements(5).execute := core.get_option('surrogate key set ?', options, false);
        statements(6).execute := core.get_option('surrogate key set ?', options, false);
        statements(8).execute := core.get_option('surrogate key set ?', options, false);
        statements(9).execute := not core.get_option('surrogate key set ?', options, false);
        statements(11).execute := not core.get_option('partition name set ?', options, false) and core.get_option('method', options, 'full') = 'full';
        statements(12).execute := core.get_option('partition name set ?', options, false) and core.get_option('method', options, 'full') = 'full';
        statements(13).execute := core.get_option('method', options, 'full') = 'full';
        statements(14).execute := core.get_option('method', options, 'full') = 'merge';
        statements(16).execute := core.get_option('surrogate key set ?', options, false);
        statements(17).execute := core.get_option('surrogate key set ?', options, false);
        core.bind('$', 'analyze_partition_clause', get_property('analyze partition clause', core.get_option('partition name', options)));
        core.bind('$', 'integration_01_join', get_columns(source_table_name, pattern => 's.column_name = t.column_name and ', columns_in => core.get_option('natural key', options), remove_last_occurence => ' and '));
        core.bind('$', 'integration_01_natural_key', get_columns(source_table_name, pattern => 's.column_name, ', columns_in => core.get_option('natural key', options), remove_last_occurence => ', '));
        core.bind('$', 'integration_02_columns_expression', get_columns(target_table_name, pattern => 'nvl(i01.column_name, t.column_name) column_name, ', columns_in => core.get_option('natural key', options) || ', ' || core.get_option('surrogate key', options), remove_last_occurence => ', '));
        core.bind('$', 'integration_02_join', get_columns(source_table_name, pattern => 'i01.column_name = t.column_name and ', columns_in => core.get_option('natural key', options), remove_last_occurence => ' and '));
        core.bind('$', 'integration_03_01_columns_expression', get_columns(source_table_name, pattern => 'nvl(s.column_name, t.column_name) column_name, ', columns_not_in => core.get_option('natural key', options), remove_last_occurence => ', '));
        core.bind('$', 'integration_03_01_join_01', get_columns(source_table_name, pattern => 'i02.column_name = s.column_name and ', columns_in => core.get_option('natural key', options), remove_last_occurence => ' and '));
        core.bind('$', 'integration_03_01_join_02', get_columns(source_table_name, pattern => 'i02.column_name = t.column_name and ', columns_in => core.get_option('natural key', options), remove_last_occurence => ' and '));
        core.bind('$', 'integration_03_01_natural_key', get_columns(source_table_name, pattern => 'i02.column_name, ', columns_in => core.get_option('natural key', options), remove_last_occurence => ', '));
        core.bind('$', 'integration_03_02_columns_expression', get_columns(source_table_name, pattern => 'nvl(s.column_name, t.column_name) column_name, ', remove_last_occurence => ', '));
        core.bind('$', 'integration_03_02_join_01', get_columns(source_table_name, pattern => 's.column_name = t.column_name and ', columns_in => core.get_option('natural key', options), remove_last_occurence => ' and '));
        core.bind('$', 'integration_03_target_only_columns', get_columns(target_table_name, pattern => ', t.column_name', columns_not_in => get_columns(source_table_name, separator => ',') || case core.get_option('surrogate key set ?', options, false) when true then ', ' || core.get_option('surrogate key', options) else '' end));
        core.bind('$', 'integration_table_01_short_name', get_property('table short name', get_property('integration table 01 name', target_table_name)));
        core.bind('$', 'integration_table_02_short_name', get_property('table short name', get_property('integration table 02 name', target_table_name)));
        core.bind('$', 'integration_table_03_short_name', get_property('table short name', get_property('integration table 03 name', target_table_name)));
        core.bind('$', 'merge_condition', get_columns(source_table_name, pattern => 't.column_name = i03.column_name and ', columns_in => core.get_option('natural key', options), remove_last_occurence => ' and '));
        core.bind('$', 'merge_insert_columns', get_columns(target_table_name, pattern => 'i03.column_name, ', remove_last_occurence => ', '));
        core.bind('$', 'merge_update_clause', get_columns(source_table_name, start_text => 'when matched then update set ', pattern => 't.column_name = i03.column_name, ', columns_not_in => core.get_option('natural key', options), remove_last_occurence => ' , '));
        core.bind('$', 'partition_clause', get_property('partition clause', core.get_option('partition name', options)));
        core.bind('$', 'partition_name', core.get_option('partition name', options));
        core.bind('$', 'source_table_name', source_table_name);
        core.bind('$', 'staging_area', core.get_option('staging area', options, get_property('table owner name', target_table_name)));
        core.bind('$', 'surrogate_key', core.get_option('surrogate key', options));
        core.bind('$', 'target_columns', get_columns(target_table_name, separator => ', '));
        core.bind('$', 'target_table_owner_name', get_property('table owner name', target_table_name));
        core.bind('$', 'target_table_short_name', get_property('table short name', target_table_name));
        core.plsql(statements);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end incremental_update;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end data_integration;
/