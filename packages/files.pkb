create or replace package body out.files is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function get_property(property_name varchar2, text varchar2) return varchar2 is
        property_value core.text_t;
        solved_text core.text_t;
        directory_name all_directories.directory_name%type;
        directory_path all_directories.directory_path%type;
        filename core.text_t;
        force core.text_t;
        keep_input_files core.text_t;
        compress_level core.text_t;
        password core.text_t;
        recursive core.text_t;
    begin
        solved_text := core.solve(text);
        case lower(property_name)
            when 'compress level' then
                compress_level := case when solved_text is not null then '-' || solved_text end;
                property_value := compress_level;
            when 'directory name' then
                directory_name := substr('o_' || regexp_substr(regexp_substr(solved_text, '[^/]+$'), '[^\.]+', 1, 1), 1, 128);
                property_value := directory_name;
            when 'directory path' then
                filename := regexp_substr(solved_text, '[^/]+$');
                directory_path := replace(solved_text, '/' || filename);
                property_value := directory_path;
            when 'filename' then
                filename := regexp_substr(solved_text, '[^/]+$');
                property_value := filename;
            when 'force' then
                force := case when lower(solved_text) = 'true' then '-f' end;
                property_value := force;
            when 'keep input files' then
                keep_input_files := case when lower(solved_text) = 'false' then '-m' end;
                property_value := keep_input_files;
            when 'password' then
                password := case when solved_text is not null then '-P ' || solved_text end;
                property_value := password;
            when 'recursive' then
                recursive := case when lower(solved_text) = 'true' then '-r' end;
                property_value := recursive;
            else
                raise_application_error(-20000, 'Invalid property name (' || property_name || ').');
        end case;
        return property_value;
    end get_property;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure copy(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            cp $recursive $source_filename $target_filename
        ]';
        core.bind('$', 'recursive', get_property('recursive', core.get_option('recursive', options)));
        core.bind('$', 'source_filename', source_filename);
        core.bind('$', 'target_filename', target_filename);
        output := core.shell(statement);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end copy;

    procedure move(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            mv $source_filename $target_filename
        ]';
        core.bind('$', 'source_filename', source_filename);
        core.bind('$', 'target_filename', target_filename);
        output := core.shell(statement);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end move;

    procedure remove(filename varchar2, options varchar2 default null) is
        statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            rm $force $recursive $filename
        ]';
        core.bind('$', 'filename', filename);
        core.bind('$', 'force', get_property('force', core.get_option('force', options)));
        core.bind('$', 'recursive', get_property('recursive', core.get_option('recursive', options)));
        output := core.shell(statement);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end remove;

    procedure wait(filename varchar2, options varchar2 default null) is
        statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            while [ ! -f $filename ]; do sleep $polling_interval; done
        ]';
        core.bind('$', 'filename', filename);
        core.bind('$', 'polling_interval', core.get_option('polling interval', options, '60'));
        output := core.shell(statement);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end wait;

    procedure load(table_name varchar2, filename varchar2, attributes varchar2, options varchar2) is
    begin
        null;
    end load;

    procedure unload(filename varchar2, table_name varchar2, options varchar2 default null) is
        statements core.statements_t;
    begin
        case core.get_option('file format', options, 'delimited')
            when 'delimited' then
                statements(1).code := q'[
                    drop directory $directory_name
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $directory_name as '$directory_path'
                ]';
                statements(3).code := q'[
                    declare
                        buffer varchar2(32767);
                        column_count pls_integer;
                        columns_description dbms_sql.desc_tab2;
                        file utl_file.file_type;
                        table_cursor pls_integer;
                        row_count pls_integer;
                    begin
                        execute immediate 'alter session set nls_date_format = ''$date_format''';
                        table_cursor := dbms_sql.open_cursor;
                        dbms_sql.parse(table_cursor, 'select * from $table_name', dbms_sql.native);
                        dbms_sql.describe_columns2(table_cursor, column_count, columns_description);
                        for i in 1 .. column_count loop
                            dbms_sql.define_column(table_cursor, i, buffer, 32767);
                        end loop;
                        row_count := dbms_sql.execute(table_cursor);
                        file := utl_file.fopen(upper('$directory_name'), '$filename', 'w', 32767);
                        if $generate_header then
                            for i in 1 .. column_count loop
                                if i > 1 then
                                    utl_file.put(file, '$field_separator');
                                end if;
                                utl_file.put(file, columns_description(i).col_name);
                            end loop;
                            utl_file.putf(file, '$record_separator');
                        end if;
                        loop
                            exit when dbms_sql.fetch_rows(table_cursor) = 0;
                            for i in 1 .. column_count loop
                                if i > 1 then
                                    utl_file.put(file, '$field_separator');
                                end if;
                                dbms_sql.column_value(table_cursor, i, buffer);
                                if columns_description(i).col_type in (
                                    dbms_types.typecode_char,
                                    dbms_types.typecode_clob,
                                    dbms_types.typecode_nchar,
                                    dbms_types.typecode_nclob,
                                    dbms_types.typecode_nvarchar2,
                                    dbms_types.typecode_varchar,
                                    dbms_types.typecode_varchar2
                                ) then
                                    utl_file.put(file, '$text_delimiter' || buffer || '$text_delimiter');
                                else
                                    utl_file.put(file, buffer);
                                end if;
                            end loop;
                            utl_file.putf(file, '$record_separator');
                        end loop;
                        utl_file.fclose(file);
                        dbms_sql.close_cursor(table_cursor);
                        execute immediate 'alter session set nls_date_format = ''$user_date_format''';
                    exception
                        when others then
                            if utl_file.is_open(file) then
                                utl_file.fclose(file);
                            end if;
                            if dbms_sql.is_open(table_cursor) then
                                dbms_sql.close_cursor(table_cursor);
                            end if;
                            execute immediate 'alter session set nls_date_format = ''$user_date_format''';
                            raise;
                    end;
                ]';
                statements(4).code := q'[
                    drop directory $directory_name
                ]';
            else
                raise_application_error(-20000, 'Unsupported ' || core.get_option('file format', options) || ' file format.');
        end case;
        core.bind('$', 'date_format', core.get_option('date format', options, 'yyyy/mm/dd hh24:mi:ss'));
        core.bind('$', 'directory_name', get_property('directory name', filename));
        core.bind('$', 'directory_path', get_property('directory path', filename));
        core.bind('$', 'field_separator', core.get_option('field separator', options, ','));
        core.bind('$', 'filename', get_property('filename', filename));
        core.bind('$', 'generate_header', core.get_option('generate header', options, 'true'));
        core.bind('$', 'record_separator', core.get_option('record separator', options, '\n'));
        core.bind('$', 'table_name', table_name);
        core.bind('$', 'text_delimiter', core.get_option('text delimiter', options));
        core.bind('$', 'user_date_format', sys_context('userenv', 'nls_date_format'));
        core.plsql(statements);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end unload;

    procedure zip(archive_name varchar2, filename varchar2, options varchar2 default null) is
        statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            zip $keep_input_files $compress_level $password $recursive $archive_name $filename
        ]';
        core.bind('$', 'archive_name', archive_name);
        core.bind('$', 'compress_level', get_property('compress level', core.get_option('compress level', options)));
        core.bind('$', 'filename', filename);
        core.bind('$', 'keep_input_files', get_property('keep input files', core.get_option('keep input files', options, 'false')));
        core.bind('$', 'password', get_property('password', core.get_option('password', options)));
        core.bind('$', 'recursive', get_property('recursive', core.get_option('recursive', options)));
        output := core.shell(statement);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end zip;

    procedure unzip(directory_name varchar2, archive_name varchar2, options varchar2 default null) is
        statement core.statement_t;
        output core.text_t;
    begin
        internal.log_session_step('start');
        statement.code := q'[
            unzip -o $password $archive_name -d $directory_name && if [ "false" == "$keep_input_files" ]; then rm $archive_name; fi
        ]';
        core.bind('$', 'archive_name', archive_name);
        core.bind('$', 'directory_name', directory_name);
        core.bind('$', 'keep_', core.get_option('keep input files', options, 'false'));
        core.bind('$', 'password', get_property('password', core.get_option('password', options)));
        output := core.shell(statement);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end unzip;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end files;
/