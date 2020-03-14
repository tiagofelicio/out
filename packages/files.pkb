create or replace package body out.files is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function get_property(property_name varchar2, text varchar2) return varchar2 is
        property_value core.string_t;
        solved_text core.string_t;
        force_flag core.string_t;
        keep_flag core.string_t;
        level_flag core.string_t;
        password_flag core.string_t;
        recursive_flag core.string_t;
    begin
        solved_text := core.solve(text);
        case lower(property_name)
            when 'force flag' then
                force_flag := case when lower(solved_text) = 'true' then '-f' end;
                property_value := force_flag;
            when 'keep flag' then
                keep_flag := case when lower(solved_text) = 'false' then '-m' end;
                property_value := keep_flag;
            when 'level flag' then
                level_flag := case when solved_text is not null then '-' || solved_text end;
                property_value := level_flag;
            when 'password flag' then
                password_flag := case when solved_text is not null then '-P ' || solved_text end;
                property_value := password_flag;
            when 'recursive flag' then
                recursive_flag := case when lower(solved_text) = 'true' then '-r' end;
                property_value := recursive_flag;
            else
                raise_application_error(-20000, 'Invalid property name (' || property_name || ').');
        end case;
        return property_value;
    end get_property;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure copy(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            cp $recursive_flag $source_filename $target_filename
        ]';
        core.bind('$', 'source_filename', source_filename);
        core.bind('$', 'target_filename', target_filename);
        core.bind('$', 'recursive_flag', get_property('recursive flag', core.get_option('recursive', options)));
        output := core.shell(command);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end copy;

    procedure move(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            mv $source_filename $target_filename
        ]';
        core.bind('$', 'source_filename', source_filename);
        core.bind('$', 'target_filename', target_filename);
        output := core.shell(command);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end move;

    procedure remove(filename varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            rm $force_flag $recursive_flag $filename
        ]';
        core.bind('$', 'filename', filename);
        core.bind('$', 'force_flag', get_property('force flag', core.get_option('force', options)));
        core.bind('$', 'recursive_flag', get_property('recursive flag', core.get_option('recursive', options)));
        output := core.shell(command);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end remove;

    procedure wait(filename varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            while [ ! -f $filename ]; do sleep $polling_interval; done
        ]';
        core.bind('$', 'filename', filename);
        core.bind('$', 'polling_interval', core.get_option('polling interval', options, '60'));
        output := core.shell(command);
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

    procedure unload(filename varchar2, table_name varchar2, options varchar2) is
        buffer varchar2(32767);
        file utl_file.file_type;
        table_description dbms_sql.desc_tab2;
        table_column_count pls_integer;
        table_cursor pls_integer;
        table_rows_count pls_integer;
        -- heading numero de linhas 
        -- record separator \r\n ou \n
        -- field separator pipe
        -- text delimiter "
        -- decimal delimiter . ou ,
        field_separator core.string_t := '|';
        l_is_str BOOLEAN;
    begin
        table_cursor := dbms_sql.open_cursor;
        dbms_sql.parse(table_cursor, 'select * from ' || table_name, dbms_sql.native);
        dbms_sql.describe_columns2(table_cursor, table_column_count, table_description);
        for i in 1 .. table_column_count loop
            dbms_sql.define_column(table_cursor, i, buffer, 32767);
        end loop;
        table_rows_count := dbms_sql.execute(table_cursor);
        file := utl_file.fopen('O_DH_COUNTRY_CODES_01', filename, 'w', 32767);
        -- output header
        for i in 1 .. table_column_count loop
            if i > 1 then
                utl_file.put(file, field_separator);
            end if;
            utl_file.put(file, table_description(i).col_name);
        end loop;
        utl_file.new_line(file);
        -- output data
        loop
            exit when dbms_sql.fetch_rows(table_cursor) = 0;
            for i in 1 .. table_column_count loop
                if i > 1 then
                    utl_file.put(file, field_separator);
                end if;
                -- Check if this is a string column.
                l_is_str := false;
                if table_description(i).col_type in (
                    dbms_types.typecode_char,
                    dbms_types.typecode_clob,
                    dbms_types.typecode_nchar,
                    dbms_types.typecode_nclob,
                    dbms_types.typecode_nvarchar2,
                    dbms_types.typecode_varchar,
                    dbms_types.typecode_varchar2
                ) then
                    l_is_str := true;
                end if;
                dbms_sql.column_value(table_cursor, i, buffer);
                -- optionally add quotes for strings
                if true and l_is_str then
                    utl_file.put(file, '"');
                    utl_file.put(file, buffer);
                    utl_file.put(file, '"');
                else
                    utl_file.put(file, buffer);
                end if;
            end loop;
            utl_file.new_line(file);
        end loop;
        utl_file.fclose(file);
        dbms_sql.close_cursor(table_cursor);
    exception
        when others then
            if utl_file.is_open(file) then
                utl_file.fclose(file);
            end if;
            if dbms_sql.is_open(table_cursor) then
                dbms_sql.close_cursor(table_cursor);
            end if;
            raise;
    end unload;

    procedure zip(archive_name varchar2, filename varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            zip $keep_flag $level_flag $password_flag $recursive_flag $archive_name $filename
        ]';
        core.bind('$', 'archive_name', archive_name);
        core.bind('$', 'filename', filename);
        core.bind('$', 'keep_flag', get_property('keep flag', core.get_option('keep', options, 'false')));
        core.bind('$', 'level_flag', get_property('level flag', core.get_option('level', options)));
        core.bind('$', 'password_flag', get_property('password flag', core.get_option('password', options)));
        core.bind('$', 'recursive_flag', get_property('recursive flag', core.get_option('recursive', options)));
        output := core.shell(command);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end zip;

    procedure unzip(directory_name varchar2, archive_name varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            unzip -o $password_flag $archive_name -d $directory_name && if [ "false" == "$keep" ]; then rm $archive_name; fi
        ]';
        core.bind('$', 'archive_name', archive_name);
        core.bind('$', 'directory_name', directory_name);
        core.bind('$', 'keep', core.get_option('keep', options, 'false'));
        core.bind('$', 'password_flag', get_property('password flag', core.get_option('password', options)));
        output := core.shell(command);
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