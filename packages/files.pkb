create or replace package body out.files is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure copy(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            cp $files.copy.(options).recursive $files.copy.source_filename $files.copy.target_filename
        ]';
        core.bind('$', 'files.copy.(options).recursive', options);
        core.bind('$', 'files.copy.source_filename', source_filename);
        core.bind('$', 'files.copy.target_filename', target_filename);
        execute.shell(statement);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end copy;

    procedure move(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            mv $files.copy.source_filename $files.copy.target_filename
        ]';
        core.bind('$', 'files.copy.source_filename', source_filename);
        core.bind('$', 'files.copy.target_filename', target_filename);
        execute.shell(statement);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end move;

    procedure remove(filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            rm $files.remove.(options).force $files.remove.(options).recursive $files.remove.filename
        ]';
        core.bind('$', 'files.remove.(options).force', options);
        core.bind('$', 'files.remove.(options).recursive', options);
        core.bind('$', 'files.remove.filename', filename);
        execute.shell(statement);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end remove;

    procedure wait(filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            while [ ! -f $files.wait.filename ]; do sleep $files.wait.(options).polling_interval; done
        ]';
        core.bind('$', 'files.wait.(options).polling_interval', options);
        core.bind('$', 'files.wait.filename', filename);
        execute.shell(statement);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end wait;

    procedure load(table_name varchar2, filename varchar2, attributes varchar2, options varchar2 default null) is
        statements types.statements;
    begin
        logger.session_step('start');
        case core.get_property_value('files.load.(options).file_format', options)
            when 'delimited' then
                statements(1).code := q'[
                    drop directory $files.load.(table_name).directory_name
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $files.load.(table_name).directory_name as '$files.load.(filename).directory_path'
                ]';
                statements(3).code := q'[
                    drop table $files.load.(table_name).external_table_name purge
                ]';
                statements(3).ignore_error := -00942;
                statements(4).code := q'[
                    create table $files.load.(table_name).external_table_name (
                        $files.load.(attributes).external_table_columns_definition
                    )
                    organization external (
                        type oracle_loader
                        default directory $files.load.(table_name).directory_name
                        access parameters (
                            records delimited by '$files.load.(options).record_separator'
                            skip $files.load.(options).heading
                            nobadfile
                            nodiscardfile
                            nologfile
                            fields terminated by '$files.load.(options).field_separator' $files.load.(options).text_delimiter
                            missing field values are null (
                                $files.load.(attributes).field_list_clause
                            )
                        )
                        location ('$files.load.filename')
                    )
                    nomonitoring
                    parallel
                    reject limit 0
                ]';
                statements(5).code := q'[
                    drop table $files.load.table_name purge
                ]';
                statements(5).ignore_error := -00942;
                statements(6).code := q'[
                    create table $files.load.table_name nologging pctfree 0 compress parallel as
                    select
                        $files.load.(attributes).table_columns
                    from $files.load.(table_name).external_table_name
                ]';
                statements(7).code := q'[
                    begin
                        dbms_stats.gather_table_stats(
                            ownname => '$files.load.(table_name).table_owner_name',
                            tabname => '$files.load.(table_name).table_short_name',
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
                    drop table $files.load.(table_name).external_table_name purge
                ]';
                statements(9).code := q'[
                    drop directory $files.load.(table_name).directory_name
                ]';
                core.bind('$', 'files.load.(attributes).external_table_columns_definition', attributes);
                core.bind('$', 'files.load.(attributes).field_list_clause', attributes);
                core.bind('$', 'files.load.(attributes).table_columns', attributes);
                core.bind('$', 'files.load.(filename).directory_path', filename);
                core.bind('$', 'files.load.(options).field_separator', options);
                core.bind('$', 'files.load.(options).heading', options);
                core.bind('$', 'files.load.(options).record_separator', options);
                core.bind('$', 'files.load.(options).text_delimiter', options);
                core.bind('$', 'files.load.(table_name).directory_name', table_name);
                core.bind('$', 'files.load.(table_name).external_table_name', table_name);
                core.bind('$', 'files.load.(table_name).table_owner_name', table_name);
                core.bind('$', 'files.load.(table_name).table_short_name', table_name);
                core.bind('$', 'files.load.filename', filename);
                core.bind('$', 'files.load.table_name', table_name);
            when 'large object' then
                statements(1).code := q'[
                    drop directory $files.load.(table_name).directory_name
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $files.load.(table_name).directory_name as '$files.load.(filename).directory_path'
                ]';
                statements(3).code := q'[
                    drop table $files.load.table_name purge
                ]';
                statements(3).ignore_error := -00942;
                statements(4).code := q'[
                    create table $files.load.table_name (
                        content clob
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
                        insert into $files.load.table_name (content) values (empty_clob()) return content into l_clob;
                        l_bfile := bfilename(upper('$files.load.(table_name).directory_name'), '$files.load.filename');
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
                            ownname => '$files.load.(table_name).table_owner_name',
                            tabname => '$files.load.(table_name).table_short_name',
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
                    drop directory $files.load.(table_name).directory_name
                ]';
                core.bind('$', 'files.load.(filename).directory_path', filename);
                core.bind('$', 'files.load.(table_name).directory_name', table_name);
                core.bind('$', 'files.load.(table_name).table_owner_name', table_name);
                core.bind('$', 'files.load.(table_name).table_short_name', table_name);
                core.bind('$', 'files.load.filename', filename);
                core.bind('$', 'files.load.table_name', table_name);
        end case;
        execute.plsql(statements);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end load;

    procedure unload(filename varchar2, table_name varchar2, options varchar2 default null) is
        statements types.statements;
    begin
        logger.session_step('start');
        case core.get_property_value('files.unload.(options).file_format', options)
            when 'delimited' then
                statements(1).code := q'[
                    drop directory $files.unload.(filename).directory_name
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $files.unload.(filename).directory_name as '$files.unload.(filename).directory_path'
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
                        execute immediate 'alter session set nls_date_format = ''$files.unload.(options).date_format''';
                        table_cursor := dbms_sql.open_cursor;
                        dbms_sql.parse(table_cursor, 'select * from $files.unload.table_name', dbms_sql.native);
                        dbms_sql.describe_columns2(table_cursor, column_count, columns_description);
                        for i in 1 .. column_count loop
                            dbms_sql.define_column(table_cursor, i, buffer, 32767);
                        end loop;
                        row_count := dbms_sql.execute(table_cursor);
                        file := utl_file.fopen(upper('$files.unload.(filename).directory_name'), '$files.unload.filename', 'w', 32767);
                        if $files.unload.(options).generate_header then
                            for i in 1 .. column_count loop
                                if i > 1 then
                                    utl_file.put(file, '$files.unload.(options).field_separator');
                                end if;
                                utl_file.put(file, columns_description(i).col_name);
                            end loop;
                            utl_file.putf(file, '$files.unload.(options).record_separator');
                        end if;
                        loop
                            exit when dbms_sql.fetch_rows(table_cursor) = 0;
                            for i in 1 .. column_count loop
                                if i > 1 then
                                    utl_file.put(file, '$files.unload.(options).field_separator');
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
                                    utl_file.put(file, '$files.unload.(options).text_delimiter' || buffer || '$files.unload.(options).text_delimiter');
                                else
                                    utl_file.put(file, buffer);
                                end if;
                            end loop;
                            utl_file.putf(file, '$files.unload.(options).record_separator');
                        end loop;
                        utl_file.fclose(file);
                        dbms_sql.close_cursor(table_cursor);
                        execute immediate 'alter session set nls_date_format = ''$files.unload.{nls_date_format}''';
                    exception
                        when others then
                            if utl_file.is_open(file) then
                                utl_file.fclose(file);
                            end if;
                            if dbms_sql.is_open(table_cursor) then
                                dbms_sql.close_cursor(table_cursor);
                            end if;
                            execute immediate 'alter session set nls_date_format = ''$files.unload.{nls_date_format}''';
                            raise;
                    end;
                ]';
                statements(4).code := q'[
                    drop directory $files.unload.(filename).directory_name
                ]';
                core.bind('$', 'files.unload.(filename).directory_name', filename);
                core.bind('$', 'files.unload.(filename).directory_path', filename);
                core.bind('$', 'files.unload.(options).date_format', options);
                core.bind('$', 'files.unload.(options).field_separator', options);
                core.bind('$', 'files.unload.(options).generate_header', options);
                core.bind('$', 'files.unload.(options).record_separator', options);
                core.bind('$', 'files.unload.(options).text_delimiter', options);
                core.bind('$', 'files.unload.{nls_date_format}', sys_context('userenv', 'nls_date_format'));
                core.bind('$', 'files.unload.filename', filename);
                core.bind('$', 'files.unload.table_name', table_name);
        end case;
        execute.plsql(statements);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end unload;

    procedure zip(archive_name varchar2, filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            zip $files.zip.(options).keep_input_files $files.zip.(options).compress_level $files.zip.(options).password $files.zip.(options).recursive $files.zip.archive_name $files.zip.filename
        ]';
        core.bind('$', 'files.zip.(options).compress_level', options);
        core.bind('$', 'files.zip.(options).keep_input_files', options);
        core.bind('$', 'files.zip.(options).password', options);
        core.bind('$', 'files.zip.(options).recursive', options);
        core.bind('$', 'files.zip.archive_name', archive_name);
        core.bind('$', 'files.zip.filename', filename);
        execute.shell(statement);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end zip;

    procedure unzip(directory_name varchar2, archive_name varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            unzip -o $files.unzip.(options).password $files.unzip.archive_name -d $files.unzip.directory_name && if [ "false" == "$files.unzip.(options).keep_input_files" ]; then rm $files.unzip.archive_name; fi
        ]';
        core.bind('$', 'files.unzip.(options).keep_input_files', options);
        core.bind('$', 'files.unzip.(options).password', options);
        core.bind('$', 'files.unzip.archive_name', archive_name);
        core.bind('$', 'files.unzip.directory_name', directory_name);
        execute.shell(statement);
        core.unbind('$');
        logger.session_step('done');
    exception
        when others then
            core.unbind('$');
            logger.session_step('error', sqlerrm);
    end unzip;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end files;
/