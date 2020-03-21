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
        core.set('files.copy.(options).recursive', options);
        core.set('files.copy.source_filename', source_filename);
        core.set('files.copy.target_filename', target_filename);
        execute.shell(statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end copy;

    procedure move(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            mv $files.move.source_filename $files.move.target_filename
        ]';
        core.set('files.move.source_filename', source_filename);
        core.set('files.move.target_filename', target_filename);
        execute.shell(statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end move;

    procedure remove(filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            rm $files.remove.(options).force $files.remove.(options).recursive $files.remove.filename
        ]';
        core.set('files.remove.(options).force', options);
        core.set('files.remove.(options).recursive', options);
        core.set('files.remove.filename', filename);
        execute.shell(statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end remove;

    procedure wait(filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            while [ ! -f $files.wait.filename ]; do sleep $files.wait.(options).polling_interval; done
        ]';
        core.set('files.wait.(options).polling_interval', options);
        core.set('files.wait.filename', filename);
        execute.shell(statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end wait;

    procedure load(work_table_name varchar2, filename varchar2, attributes varchar2, options varchar2 default null) is
        statements types.statements;
    begin
        logger.session_step('start');
        core.set('files.load.(options).file_format', options);
        case core.get('files.load.(options).file_format')
            when 'delimited' then
                statements(1).code := q'[
                    drop directory $files.load.{directory_name}
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $files.load.{directory_name} as '$files.load.{directory_path}'
                ]';
                statements(3).code := q'[
                    drop table $files.load.{external_table_name}
                ]';
                statements(3).ignore_error := -00942;
                statements(4).code := q'[
                    create table $files.load.{external_table_name} (
                        $files.load.{external_table_columns}
                    )
                    organization external (
                        type oracle_loader
                        default directory $files.load.{directory_name}
                        access parameters (
                            records delimited by '$files.load.(options).record_separator'
                            skip $files.load.(options).heading
                            nobadfile
                            nodiscardfile
                            nologfile
                            fields terminated by '$files.load.(options).field_separator' $files.load.(options).text_delimiter
                            missing field values are null (
                                $files.load.{external_table_field_list_clause}
                            )
                        )
                        location ('$files.load.filename')
                    )
                    nomonitoring
                    parallel
                    reject limit 0
                ]';
                statements(5).code := q'[
                    truncate table $files.load.work_table_name drop all storage
                ]';
                statements(5).ignore_error := -00942;
                statements(6).code := q'[
                    drop table $files.load.work_table_name purge
                ]';
                statements(6).ignore_error := -00942;
                statements(7).code := q'[
                    create global temporary table $files.load.work_table_name on commit preserve rows parallel as
                    select
                        $files.load.{work_table_columns}
                    from $files.load.{external_table_name}
                ]';
                statements(8).code := q'[
                    drop table $files.load.{external_table_name} purge
                ]';
                statements(9).code := q'[
                    drop directory $files.load.{directory_name}
                ]';
                core.set('files.load.{directory_name}', work_table_name);
                core.set('files.load.{directory_path}', filename);
                core.set('files.load.{external_table_name}', work_table_name);
                core.set('files.load.{external_table_columns}', attributes);
                core.set('files.load.{external_table_field_list_clause}', attributes);
                core.set('files.load.{work_table_columns}', attributes);
                core.set('files.load.(options).field_separator', options);
                core.set('files.load.(options).heading', options);
                core.set('files.load.(options).record_separator', options);
                core.set('files.load.(options).text_delimiter', options);
                core.set('files.load.filename', filename);
                core.set('files.load.work_table_name', work_table_name);
            when 'large object' then
                statements(1).code := q'[
                    drop directory $files.load.{directory_name}
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $files.load.{directory_name} as '$files.load.{directory_path}'
                ]';
                statements(3).code := q'[
                    drop table $files.load.table_name purge
                ]';
                statements(3).ignore_error := -00942;
                statements(4).code := q'[
                    create global temporary table $files.load.work_table_name (
                        content clob
                    )
                    on commit preserve rows
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
                        insert into $files.load.work_table_name (content) values (empty_clob()) return content into l_clob;
                        l_bfile := bfilename(upper('$files.load.{directory_name}'), '$files.load.filename');
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
                    exception
                        when others then
                            dbms_lob.fileclose(l_bfile);
                            raise;
                    end;
                ]';
                statements(6).code := q'[
                    drop directory $files.load.{directory_name}
                ]';
                core.set('files.load.{directory_name}', work_table_name);
                core.set('files.load.{directory_path}', filename);
                core.set('files.load.filename', filename);
                core.set('files.load.table_name', work_table_name);
        end case;
        execute.plsql(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end load;

    procedure unload(filename varchar2, table_name varchar2, options varchar2 default null) is
        statements types.statements;
    begin
        logger.session_step('start');
        core.set('files.unload.(options).file_format', options);
        case core.get('files.unload.(options).file_format')
            when 'delimited' then
                statements(1).code := q'[
                    drop directory $files.unload.{directory_name}
                ]';
                statements(1).ignore_error := -04043;
                statements(2).code := q'[
                    create directory $files.unload.{directory_name} as '$files.unload.{directory_path}'
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
                        file := utl_file.fopen(upper('$files.unload.{directory_name}'), '$files.unload.filename', 'w', 32767);
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
                    drop directory $files.unload.{directory_name}
                ]';
                core.set('files.unload.{directory_name}', filename);
                core.set('files.unload.{directory_path}', filename);
                core.set('files.unload.{nls_date_format}');
                core.set('files.unload.(options).date_format', options);
                core.set('files.unload.(options).field_separator', options);
                core.set('files.unload.(options).generate_header', options);
                core.set('files.unload.(options).record_separator', options);
                core.set('files.unload.(options).text_delimiter', options);
                core.set('files.unload.filename', filename);
                core.set('files.unload.table_name', table_name);
        end case;
        execute.plsql(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end unload;

    procedure zip(archive_name varchar2, filename varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            zip $files.zip.(options).keep_input_files $files.zip.(options).compress_level $files.zip.(options).password $files.zip.(options).recursive $files.zip.archive_name $files.zip.filename
        ]';
        core.set('files.zip.(options).compress_level', options);
        core.set('files.zip.(options).keep_input_files', options);
        core.set('files.zip.(options).password', options);
        core.set('files.zip.(options).recursive', options);
        core.set('files.zip.archive_name', archive_name);
        core.set('files.zip.filename', filename);
        execute.shell(statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end zip;

    procedure unzip(directory_name varchar2, archive_name varchar2, options varchar2 default null) is
        statement types.statement;
    begin
        logger.session_step('start');
        statement.code := q'[
            unzip -o $files.unzip.(options).password $files.unzip.archive_name -d $files.unzip.directory_name && if [ "false" == "$files.unzip.(options).keep_input_files" ]; then rm $files.unzip.archive_name; fi
        ]';
        core.set('files.unzip.(options).keep_input_files', options);
        core.set('files.unzip.(options).password', options);
        core.set('files.unzip.archive_name', archive_name);
        core.set('files.unzip.directory_name', directory_name);
        execute.shell(statement);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end unzip;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end files;
/