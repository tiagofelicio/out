create or replace package body out.core is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    internal_binds types.binds;
    user_binds types.binds;

    function get_option_value(option_name varchar2, options varchar2, option_default_value varchar2 default null) return varchar2 is
        option_value types.text;
    begin
        option_value := lower(trim(regexp_substr(options, replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1)));
        return nvl(option_value, option_default_value);
    end get_option_value;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure bind(variable_type varchar2, variable_name varchar2, variable_value varchar2) is
    begin
        case variable_type
            when '$' then
                internal_binds('$' || variable_name) := get_property_value(variable_name, variable_value);
            when '#' then
                user_binds('#' || variable_name) := variable_value;
            else
                raise_application_error(-20000, 'Unsupported varible type ' || variable_type || ';');
        end case;
    end bind;

    procedure unbind(variable_type varchar2, variable_name varchar2 default null) is
    begin
        case variable_type
            when '$' then
                if variable_name is null then
                    internal_binds.delete;
                else
                    internal_binds.delete('$' || variable_name);
                end if;
            when '#' then
                if variable_name is null then
                    user_binds.delete;
                else
                    user_binds.delete('#' || variable_name);
                end if;
            else
                raise_application_error(-20000, 'Unsupported varible type ' || variable_type || ';');
        end case;
    end unbind;

    procedure dump is
        internal_variable_name types.text;
        internal_variable_value types.text;
        user_variable_name types.text;
        user_variable_value types.text;
    begin
        dbms_output.put_line('=============================== out ===============================');
        dbms_output.put_line('');
        dbms_output.put_line('debug is ' || case when debug then 'on' else 'off' end || '.');
        dbms_output.put_line('');
        dbms_output.put_line('============================== binds ==============================');
        dbms_output.put_line('');
        internal_variable_name := internal_binds.first;
        while internal_variable_name is not null loop
            internal_variable_value := internal_binds(internal_variable_name);
            dbms_output.put_line('    ' || internal_variable_name || ' : ' || internal_variable_value);
            internal_variable_name := internal_binds.next(internal_variable_name);
        end loop;
        user_variable_name := user_binds.first;
        while user_variable_name is not null loop
            user_variable_value := user_binds(user_variable_name);
            dbms_output.put_line('    ' || user_variable_name || ' : ' || user_variable_value);
            user_variable_name := user_binds.next(user_variable_name);
        end loop;
        dbms_output.put_line('');
        dbms_output.put_line('===================================================================');
    end dump;

    function get_option(option_name varchar2, options varchar2, default_value varchar2 default null) return varchar2 is
        option_value types.text;
    begin
        --option_value := lower(regexp_substr(solve(options), replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1));
        option_value := lower(regexp_substr(options, replace(option_name, ' ', '[[:space:]]+') || '[[:space:]]+=>[[:space:]]+(.+)', 1, 1, 'mix', 1));
        return nvl(option_value, default_value);
    end get_option;

    function get_option(option_name varchar2, options varchar2, defaul_value boolean) return boolean is
        option_value types.text;
        option_boolean_value boolean;
    begin
        option_value := get_option(replace(option_name, ' set ?'), options);
        if option_value is null or length(option_value) = 0 then
            option_boolean_value := defaul_value;
        elsif option_name like '% set ?' then
            option_boolean_value := true;
        else
            case option_value
                when 'false' then
                    option_boolean_value := false;
                when 'true' then
                    option_boolean_value := true;
                else
                    raise_application_error(-20000, 'Invalid boolean option value (' || option_value || ').');
            end case;
        end if;
        return option_boolean_value;
    end get_option;

    function get_property_value(property_name varchar2, argument varchar2) return varchar2 is
        option_value types.text;
        property_value types.text;
        solved_argument types.text;
    begin
        solved_argument := core.solve(argument);
        case property_name
            when 'files.copy.(options).recursive' then
                option_value := get_option_value('recursive', solved_argument, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-R';
                end case;
            when 'files.copy.source_filename' then
                property_value := solved_argument;
            when 'files.copy.target_filename' then
                property_value := solved_argument;
            when 'files.load.(attributes).external_table_columns_definition' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(solved_argument || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(solved_argument, line_start, line_end - 1));
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
            when 'files.load.(attributes).field_list_clause' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(solved_argument || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(solved_argument, line_start, line_end - 1));
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
            when 'files.load.(attributes).table_columns' then
                declare
                    line_start number;
                    line_end number;
                    line types.text;
                begin
                    line_start := 1;
                    property_value := '';
                    loop
                        line_end := nvl(instr(substr(solved_argument || chr(10), line_start), chr(10)), 0);
                        exit when line_end = 0;
                        line := trim(substr(solved_argument, line_start, line_end - 1));
                        line := trim(substr(line, 1, instr(line, ' ') - 1));
                        if  length(line) > 0 then
                            property_value := property_value || chr(10) || lpad(' ', 24) || line || ',';
                        end if;
                        line_start := line_start + line_end + 1;
                    end loop;
                    property_value := trim(rtrim(ltrim(property_value, chr(10)), ','));
                end;
            when 'files.load.(filename).directory_path' then
                property_value := replace(solved_argument, '/' || regexp_substr(solved_argument, '[^/]+$'));
            when 'files.load.(options).field_separator' then
                option_value := get_option_value('field separator', solved_argument, ',');
                property_value := option_value;
            when 'files.load.(options).file_format' then
                option_value := get_option_value('file format', solved_argument, 'delimited');
                case option_value
                    when 'delimited' then
                        property_value := 'delimited';
                    when 'large object' then
                        property_value := 'large object';
                end case;
            when 'files.load.(options).heading' then
                option_value := get_option_value('heading', solved_argument, '1');
                property_value := option_value;
            when 'files.load.(options).record_separator' then
                option_value := get_option_value('record separator', solved_argument, '\n');
                property_value := option_value;
            when 'files.load.(options).text_delimiter' then
                option_value := get_option_value('text delimiter', solved_argument);
                case
                    when option_value is not null then
                        property_value := 'optionally enclosed by ''' || option_value || '''';
                    else
                        property_value := '';
                end case;
            when 'files.load.(table_name).directory_name' then
                property_value := substr('o_' || regexp_substr(solved_argument, '[^\.]+', 1, 2), 1, 128);
            when 'files.load.(table_name).external_table_name' then
                property_value := regexp_substr(solved_argument, '[^\.]+', 1, 1) || '.' || substr('o_' || regexp_substr(solved_argument, '[^\.]+', 1, 2), 1, 128);
            when 'files.load.(table_name).table_owner_name' then
                property_value := regexp_substr(solved_argument, '[^\.]+', 1, 1);
            when 'files.load.(table_name).table_short_name' then
                property_value := regexp_substr(solved_argument, '[^\.]+', 1, 2);
            when 'files.load.filename' then
                property_value := regexp_substr(solved_argument, '[^/]+$');
            when 'files.load.table_name' then
                property_value := solved_argument;
            when 'files.move.source_filename' then
                property_value := solved_argument;
            when 'files.move.target_filename' then
                property_value := solved_argument;
            when 'files.remove.filename' then
                property_value := solved_argument;
            when 'files.remove.(options).force' then
                option_value := get_option_value('force', solved_argument, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-f';
                end case;
            when 'files.remove.(options).recursive' then
                option_value := get_option_value('recursive', solved_argument, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-r';
                end case;
            when 'files.unload.(filename).directory_name' then
                property_value := substr('o_' || regexp_substr(regexp_substr(solved_argument, '[^/]+$'), '[^\.]+', 1, 1), 1, 128);
            when 'files.unload.(filename).directory_path' then
                property_value := replace(solved_argument, '/' || regexp_substr(solved_argument, '[^/]+$'));
            when 'files.unload.(options).date_format' then
                option_value := get_option_value('date format', solved_argument, 'yyyy/mm/dd hh24:mi:ss');
                property_value := option_value;
            when 'files.unload.(options).field_separator' then
                option_value := get_option_value('field separator', solved_argument, ',');
                property_value := option_value;
            when 'files.unload.(options).file_format' then
                option_value := get_option_value('file format', solved_argument, 'delimited');
                case option_value
                    when 'delimited' then
                        property_value := 'delimited';
                end case;
            when 'files.unload.(options).generate_header' then
                option_value := get_option_value('generate header', solved_argument, 'true');
                case option_value
                    when 'false' then
                        property_value := 'false';
                    when 'true' then
                        property_value := 'true';
                end case;
            when 'files.unload.(options).record_separator' then
                option_value := get_option_value('record separator', solved_argument, '\n');
                property_value := option_value;
            when 'files.unload.(options).text_delimiter' then
                option_value := get_option_value('text delimiter', solved_argument);
                property_value := option_value;
            when 'files.unload.{nls_date_format}' then
                property_value := solved_argument;
            when 'files.unload.filename' then
                property_value := solved_argument;
            when 'files.unload.table_name' then
                property_value := solved_argument;
            when 'files.unzip.archive_name' then
                property_value := solved_argument;
            when 'files.unzip.directory_name' then
                property_value := solved_argument;
            when 'files.unzip.(options).keep_input_files' then
                option_value := get_option_value('keep input files', solved_argument, 'false');
                property_value := option_value;
            when 'files.unzip.(options).password' then
                option_value := get_option_value('password', solved_argument);
                case
                    when option_value is not null then
                        property_value := '-P ' || option_value;
                    else
                        property_value := '';
                end case;
            when 'files.wait.filename' then
                property_value := solved_argument;
            when 'files.wait.(options).polling_interval' then
                option_value := get_option_value('polling interval', solved_argument, '60');
                property_value := option_value;
            when 'files.zip.archive_name' then
                property_value := solved_argument;
            when 'files.zip.filename' then
                property_value := solved_argument;
            when 'files.zip.(options).compress_level' then
                option_value := get_option_value('compress level', solved_argument, '6');
                property_value := '-' || option_value;
            when 'files.zip.(options).keep_input_files' then
                option_value := get_option_value('keep input files', solved_argument, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-m';
                end case;
            when 'files.zip.(options).password' then
                option_value := get_option_value('password', solved_argument);
                case
                    when option_value is not null then
                        property_value := '-P ' || option_value;
                    else
                        property_value := '';
                end case;
            when 'files.zip.(options).recursive' then
                option_value := get_option_value('recursive', solved_argument, 'false');
                case option_value
                    when 'false' then
                        property_value := '';
                    when 'true' then
                        property_value := '-r';
                end case;
        end case;
        return property_value;
    exception
        when others then
            if sqlcode = -06592 then
                raise_application_error(-20000, 'Unexpected value "' || option_value || '" for option ' || property_name || ';');
            else
                raise;
            end if;
    end get_property_value;

    function solve(text varchar2) return varchar2 is
        solved_text types.text;
        internal_variable_name types.text;
        internal_variable_value types.text;
        user_variable_name types.text;
        user_variable_value types.text;
    begin
        solved_text := text;
        internal_variable_name := internal_binds.first;
        while internal_variable_name is not null loop
            internal_variable_value := internal_binds(internal_variable_name);
            solved_text := replace(solved_text, internal_variable_name, internal_variable_value);
            internal_variable_name := internal_binds.next(internal_variable_name);
        end loop;
        user_variable_name := user_binds.first;
        while user_variable_name is not null loop
            user_variable_value := user_binds(user_variable_name);
            solved_text := replace(solved_text, user_variable_name, user_variable_value);
            user_variable_name := user_binds.next(user_variable_name);
        end loop;
        return solved_text;
    end solve;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end core;
/