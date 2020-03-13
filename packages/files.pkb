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
    begin
        null;
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