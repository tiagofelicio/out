create or replace package body out.files is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    function get_property(property_name varchar2, text varchar2) return varchar2 is
        property_value core.string_t;
        solved_text core.string_t;
        directory_flag core.string_t;
        force_flag core.string_t;
    begin
        solved_text := core.solve(text);
        case lower(property_name)
            when 'directory flag' then
                directory_flag := case when lower(solved_text) = 'true' then '-R' end;
                property_value := directory_flag;
            when 'force flag' then
                force_flag := case when lower(solved_text) = 'true' then '-f' end;
                property_value := force_flag;
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
            cp $directory_flag $force_flag $source_filename $target_filename
        ]';
        core.bind('$', 'source_filename', source_filename);
        core.bind('$', 'target_filename', target_filename);
        core.bind('$', 'directory_flag', get_property('directory flag', core.get_option('directory', options)));
        core.bind('$', 'force_flag', get_property('force flag', core.get_option('force', options)));
        output := core.shell(command);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end copy;

    procedure move(target_filename varchar2, source_filename varchar2, options varchar2 default null) is
    begin
        null;
    end move;

    procedure remove(filename varchar2, options varchar2 default null) is
        command core.string_t;
        output core.string_t;
    begin
        internal.log_session_step('start');
        command := q'[
            rm $directory_flag $force_flag $filename
        ]';
        core.bind('$', 'filename', filename);
        core.bind('$', 'directory_flag', get_property('directory flag', core.get_option('directory', options)));
        core.bind('$', 'force_flag', get_property('force flag', core.get_option('force', options)));
        output := core.shell(command);
        core.unbind('$');
        internal.log_session_step('done');
    exception
        when others then
            core.unbind('$');
            internal.log_session_step('error', sqlerrm);
    end remove;

    procedure wait(filename varchar2, options varchar2) is
    begin
        null;
    end wait;

    procedure load(table_name varchar2, filename varchar2, attributes varchar2, options varchar2) is
    begin
        null;
    end load;

    procedure unload(filename varchar2, table_name varchar2, options varchar2) is
    begin
        null;
    end unload;

    procedure zip(archive_name varchar2, filename varchar2, options varchar2) is
    begin
        null;
    end zip;

    procedure unzip(filename varchar2, archive_name varchar2, options varchar2) is
    begin
        null;
    end unzip;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end files;
/