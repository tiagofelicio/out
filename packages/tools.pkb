create or replace package body out.tools is

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
                directory_flag := case when lower(solved_text) = 'true' then '-r' end;
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

    procedure remove_file(filename varchar2, options varchar2 default null) is
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
    end remove_file;

    function shell(command varchar2, options varchar2 default null) return clob is
        output core.string_t;
    begin
        output := core.shell(command, log => false);
        return output;
    exception
        when others then
            if core.get_option('ignore errors', options, false) then
                return null;
            else
                raise;
            end if;
    end shell;

    procedure shell(command varchar2) is
        output core.string_t;
    begin
        internal.log_session_step('start');
        output := core.shell(command);
        internal.log_session_step('done');
    exception
        when others then
            internal.log_session_step('error', sqlerrm);
    end shell;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end tools;
/