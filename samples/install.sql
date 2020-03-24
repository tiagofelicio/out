-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : install.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @install.sql (base-folder) (password) (metadata-tablespace) (data-tablespace) (temporary-tablespace)
-- Last Modified : 2020/03/24
-- ----------------------------------------------------------------------------------------------------------------------------

create user metadata identified by &2
default tablespace &3
temporary tablespace &5
quota unlimited on &3;

grant connect to metadata;
grant create procedure to metadata;
grant select any table to metadata;

grant select on out.sessions to metadata;
grant select on out.session_steps to metadata;
grant select on out.session_step_tasks to metadata;
grant select on out.version to metadata;

grant execute on out.bind to metadata;
grant execute on out.process to metadata;
grant execute on out.data_integration to metadata;
grant execute on out.files to metadata;
grant execute on out.internet to metadata;
grant execute on out.utilities to metadata;

create user dw identified by &2
default tablespace &4
temporary tablespace &5
quota unlimited on &4;

create user hub identified by &2
default tablespace &3
temporary tablespace &5
quota unlimited on &3;

grant connect to data;
grant resource to data;

@&1/samples/tables.sql

@&1/samples/processes/api.pks
@&1/samples/processes/hub.pks
@&1/samples/processes/hub.pkb
@&1/samples/processes/dw.pks
@&1/samples/processes/dw.pkb
@&1/samples/processes/load.pks
@&1/samples/processes/load.pkb

