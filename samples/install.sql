-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : install.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @install.sql (base-folder) (password) (metadata-tablespace) (data-tablespace) (temporary-tablespace)
-- Last Modified : 2020/03/25
-- ----------------------------------------------------------------------------------------------------------------------------

create user metadata identified by &2
default tablespace &3
temporary tablespace &5
quota unlimited on &3;

grant connect to metadata;
grant resource to metadata;
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

create user dhub identified by &2
default tablespace &4
temporary tablespace &5
quota unlimited on &4;

grant connect to dhub;
grant resource to dhub;

create user dwh identified by &2
default tablespace &4
temporary tablespace &5
quota unlimited on &4;

grant connect to dwh;
grant resource to dwh;

@&1/samples/tables.sql

@&1/samples/processes/api.pks
@&1/samples/processes/dhub.pks
@&1/samples/processes/dhub.pkb
@&1/samples/processes/dwh.pks
@&1/samples/processes/dwh.pkb
@&1/samples/processes/load.pks
@&1/samples/processes/load.pkb

