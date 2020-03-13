-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : install.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @install.sql (base-folder) (password) (metadata-tablespace) (stage-tablespace) (data-tablespace) (temporary-tablespace)
-- Last Modified : 2020/03/13
-- ----------------------------------------------------------------------------------------------------------------------------

create user metadata identified by &2
default tablespace &3
temporary tablespace &6
quota unlimited on &3;

grant connect to metadata;
grant create procedure to metadata;

grant create any directory to metadata;
grant drop any directory to metadata;
grant create any table to metadata;
grant alter any table to metadata;
grant drop any table to metadata;
grant analyze any to metadata;
grant select any table to metadata;
grant insert any table to metadata;
grant update any table to metadata;
grant delete any table to metadata;

grant execute on out.bind to metadata;
grant execute on out.debug to metadata;
grant execute on out.dump to metadata;
grant execute on out.process to metadata;
grant execute on out.data_integration to metadata;
grant execute on out.tools to metadata;

create user stage identified by &2
default tablespace &4
temporary tablespace &6
quota unlimited on &4;

grant connect to stage;
grant resource to stage;

create user data identified by &2
default tablespace &5
temporary tablespace &6
quota unlimited on &5;

grant connect to data;
grant resource to data;
