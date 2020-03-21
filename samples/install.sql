-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : install.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @install.sql (base-folder) (password) (metadata-tablespace) (data-tablespace) (temporary-tablespace)
-- Last Modified : 2020/03/13
-- ----------------------------------------------------------------------------------------------------------------------------

create user metadata identified by &2
default tablespace &3
temporary tablespace &5
quota unlimited on &3;

grant connect to metadata;
grant create procedure to metadata;

grant execute on out.bind to metadata;
grant execute on out.debug to metadata;
grant execute on out.dump to metadata;
grant execute on out.process to metadata;
grant execute on out.data_integration to metadata;
grant execute on out.files to metadata;
grant execute on out.utilities to metadata;

create user data identified by &2
default tablespace &4
temporary tablespace &5
quota unlimited on &4;

grant connect to data;
grant resource to data;

create table data.dh_world_cities (
    name varchar2(250),
    country varchar2(250),
    subcountry varchar2(250),
    geonameid number,
    resource_name varchar2(250)
);

