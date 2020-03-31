-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : install.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @install.sql (base-folder) (password) (default-tablespace) (stage-tablespace) (temporary-tablespace)
-- Last Modified : 2020/03/24
-- ----------------------------------------------------------------------------------------------------------------------------

create user out identified by &2
default tablespace &4
temporary tablespace &5;

alter user out quota unlimited on &3;
alter user out quota unlimited on &4;

grant resource to out;
grant create any directory to out;
grant drop any directory to out;
grant create any table to out;
grant alter any table to out;
grant drop any table to out;
grant analyze any to out;
grant select any table to out;
grant insert any table to out;
grant update any table to out;
grant delete any table to out;
grant execute on dbms_lock to out;

begin
    dbms_java.grant_permission(
        grantee => 'OUT',
        permission_type => 'java.io.FilePermission',
        permission_name => '<<ALL FILES>>',
        permission_action => 'read,write,delete,execute'
    );
end;
/

create table out.version (
    banner varchar2(150 byte)
)
pctfree 0
compress
nologging
noparallel
tablespace &3;

insert into out.version (banner) values ('Oracle Unified Toolkit for Oracle Database');
insert into out.version (banner) values ('Developed by TAF');
insert into out.version (banner) values ('OUT Version: 2020.03');
commit;

create table out.sessions (
    no number,
    name varchar2(500 byte),
    begin date,
    end date,
    duration number generated always as (round((end - begin) * 24 * 60 * 60)) virtual,
    status varchar2(1 byte),
    username varchar2(250 byte),
    work number,
    error clob,
    constraint sessions_pk primary key (no) using index pctfree 20 compute statistics nologging
)
pctfree 20
row store compress advanced
nologging
lob(error) store as securefile (
    nocache nologging compress high deduplicate
)
noparallel
tablespace &3;

create table out.session_steps (
    session_no number,
    no number,
    name varchar2(500 byte),
    begin date,
    end date,
    duration number generated always as (round((end - begin) * 24 * 60 * 60)) virtual,
    status varchar2(1 byte),
    work number,
    error clob,
    constraint session_steps_pk primary key (session_no, no) using index pctfree 20 compute statistics nologging,
    constraint session_steps_fk_sessions foreign key (session_no) references out.sessions (no)
)
pctfree 20
row store compress advanced
nologging
lob(error) store as securefile (
    nocache nologging compress high deduplicate
)
noparallel
tablespace &3;

create table out.session_step_tasks (
    session_no number,
    session_step_no number,
    no number,
    begin date,
    end date,
    duration number generated always as (round((end - begin) * 24 * 60 * 60)) virtual,
    status varchar2(1 byte),
    work number,
    code clob,
    error clob,
    constraint session_steps_tasks_pk primary key (session_no, session_step_no, no) using index pctfree 20 compute statistics nologging,
    constraint session_step_tasks_fk_session_steps foreign key (session_no, session_step_no) references out.session_steps (session_no, no)
)
pctfree 20
row store compress advanced
nologging
lob(code) store as securefile (
    nocache nologging compress high deduplicate
),
lob(error) store as securefile (
    nocache nologging compress high deduplicate
)
noparallel
tablespace &3;

begin
    dbms_stats.gather_schema_stats(
        ownname => 'out',
        estimate_percent => dbms_stats.auto_sample_size,
        method_opt => 'for all columns size auto',
        degree => dbms_stats.auto_degree,
        granularity => 'auto',
        cascade => dbms_stats.auto_cascade,
        no_invalidate => dbms_stats.auto_invalidate
    );
end;
/

@&1/out/java/OUTTools.pls

@&1/out/packages/types.pks
@&1/out/packages/types.pkb
@&1/out/packages/core.pks
@&1/out/packages/core.pkb
@&1/out/packages/logger.pks
@&1/out/packages/logger.pkb
@&1/out/packages/execute.pks
@&1/out/packages/execute.pkb
@&1/out/packages/data_integration.pks
@&1/out/packages/data_integration.pkb
@&1/out/packages/files.pks
@&1/out/packages/files.pkb
@&1/out/packages/internet.pks
@&1/out/packages/internet.pkb
@&1/out/packages/utilities.pks
@&1/out/packages/utilities.pkb

@&1/out/procedures/bind.pls
@&1/out/procedures/process.pls
