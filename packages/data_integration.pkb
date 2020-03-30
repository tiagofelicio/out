create or replace package body out.data_integration is

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

    procedure check_not_null(work_table_name varchar2, column_name varchar2) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).plsql.code := q'[
            alter table $data_integration.check_not_null.work_table_name add constraint $data_integration.check_not_null.work_table_name_ck check ($data_integration.check_not_null.column_name is not null)
        ]';
        statements(2).plsql.code := q'[
            alter table $data_integration.check_not_null.work_table_name drop constraint $data_integration.check_not_null.work_table_name_ck
        ]';
        core.set('data_integration.check_not_null.work_table_name', work_table_name);
        core.set('data_integration.check_not_null.column_name', column_name);
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end check_not_null;

    procedure check_unique_key(work_table_name varchar2, columns_name varchar2) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).plsql.code := q'[
            alter table $data_integration.check_unique_key.work_table_name add constraint $data_integration.check_unique_key.work_table_name_uk unique ($data_integration.check_unique_key.columns_name)
        ]';
        statements(2).plsql.code := q'[
            alter table $data_integration.check_unique_key.work_table_name drop constraint $data_integration.check_unique_key.work_table_name_uk
        ]';
        core.set('data_integration.check_unique_key.work_table_name', work_table_name);
        core.set('data_integration.check_unique_key.columns_name', columns_name);
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end check_unique_key;

    procedure check_primary_key(work_table_name varchar2, columns_name varchar2) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).plsql.code := q'[
            alter table $data_integration.check_primary_key.work_table_name add constraint $data_integration.check_primary_key.work_table_name_pk primary key ($data_integration.check_primary_key.columns_name)
        ]';
        statements(2).plsql.code := q'[
            alter table $data_integration.check_primary_key.work_table_name drop constraint $data_integration.check_primary_key.work_table_name_pk
        ]';
        core.set('data_integration.check_primary_key.work_table_name', work_table_name);
        core.set('data_integration.check_primary_key.columns_name', columns_name);
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end check_primary_key;

    procedure create_table(work_table_name varchar2, statement varchar2) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).ignore_error := -00942;
        statements(1).plsql.code := q'[
            drop table $data_integration.create_table.work_table_name purge
        ]';
        statements(2).plsql.code := q'[
            create table $data_integration.create_table.work_table_name pctfree 0 nologging compress parallel as
            $data_integration.create_table.statement
        ]';
        core.set('data_integration.create_table.work_table_name', work_table_name);
        core.set('data_integration.create_table.statement', statement);
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end create_table;

    procedure drop_table(work_table_name varchar2) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).plsql.code := q'[
            drop table $data_integration.drop_table.work_table_name purge
        ]';
        core.set('data_integration.drop_table.work_table_name', work_table_name);
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end drop_table;

    procedure append(target_table_name varchar2, work_table_name varchar2, options varchar2 default null) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).plsql.code := q'[
            truncate table $data_integration.append.target_table_name drop storage
        ]';
        statements(2).ignore_error := -14312;
        statements(2).plsql.code := q'[
            alter table $data_integration.append.target_table_name add partition $data_integration.append.<partition_name> values ($data_integration.append.(options).partition_value)
        ]';
        statements(3).plsql.code := q'[
            alter table $data_integration.append.target_table_name truncate partition $data_integration.append.<partition_name> drop storage
        ]';
        statements(4).plsql.code := q'[
            insert /*+ append parallel */ into $data_integration.append.target_table_name $data_integration.append.{partition_clause} nologging (
                $data_integration.append.{work_table_columns}
            )
            select
                $data_integration.append.{work_table_columns}
            from $data_integration.append.work_table_name
        ]';
        statements(5).plsql.code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$data_integration.append.{target_table_owner_name}',
                    tabname => '$data_integration.append.{target_table_short_name}',
                    $data_integration.append.{analyze_partition_clause}
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'auto',
                    cascade => dbms_stats.auto_cascade,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        core.set('data_integration.append.target_table_name', target_table_name);
        core.set('data_integration.append.work_table_name', work_table_name);
        core.set('data_integration.append.(options).partition_name', options);
        core.set('data_integration.append.(options).partition_value', options);
        core.set('data_integration.append.(options).truncate_partition', options);
        core.set('data_integration.append.(options).truncate_table', options);
        core.set('data_integration.append.<partition_name>');
        core.set('data_integration.append.{analyze_partition_clause}');
        core.set('data_integration.append.{partition_clause}');
        core.set('data_integration.append.{target_table_owner_name}');
        core.set('data_integration.append.{target_table_short_name}');
        core.set('data_integration.append.{work_table_columns}');
        statements(1).execute := types.to_boolean(core.get('data_integration.append.(options).truncate_table'));
        statements(2).execute := core.isset('data_integration.append.(options).partition_value');
        statements(3).execute := types.to_boolean(core.get('data_integration.append.(options).truncate_partition'));
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end append;

    procedure incremental_update(target_table_name varchar2, work_table_name varchar2, options varchar2) is
        statements types.statements;
    begin
        logger.session_step('start');
        statements(1).ignore_error := -00942;
        statements(1).plsql.code := q'[
            drop table $data_integration.incremental_update.{interation_table_base_name}_01 purge
        ]';
        statements(2).plsql.code := q'[
            create table $data_integration.incremental_update.{interation_table_base_name}_01 pctfree 0 nologging compress parallel as
            select
                nvl((select max($data_integration.incremental_update.(options).surrogate_key) from $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause}), 0) + rownum $data_integration.incremental_update.(options).surrogate_key,
                $data_integration.incremental_update.{interation_01_columns}
            from $data_integration.incremental_update.work_table_name w
            left join $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause} t on
                $data_integration.incremental_update.{interation_01_join}
            where
                t.$data_integration.incremental_update.(options).surrogate_key is null
        ]';
        statements(3).ignore_error := -00942;
        statements(3).plsql.code := q'[
            drop table $data_integration.incremental_update.{interation_table_base_name}_02 purge
        ]';
        statements(4).plsql.code := q'[
            create table $data_integration.incremental_update.{interation_table_base_name}_02 pctfree 0 nologging compress parallel as
            select
                nvl(i01.$data_integration.incremental_update.(options).surrogate_key, t.$data_integration.incremental_update.(options).surrogate_key) $data_integration.incremental_update.(options).surrogate_key,
                $data_integration.incremental_update.{interation_02_columns}
            from $data_integration.incremental_update.{interation_table_base_name}_01 i01
            full join $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause} t on
                $data_integration.incremental_update.{interation_02_join}
        ]';
        statements(5).ignore_error := -00942;
        statements(5).plsql.code := q'[
            drop table $data_integration.incremental_update.{interation_table_base_name}_03 purge
        ]';
        statements(6).plsql.code := q'[
            create table $data_integration.incremental_update.{interation_table_base_name}_03 pctfree 0 nologging compress parallel as
            select
                i02.$data_integration.incremental_update.(options).surrogate_key,
                $data_integration.incremental_update.{interation_03_01_natural_key_columns},
                $data_integration.incremental_update.{interation_03_01_columns}
                $data_integration.incremental_update.{interation_03_target_only_columns}
            from $data_integration.incremental_update.{interation_table_base_name}_02 i02
            left join $data_integration.incremental_update.work_table_name w on
                $data_integration.incremental_update.{interation_03_01_join_01}
            left join $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause} t on
                $data_integration.incremental_update.{interation_03_01_join_02}
        ]';
        statements(7).plsql.code := q'[
            create table $data_integration.incremental_update.{interation_table_base_name}_03 pctfree 0 nologging compress parallel as
            select
                $data_integration.incremental_update.{interation_03_02_columns}
                $data_integration.incremental_update.{interation_03_target_only_columns}
            from $data_integration.incremental_update.work_table_name w
            full join $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause} t on
                $data_integration.incremental_update.{interation_03_02_join}
        ]';
        statements(8).plsql.code := q'[
            truncate table $data_integration.incremental_update.target_table_name drop storage
        ]';
        statements(9).plsql.code := q'[
            alter table $data_integration.incremental_update.target_table_name truncate partition $data_integration.incremental_update.(options).partition_name drop storage
        ]';
        statements(10).plsql.code := q'[
            insert /*+ append parallel */ into $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause} nologging (
                $data_integration.incremental_update.{target_table_columns}
            )
            select
                $data_integration.incremental_update.{target_table_columns}
            from $data_integration.incremental_update.{interation_table_base_name}_03
        ]';
        statements(11).plsql.code := q'[
            merge /*+ append parallel */ into $data_integration.incremental_update.target_table_name $data_integration.incremental_update.{partition_clause} t
                using $data_integration.incremental_update.{interation_table_base_name}_03 i03 on (
                    $data_integration.incremental_update.{merge_condition}
                )
                $data_integration.incremental_update.{merge_update_clause}
                when not matched then
                    insert (
                        $data_integration.incremental_update.{merge_target_table_columns}
                    ) values (
                        $data_integration.incremental_update.{merge_insert_columns}
                    )
        ]';
        statements(12).plsql.code := q'[
            begin
                dbms_stats.gather_table_stats(
                    ownname => '$data_integration.incremental_update.{target_table_owner_name}',
                    tabname => '$data_integration.incremental_update.{target_table_short_name}',
                    $data_integration.incremental_update.{analyze_partition_clause}
                    estimate_percent => dbms_stats.auto_sample_size,
                    method_opt => 'for all columns size auto',
                    degree => dbms_stats.auto_degree,
                    granularity => 'auto',
                    cascade => dbms_stats.auto_cascade,
                    no_invalidate => dbms_stats.auto_invalidate
                );
            end;
        ]';
        statements(13).plsql.code := q'[
            drop table $data_integration.incremental_update.{interation_table_base_name}_01 purge
        ]';
        statements(14).plsql.code := q'[
            drop table $data_integration.incremental_update.{interation_table_base_name}_02 purge
        ]';
        statements(15).plsql.code := q'[
            drop table $data_integration.incremental_update.{interation_table_base_name}_03 purge
        ]';
        core.set('data_integration.incremental_update.target_table_name', target_table_name);
        core.set('data_integration.incremental_update.work_table_name', work_table_name);
        core.set('data_integration.incremental_update.(options).method', options);
        core.set('data_integration.incremental_update.(options).natural_key', options);
        core.set('data_integration.incremental_update.(options).partition_name', options);
        core.set('data_integration.incremental_update.(options).surrogate_key', options);
        core.set('data_integration.incremental_update.{analyze_partition_clause}');
        core.set('data_integration.incremental_update.{interation_01_columns}');
        core.set('data_integration.incremental_update.{interation_01_join}');
        core.set('data_integration.incremental_update.{interation_02_columns}');
        core.set('data_integration.incremental_update.{interation_02_join}');
        core.set('data_integration.incremental_update.{interation_03_01_columns}');
        core.set('data_integration.incremental_update.{interation_03_01_join_01}');
        core.set('data_integration.incremental_update.{interation_03_01_join_02}');
        core.set('data_integration.incremental_update.{interation_03_01_natural_key_columns}');
        core.set('data_integration.incremental_update.{interation_03_02_columns}');
        core.set('data_integration.incremental_update.{interation_03_02_join}');
        core.set('data_integration.incremental_update.{interation_03_target_only_columns}');
        core.set('data_integration.incremental_update.{interation_table_base_name}');
        core.set('data_integration.incremental_update.{merge_condition}');
        core.set('data_integration.incremental_update.{merge_insert_columns}');
        core.set('data_integration.incremental_update.{merge_target_table_columns}');
        core.set('data_integration.incremental_update.{merge_update_clause}');
        core.set('data_integration.incremental_update.{partition_clause}');
        core.set('data_integration.incremental_update.{target_table_columns}');
        core.set('data_integration.incremental_update.{target_table_owner_name}');
        core.set('data_integration.incremental_update.{target_table_short_name}');
        statements(1).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(2).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(3).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(4).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(6).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(7).execute := not core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(8).execute := not core.isset('data_integration.incremental_update.(options).partition_name') and core.get('data_integration.incremental_update.(options).method') = 'full join';
        statements(9).execute := core.isset('data_integration.incremental_update.(options).partition_name') and core.get('data_integration.incremental_update.(options).method') = 'full join';
        statements(10).execute := core.get('data_integration.incremental_update.(options).method') = 'full join';
        statements(11).execute := core.get('data_integration.incremental_update.(options).method') = 'merge';
        statements(13).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        statements(14).execute := core.isset('data_integration.incremental_update.(options).surrogate_key');
        core.execute(statements);
        logger.session_step('done');
    exception
        when others then
            logger.session_step('error', sqlerrm);
    end incremental_update;

    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------

end data_integration;
/