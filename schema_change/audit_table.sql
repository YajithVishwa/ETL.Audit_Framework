
-- Seperate schema for AUDIT
create schema audit;

create table audit.etl_audit
(
"id" text primary key,
job_name text not null,
task_name text not null,
status char(1), -- S - starting, P - processing, C - completed, F - Failed
start_time TIMESTAMPTZ,
end_time TIMESTAMPTZ,
src_system text,
src_database text,
src_table text,
src_count int,
tgt_system text,
tgt_database text,
tgt_table text,
tgt_count int,
load_type text,
error_msg text,
record_inserted int,
record_deleted int,
record_updated int,
insert_dttm TIMESTAMPTZ not null,
update_dttm TIMESTAMPTZ
);