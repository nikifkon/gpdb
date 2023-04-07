/*
 * Greenplum System Summary Views
 *
 * Portions Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 * src/backend/catalog/system_views_gp_summary.sql
 *

 * This file contains summary views for various Greenplum system catalog
 * views. These summary views are designed to provide aggregated or averaged
 * information for partitioned and replicated tables, considering multiple
 * segments in a Greenplum database.
 *
 * Note: this file is read in single-user -j mode, which means that the
 * command terminator is semicolon-newline-newline; whenever the backend
 * sees that, it stops and executes what it's got.  If you write a lot of
 * statements without empty lines between, they'll all get quoted to you
 * in any error message about one of them, so don't do that.  Also, you
 * cannot write a semicolon immediately followed by an empty line in a
 * string literal (including a function body!) or a multiline comment.
 */

CREATE VIEW gp_stat_archiver_summary AS
SELECT
    sum(gsa.archived_count) as archived_count,
    max(gsa.last_archived_wal) as last_archived_wal,
    max(gsa.last_archived_time) as last_archived_time,
    sum(gsa.failed_count) as failed_count,
    max(gsa.last_failed_wal) as last_failed_wal,
    max(gsa.last_failed_time) as last_failed_time,
    max(gsa.stats_reset) as stats_reset
FROM
    gp_stat_archiver gsa;

CREATE VIEW gp_stat_bgwriter_summary AS
SELECT
    sum(checkpoints_timed) as checkpoints_timed,
    sum(checkpoints_req) as checkpoints_req,
    sum(checkpoint_write_time) as checkpoint_write_time,
    sum(checkpoint_sync_time) as checkpoint_sync_time,
    sum(buffers_checkpoint) as buffers_checkpoint,
    sum(buffers_clean) as buffers_clean,
    sum(maxwritten_clean) as maxwritten_clean,
    sum(buffers_backend) as buffers_backend,
    sum(buffers_backend_fsync) as buffers_backend_fsync,
    sum(buffers_alloc) as buffers_alloc,
    max(stats_reset) as stats_reset
FROM
    gp_stat_bgwriter gsb;

CREATE VIEW gp_stat_wal_summary AS
SELECT
    sum(gsw.wal_records) as wal_records,
    sum(gsw.wal_fpw) as wal_fpw,
    sum(gsw.wal_bytes) as wal_bytes,
    sum(gsw.wal_buffers_full) as wal_buffers_full,
    sum(gsw.wal_write) as wal_write,
    sum(gsw.wal_sync) as wal_sync,
    sum(gsw.wal_write_time) as wal_write_time,
    sum(gsw.wal_sync_time) as wal_sync_time,
    max(gsw.stats_reset) as stats_reset
from
    gp_stat_wal gsw;

CREATE VIEW gp_stat_database_summary AS
SELECT
    datid,
    datname,
    sum(numbackends) as numbackends,
    max(xact_commit) as xact_commit,
    max(xact_rollback) as xact_rollback,
    sum(blks_read) as blks_read,
    sum(blks_hit) as blks_hit,
    sum(tup_returned) as tup_returned,
    sum(tup_fetched) as tup_fetched,
    sum(tup_inserted) as tup_inserted,
    sum(tup_updated) as tup_updated,
    sum(tup_deleted) as tup_deleted,
    max(conflicts) as conflicts,
    sum(temp_files) as temp_files,
    sum(temp_bytes) as temp_bytes,
    sum(deadlocks) as deadlocks,
    sum(checksum_failures) as checksum_failures,
    max(checksum_last_failure) as checksum_last_failure,
    sum(blk_read_time) as blk_read_time,
    sum(blk_write_time) as blk_write_time,
    max(stats_reset) as stats_reset
FROM
    gp_stat_database sdb
GROUP BY
    datid,
    datname;


-- Gather data from segments on user tables, and use data on coordinator on system tables.
CREATE VIEW gp_stat_all_tables_summary AS
SELECT
    s.relid,
    s.schemaname,
    s.relname,
    m.seq_scan,
    m.seq_tup_read,
    m.idx_scan,
    m.idx_tup_fetch,
    m.n_tup_ins,
    m.n_tup_upd,
    m.n_tup_del,
    m.n_tup_hot_upd,
    m.n_live_tup,
    m.n_dead_tup,
    m.n_mod_since_analyze,
    s.last_vacuum,
    s.last_autovacuum,
    s.last_analyze,
    s.last_autoanalyze,
    s.vacuum_count,
    s.autovacuum_count,
    s.analyze_count,
    s.autoanalyze_count
FROM
    (SELECT
         allt.relid,
         allt.schemaname,
         allt.relname,
         case when d.policytype = 'r' then (sum(seq_scan)/d.numsegments)::bigint else sum(seq_scan) end seq_scan,
         case when d.policytype = 'r' then (sum(seq_tup_read)/d.numsegments)::bigint else sum(seq_tup_read) end seq_tup_read,
         case when d.policytype = 'r' then (sum(idx_scan)/d.numsegments)::bigint else sum(idx_scan) end idx_scan,
         case when d.policytype = 'r' then (sum(idx_tup_fetch)/d.numsegments)::bigint else sum(idx_tup_fetch) end idx_tup_fetch,
         case when d.policytype = 'r' then (sum(n_tup_ins)/d.numsegments)::bigint else sum(n_tup_ins) end n_tup_ins,
         case when d.policytype = 'r' then (sum(n_tup_upd)/d.numsegments)::bigint else sum(n_tup_upd) end n_tup_upd,
         case when d.policytype = 'r' then (sum(n_tup_del)/d.numsegments)::bigint else sum(n_tup_del) end n_tup_del,
         case when d.policytype = 'r' then (sum(n_tup_hot_upd)/d.numsegments)::bigint else sum(n_tup_hot_upd) end n_tup_hot_upd,
         case when d.policytype = 'r' then (sum(n_live_tup)/d.numsegments)::bigint else sum(n_live_tup) end n_live_tup,
         case when d.policytype = 'r' then (sum(n_dead_tup)/d.numsegments)::bigint else sum(n_dead_tup) end n_dead_tup,
         case when d.policytype = 'r' then (sum(n_mod_since_analyze)/d.numsegments)::bigint else sum(n_mod_since_analyze) end n_mod_since_analyze,
         max(last_vacuum) as last_vacuum,
         max(last_autovacuum) as last_autovacuum,
         max(last_analyze) as last_analyze,
         max(last_autoanalyze) as last_autoanalyze,
         max(vacuum_count) as vacuum_count,
         max(autovacuum_count) as autovacuum_count,
         max(analyze_count) as analyze_count,
         max(autoanalyze_count) as autoanalyze_count
     FROM
         gp_dist_random('pg_stat_all_tables') allt
         inner join pg_class c
               on allt.relid = c.oid
         left outer join gp_distribution_policy d
              on allt.relid = d.localoid
     WHERE
        relid >= 16384
        and (
            d.localoid is not null
            or c.relkind in ('o', 'b', 'M')
            )
     GROUP BY allt.relid, allt.schemaname, allt.relname, d.policytype, d.numsegments

     UNION ALL

     SELECT
         *
     FROM
         pg_stat_all_tables
     WHERE
             relid < 16384) m, pg_stat_all_tables s
WHERE m.relid = s.relid;

CREATE VIEW gp_stat_user_tables_summary AS
    SELECT * FROM gp_stat_all_tables_summary
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW gp_stat_sys_tables_summary AS
    SELECT * FROM gp_stat_all_tables_summary
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'pg_aoseg') OR
          schemaname ~ '^pg_toast';

CREATE VIEW gp_stat_xact_all_tables_summary AS
SELECT
    sxa.relid,
    sxa.schemaname,
    sxa.relname,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.seq_scan)/dst.numsegments)::bigint ELSE sum(sxa.seq_scan) END AS seq_scan,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.seq_tup_read)/dst.numsegments)::bigint ELSE sum(sxa.seq_tup_read) END AS seq_tup_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.idx_scan)/dst.numsegments)::bigint ELSE sum(sxa.idx_scan) END AS idx_scan,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.idx_tup_fetch)/dst.numsegments)::bigint ELSE sum(sxa.idx_tup_fetch) END AS idx_tup_fetch,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.n_tup_ins)/dst.numsegments)::bigint ELSE sum(sxa.n_tup_ins) END AS n_tup_ins,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.n_tup_upd)/dst.numsegments)::bigint ELSE sum(sxa.n_tup_upd) END AS n_tup_upd,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.n_tup_del)/dst.numsegments)::bigint ELSE sum(sxa.n_tup_del) END AS n_tup_del,
    CASE WHEN dst.policytype = 'r' THEN (sum(sxa.n_tup_hot_upd)/dst.numsegments)::bigint ELSE sum(sxa.n_tup_hot_upd) END AS n_tup_hot_upd
FROM
    gp_stat_xact_all_tables sxa
    LEFT OUTER JOIN gp_distribution_policy dst
         ON sxa.relid = dst.localoid
GROUP BY
    sxa.relid,
    sxa.schemaname,
    sxa.relname,
    dst.policytype,
    dst.numsegments;

CREATE VIEW gp_stat_xact_sys_tables_summary as
    SELECT * FROM gp_stat_xact_all_tables_summary
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'pg_aoseg') OR
          schemaname ~ '^pg_toast';

CREATE VIEW gp_stat_xact_user_tables_summary AS
    SELECT * FROM gp_stat_xact_all_tables_summary
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

-- Gather data from segments on user tables, and use data on coordinator on system tables.
CREATE VIEW gp_stat_all_indexes_summary AS
SELECT
    s.relid,
    s.indexrelid,
    s.schemaname,
    s.relname,
    s.indexrelname,
    m.idx_scan,
    m.idx_tup_read,
    m.idx_tup_fetch
FROM
    (SELECT
         relid,
         indexrelid,
         schemaname,
         relname,
         indexrelname,
         sum(idx_scan) as idx_scan,
         sum(idx_tup_read) as idx_tup_read,
         sum(idx_tup_fetch) as idx_tup_fetch
     FROM
         gp_dist_random('pg_stat_all_indexes')
     WHERE
             relid >= 16384
     GROUP BY relid, indexrelid, schemaname, relname, indexrelname

     UNION ALL

     SELECT
         *
     FROM
         pg_stat_all_indexes
     WHERE
             relid < 16384) m, pg_stat_all_indexes s
WHERE m.relid = s.relid;

CREATE VIEW gp_stat_sys_indexes_summary AS
    SELECT * FROM gp_stat_all_indexes_summary
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'pg_aoseg') OR
          schemaname ~ '^pg_toast';

CREATE VIEW gp_stat_user_indexes_summary AS
    SELECT * FROM gp_stat_all_indexes_summary
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW gp_statio_all_tables_summary as
SELECT
    sat.relid,
    sat.schemaname,
    sat.relname,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.heap_blks_read)/dst.numsegments)::bigint ELSE sum(sat.heap_blks_read) END AS heap_blks_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.heap_blks_hit)/dst.numsegments)::bigint ELSE sum(sat.heap_blks_hit) END AS heap_blks_hit,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.idx_blks_read)/dst.numsegments)::bigint ELSE sum(sat.idx_blks_read) END AS idx_blks_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.idx_blks_hit)/dst.numsegments)::bigint ELSE sum(sat.idx_blks_hit) END AS idx_blks_hit,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.toast_blks_read)/dst.numsegments)::bigint ELSE sum(sat.toast_blks_read) END AS toast_blks_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.toast_blks_hit)/dst.numsegments)::bigint ELSE sum(sat.toast_blks_hit) END AS toast_blks_hit,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.tidx_blks_read)/dst.numsegments)::bigint ELSE sum(sat.tidx_blks_read) END AS tidx_blks_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sat.tidx_blks_hit)/dst.numsegments)::bigint ELSE sum(sat.tidx_blks_hit) END AS tidx_blks_hit
FROM
    gp_statio_all_tables sat
    LEFT OUTER JOIN gp_distribution_policy dst
         ON sat.relid = dst.localoid
GROUP BY
    sat.relid,
    sat.schemaname,
    sat.relname,
    dst.policytype,
    dst.numsegments;

CREATE VIEW gp_statio_sys_tables_summary AS
    SELECT * FROM gp_statio_all_tables_summary
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'pg_aoseg') OR
          schemaname ~ '^pg_toast';

CREATE VIEW gp_statio_user_tables_summary AS
    SELECT * FROM gp_stat_all_tables_summary
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW gp_statio_all_sequences_summary as
SELECT
    sas.relid,
    sas.schemaname,
    sas.relname,
    CASE WHEN dst.policytype = 'r' THEN (sum(sas.blks_read)/dst.numsegments)::bigint ELSE sum(sas.blks_read) END AS blks_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sas.blks_hit)/dst.numsegments)::bigint ELSE sum(sas.blks_hit) END AS blks_hit
FROM
    gp_statio_all_sequences sas
    LEFT OUTER JOIN gp_distribution_policy dst
         ON sas.relid = dst.localoid
GROUP BY
    sas.relid,
    sas.schemaname,
    sas.relname,
    dst.policytype,
    dst.numsegments;

CREATE VIEW gp_statio_sys_sequences_summary AS
    SELECT * FROM gp_statio_all_sequences_summary
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'pg_aoseg') OR
          schemaname ~ '^pg_toast';

CREATE VIEW gp_statio_user_sequences_summary AS
    SELECT * FROM gp_statio_all_sequences_summary
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW gp_statio_all_indexes_summary AS
SELECT
    sai.relid,
    sai.indexrelid,
    sai.schemaname,
    sai.relname,
    sai.indexrelname,
    CASE WHEN dst.policytype = 'r' THEN (sum(sai.idx_blks_read)/dst.numsegments)::bigint ELSE sum(sai.idx_blks_read) END AS idx_blks_read,
    CASE WHEN dst.policytype = 'r' THEN (sum(sai.idx_blks_hit)/dst.numsegments)::bigint ELSE sum(sai.idx_blks_hit) END AS idx_blks_hit
FROM
    gp_statio_all_indexes sai
    LEFT OUTER JOIN gp_distribution_policy dst
         ON sai.relid = dst.localoid
GROUP BY
    sai.relid,
    sai.indexrelid,
    sai.schemaname,
    sai.relname,
    sai.indexrelname,
    dst.policytype,
    dst.numsegments;

CREATE VIEW gp_statio_sys_indexes_summary AS
    SELECT * FROM gp_statio_all_indexes_summary
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'pg_aoseg') OR
          schemaname ~ '^pg_toast';

CREATE VIEW gp_statio_user_indexes_summary AS
    SELECT * FROM gp_statio_all_indexes_summary
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW gp_stat_user_functions_summary AS
SELECT
    funcid,
    schemaname,
    funcname,
    sum(calls) AS calls,
    sum(total_time) AS total_time,
    sum(self_time) AS self_time
FROM
    gp_stat_user_functions guf
GROUP BY
    funcid,
    schemaname,
    funcname;

CREATE VIEW gp_stat_xact_user_functions_summary AS
SELECT
    xuf.funcid,
    xuf.schemaname,
    xuf.funcname,
    sum(xuf.calls) AS calls,
    sum(xuf.total_time) AS total_time,
    sum(xuf.self_time) AS self_time
FROM
    gp_stat_xact_user_functions xuf
GROUP BY
    xuf.funcid,
    xuf.schemaname,
    xuf.funcname;

CREATE VIEW gp_stat_slru_summary AS
SELECT
    gss.name,
    sum(gss.blks_zeroed) AS blks_zeroed,
    sum(gss.blks_hit) AS blks_hit,
    sum(gss.blks_read) AS blks_read,
    sum(gss.blks_written) AS blks_written,
    sum(gss.blks_exists) AS blks_exists,
    sum(gss.flushes) AS flushes,
    sum(gss.truncates) AS truncates,
    max(gss.stats_reset) AS stats_reset
FROM
    gp_stat_slru gss
GROUP BY
    gss.name;


CREATE VIEW gp_stat_progress_vacuum_summary AS
SELECT
    a.datid,
    a.relid,
    a.phase,
    c.relname,
    d.policytype,
    CASE
        WHEN d.policytype = 'r' THEN AVG(a.heap_blks_total)
        ELSE SUM(a.heap_blks_total)
        END AS heap_blks_total,
    CASE
        WHEN d.policytype = 'r' THEN AVG(a.heap_blks_scanned)
        ELSE SUM(a.heap_blks_scanned)
        END AS heap_blks_scanned,
    CASE
        WHEN d.policytype = 'r' THEN AVG(a.heap_blks_vacuumed)
        ELSE SUM(a.heap_blks_vacuumed)
        END AS heap_blks_vacuumed,
    CASE
        WHEN d.policytype = 'r' THEN AVG(a.index_vacuum_count)
        ELSE SUM(a.index_vacuum_count)
        END AS index_vacuum_count,
    CASE
        WHEN d.policytype = 'r' THEN AVG(a.max_dead_tuples)
        ELSE SUM(a.max_dead_tuples)
        END AS max_dead_tuples,
    CASE
        WHEN d.policytype = 'r' THEN AVG(a.num_dead_tuples)
        ELSE SUM(a.num_dead_tuples)
        END AS num_dead_tuples
FROM
    gp_stat_progress_vacuum a
        JOIN
    pg_class c ON a.relid = c.oid
        JOIN
    gp_distribution_policy d ON c.oid = d.localoid
GROUP BY
    a.datid, a.relid, a.phase, c.relname, d.policytype;

CREATE OR REPLACE VIEW gp_stat_progress_analyze_summary AS
SELECT
    a.datid,
    a.relid,
    a.phase,
    c.relname,
    d.policytype,
    case when d.policytype = 'r' then (sum(a.sample_blks_total)/d.numsegments)::bigint else sum(a.sample_blks_total) end sample_blks_total,
    case when d.policytype = 'r' then (sum(a.sample_blks_scanned)/d.numsegments)::bigint else sum(a.sample_blks_scanned) end sample_blks_scanned,
    case when d.policytype = 'r' then (sum(a.ext_stats_total)/d.numsegments)::bigint else sum(a.ext_stats_total) end ext_stats_total,
    case when d.policytype = 'r' then (sum(a.ext_stats_computed)/d.numsegments)::bigint else sum(a.ext_stats_computed) end ext_stats_computed,
    case when d.policytype = 'r' then (sum(a.child_tables_total)/d.numsegments)::bigint else sum(a.child_tables_total) end child_tables_total,
    case when d.policytype = 'r' then (sum(a.child_tables_done)/d.numsegments)::bigint else sum(a.child_tables_done) end child_tables_done
FROM gp_stat_progress_analyze a
    JOIN pg_class c ON a.relid = c.oid
    JOIN gp_distribution_policy d ON c.oid = d.localoid
GROUP BY a.datid, a.relid, a.phase, c.relname, d.policytype, d.numsegments;

CREATE OR REPLACE VIEW gp_stat_progress_cluster_summary AS
SELECT
    a.datid,
    a.datname,
    a.relid,
    a.command,
    a.phase,
    a.cluster_index_relid,
    d.policytype,
    case when d.policytype = 'r' then (sum(a.heap_tuples_scanned)/d.numsegments)::bigint else sum(a.heap_tuples_scanned) end heap_tuples_scanned,
    case when d.policytype = 'r' then (sum(a.heap_tuples_written)/d.numsegments)::bigint else sum(a.heap_tuples_written) end heap_tuples_written,
    case when d.policytype = 'r' then (sum(a.heap_blks_total)/d.numsegments)::bigint else sum(a.heap_blks_total) end heap_blks_total,
    case when d.policytype = 'r' then (sum(a.heap_blks_scanned)/d.numsegments)::bigint else sum(a.heap_blks_scanned) end heap_blks_scanned,
    case when d.policytype = 'r' then (sum(a.index_rebuild_count)/d.numsegments)::bigint else sum(a.index_rebuild_count) end index_rebuild_count
FROM gp_stat_progress_cluster a
         JOIN pg_class c ON a.relid = c.oid
         JOIN gp_distribution_policy d ON c.oid = d.localoid
GROUP BY a.datid, a.datname, a.relid, a.command, a.phase, a.cluster_index_relid, d.policytype, d.numsegments;
