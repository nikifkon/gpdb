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
