/*
 * dbsize.c
 *		Database object size functions, and related inquiries
 *
 * Copyright (c) 2002-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dbsize.c
 *
 */

#include "postgres.h"

#include <sys/stat.h>
#include <glob.h>

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_namespace_d.h"
#include "libpq-fe.h"
#include "foreign/fdwapi.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "utils/snapmgr.h"

/* Divide by two and round away from zero */
#define half_rounded(x)   (((x) + ((x) < 0 ? -1 : 1)) / 2)

static int64 calculate_total_relation_size(Relation rel);

/**
 * Some functions are peculiar in that they do their own dispatching.
 * They do not work on entry db since we do not support dispatching
 * from entry-db currently.
 */
#define ERROR_ON_ENTRY_DB()	\
	if (Gp_role == GP_ROLE_EXECUTE && IS_QUERY_DISPATCHER())	\
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),	\
						errmsg("This query is not currently supported by GPDB.")))

static void build_filenode2oid_hash(GpScanData *gpScanData);
static void RelfilenodeMapInvalidate(GpScanData *gpScanData);
static Oid get_idx_parent_oid(Oid indexrelid);
static void getAllUserTableSpace(GpScanData *gpScanData);
#define GP_FILENODE_SCAN_NATTR 2
#define GP_FILENODE_STAT_NATTR 7
/*
 * Helper function to dispatch a size-returning command.
 *
 * Dispatches the given SQL query to segments, and sums up the results.
 * The query is expected to return one int8 value.
 */
int64
get_size_from_segDBs(const char *cmd)
{
	int64		result;
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	CdbDispatchCommand(cmd, DF_WITH_SNAPSHOT, &cdb_pgresults);

	result = 0;
	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		Datum		value;
		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}
		if (PQntuples(pgresult) != 1 || PQnfields(pgresult) != 1)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							PQntuples(pgresult), PQnfields(pgresult))));
		}
		if (PQgetisnull(pgresult, 0, 0))
			value = 0;
		else
			value = DirectFunctionCall1(int8in,
										CStringGetDatum(PQgetvalue(pgresult, 0, 0)));
		result += value;
	}

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return result;
}

/* Return physical size of directory contents, or 0 if dir doesn't exist */
static int64
db_dir_size(const char *path)
{
	int64		dirsize = 0;
	struct dirent *direntry;
	DIR		   *dirdesc;
	char		filename[MAXPGPATH * 2];

	dirdesc = AllocateDir(path);

	if (!dirdesc)
		return 0;

	while ((direntry = ReadDir(dirdesc, path)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(filename, sizeof(filename), "%s/%s", path, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", filename)));
		}
		dirsize += fst.st_size;
	}

	FreeDir(dirdesc);
	return dirsize;
}

/*
 * calculate size of database in all tablespaces
 */
static int64
calculate_database_size(Oid dbOid)
{
	int64		totalsize;
	DIR		   *dirdesc;
	struct dirent *direntry;
	char		dirpath[MAXPGPATH];
	char		pathname[MAXPGPATH + 13 + MAX_DBID_STRING_LENGTH + 1 + sizeof(GP_TABLESPACE_VERSION_DIRECTORY)];
	AclResult	aclresult;

	/*
	 * User must have connect privilege for target database or be a member of
	 * pg_read_all_stats
	 */
	aclresult = pg_database_aclcheck(dbOid, GetUserId(), ACL_CONNECT);
	if (aclresult != ACLCHECK_OK &&
		!is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
	{
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(dbOid));
	}

	/* Shared storage in pg_global is not counted */

	/* Include pg_default storage */
	snprintf(pathname, sizeof(pathname), "base/%u", dbOid);
	totalsize = db_dir_size(pathname);

	/* Scan the non-default tablespaces */
	snprintf(dirpath, MAXPGPATH, "pg_tblspc");
	dirdesc = AllocateDir(dirpath);

	while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "pg_tblspc/%s/%s/%u",
				 direntry->d_name, GP_TABLESPACE_VERSION_DIRECTORY, dbOid);
		totalsize += db_dir_size(pathname);
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_database_size_oid(PG_FUNCTION_ARGS)
{
	Oid			dbOid = PG_GETARG_OID(0);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_database_size(dbOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_database_size(%u)", dbOid);

		size += get_size_from_segDBs(sql);
	}

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
pg_database_size_name(PG_FUNCTION_ARGS)
{
	Name		dbName = PG_GETARG_NAME(0);
	Oid			dbOid = get_database_oid(NameStr(*dbName), false);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_database_size(dbOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_database_size(%s)",
					   quote_literal_cstr(NameStr(*dbName)));

		size += get_size_from_segDBs(sql);
	}

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}


/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
static int64
calculate_tablespace_size(Oid tblspcOid)
{
	char		tblspcPath[MAXPGPATH];
	char		pathname[MAXPGPATH * 2];
	int64		totalsize = 0;
	DIR		   *dirdesc;
	struct dirent *direntry;
	AclResult	aclresult;

	/*
	 * User must be a member of pg_read_all_stats or have CREATE privilege for
	 * target tablespace, either explicitly granted or implicitly because it
	 * is default for current database.
	 */
	if (tblspcOid != MyDatabaseTableSpace &&
		!is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
	{
		aclresult = pg_tablespace_aclcheck(tblspcOid, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE,
						   get_tablespace_name(tblspcOid));
	}

	if (tblspcOid == DEFAULTTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "base");
	else if (tblspcOid == GLOBALTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "global");
	else
		snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s", tblspcOid,
				 GP_TABLESPACE_VERSION_DIRECTORY);

	dirdesc = AllocateDir(tblspcPath);

	if (!dirdesc)
		return -1;

	while ((direntry = ReadDir(dirdesc, tblspcPath)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "%s/%s", tblspcPath, direntry->d_name);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", pathname)));
		}

		if (S_ISDIR(fst.st_mode))
			totalsize += db_dir_size(pathname);

		totalsize += fst.st_size;
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
	Oid			tblspcOid = PG_GETARG_OID(0);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_tablespace_size(tblspcOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_tablespace_size(%u)", tblspcOid);

		size += get_size_from_segDBs(sql);
	}

	if (size < 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
	Name		tblspcName = PG_GETARG_NAME(0);
	Oid			tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_tablespace_size(tblspcOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_tablespace_size(%s)",
					   quote_literal_cstr(NameStr(*tblspcName)));

		size += get_size_from_segDBs(sql);
	}

	if (size < 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}


/*
 * calculate size of (one fork of) a relation
 *
 * Iterator over all files belong to the relation and do stat.
 * The obviously better way is to use glob.  For whatever reason,
 * glob is extremely slow if there are lots of relations in the
 * database.  So we handle all cases, instead.
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
static int64
calculate_relation_size(Relation rel, ForkNumber forknum)
{
	int64		totalsize = 0;
	char	   *relationpath;
	char		pathname[MAXPGPATH];
	unsigned int segcount = 0;

	/* Call into the tableam api for AO/AOCO relations */
	if (RelationIsAppendOptimized(rel))
		return table_relation_size(rel, forknum);

	relationpath = relpathbackend(rel->rd_node, rel->rd_backend, forknum);

	/* Ordinary relation, including heap and index.
	 * They take form of relationpath, or relationpath.%d
	 * There will be no holes, therefore, we can stop when
	 * we reach the first non-existing file.
	 */
	for (segcount = 0;; segcount++)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (segcount == 0)
			snprintf(pathname, MAXPGPATH, "%s",
					 relationpath);
		else
			snprintf(pathname, MAXPGPATH, "%s.%u",
					 relationpath, segcount);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				break;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file %s: %m", pathname)));
		}
		totalsize += fst.st_size;
	}

	/* RELSTORAGE_VIRTUAL has no space usage */
	return totalsize;
}

Datum
pg_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	text	   *forkName = PG_GETARG_TEXT_PP(1);
	ForkNumber	forkNumber;
	Relation	rel;
	int64		size = 0;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);

	/*
	 * Before 9.2, we used to throw an error if the relation didn't exist, but
	 * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
	 * less robust, because while we scan pg_class with an MVCC snapshot,
	 * someone else might drop the table. It's better to return NULL for
	 * already-dropped tables than throw an error and abort the whole query.
	 */
	if (rel == NULL)
		PG_RETURN_NULL();

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		FdwRoutine *fdwroutine;
		bool        ok = false;

		fdwroutine = GetFdwRoutineForRelation(rel, false);

		if (fdwroutine->GetRelationSizeOnSegment != NULL)
			ok = fdwroutine->GetRelationSizeOnSegment(rel, &size);

		if (!ok)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot calculate this foreign table size",
							RelationGetRelationName(rel))));

		relation_close(rel, AccessShareLock);

		PG_RETURN_INT64(size);

	}

	forkNumber = forkname_to_number(text_to_cstring(forkName));

	size = calculate_relation_size(rel, forkNumber);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_relation_size(%u, '%s')", relOid,
					   forkNames[forkNumber]);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * Calculate total on-disk size of a TOAST relation, including its indexes.
 * Must not be applied to non-TOAST relations.
 */
static int64
calculate_toast_table_size(Oid toastrelid)
{
	int64		size = 0;
	Relation	toastRel;
	ForkNumber	forkNum;
	ListCell   *lc;
	List	   *indexlist;

	toastRel = relation_open(toastrelid, AccessShareLock);

	/* toast heap size, including FSM and VM size */
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		size += calculate_relation_size(toastRel, forkNum);

	/* toast index size, including FSM and VM size */
	indexlist = RelationGetIndexList(toastRel);

	/* Size is calculated using all the indexes available */
	foreach(lc, indexlist)
	{
		Relation	toastIdxRel;

		toastIdxRel = relation_open(lfirst_oid(lc),
									AccessShareLock);
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			size += calculate_relation_size(toastIdxRel, forkNum);

		relation_close(toastIdxRel, AccessShareLock);
	}
	list_free(indexlist);
	relation_close(toastRel, AccessShareLock);

	return size;
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 * GPDB: Also includes aoseg, aoblkdir, and aovisimap tables
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
static int64
calculate_table_size(Relation rel)
{
	int64		size = 0;
	ForkNumber	forkNum;

	if (!RelationIsValid(rel))
		return 0;

	/*
	 * heap size, including FSM and VM
	 */
	if (rel->rd_node.relNode != 0)
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			size += calculate_relation_size(rel, forkNum);
	}

	/*
	 * Size of toast relation
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
		size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);

	if (RelationIsAppendOptimized(rel))
	{
		Oid	auxRelIds[3];
		GetAppendOnlyEntryAuxOids(rel, &auxRelIds[0],
								 &auxRelIds[1],
								 &auxRelIds[2]);

		for (int i = 0; i < 3; i++)
		{
			Relation auxRel;

			if (!OidIsValid(auxRelIds[i]))
				continue;

			if ((auxRel = try_relation_open(auxRelIds[i], AccessShareLock, false)) != NULL)
			{
				size += calculate_total_relation_size(auxRel);
				relation_close(auxRel, AccessShareLock);
			}
			else
			{
				/*
				 * This error may occur when the auxiliary relations' records of
				 * the appendonly table are corrupted.
				 */
				elog(ERROR, "invalid auxiliary relation oid %u for appendonly relation '%s'",
							auxRelIds[i], rel->rd_rel->relname.data);
			}
		}
	}

	return size;
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
static int64
calculate_indexes_size(Relation rel)
{
	int64		size = 0;

	/*
	 * Aggregate all indexes on the given relation
	 */
	if (rel->rd_rel->relhasindex)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *cell;

		foreach(cell, index_oids)
		{
			Oid			idxOid = lfirst_oid(cell);
			Relation	idxRel;
			ForkNumber	forkNum;

			idxRel = try_relation_open(idxOid, AccessShareLock, false);

			if (RelationIsValid(idxRel))
			{
				for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
					size += calculate_relation_size(idxRel, forkNum);

				relation_close(idxRel, AccessShareLock);
			}
		}

		list_free(index_oids);
	}

	return size;
}

Datum
pg_table_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_table_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_table_size(%u)", relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}


static void
getAllUserTableSpace(GpScanData *gpScanData)
{
	HASH_SEQ_STATUS status;
	TablespaceMapEntry *entry;

	hash_seq_init(&status, gpScanData->TablespaceMapHash);
	while ((entry = (TablespaceMapEntry *) hash_seq_search(&status)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		if (gpScanData->numTablespaces == MAX_GP_TABLESPACE)
			elog(ERROR, "table space num exceeds maximum %d", MAX_GP_TABLESPACE);
		gpScanData->tableSpaces[gpScanData->numTablespaces++] = entry->path;
		if (hash_search(gpScanData->TablespaceMapHash,
						(void *) &entry->reltablespace,
						HASH_REMOVE,
						NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

static Oid
get_idx_parent_oid(Oid indexrelid)
{
	HeapTuple	tuple;
	Oid			result = 0;

	tuple = SearchSysCache(INDEXRELID,
						   ObjectIdGetDatum(indexrelid),
						   0, 0, 0);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_index pgIndexStruct;

		pgIndexStruct = (Form_pg_index) GETSTRUCT(tuple);
		result = pgIndexStruct->indrelid;
		ReleaseSysCache(tuple);
	}

	return result;
}

static void
RelfilenodeMapInvalidate(GpScanData *gpScanData)
{
	HASH_SEQ_STATUS status;
	RelfilenodeScanMapEntry *entry;

	hash_seq_init(&status, gpScanData->RelfilenodeMapHash);
	while ((entry = (RelfilenodeScanMapEntry *) hash_seq_search(&status)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		if (hash_search(gpScanData->RelfilenodeMapHash,
						(void *) &entry->relfilenode,
						HASH_REMOVE,
						NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

static void
build_filenode2oid_hash(GpScanData *gpScanData)
{
	Relation	relation;
	HeapTuple	tuple;
	SysScanDesc scandesc;
	RelfilenodeScanMapEntry *entry;
	bool		found;
	bool		filter = false;
	char		buf[MAXPGPATH];
	Oid			real_oid = 0;
	Oid			relfilenodeKey;
	HTAB		*filterOidHash = NULL;
	HASHCTL		filterOidHashCtl;

	relation = heap_open(RelationRelationId, AccessShareLock);
	scandesc = systable_beginscan(relation,
								  InvalidOid,
								  false,
								  NULL,
								  0,
								  NULL);
	if (gpScanData->roleid != InvalidOid)
		filter = true;
	if (gpScanData->nspoid != InvalidOid)
		filter = true;
	if (filter)
	{
		if (CacheMemoryContext == NULL)
			CreateCacheMemoryContext();
		MemSet(&filterOidHashCtl, 0, sizeof(HASHCTL));
		filterOidHashCtl.keysize = sizeof(Oid);
		filterOidHashCtl.entrysize = sizeof(Oid);
		filterOidHashCtl.hash = oid_hash;
		filterOidHashCtl.hcxt = CacheMemoryContext;
		filterOidHash = hash_create("FilterOID cache", 64, &filterOidHashCtl,
									HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	while ((tuple = systable_getnext(scandesc)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		Form_pg_class pgClassStruct;
		Oid			objid = ((Form_pg_type) GETSTRUCT(tuple))->oid;

		/* actual pg_class.oid */
		pgClassStruct = (Form_pg_class) GETSTRUCT(tuple);

		/* filter by owner */
		if (gpScanData->roleid != InvalidOid)
			if (pgClassStruct->relowner != gpScanData->roleid)
				continue;

		/* filter by namespace */
		if (gpScanData->nspoid != InvalidOid)
		{
			if (pgClassStruct->relnamespace != gpScanData->nspoid
				&& !IsToastClass(pgClassStruct)
				&& pgClassStruct->relnamespace != PG_AOSEGMENT_NAMESPACE)
				continue;
		}

		relfilenodeKey = pgClassStruct->relfilenode;

		/*
		 * In GPDB6, some catalog table's relfilenode is zero, these oid and
		 * filenode maps were stored in pg_filenode.map. We get these filenode
		 * id by Relation type as workaround
		 */
		if (relfilenodeKey == 0)
		{
			Relation	rel = RelationIdGetRelation(objid);

			if (rel != NULL)
			{
				relfilenodeKey = rel->rd_node.relNode;
				RelationClose(rel);
			}
		}
		entry = hash_search(gpScanData->RelfilenodeMapHash, (void *) &relfilenodeKey, HASH_ENTER, &found);

		/* Search parent relation oid of this pg_class tuple */
		/* toast */
		if (pgClassStruct->relnamespace == PG_TOAST_NAMESPACE)
		{
			if (pgClassStruct->relkind == RELKIND_TOASTVALUE)
			{
				sscanf(pgClassStruct->relname.data, "pg_toast_%u", &real_oid);
			}
			else if (pgClassStruct->relkind == RELKIND_INDEX)
			{
				sscanf(pgClassStruct->relname.data, "pg_toast_%u_index", &real_oid);
			}
		}
		/* ao */
		else if (pgClassStruct->relnamespace == PG_AOSEGMENT_NAMESPACE)
		{
			/*
			 * pg_aoseg_{oid}, pg_aovisimap_{oid}, pg_aovisimap_{oid}_index,
			 * pg_aocsseg_{oid}, pg_aoblkdir_{oid}, pg_aoblkdir_{oid}_index
			 */
			sscanf(pgClassStruct->relname.data, "%[^1234567890]%u", buf, &real_oid);
		}
		/* ordinary index */
		else if (pgClassStruct->relkind == RELKIND_INDEX)
		{
			real_oid = get_idx_parent_oid(objid);
		}
		else
		{
			real_oid = objid;
			if (filter && filterOidHash != NULL)
				hash_search(filterOidHash, (void *) &real_oid, HASH_ENTER, &found);
		}

		entry->relid = real_oid;

		/*
		 * Table space hash
		 */
		if (pgClassStruct->reltablespace != DEFAULTTABLESPACE_OID
			&& pgClassStruct->reltablespace != GLOBALTABLESPACE_OID
			&& pgClassStruct->reltablespace != InvalidOid)
		{
			bool		tablespaceFound;
			TablespaceMapEntry *tbsEntry = hash_search(gpScanData->TablespaceMapHash,
									(void *) &(pgClassStruct->reltablespace),
													   HASH_ENTER,
													   &tablespaceFound);

			if (!tablespaceFound)
			{
				tbsEntry->path = GetDatabasePath(MyDatabaseId, pgClassStruct->reltablespace);
			}
		}
	}

	if (filter && filterOidHash != NULL)
	{
		/**
		 * Cleanup redundant toast/aoseg filenode if have filter
		 */
		HASH_SEQ_STATUS status;
		Oid		   *oid_entry;

		hash_seq_init(&status, gpScanData->RelfilenodeMapHash);
		while ((entry = (RelfilenodeScanMapEntry *) hash_seq_search(&status)) != NULL)
		{
			hash_search(filterOidHash, (void *) &entry->relid, HASH_FIND, &found);
			if (!found)
				hash_search(gpScanData->RelfilenodeMapHash,
							(void *) &entry->relfilenode,
							HASH_REMOVE,
							NULL);
		}

		hash_seq_init(&status, filterOidHash);
		while ((oid_entry = (Oid *) hash_seq_search(&status)) != NULL)
		{
			CHECK_FOR_INTERRUPTS();
			hash_search(filterOidHash,
						(void *) &oid_entry,
						HASH_REMOVE,
						NULL);
		}
		filterOidHash = NULL;
	}
	getAllUserTableSpace(gpScanData);
	systable_endscan(scandesc);
	heap_close(relation, AccessShareLock);
}


/*
 * Scan database directory and get each filenode size
 */
Datum
gp_filenode_scan(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	char			dbpath[MAXPGPATH];
	char			path[MAXPGPATH] = {0};
	GpScanData		*gpScanData;
	DIR				*pdir;
	bool			nulls[GP_FILENODE_SCAN_NATTR];
	Datum			values[GP_FILENODE_SCAN_NATTR];

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls */
		MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		Name		nspname = PG_GETARG_NAME(0);
		Name		rolname = PG_GETARG_NAME(1);
		TupleDesc	tupdesc = CreateTemplateTupleDesc(GP_FILENODE_SCAN_NATTR);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "reloid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "size", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Construct path of target database */
		memset(dbpath, 0, sizeof(dbpath));
		strncat(dbpath, DataDir, (MAXPGPATH - strlen(dbpath) - 1) > 0 ? (MAXPGPATH - strlen(dbpath) - 1) : 0);
		strncat(dbpath, "/", (MAXPGPATH - strlen(dbpath) - 1) > 0 ? (MAXPGPATH - strlen(dbpath) - 1) : 0);
		strncat(dbpath, DatabasePath, (MAXPGPATH - strlen(dbpath) - 1) > 0 ? (MAXPGPATH - strlen(dbpath) - 1) : 0);
		strncat(dbpath, "/", (MAXPGPATH - strlen(dbpath) - 1) > 0 ? (MAXPGPATH - strlen(dbpath) - 1) : 0);

		/* Initialize struct members for scan data */
		gpScanData = palloc(sizeof(GpScanData));
		gpScanData->idxTuples = NULL;
		gpScanData->coordinator_finish = false;
		gpScanData->cdb_pgresults.pg_results = NULL;
		gpScanData->cdb_pgresults.numResults = 0;
		gpScanData->numTablespaces = 0;
		gpScanData->curTableSpace = -1;
		gpScanData->pent = NULL;
		gpScanData->skip_readdir = false;

		/* Retrieve oids for role and name, if any are specified */
		if (strlen(NameStr(*rolname)) > 0)
			gpScanData->roleid = GetSysCacheOid1(AUTHNAME, Anum_pg_authid_oid, PointerGetDatum(NameStr(*rolname)));
		else
			gpScanData->roleid = InvalidOid;

		if (strlen(NameStr(*nspname)) > 0)
			gpScanData->nspoid = GetSysCacheOid1(NAMESPACENAME,Anum_pg_namespace_oid,  CStringGetDatum(NameStr(*nspname)));
		else
			gpScanData->nspoid = InvalidOid;

		funcctx->user_fctx = gpScanData;

		if (CacheMemoryContext == NULL)
			CreateCacheMemoryContext();

		/* Initialize the hash table. */
		MemSet(&(gpScanData->relfilenodeHashCtl), 0, sizeof(HASHCTL));
		gpScanData->relfilenodeHashCtl.keysize = sizeof(Oid);
		gpScanData->relfilenodeHashCtl.entrysize = sizeof(RelfilenodeScanMapEntry);
        gpScanData->relfilenodeHashCtl.hash = uint32_hash;
		gpScanData->relfilenodeHashCtl.hcxt = CacheMemoryContext;
		gpScanData->RelfilenodeMapHash = hash_create("RelfilenodeMap cache", 64, &(gpScanData->relfilenodeHashCtl),
								   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		MemSet(&(gpScanData->tablespaceHashCtl), 0, sizeof(HASHCTL));
		gpScanData->tablespaceHashCtl.keysize = sizeof(Oid);
		gpScanData->tablespaceHashCtl.entrysize = sizeof(TablespaceMapEntry);
        gpScanData->tablespaceHashCtl.hash = uint32_hash;
		gpScanData->tablespaceHashCtl.hcxt = CacheMemoryContext;
		gpScanData->TablespaceMapHash = hash_create("TablespaceMap cache", 64, &(gpScanData->tablespaceHashCtl),
								   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		build_filenode2oid_hash(gpScanData);

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			char	   *sql = psprintf("SELECT reloid, size FROM gp_filenode_scan('%s', '%s') WHERE size > 0 ORDER BY 1", quote_identifier(NameStr(*nspname)), quote_identifier(NameStr(*rolname)));

			CdbDispatchCommand(sql, DF_WITH_SNAPSHOT, &(gpScanData->cdb_pgresults));
			gpScanData->idxTuples = (int *) palloc(sizeof(int) * gpScanData->cdb_pgresults.numResults);
			memset(gpScanData->idxTuples, 0, sizeof(int) * gpScanData->cdb_pgresults.numResults);
		}
		pdir = opendir(dbpath);
		if (pdir == NULL)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read directory \"%s\": %m", dbpath)));
		}
		gpScanData->pdir = pdir;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	gpScanData = (GpScanData *) funcctx->user_fctx;
	while (Gp_role != GP_ROLE_DISPATCH || !gpScanData->coordinator_finish)
	{
		CHECK_FOR_INTERRUPTS();
		int						filenodeid = 0;
		int						save_filenodeid = 0;
		int						filenode_suffix = 0;
		HeapTuple				tuple;
		Datum					result;
		Oid						relfilenodeKey;
		bool					found;
		struct stat				statbuf;
		int64					size;
		RelfilenodeScanMapEntry *entry;

		if (!gpScanData->skip_readdir)
			gpScanData->pent = readdir(gpScanData->pdir);

		gpScanData->skip_readdir = false;
		if (gpScanData->pent == NULL)
		{
			closedir(gpScanData->pdir);
			gpScanData->curTableSpace++;
			if (gpScanData->curTableSpace == gpScanData->numTablespaces)
			{
				if (Gp_role == GP_ROLE_DISPATCH)
				{
					gpScanData->coordinator_finish = true;
				}
				break;
			}
			else
			{
				char	   *newpath = gpScanData->tableSpaces[gpScanData->curTableSpace];

				pdir = opendir(newpath);
				if (pdir == NULL)
				{
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not read directory \"%s\": %m",
									newpath)));
				}
				gpScanData->pdir = pdir;
				continue;
			}
		}

		if (strcmp(gpScanData->pent->d_name, ".") == 0 || strcmp(gpScanData->pent->d_name, "..") == 0)
			continue;

		sscanf(gpScanData->pent->d_name, "%u.%u", &filenodeid, &filenode_suffix);
		/* skip useless file */
		if (filenodeid == 0)
			continue;

		memset(path, 0, sizeof(path));
		if (gpScanData->curTableSpace == -1)
		{
			strncat(path, dbpath, (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
			strncat(path, gpScanData->pent->d_name, (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
		}
		else
		{
			strncat(path, gpScanData->tableSpaces[gpScanData->curTableSpace], (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
			strncat(path, "/", (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
			strncat(path, gpScanData->pent->d_name, (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
		}
		if (lstat(path, &statbuf) != 0)
			continue;

		relfilenodeKey = filenodeid;
		entry = hash_search(gpScanData->RelfilenodeMapHash, (void *) &relfilenodeKey, HASH_FIND, &found);

		if (!found)
			continue;

		size = statbuf.st_size;

		/* Hold! check if next dir is same filenodeid */
		save_filenodeid = filenodeid;
		while (save_filenodeid == filenodeid)
		{
			CHECK_FOR_INTERRUPTS();
			/*
			 * Check next file for optimize AOCO case For AO/CO, filenode name
			 * may like 123.345, 123.346, 123.532 each file is a column. This
			 * loop tries to sum them in one tuple
			 */
			gpScanData->pent = readdir(gpScanData->pdir);
			gpScanData->skip_readdir = true;
			if (gpScanData->pent == NULL)
				break;
			if (strcmp(gpScanData->pent->d_name, ".") == 0 || strcmp(gpScanData->pent->d_name, "..") == 0)
				continue;

			filenodeid = 0;
			filenode_suffix = 0;
			sscanf(gpScanData->pent->d_name, "%u.%u", &filenodeid, &filenode_suffix);
			if (filenodeid == 0)
				continue;

			if (filenodeid == save_filenodeid)
			{
				/* The filenodeid of next file is the same */
				memset(path, 0, sizeof(path));
				if (gpScanData->curTableSpace == -1)
				{
					strncat(path, dbpath, (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
					strncat(path, gpScanData->pent->d_name, (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
				}
				else
				{
					strncat(path, gpScanData->tableSpaces[gpScanData->curTableSpace], (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
					strncat(path, "/", (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
					strncat(path, gpScanData->pent->d_name, (MAXPGPATH - strlen(path) - 1) > 0 ? (MAXPGPATH - strlen(path) - 1) : 0);
				}
				if (lstat(path, &statbuf) == 0)
				{
					size += statbuf.st_size;
					gpScanData->skip_readdir = false;
				}
			}
			else
				break;
		}

		memset(nulls, 0, sizeof(nulls));
		values[0] = ObjectIdGetDatum(entry->relid);
		values[1] = Int64GetDatum(size);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	/* n-way merge */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CHECK_FOR_INTERRUPTS();
		int			r;
		int			n = gpScanData->cdb_pgresults.numResults;
		struct pg_result *pgresult;
		int			idxTuples;
		Oid		   *head_oids;
		Oid			current_relid = 0;
		Oid			next_relid = 0;
		int64		sum_file_size = 0;

		head_oids = (Oid *) palloc(sizeof(Oid) * n);
		memset(head_oids, 0, sizeof(Oid) * n);

		/* Find smallest relid from head of all ResultSets */
		for (r = 0; r < n; r++)
		{
			CHECK_FOR_INTERRUPTS();
			pgresult = gpScanData->cdb_pgresults.pg_results[r];
			idxTuples = gpScanData->idxTuples[r];

			if (idxTuples >= PQntuples(pgresult))
			{
				/* Have reached bottom of this ResultSet */
				continue;
			}

			if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
			{
				cdbdisp_clearCdbPgResults(&(gpScanData->cdb_pgresults));
				ereport(ERROR,
						(errmsg("unexpected result from segment: %d",
								PQresultStatus(pgresult))));
			}
			if (PQnfields(pgresult) != GP_FILENODE_SCAN_NATTR)
			{
				cdbdisp_clearCdbPgResults(&(gpScanData->cdb_pgresults));
				ereport(ERROR,
						(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
								PQntuples(pgresult), PQnfields(pgresult))));
			}

			/* Retrieve relid from head of current ResultSet */
			head_oids[r] = DatumGetObjectId(DirectFunctionCall1(int4in, CStringGetDatum(PQgetvalue(pgresult, idxTuples, 0))));

			if (current_relid == 0 || current_relid > head_oids[r])
				current_relid = head_oids[r];
		}

		if (current_relid > 0)
		{
			/* Visit all ResultSets again to sum counters of current_relid */
			for (r = 0; r < n; r++)
			{
				if (head_oids[r] > 0 && head_oids[r] == current_relid)
				{
					pgresult = gpScanData->cdb_pgresults.pg_results[r];
					idxTuples = gpScanData->idxTuples[r];

					sum_file_size += DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(PQgetvalue(pgresult, idxTuples, 1))));

					/* Move head pointer to next tuple for this ResultSet */
					gpScanData->idxTuples[r]++;

					/* Look ahead for all same relid */
					while (1)
					{
						CHECK_FOR_INTERRUPTS();
						idxTuples++;
						if (idxTuples >= PQntuples(pgresult))
						{
							/* Have reached bottom of this ResultSet */
							break;
						}
						next_relid = DatumGetObjectId(DirectFunctionCall1(int4in, CStringGetDatum(PQgetvalue(pgresult, idxTuples, 0))));
						if (next_relid != current_relid)
						{
							/* Finished look ahead */
							break;
						}

						sum_file_size += DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(PQgetvalue(pgresult, idxTuples, 1))));

						/* Move head pointer to next tuple for this ResultSet */
						gpScanData->idxTuples[r]++;
					}
				}
			}

			memset(nulls, 0, sizeof(nulls));
			values[0] = ObjectIdGetDatum(current_relid);
			values[1] = Int64GetDatum(sum_file_size);

			HeapTuple	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			Datum		result = HeapTupleGetDatum(tuple);

			SRF_RETURN_NEXT(funcctx, result);
		}

		/* Done merge and emit results */
		cdbdisp_clearCdbPgResults(&(gpScanData->cdb_pgresults));
	}

	RelfilenodeMapInvalidate(gpScanData);
	/* Reached the end of the entry array, we're done */
	SRF_RETURN_DONE(funcctx);
}

Datum
pg_indexes_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_indexes_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_indexes_size(%u)", relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 *	Compute the on-disk size of all files for the relation,
 *	including heap data, index data, toast data, FSM, VM.
 */
static int64
calculate_total_relation_size(Relation rel)
{
	int64		size;

	/*
	 * Aggregate the table size, this includes size of the heap, toast and
	 * toast index with free space and visibility map
	 */
	size = calculate_table_size(rel);

	/*
	 * Add size of all attached indexes as well
	 */
	size += calculate_indexes_size(rel);

	return size;
}

Datum
pg_total_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	ERROR_ON_ENTRY_DB();

	/*
	 * While we scan pg_class with an MVCC snapshot,
	 * someone else might drop the table. It's better to return NULL for
	 * already-dropped tables than throw an error and abort the whole query.
	 */
	if (get_rel_name(relOid) == NULL)
		PG_RETURN_NULL();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_total_relation_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_total_relation_size(%u)",
					   relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * formatting with size units
 */
Datum
pg_size_pretty(PG_FUNCTION_ARGS)
{
	int64		size = PG_GETARG_INT64(0);
	char		buf[64];
	int64		limit = 10 * 1024;
	int64		limit2 = limit * 2 - 1;

	if (Abs(size) < limit)
		snprintf(buf, sizeof(buf), INT64_FORMAT " bytes", size);
	else
	{
		/*
		 * We use divide instead of bit shifting so that behavior matches for
		 * both positive and negative size values.
		 */
		size /= (1 << 9);		/* keep one extra bit for rounding */
		if (Abs(size) < limit2)
			snprintf(buf, sizeof(buf), INT64_FORMAT " kB",
					 half_rounded(size));
		else
		{
			size /= (1 << 10);
			if (Abs(size) < limit2)
				snprintf(buf, sizeof(buf), INT64_FORMAT " MB",
						 half_rounded(size));
			else
			{
				size /= (1 << 10);
				if (Abs(size) < limit2)
					snprintf(buf, sizeof(buf), INT64_FORMAT " GB",
							 half_rounded(size));
				else
				{
					size /= (1 << 10);
					snprintf(buf, sizeof(buf), INT64_FORMAT " TB",
							 half_rounded(size));
				}
			}
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

static char *
numeric_to_cstring(Numeric n)
{
	Datum		d = NumericGetDatum(n);

	return DatumGetCString(DirectFunctionCall1(numeric_out, d));
}

static Numeric
int64_to_numeric(int64 v)
{
	Datum		d = Int64GetDatum(v);

	return DatumGetNumeric(DirectFunctionCall1(int8_numeric, d));
}

static bool
numeric_is_less(Numeric a, Numeric b)
{
	Datum		da = NumericGetDatum(a);
	Datum		db = NumericGetDatum(b);

	return DatumGetBool(DirectFunctionCall2(numeric_lt, da, db));
}

static Numeric
numeric_absolute(Numeric n)
{
	Datum		d = NumericGetDatum(n);
	Datum		result;

	result = DirectFunctionCall1(numeric_abs, d);
	return DatumGetNumeric(result);
}

static Numeric
numeric_half_rounded(Numeric n)
{
	Datum		d = NumericGetDatum(n);
	Datum		zero;
	Datum		one;
	Datum		two;
	Datum		result;

	zero = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));
	one = DirectFunctionCall1(int8_numeric, Int64GetDatum(1));
	two = DirectFunctionCall1(int8_numeric, Int64GetDatum(2));

	if (DatumGetBool(DirectFunctionCall2(numeric_ge, d, zero)))
		d = DirectFunctionCall2(numeric_add, d, one);
	else
		d = DirectFunctionCall2(numeric_sub, d, one);

	result = DirectFunctionCall2(numeric_div_trunc, d, two);
	return DatumGetNumeric(result);
}

static Numeric
numeric_truncated_divide(Numeric n, int64 divisor)
{
	Datum		d = NumericGetDatum(n);
	Datum		divisor_numeric;
	Datum		result;

	divisor_numeric = DirectFunctionCall1(int8_numeric,
										  Int64GetDatum(divisor));
	result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
	return DatumGetNumeric(result);
}

Datum
pg_size_pretty_numeric(PG_FUNCTION_ARGS)
{
	Numeric		size = PG_GETARG_NUMERIC(0);
	Numeric		limit,
				limit2;
	char	   *result;

	limit = int64_to_numeric(10 * 1024);
	limit2 = int64_to_numeric(10 * 1024 * 2 - 1);

	if (numeric_is_less(numeric_absolute(size), limit))
	{
		result = psprintf("%s bytes", numeric_to_cstring(size));
	}
	else
	{
		/* keep one extra bit for rounding */
		/* size /= (1 << 9) */
		size = numeric_truncated_divide(size, 1 << 9);

		if (numeric_is_less(numeric_absolute(size), limit2))
		{
			size = numeric_half_rounded(size);
			result = psprintf("%s kB", numeric_to_cstring(size));
		}
		else
		{
			/* size /= (1 << 10) */
			size = numeric_truncated_divide(size, 1 << 10);

			if (numeric_is_less(numeric_absolute(size), limit2))
			{
				size = numeric_half_rounded(size);
				result = psprintf("%s MB", numeric_to_cstring(size));
			}
			else
			{
				/* size /= (1 << 10) */
				size = numeric_truncated_divide(size, 1 << 10);

				if (numeric_is_less(numeric_absolute(size), limit2))
				{
					size = numeric_half_rounded(size);
					result = psprintf("%s GB", numeric_to_cstring(size));
				}
				else
				{
					/* size /= (1 << 10) */
					size = numeric_truncated_divide(size, 1 << 10);
					size = numeric_half_rounded(size);
					result = psprintf("%s TB", numeric_to_cstring(size));
				}
			}
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Convert a human-readable size to a size in bytes
 */
Datum
pg_size_bytes(PG_FUNCTION_ARGS)
{
	text	   *arg = PG_GETARG_TEXT_PP(0);
	char	   *str,
			   *strptr,
			   *endptr;
	char		saved_char;
	Numeric		num;
	int64		result;
	bool		have_digits = false;

	str = text_to_cstring(arg);

	/* Skip leading whitespace */
	strptr = str;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Check that we have a valid number and determine where it ends */
	endptr = strptr;

	/* Part (1): sign */
	if (*endptr == '-' || *endptr == '+')
		endptr++;

	/* Part (2): main digit string */
	if (isdigit((unsigned char) *endptr))
	{
		have_digits = true;
		do
			endptr++;
		while (isdigit((unsigned char) *endptr));
	}

	/* Part (3): optional decimal point and fractional digits */
	if (*endptr == '.')
	{
		endptr++;
		if (isdigit((unsigned char) *endptr))
		{
			have_digits = true;
			do
				endptr++;
			while (isdigit((unsigned char) *endptr));
		}
	}

	/* Complain if we don't have a valid number at this point */
	if (!have_digits)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid size: \"%s\"", str)));

	/* Part (4): optional exponent */
	if (*endptr == 'e' || *endptr == 'E')
	{
		long		exponent;
		char	   *cp;

		/*
		 * Note we might one day support EB units, so if what follows 'E'
		 * isn't a number, just treat it all as a unit to be parsed.
		 */
		exponent = strtol(endptr + 1, &cp, 10);
		(void) exponent;		/* Silence -Wunused-result warnings */
		if (cp > endptr + 1)
			endptr = cp;
	}

	/*
	 * Parse the number, saving the next character, which may be the first
	 * character of the unit string.
	 */
	saved_char = *endptr;
	*endptr = '\0';

	num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
											  CStringGetDatum(strptr),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(-1)));

	*endptr = saved_char;

	/* Skip whitespace between number and unit */
	strptr = endptr;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Handle possible unit */
	if (*strptr != '\0')
	{
		int64		multiplier = 0;

		/* Trim any trailing whitespace */
		endptr = str + VARSIZE_ANY_EXHDR(arg) - 1;

		while (isspace((unsigned char) *endptr))
			endptr--;

		endptr++;
		*endptr = '\0';

		/* Parse the unit case-insensitively */
		if (pg_strcasecmp(strptr, "bytes") == 0)
			multiplier = (int64) 1;
		else if (pg_strcasecmp(strptr, "kb") == 0)
			multiplier = (int64) 1024;
		else if (pg_strcasecmp(strptr, "mb") == 0)
			multiplier = ((int64) 1024) * 1024;

		else if (pg_strcasecmp(strptr, "gb") == 0)
			multiplier = ((int64) 1024) * 1024 * 1024;

		else if (pg_strcasecmp(strptr, "tb") == 0)
			multiplier = ((int64) 1024) * 1024 * 1024 * 1024;

		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid size: \"%s\"", text_to_cstring(arg)),
					 errdetail("Invalid size unit: \"%s\".", strptr),
					 errhint("Valid units are \"bytes\", \"kB\", \"MB\", \"GB\", and \"TB\".")));

		if (multiplier > 1)
		{
			Numeric		mul_num;

			mul_num = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
														  Int64GetDatum(multiplier)));

			num = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
													  NumericGetDatum(mul_num),
													  NumericGetDatum(num)));
		}
	}

	result = DatumGetInt64(DirectFunctionCall1(numeric_int8,
											   NumericGetDatum(num)));

	PG_RETURN_INT64(result);
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *		SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.  We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot even though the relations have been dropped.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
Datum
pg_relation_filenode(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			result;
	HeapTuple	tuple;
	Form_pg_class relform;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_AOVISIMAP:
			/* okay, these have storage */
			if (relform->relfilenode)
				result = relform->relfilenode;
			else				/* Consult the relation mapper */
				result = RelationMapOidToFilenode(relid,
												  relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			result = InvalidOid;
			break;
	}

	ReleaseSysCache(tuple);

	if (!OidIsValid(result))
		PG_RETURN_NULL();

	PG_RETURN_OID(result);
}

/*
 * Get the relation via (reltablespace, relfilenode)
 *
 * This is expected to be used when somebody wants to match an individual file
 * on the filesystem back to its table. That's not trivially possible via
 * pg_class, because that doesn't contain the relfilenodes of shared and nailed
 * tables.
 *
 * We don't fail but return NULL if we cannot find a mapping.
 *
 * InvalidOid can be passed instead of the current database's default
 * tablespace.
 */
Datum
pg_filenode_relation(PG_FUNCTION_ARGS)
{
	Oid			reltablespace = PG_GETARG_OID(0);
	Oid			relfilenode = PG_GETARG_OID(1);
	Oid			heaprel;

	/* test needed so RelidByRelfilenode doesn't misbehave */
	if (!OidIsValid(relfilenode))
		PG_RETURN_NULL();

	heaprel = RelidByRelfilenode(reltablespace, relfilenode);

	if (!OidIsValid(heaprel))
		PG_RETURN_NULL();
	else
		PG_RETURN_OID(heaprel);
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
Datum
pg_relation_filepath(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	HeapTuple	tuple;
	Form_pg_class relform;
	RelFileNode rnode;
	BackendId	backend;
	char	   *path;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOVISIMAP:
		case RELKIND_AOBLOCKDIR:
			/* okay, these have storage */

			/* This logic should match RelationInitPhysicalAddr */
			if (relform->reltablespace)
				rnode.spcNode = relform->reltablespace;
			else
				rnode.spcNode = MyDatabaseTableSpace;
			if (rnode.spcNode == GLOBALTABLESPACE_OID)
				rnode.dbNode = InvalidOid;
			else
				rnode.dbNode = MyDatabaseId;
			if (relform->relfilenode)
				rnode.relNode = relform->relfilenode;
			else				/* Consult the relation mapper */
				rnode.relNode = RelationMapOidToFilenode(relid,
														 relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			rnode.relNode = InvalidOid;
			/* some compilers generate warnings without these next two lines */
			rnode.dbNode = InvalidOid;
			rnode.spcNode = InvalidOid;
			break;
	}

	if (!OidIsValid(rnode.relNode))
	{
		ReleaseSysCache(tuple);
		PG_RETURN_NULL();
	}

	/* Determine owning backend. */
	switch (relform->relpersistence)
	{
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		case RELPERSISTENCE_TEMP:
			if (isTempOrTempToastNamespace(relform->relnamespace))
				backend = BackendIdForTempRelations();
			else
			{
				/* Do it the hard way. */
				backend = GetTempNamespaceBackendId(relform->relnamespace);
				Assert(backend != InvalidBackendId);
			}
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
			backend = InvalidBackendId; /* placate compiler */
			break;
	}

	ReleaseSysCache(tuple);

	path = relpathbackend(rnode, backend, MAIN_FORKNUM);

	PG_RETURN_TEXT_P(cstring_to_text(path));
}


