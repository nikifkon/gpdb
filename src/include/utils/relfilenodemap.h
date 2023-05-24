/*-------------------------------------------------------------------------
 *
 * relfilenodemap.h
 *	  relfilenode to oid mapping cache.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/relfilenodemap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELFILENODEMAP_H
#define RELFILENODEMAP_H

#include "cdb/cdbdispatchresult.h"
#define MAX_GP_TABLESPACE 1024

extern Oid	RelidByRelfilenode(Oid reltablespace, Oid relfilenode);


typedef struct
{
	Oid	relfilenode;	/* lookup key - must be first */
	Oid	relid;			/* oid of the relation */
} RelfilenodeScanMapEntry;

typedef struct
{
	Oid		reltablespace;	/* lookup key - must be first */
	char	*path;			/* tablespace path */
} TablespaceMapEntry;

typedef struct GpScanData
{
	int				*idxTuples;
	DIR				*pdir;
	bool			coordinator_finish;
	CdbPgResults	cdb_pgresults;
	HTAB			*RelfilenodeMapHash;
	HASHCTL			relfilenodeHashCtl;
	HTAB			*TablespaceMapHash;
	HASHCTL			tablespaceHashCtl;
	Oid				nspoid;
	Oid				roleid;
	int				numTablespaces; /* number of tablespaces to be scaned */
	char			*tableSpaces[MAX_GP_TABLESPACE];
	int				curTableSpace;	/* the current scan dirs */
	struct dirent	*pent;
	bool			skip_readdir;
} GpScanData;
#endif							/* RELFILENODEMAP_H */
