/*
 * $Id: TimesTen.h 530 2006-11-25 21:39:51Z wagnerch $
 * Copyright (c) 1994,1995,1996,1997  Tim Bunce
 *
 * You may distribute under the terms of either the GNU General Public
 * License or the Artistic License, as specified in the Perl README file.
 */

#include <timesten.h>

#define NEED_DBIXS_VERSION 9

#include <DBIXS.h>	/* from DBI. Load this after dbdodbc.h */

#include "dbivport.h" /* copied from DBI to maintain compatibility */

#include "dbdimp.h"

#include <dbd_xsh.h>	/* from DBI. Load this after dbdodbc.h */

SV      *dbd_db_get_info _((SV *dbh, int ftype));
int      dbd_db_type_info _((SV *dbh, SV *sth, int ftype));
SV	*dbd_st_cancel _((SV *sth));
int	 dbd_db_column_info _((SV *dbh, SV *sth,
	    char *catalog, char *schema, char *table, char *column));

int  dbd_db_table_info _((SV *dbh, SV *sth, char *catalog, char *schema, char *table, char *table_type));
int  dbd_db_primary_key_info _((SV *dbh, SV *sth, char *catalog, char *schema, char *table));
int  dbd_db_foreign_key_info _((SV *dbh, SV *sth, char *PK_CatalogName, char *PK_SchemaName, char *PK_TableName, char *FK_CatalogName, char *FK_SchemaName, char *FK_TableName));
void dbd_error _((SV *h, RETCODE err_rc, char *what));
void dbd_error2 _((SV *h, RETCODE err_rc, char *what, HENV henv, HDBC hdbc, HSTMT hstmt));
int dbd_db_execdirect _(( SV *dbh, char *statement ));


/* end of ODBC.h */
