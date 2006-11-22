#include "TimesTen.h"

DBISTATE_DECLARE;

MODULE = DBD::TimesTen    PACKAGE = DBD::TimesTen

INCLUDE: TimesTen.xsi

MODULE = DBD::TimesTen    PACKAGE = DBD::TimesTen::st

void 
_ColAttributes(sth, colno, ftype)
	SV *	sth
	int		colno
	int		ftype
	CODE:
	ST(0) = timesten_col_attributes(sth, colno, ftype);

void
_Cancel(sth)
    SV *	sth

    CODE:
	ST(0) = timesten_cancel(sth);		

void
_tables(dbh, sth, catalog, schema, table, type)
	SV *	dbh
	SV *	sth
	char *	catalog
	char *	schema
	char *  table
	char *	type
	CODE:
	/* list all tables and views (0 as last parameter) */
	ST(0) = dbd_st_tables(dbh, sth, catalog, schema, table, type) ? &sv_yes : &sv_no;

void
_primary_keys(dbh, sth, catalog, schema, table)
    SV * 	dbh
    SV *	sth
    char *	catalog
    char *	schema
    char *	table
    CODE:
    ST(0) = dbd_st_primary_keys(dbh, sth, catalog, schema, table) ? &sv_yes : &sv_no;


void
DescribeCol(sth, colno)
	SV *sth
	int colno

	PPCODE:

	char ColumnName[SQL_MAX_COLUMN_NAME_LEN];
	I16 NameLength;
	I16 DataType;
	U32 ColumnSize;
	I16 DecimalDigits;
	I16 Nullable;
	int rc;

	rc = timesten_describe_col(sth, colno, ColumnName, sizeof(ColumnName), &NameLength,
			&DataType, &ColumnSize, &DecimalDigits, &Nullable);
	if (rc) {
		XPUSHs(newSVpv(ColumnName, 0));
		XPUSHs(newSViv(DataType));
		XPUSHs(newSViv(ColumnSize));
		XPUSHs(newSViv(DecimalDigits));
		XPUSHs(newSViv(Nullable));
	}

# ------------------------------------------------------------
# database level interface
# ------------------------------------------------------------
MODULE = DBD::TimesTen    PACKAGE = DBD::TimesTen::db

void
_ExecDirect( dbh, stmt )
SV *        dbh
SV *        stmt
CODE:
{
   STRLEN lna;
   char *pstmt = SvOK(stmt) ? SvPV(stmt,lna) : "";
   ST(0) = sv_2mortal(newSViv( (IV)dbd_db_execdirect( dbh, pstmt ) ) );
}


void
_columns(dbh, sth, catalog, schema, table, column)
	SV *	dbh
	SV *	sth
	char *	catalog
	char *	schema
	char *	table
	char *	column
	CODE:
	ST(0) = timesten_db_columns(dbh, sth, catalog, schema, table, column) ? &sv_yes : &sv_no;

void 
_GetInfo(dbh, ftype)
	SV *	dbh
	int		ftype
	CODE:
	ST(0) = timesten_get_info(dbh, ftype);

void
_GetTypeInfo(dbh, sth, ftype)
	SV *	dbh
	SV *	sth
	int		ftype
	CODE:
	ST(0) = timesten_get_type_info(dbh, sth, ftype) ? &sv_yes : &sv_no;

void 
_GetStatistics(dbh, sth, CatalogName, SchemaName, TableName, Unique)
	SV *	dbh
	SV *	sth
	char *	CatalogName
	char *	SchemaName
	char *	TableName
	int		Unique
	CODE:
	ST(0) = timesten_get_statistics(dbh, sth, CatalogName, SchemaName, TableName, Unique) ? &sv_yes : &sv_no;

void 
_GetPrimaryKeys(dbh, sth, CatalogName, SchemaName, TableName)
	SV *	dbh
	SV *	sth
	char *	CatalogName
	char *	SchemaName
	char *	TableName
	CODE:
	ST(0) = timesten_st_primary_keys(dbh, sth, CatalogName, SchemaName, TableName) ? &sv_yes : &sv_no;

void 
_GetSpecialColumns(dbh, sth, Identifier, CatalogName, SchemaName, TableName, Scope, Nullable)
	SV *	dbh
	SV *	sth
	int     Identifier
	char *	CatalogName
	char *	SchemaName
	char *	TableName
    int     Scope
    int     Nullable
	CODE:
	ST(0) = timesten_get_special_columns(dbh, sth, Identifier, CatalogName, SchemaName, TableName, Scope, Nullable) ? &sv_yes : &sv_no;

void 
_GetForeignKeys(dbh, sth, PK_CatalogName, PK_SchemaName, PK_TableName, FK_CatalogName, FK_SchemaName, FK_TableName)
	SV *	dbh
	SV *	sth
	char *	PK_CatalogName
	char *	PK_SchemaName
	char *	PK_TableName
	char *	FK_CatalogName
	char *	FK_SchemaName
	char *	FK_TableName
	CODE:
	ST(0) = timesten_get_foreign_keys(dbh, sth, PK_CatalogName, PK_SchemaName, PK_TableName, FK_CatalogName, FK_SchemaName, FK_TableName) ? &sv_yes : &sv_no;

void
GetFunctions(dbh, func)
	SV *	dbh
	unsigned short func
	PPCODE:
	UWORD pfExists[100];
	RETCODE rc;
	int i;
	int j;
	D_imp_dbh(dbh);
	rc = SQLGetFunctions(imp_dbh->hdbc, func, pfExists);
	if (SQL_ok(rc)) {
	   switch (func) {
	      case SQL_API_ALL_FUNCTIONS:
			for (i = 0; i < 100; i++) {
				XPUSHs(pfExists[i] ? &sv_yes : &sv_no);
			}
			break;
	      default:
		XPUSHs(pfExists[0] ? &sv_yes : &sv_no);
	   }
	}

MODULE = DBD::TimesTen    PACKAGE = DBD::TimesTen::db

MODULE = DBD::TimesTen    PACKAGE = DBD::TimesTen::dr

void
data_sources(drh, attr = NULL)
	SV* drh;
    SV* attr;
  PROTOTYPE: $;$
  PPCODE:
    {
//#ifdef DBD_ODBC_NO_DATASOURCES
#if 1
		/*	D_imp_drh(drh);
			imp_drh->henv = SQL_NULL_HENV;
			dbd_error(drh, (RETCODE) SQL_ERROR, "data_sources: SOLID doesn't implement SQLDataSources()");*/
		XSRETURN(0);
#else
	int numDataSources = 0;
	UWORD fDirection = SQL_FETCH_FIRST;
	RETCODE rc;
        UCHAR dsn[SQL_MAX_DSN_LENGTH+1+13 /* strlen("DBI:TimesTen:") */];
        SWORD dsn_length;
        UCHAR description[256];
        SWORD description_length;
	D_imp_drh(drh);

	if (!imp_drh->connects) {
	    rc = SQLAllocEnv(&imp_drh->henv);
	    if (!SQL_ok(rc)) {
		imp_drh->henv = SQL_NULL_HENV;
		dbd_error(drh, rc, "data_sources/SQLAllocEnv");
		XSRETURN(0);
	    }
	}
	strcpy(dsn, "DBI:TimesTen:");
	while (1) {
            rc = SQLDataSources(imp_drh->henv, fDirection,
                                dsn+13, /* strlen("DBI:TimesTen:") */
                                SQL_MAX_DSN_LENGTH, 
								&dsn_length,
                                description, sizeof(description),
                                &description_length);
       	    if (!SQL_ok(rc)) {
                if (rc != SQL_NO_DATA_FOUND) {
		    /*
		     *  Temporarily increment imp_drh->connects, so
		     *  that dbd_error uses our henv.
		     */
		    imp_drh->connects++;
		    dbd_error(drh, rc, "data_sources/SQLDataSources");
		    imp_drh->connects--;
                }
                break;
            }
            ST(numDataSources++) = newSVpv(dsn, dsn_length+13 /* strlen("dbi:TimesTen:") */ );
	    fDirection = SQL_FETCH_NEXT;
	}
	if (!imp_drh->connects) {
	    SQLFreeEnv(imp_drh->henv);
	    imp_drh->henv = SQL_NULL_HENV;
	}
	XSRETURN(numDataSources);
#endif /* no data sources */
    }

