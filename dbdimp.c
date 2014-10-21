/* $Id: dbdimp.c 547 2006-11-26 23:05:14Z wagnerch $
 * 
 * portions Copyright (c) 1994,1995,1996,1997  Tim Bunce
 * portions Copyright (c) 1997 Thomas K. Wenrich
 * portions Copyright (c) 1997-2001 Jeff Urlwin
 * portions Copyright (c) 2006 Chad Wagner
 *
 * You may distribute under the terms of either the GNU General Public
 * License or the Artistic License, as specified in the Perl README file.
 *
 */

#include "TimesTen.h"

static const char *S_SqlTypeToString (SWORD sqltype);
static const char *S_SqlCTypeToString (SWORD sqltype);
static const char *cSqlTables = "SQLTables(%s,%s,%s,%s)";
static const char *cSqlPrimaryKeys = "SQLPrimaryKeys(%s,%s,%s)";
static const char *cSqlForeignKeys = "SQLForeignKeys(%s,%s,%s,%s,%s,%s)";
static const char *cSqlColumns = "SQLColumns(%s,%s,%s,%s)";
static const char *cSqlGetTypeInfo = "SQLGetTypeInfo(%d)";
static void        dbd_error_flush(SV *h);
static int         dbd_describe(SV *h, imp_sth_t *imp_sth);
static int         build_results(SV *sth, RETCODE orc);
static void        dbd_preparse(imp_sth_t *imp_sth, char *statement);
static int         _dbd_rebind_ph(SV *sth, imp_sth_t *imp_sth, phs_t *phs);
static void        _dbd_get_param_type(SV *sth, imp_sth_t *imp_sth, phs_t *phs);


/* for sanity/ease of use with potentially null strings */
#define XXSAFECHAR(p) ((p) ? (p) : "(null)")

/* unique value for db attrib that won't conflict with SQL types, just
 * increment by one if you are adding! */
#define TT_IGNORE_NAMED_PLACEHOLDERS   0x8332
#define TT_DEFAULT_BIND_TYPE           0x8333
#define TT_ROWCACHESIZE                0x8334
#define TT_EXEC_DIRECT                 0x8335
#define TT_QUERY_TIMEOUT               0x8336

/*
 * TT_DEFAULT_BIND_TYPE_VALUE is now set to 0, which means that
 * DBD::TimesTen will call SQLDescribeParam to find out what type of
 * binding should be set.  If SQLDescribeParam fails, then the bind type
 * will be set to SQL_VARCHAR as a backup.
 *
 */
#define TT_DEFAULT_BIND_TYPE_VALUE	0



DBISTATE_DECLARE;


void
   dbd_init(dbistate)
   dbistate_t *dbistate;
{
   DBIS = dbistate;
}

static RETCODE timesten_set_query_timeout(SV *h, HSTMT hstmt, UV query_timeout)
{
   RETCODE rc;
   D_imp_xxh(h);
   if (DBIc_TRACE_LEVEL(imp_xxh) >= 2) {
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "   Set timeout to: %d\n", query_timeout);
   }
   rc = SQLSetStmtOption(hstmt, SQL_QUERY_TIMEOUT, query_timeout);
   if (!SQL_ok(rc)) {
      /* raise warnings if setting fails, but don't die? */
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_xxh), "    Failed to set Statement ATTR Query Timeout to %d\n", (int)query_timeout);
   }
   return rc;
}

static void timesten_handle_outparams(imp_sth_t *imp_sth, int debug)
{
   int i = (imp_sth->out_params_av) ? AvFILL(imp_sth->out_params_av)+1 : 0;
   if (debug >= 3)
      PerlIO_printf(DBIc_LOGPIO(imp_sth),
		    "       handling %d output parameters\n", i);
   while (--i >= 0) {
      phs_t *phs = (phs_t*)(void*)SvPVX(AvARRAY(imp_sth->out_params_av)[i]);
      SV *sv = phs->sv;
      if (debug >= 8) {
	 PerlIO_printf(DBIc_LOGPIO(imp_sth),
		       "       out %s has length of %d\n",
		       phs->name, phs->cbValue);
      }

      /* phs->cbValue has been updated by ODBC to hold the length of the result	*/
      if (phs->cbValue != SQL_NULL_DATA) {	/* is okay	*/
	 /*
	  * When ODBC fills an output parameter buffer, the size of the
	  * data that were available is written into the memory location
	  * provided by cbValue pointer argument during the SQLBindParameter() call.
	  * (In this case, the cbValue pointer has been set to &phs->cbValue).
	  *
	  * If the number of bytes available exceeds the size of the output buffer,
	  * ODBC will truncate the data such that it fits in the available buffer.
	  * However, the cbValue will still reflect the size of the data before it
	  * was truncated.
	  *
	  * This fact provides us a way to detect truncation on this particular
	  * output parameter.  Otherwise, the only way to detect truncation is
	  * through a follow-up to a SQL_SUCCESS_WITH_INFO result.  Such a call
	  * cannot return enough information to state exactly where the truncation
	  * occurred.
	  * -jeremy
	  */

	 if (phs->cbValue > phs->maxlen) {
	    /* a truncation occurred */
	    SvPOK_only(sv);
	    SvCUR_set(sv, phs->maxlen);
	    *SvEND(sv) = '\0';

	    if (debug >= 2) {
	       PerlIO_printf(DBIc_LOGPIO(imp_sth),
			     "       out %s = '%s'\t(TRUNCATED from %d to %ld)\n",
			     phs->name, SvPV(sv,na), phs->cbValue, (long)phs->maxlen);
	    }
	 } else {
	    /* no truncation occurred */
	    SvPOK_only(sv);
	    SvCUR_set(sv, phs->cbValue);
	    *SvEND(sv) = '\0';
	    if (phs->cbValue == phs->maxlen &&
		(phs->sql_type == SQL_NUMERIC ||
		 phs->sql_type == SQL_DECIMAL ||
		 phs->sql_type == SQL_INTEGER ||
		 phs->sql_type == SQL_SMALLINT ||
		 phs->sql_type == SQL_FLOAT ||
		 phs->sql_type == SQL_REAL ||
		 phs->sql_type == SQL_DOUBLE)) {
	       /*
		* fix up for oracle, which leaves the buffer at the size
		* requested, but only returns a few characters.  The
		* intent is to truncate down to the actual number of
		* characters necessary.  Need to find the first null
		* byte and set the length there.
		*/
	       char *pstart = SvPV_nolen(sv);
	       char *p = pstart;
	       while (*p != '\0') {
		  p++;
	       }

	       if (debug >= 2) {
		  PerlIO_printf(DBIc_LOGPIO(imp_sth),
				"       out %s = '%s'\t(len %ld), is numeric end of buffer = %d\n",
				phs->name, SvPV(sv,na), (long)phs->cbValue, phs->sql_type, p - pstart);
	       }
	       SvCUR_set(sv, p - pstart);
	    }
	 }
      } else {			/* is NULL	*/
	 (void)SvOK_off(phs->sv);
	 if (debug >= 2)
	    PerlIO_printf(DBIc_LOGPIO(imp_sth),
			  "       out %s = undef (NULL)\n",
			  phs->name);
      }
   }
}

static int
   build_results(sth, orc)
   SV *	 sth;
RETCODE orc;
{
   RETCODE rc;
   D_imp_sth(sth);
   dTHR;

   if ((DBIc_TRACE_LEVEL(imp_sth)) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_sth), "    build_results sql f%d\n\t%s\n",
		    imp_sth->hstmt, imp_sth->statement);

   /* init sth pointers */
   imp_sth->fbh = NULL;
   imp_sth->ColNames = NULL;
   imp_sth->RowBuffer = NULL;
   imp_sth->RowCount = -1;
   imp_sth->eod = -1;

   if (!dbd_describe(sth, imp_sth)) {
      if (DBIc_TRACE_LEVEL(imp_sth) > 0) {
	 PerlIO_printf(DBIc_LOGPIO(imp_sth), "dbd_describe failed, build_results...!\n");
      }
      SQLFreeStmt(imp_sth->hstmt, SQL_DROP);
      imp_sth->hstmt = SQL_NULL_HSTMT;
      return 0; /* dbd_describe already called dbd_error()	*/
   }

   if (DBIc_TRACE_LEVEL(imp_sth) > 0) {
      PerlIO_printf(DBIc_LOGPIO(imp_sth), "dbd_describe build_results #2...!\n");
   }
   if (dbd_describe(sth, imp_sth) <= 0) {
      if (DBIc_TRACE_LEVEL(imp_sth) > 0) {
	 PerlIO_printf(DBIc_LOGPIO(imp_sth), "dbd_describe build_results #3...!\n");
      }
      
      return 0;
   }

   DBIc_IMPSET_on(imp_sth);

   if (orc != SQL_NO_DATA_FOUND) {
      imp_sth->RowCount = -1;
      rc = SQLRowCount(imp_sth->hstmt, &imp_sth->RowCount);
      dbd_error(sth, rc, "build_results/SQLRowCount");
      if (rc != SQL_SUCCESS) {
	 return -1;
      }
   } else {
      imp_sth->RowCount = 0;
   }

   DBIc_ACTIVE_on(imp_sth); /* XXX should only set for select ?	*/
   imp_sth->eod = SQL_SUCCESS;
   return 1;
}

int
   dbd_discon_all(drh, imp_drh)
   SV *drh;
imp_drh_t *imp_drh;
{
   dTHR;

   /* The disconnect_all concept is flawed and needs more work */
   if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
      sv_setiv(DBIc_ERR(imp_drh), (IV)1);
      sv_setpv(DBIc_ERRSTR(imp_drh),
	       (char*)"disconnect_all not implemented");
      DBIh_EVENT2(drh, ERROR_event,
		  DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
      return FALSE;
   }  
   return FALSE;
}


/* error : <=(-2), ok row count : >=0, unknown count : (-1)   */
int dbd_db_execdirect( SV *dbh,
		       char *statement )
{
   D_imp_dbh(dbh);
   SQLRETURN ret;
   SQLINTEGER rows;
   SQLHSTMT stmt;

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(dbh, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   ret = SQLAllocStmt( imp_dbh->hdbc, &stmt );
   if (!SQL_ok(ret)) {
      dbd_error(dbh, ret, "dbd_db_execdirect/SQLAllocStmt");
      return(-2);
   }

   if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    SQLExecDirect sql %s\n",
		    statement);

   if (imp_dbh->ttQueryTimeout) {
      ret = timesten_set_query_timeout(dbh, stmt, imp_dbh->ttQueryTimeout);
      if (!SQL_ok(ret)) {
	 dbd_error(dbh, ret, "dbd_db_execdirect/set_query_timeout");
      }
      /* don't fail if the query timeout can't be set. */
   }
   ret = SQLExecDirect(stmt, (SQLCHAR *)statement, SQL_NTS);
   if (DBIc_TRACE_LEVEL(imp_dbh) >= 2) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "    dbd_db_execdirect (rc = %d)...\n", ret);
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }
   if (!SQL_ok(ret) && ret != SQL_NO_DATA_FOUND) {
      dbd_error(dbh, ret, "dbd_db_execdirect/SQLExecDirect");
      if (ret < 0)  {
	 rows = -2;
      }
      else {
	 rows = -3; /* ?? */
      }
   }
   else {
      if (ret == SQL_NO_DATA_FOUND) {
	 rows = 0;
      } else {
	 ret = SQLRowCount(stmt, &rows);
	 if (!SQL_ok(ret)) {
	    dbd_error(dbh, ret, "dbd_db_execdirect/SQLRowCount");
	    if (ret < 0)
	       rows = -1;
	 }
      }
   }
   ret = SQLFreeStmt(stmt, SQL_DROP);
   if (!SQL_ok(ret)) {
      dbd_error(dbh, ret, "dbd_db_execdirect/SQLFreeStmt");
   }

   return (int)rows;
} 

void
   dbd_db_destroy(dbh, imp_dbh)
   SV *dbh;
imp_dbh_t *imp_dbh;
{
#if 0
   PerlIO_printf(DBIc_LOGPIO(imp_dbh), "  dbd_db_destroy (%d)\n", imp_dbh->com.std.kids);
#endif
   if (DBIc_ACTIVE(imp_dbh))
      dbd_db_disconnect(dbh, imp_dbh);
   /* Nothing in imp_dbh to be freed	*/

   DBIc_IMPSET_off(imp_dbh);
   if (DBIc_TRACE_LEVEL(imp_dbh) >= 8) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "  DBD::TimesTen Disconnected!\n");
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }
}


/*------------------------------------------------------------
connecting to a data source.
Allocates henv and hdbc.
------------------------------------------------------------*/
int
   dbd_db_login(dbh, imp_dbh, dbname, uid, pwd)
   SV *dbh; imp_dbh_t *imp_dbh; char *dbname; char *uid; char *pwd;
{
   return dbd_db_login6(dbh, imp_dbh, dbname, uid, pwd, Nullsv);
}

int
   dbd_db_login6(dbh, imp_dbh, dbname, uid, pwd, attr)
   SV *dbh;
imp_dbh_t *imp_dbh;
char *dbname;
char *uid;
char *pwd;
SV   *attr;
{
   D_imp_drh_from_dbh;
   /* int ret; */
   dTHR;

   RETCODE rc;

   char dbname_local[512];

   /*
    * for SQLDriverConnect
    */
   char szConnStrOut[2048];
   SQLSMALLINT cbConnStrOut;
   SV **query_timeout_sv;
   UV   query_timeout = 0;

   if (!imp_drh->connects) {
      rc = SQLAllocEnv(&imp_drh->henv);		
      dbd_error(dbh, rc, "db_login/SQLAllocEnv");
      if (!SQL_ok(rc))
	 return 0;
   }
   imp_dbh->henv = imp_drh->henv;	/* needed for dbd_error */

   rc = SQLAllocConnect(imp_drh->henv, &imp_dbh->hdbc);
   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "db_login/SQLAllocConnect");
      if (imp_drh->connects == 0) {
	 SQLFreeEnv(imp_drh->henv);
	 imp_drh->henv = SQL_NULL_HENV;
      }
      return 0;
   }

   /*
    * SQLDriverConnect handles/maps/fixes db connections and can optionally
    * add a dialog box to the application.  
    */
   if (strlen (uid) > 0 && strlen (pwd) > 0)
   {
      sprintf(dbname_local, "%s;UID=%s;PWD=%s", dbname, uid, pwd);
      /*
       * Removed resetting these due to issues, potentially causing
       * errors connecting
       * uid=NULL;
       * pwd=NULL;
       */
      dbname = dbname_local;
   }

   if (DBIc_TRACE_LEVEL(imp_dbh) >= 8)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "Driver connect '%s', '%s', 'xxxx'\n", dbname, uid);

   rc = SQLDriverConnect(imp_dbh->hdbc,
			 0, /* no hwnd */
			 dbname,
			 (SQLSMALLINT)strlen(dbname),
			 szConnStrOut,
			 sizeof(szConnStrOut),
			 &cbConnStrOut,
			 SQL_DRIVER_NOPROMPT /* no dialog box (for now) */
			);

   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "db_login/SQLDriverConnect");
      SQLFreeConnect(imp_dbh->hdbc);
      if (imp_drh->connects == 0) {
	 SQLFreeEnv(imp_drh->henv);
	 imp_drh->henv = SQL_NULL_HENV;
      }
      return 0;
   } else if (rc == SQL_SUCCESS_WITH_INFO) {
      /* Consume informational diagnostics */
      dbd_error_flush(dbh);
   }

   /* DBI spec requires AutoCommit on */
   rc = SQLSetConnectOption(imp_dbh->hdbc,
			    SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON);
   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "dbd_db_login/SQLSetConnectOption");
      SQLFreeConnect(imp_dbh->hdbc);
      if (imp_drh->connects == 0) {
	 SQLFreeEnv(imp_drh->henv);
	 imp_drh->henv = SQL_NULL_HENV;
      }
      return 0;
   }

   /* default ignoring named parameters to false */
   imp_dbh->ttIgnoreNamedPlaceholders = 0;
   imp_dbh->ttDefaultBindType = TT_DEFAULT_BIND_TYPE_VALUE;
   imp_dbh->ttQueryTimeout = 0; /* default vaule for query timeout is indefinite (0) */
   imp_dbh->ttExecDirect = 0;	/* default to not having SQLExecDirect used */
   imp_dbh->RowCacheSize = 1;	/* default value for now */

   DBIc_set(imp_dbh,DBIcf_AutoCommit, 1);

   DBD_ATTRIB_GET_IV(attr, "ttQueryTimeout",strlen("ttQueryTimeout"), query_timeout_sv, query_timeout);
   if (query_timeout) {
      imp_dbh->ttQueryTimeout = query_timeout;
      if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    Setting DBH query timeout to %d\n", (int)query_timeout);
   }      

   imp_drh->connects++;
   DBIc_IMPSET_on(imp_dbh);	/* imp_dbh set up now			*/
   DBIc_ACTIVE_on(imp_dbh);	/* call disconnect before freeing	*/
   return 1;
}


int
   dbd_db_disconnect(dbh, imp_dbh)
   SV *dbh;
imp_dbh_t *imp_dbh;
{
   RETCODE rc;
   D_imp_drh_from_dbh;
   UDWORD autoCommit = 0;
   dTHR;

   /* We assume that disconnect will always work	*/
   /* since most errors imply already disconnected.	*/
#if 0
   PerlIO_printf(DBIc_LOGPIO(imp_dbh), "  dbd_db_disconnect\n");
#endif
   DBIc_ACTIVE_off(imp_dbh);

   /* If not autocommit, should we rollback?  I don't think that's
    * appropriate.  -- TBD: Need to check this, maybe we should
    * rollback?
    */

   rc = SQLGetConnectOption(imp_dbh->hdbc, SQL_AUTOCOMMIT, &autoCommit);
   /* quietly handle a problem with SQLGetConnectOption() */
   if (!SQL_ok(rc) || rc == SQL_SUCCESS_WITH_INFO) {
      dbd_error_flush(dbh);
   }
   else {
      if (!autoCommit) {
	 rc = dbd_db_rollback(dbh, imp_dbh);
	 if (DBIc_TRACE_LEVEL(imp_dbh) > 1) {
	    PerlIO_printf(DBIc_LOGPIO(imp_dbh), "** auto-rollback due to disconnect without commit returned %d\n", rc);
	 }
      }
   }
   rc = SQLDisconnect(imp_dbh->hdbc);
   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "dbd_db_disconnect/SQLDisconnect");
      /* return 0;	XXX if disconnect fails, fall through... */
   }

   SQLFreeConnect(imp_dbh->hdbc);
   imp_dbh->hdbc = SQL_NULL_HDBC;
   imp_drh->connects--;
   if (imp_drh->connects == 0) {
      SQLFreeEnv(imp_drh->henv);
      imp_drh->henv = SQL_NULL_HENV;
   }
   /* We don't free imp_dbh since a reference still exists	*/
   /* The DESTROY method is the only one to 'free' memory.	*/
   /* Note that statement objects may still exists for this dbh!	*/

   return 1;
}


int
   dbd_db_commit(dbh, imp_dbh)
   SV *dbh;
imp_dbh_t *imp_dbh;
{
   RETCODE rc;
   dTHR;

   rc = SQLTransact(imp_dbh->henv, imp_dbh->hdbc, SQL_COMMIT); 
   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "db_commit/SQLTransact");
      return 0;
   }
   /* support for DBI 1.20 begin_work */
   if (DBIc_has(imp_dbh, DBIcf_BegunWork)) {
      /* reset autocommit */
      rc = SQLSetConnectOption(imp_dbh->hdbc, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON);
      DBIc_off(imp_dbh,DBIcf_BegunWork);
   }
   return 1;
}

int
   dbd_db_rollback(dbh, imp_dbh)
   SV *dbh;
imp_dbh_t *imp_dbh;
{
   RETCODE rc;
   dTHR;

   rc = SQLTransact(imp_dbh->henv, imp_dbh->hdbc, SQL_ROLLBACK);
   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "db_rollback/SQLTransact");
      return 0;
   }
   /* support for DBI 1.20 begin_work */
   if (DBIc_has(imp_dbh, DBIcf_BegunWork)) {
      /*  reset autocommit */
      rc = SQLSetConnectOption(imp_dbh->hdbc, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON);
      DBIc_off(imp_dbh,DBIcf_BegunWork);
   }
   return 1;
}

/*------------------------------------------------------------
replacement for odbc_error.
empties entire ODBC error queue.
------------------------------------------------------------*/
void
   dbd_error(h, err_rc, what)
   SV *h;
RETCODE err_rc;
char *what;
{
   D_imp_xxh(h);
   dTHR;
   SV *errstr;

   struct imp_dbh_st *imp_dbh = NULL;
   struct imp_sth_st *imp_sth = NULL;
   HENV henv = SQL_NULL_HENV;
   HDBC hdbc = SQL_NULL_HDBC;
   HSTMT hstmt = SQL_NULL_HSTMT;

   switch(DBIc_TYPE(imp_xxh)) {
      case DBIt_ST:
	 imp_sth = (struct imp_sth_st *)(imp_xxh);
	 imp_dbh = (struct imp_dbh_st *)(DBIc_PARENT_COM(imp_sth));
	 hstmt = imp_sth->hstmt;
	 break;
      case DBIt_DB:
	 imp_dbh = (struct imp_dbh_st *)(imp_xxh);
	 break;
      default:
	 croak("panic: dbd_error on bad handle type");
   }

   /*
    * If status is SQL_SUCCESS, there's no error, so we can just return.
    * There may be status or other non-error messsages though.
    * We want those messages if the debug level is set to at least 3.
    * If an error handler is installed, let it decide what messages
    * should or shouldn't be reported.
    */
   if (err_rc == SQL_SUCCESS && DBIc_TRACE_LEVEL(imp_dbh) < 3)
      return; 

   /* We will always have imp_dbh at this point. */
   henv = imp_dbh->henv;
   hdbc = imp_dbh->hdbc;

   errstr = DBIc_ERRSTR(imp_xxh);
   sv_setpvn(errstr, "", 0);
   sv_setpvn(DBIc_STATE(imp_xxh), "00000", 5);

   while(henv != SQL_NULL_HENV) {
      UCHAR sqlstate[SQL_SQLSTATE_SIZE+1];
      UCHAR ErrorMsg[SQL_MAX_MESSAGE_LENGTH];
      SWORD ErrorMsgLen;
      SDWORD NativeError;
      RETCODE rc = 0;

      if (DBIc_TRACE_LEVEL(imp_dbh) >= 3)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "dbd_error: err_rc=%d rc=%d s/d/e: %d/%d/%d\n", 
		       err_rc, rc, hstmt,hdbc,henv);

      while( (rc=SQLError(henv, hdbc, hstmt,
			  sqlstate, &NativeError,
			  ErrorMsg, sizeof(ErrorMsg)-1, &ErrorMsgLen
			 )) == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
	 sv_setpvn(DBIc_STATE(imp_xxh), sqlstate, 5);

	 if (SvCUR(errstr) > 0) {
	    sv_catpv(errstr, "\n");
	    /* JLU: attempt to get a reasonable error	*/
	    /* from first SQLError result on lowest handle	*/
	    sv_setpv(DBIc_ERR(imp_xxh), sqlstate);
	 }
	 sv_catpvn(errstr, ErrorMsg, ErrorMsgLen);
	 sv_catpv(errstr, " (SQL-");
	 sv_catpv(errstr, sqlstate);
	 sv_catpv(errstr, ")");

	 if (DBIc_TRACE_LEVEL(imp_dbh) >= 3)
	    PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
			  "dbd_error: SQL-%s (native %d): %s\n",
			  sqlstate, NativeError, SvPVX(errstr));
      }
      if (rc != SQL_NO_DATA_FOUND) {	/* should never happen */
	 if (DBIc_TRACE_LEVEL(imp_xxh))
	    PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
			  "dbd_error: SQLError returned %d unexpectedly.\n", rc);
	 if (!SvTRUE(errstr)) { /* set some values to indicate the problem */
	    sv_setpvn(DBIc_STATE(imp_xxh), "IM008", 5); /* "dialog failed" */
	    sv_catpv(errstr, "(Unable to fetch information about the error)");
	 }
      }

      /* climb up the tree each time round the loop		*/
      if (hstmt != SQL_NULL_HSTMT)
         hstmt = SQL_NULL_HSTMT;
      else if (hdbc != SQL_NULL_HDBC)
         hdbc = SQL_NULL_HDBC;
      else
         henv = SQL_NULL_HENV;
   }

   if (!SQL_ok(err_rc) && err_rc != SQL_NO_DATA_FOUND) {
      /* Set DBIc_ERR here so that a non-error err_rc is not flagged as
       * an error.
       */
      sv_setiv(DBIc_ERR(imp_xxh), (IV)err_rc);
      if (what) {
	 char buf[10];
	 sprintf(buf, " err=%d", err_rc);
	 sv_catpv(errstr, "(DBD: ");
	 sv_catpv(errstr, what);
	 sv_catpv(errstr, buf);
	 sv_catpv(errstr, ")");
      }

      DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);

      if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "%s error %d recorded: %s\n",
		       what, err_rc, SvPV(errstr,na));
   }
}


/*-------------------------------------------------------------------------
dbd_preparse: 
- scan for placeholders (? and :xx style) and convert them to ?.
- builds translation table to convert positional parameters of the 
execute() call to :nn type placeholders.
We need two data structures to translate this stuff:
- a hash to convert positional parameters to placeholders
- an array, representing the actual '?' query parameters.
%param = (name1=>plh1, name2=>plh2, ..., name_n=>plh_n)   #
@qm_param = (\$param{'name1'}, \$param{'name2'}, ...) 
-------------------------------------------------------------------------*/
static void
   dbd_preparse(imp_sth, statement)
   imp_sth_t *imp_sth;
char *statement;
{
   dTHR;
   bool in_literal = FALSE;
   char *src, *start, *dest;
   phs_t phs_tpl, *phs;
   SV *phs_sv;
   int idx=0, style=0, laststyle=0;
   STRLEN namelen;
   char name[256];
   SV **svpp;
   char ch;
   char literal_ch = '\0';

   /* allocate room for copy of statement with spare capacity	*/
   imp_sth->statement = (char*)safemalloc(strlen(statement)+1);

   /* initialize phs ready to be cloned per placeholder	*/
   memset(&phs_tpl, 0, sizeof(phs_tpl));
   phs_tpl.ftype = 1;	/* VARCHAR2 */
   phs_tpl.sv = &sv_undef;

   src  = statement;
   dest = imp_sth->statement;
   if (DBIc_TRACE_LEVEL(imp_sth) >= 5)
      PerlIO_printf(DBIc_LOGPIO(imp_sth), "    ignore named placeholders = %d\n",
		    imp_sth->ttIgnoreNamedPlaceholders);
   while(*src) {
      /*
       * JLU 10/6/2000 fixed to make the literal a " instead of '
       * JLU 1/28/2001 fixed to make literals either " or ', but deal
       * with ' "foo" ' or " foo's " correctly (just to be safe).
       * 
       */
      if (*src == '"' || *src == '\'') {
	 if (!in_literal) {
	    literal_ch = *src;
	    in_literal = 1;
	 } else {
	    if (*src == literal_ch) {
	       in_literal = 0;
	    }
	 }
      }
      if ((*src != ':' && *src != '?') || in_literal) {
	 *dest++ = *src++;
	 continue;
      }
      start = dest;			/* save name inc colon	*/ 
      ch = *src++;
      if (ch == '?') {                /* X/Open standard	*/ 
	 idx++;
	 sprintf(name, "%d", idx);
	 *dest++ = ch;
	 style = 3;
      }
      else if (isDIGIT(*src)) {       /* ':1'		*/
	 char *p = name;
	 *dest++ = '?';
	 idx = atoi(src);
	 while(isDIGIT(*src))
	    *p++ = *src++;
	 *p = 0;
	 style = 1;
      } 
      else if (!imp_sth->ttIgnoreNamedPlaceholders && isALNUM(*src)) {
	 /* ':foo' is valid, only if we are ignoring named
	  * parameters
	  */
	 char *p = name;
	 *dest++ = '?';

	 while(isALNUM(*src))	/* includes '_'	*/
	    *p++ = *src++;
	 *p = 0;
	 if (DBIc_TRACE_LEVEL(imp_sth) >= 5)
	    PerlIO_printf(DBIc_LOGPIO(imp_sth), "    found named parameter = %s\n",
			  name);
	 style = 2;
      } 
      else {			/* perhaps ':=' PL/SQL construct */
	 *dest++ = ch;
	 continue;
      }
      *dest = '\0';			/* handy for debugging	*/
      if (laststyle && style != laststyle)
	 croak("Can't mix placeholder styles (%d/%d)",style,laststyle);
      laststyle = style;

      if (imp_sth->all_params_hv == NULL)
	 imp_sth->all_params_hv = newHV();
      namelen = strlen(name);

      svpp = hv_fetch(imp_sth->all_params_hv, name, namelen, 0);
      if (svpp == NULL) {
	 /* create SV holding the placeholder */
	 phs_sv = newSVpv((char*)&phs_tpl, sizeof(phs_tpl)+namelen+1);
	 phs = (phs_t*)SvPVX(phs_sv);
	 strcpy(phs->name, name);
	 phs->idx = idx;

	 /* store placeholder to all_params_hv */
	 svpp = hv_store(imp_sth->all_params_hv, name, namelen, phs_sv, 0);
      }
   }
   *dest = '\0';
   if (imp_sth->all_params_hv) {
      DBIc_NUM_PARAMS(imp_sth) = (int)HvKEYS(imp_sth->all_params_hv);
      if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_sth), "    dbd_preparse scanned %d distinct placeholders\n",
		       (int)DBIc_NUM_PARAMS(imp_sth));
   }
}


int
   dbd_db_table_info(dbh, sth, catalog, schema, table, table_type)
   SV *dbh;
SV *sth;
char *catalog;
char *schema;
char *table;
char *table_type;
{
   D_imp_dbh(dbh);
   D_imp_sth(sth);
   RETCODE rc;
   dTHR;

   imp_sth->henv = imp_dbh->henv;	/* needed for dbd_error */
   imp_sth->hdbc = imp_dbh->hdbc;

   imp_sth->done_desc = 0;

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   rc = SQLAllocStmt(imp_dbh->hdbc, &imp_sth->hstmt);
   if (rc != SQL_SUCCESS) {
      dbd_error(sth, rc, "dbd_db_table_info/SQLAllocStmt");
      return 0;
   }

   /* just for sanity, later.  Any internals that may rely on this (including */
   /* debugging) will have valid data */
   imp_sth->statement = (char *)safemalloc(strlen(cSqlTables)+
					   strlen(XXSAFECHAR(catalog)) +
					   strlen(XXSAFECHAR(schema)) +
					   strlen(XXSAFECHAR(table)) +
					   strlen(XXSAFECHAR(table_type))+1);
   sprintf(imp_sth->statement, cSqlTables, XXSAFECHAR(catalog),
	   XXSAFECHAR(schema), XXSAFECHAR(table), XXSAFECHAR(table_type));

   rc = SQLTables(imp_sth->hstmt,
		  (catalog && *catalog) ? catalog : 0, SQL_NTS,
		  (schema && *schema) ? schema : 0, SQL_NTS,
		  (table && *table) ? table : 0, SQL_NTS,
		  table_type && *table_type ? table_type : 0, SQL_NTS		/* type (view, table, etc) */
		 );

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "   Tables result %d (%s)\n",
		    rc, table_type ? table_type : "(null)");

   dbd_error(sth, rc, "dbd_db_table_info/SQLTables");
   if (!SQL_ok(rc)) {
      SQLFreeStmt(imp_sth->hstmt, SQL_DROP);
      imp_sth->hstmt = SQL_NULL_HSTMT;
      return 0;
   }

   return build_results(sth,rc);
}

int
   dbd_db_primary_key_info(dbh, sth, catalog, schema, table)
   SV *dbh;
SV *sth;
char *catalog;
char *schema;
char *table;
{
   dTHR;
   D_imp_dbh(dbh);
   D_imp_sth(sth);
   RETCODE rc;

   imp_sth->henv = imp_dbh->henv;	/* needed for dbd_error */
   imp_sth->hdbc = imp_dbh->hdbc;

   imp_sth->done_desc = 0;

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   rc = SQLAllocStmt(imp_dbh->hdbc, &imp_sth->hstmt);
   if (rc != SQL_SUCCESS) {
      dbd_error(sth, rc, "dbd_db_primary_key_info/SQLAllocStmt");
      return 0;
   }

   /* just for sanity, later.  Any internals that may rely on this (including */
   /* debugging) will have valid data */
   imp_sth->statement = (char *)safemalloc(strlen(cSqlPrimaryKeys)+
					   strlen(XXSAFECHAR(catalog))+
					   strlen(XXSAFECHAR(schema))+
					   strlen(XXSAFECHAR(table))+1);

   sprintf(imp_sth->statement,
	   cSqlPrimaryKeys, XXSAFECHAR(catalog), XXSAFECHAR(schema),
	   XXSAFECHAR(table));

   rc = SQLPrimaryKeys(imp_sth->hstmt,
		       (catalog && *catalog) ? catalog : 0, SQL_NTS,
		       (schema && *schema) ? schema : 0, SQL_NTS,
		       (table && *table) ? table : 0, SQL_NTS);

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "SQLPrimaryKeys call: cat = %s, schema = %s, table = %s\n",
		    XXSAFECHAR(catalog), XXSAFECHAR(schema), XXSAFECHAR(table));

   dbd_error(sth, rc, "dbd_db_primary_key_info/SQLPrimaryKeys");

   if (!SQL_ok(rc)) {
      SQLFreeStmt(imp_sth->hstmt, SQL_DROP);
      imp_sth->hstmt = SQL_NULL_HSTMT;
      return 0;
   }

   return build_results(sth,rc);
}

int
   dbd_st_prepare(sth, imp_sth, statement, attribs)
   SV *sth;
imp_sth_t *imp_sth;
char *statement;
SV *attribs;
{
   dTHR;
   D_imp_dbh_from_sth;
   RETCODE rc;

   imp_sth->done_desc = 0;
   imp_sth->henv = imp_dbh->henv;	/* needed for dbd_error */
   imp_sth->hdbc = imp_dbh->hdbc;
   imp_sth->ttIgnoreNamedPlaceholders = imp_dbh->ttIgnoreNamedPlaceholders;
   imp_sth->ttDefaultBindType = imp_dbh->ttDefaultBindType;
   imp_sth->ttQueryTimeout = imp_dbh->ttQueryTimeout;
   if (DBIc_TRACE_LEVEL(imp_dbh) >= 5)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    initializing sth query timeout to %d\n", (int)imp_dbh->ttQueryTimeout);

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, 0, "Can not allocate statement when disconnected from the database");
   }

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   rc = SQLAllocStmt(imp_dbh->hdbc, &imp_sth->hstmt);
   if (!SQL_ok(rc)) {
      dbd_error(sth, rc, "dbd_st_prepare/SQLAllocStmt");
      return 0;
   }

   imp_sth->ttExecDirect = imp_dbh->ttExecDirect;

   {
      SV **exec_direct_sv;
      /* if the attribute is there, let it override what the default
       * value from the dbh is (set above).
       */
      if ((exec_direct_sv = DBD_ATTRIB_GET_SVP(attribs, "ttExecDirect", strlen("ttExecDirect"))) != NULL) {
	 imp_sth->ttExecDirect = SvIV(*exec_direct_sv) != 0;
      }
   }
   /* scan statement for '?', ':1' and/or ':foo' style placeholders	*/
   dbd_preparse(imp_sth, statement);

   /* Hold this statement for subsequent call of dbd_execute */
   if (!imp_sth->ttExecDirect) {
      /* parse the (possibly edited) SQL statement */
      rc = SQLPrepare(imp_sth->hstmt, 
		      imp_sth->statement, strlen(imp_sth->statement));
      if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    SQLPrepare returned %d\n\n",
		       rc);

      if (!SQL_ok(rc)) {
	 dbd_error(sth, rc, "dbd_st_prepare/SQLPrepare");
	 SQLFreeStmt(imp_sth->hstmt, SQL_DROP);
	 imp_sth->hstmt = SQL_NULL_HSTMT;
	 return 0;
      }
   }
   if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    dbd_st_prepare'd sql f%d, ExecDirect=%d\n\t%s\n",
		    imp_sth->hstmt, imp_sth->ttExecDirect, imp_sth->statement);

   /* init sth pointers */
   imp_sth->henv = imp_dbh->henv;
   imp_sth->hdbc = imp_dbh->hdbc;
   imp_sth->fbh = NULL;
   imp_sth->ColNames = NULL;
   imp_sth->RowBuffer = NULL;
   imp_sth->RowCount = -1;
   imp_sth->eod = -1;

   /* 
    * If ttQueryTimeout is set
    * we need to set the SQL_ATTR_QUERY_TIMEOUT
    */
   if (imp_sth->ttQueryTimeout){
      timesten_set_query_timeout(sth, imp_sth->hstmt, imp_sth->ttQueryTimeout);
      if (!SQL_ok(rc)) {
	 dbd_error(sth, rc, "dbd_st_prepare/set_query_timeout");
      }
      /* don't fail if the query timeout can't be set. */
   }

   DBIc_IMPSET_on(imp_sth);
   return 1;
}


static const char *
   S_SqlTypeToString (SWORD sqltype)
{
   switch(sqltype) {
      case SQL_CHAR:	return "CHAR";
      case SQL_NUMERIC:	return "NUMERIC";
      case SQL_DECIMAL:	return "DECIMAL";
      case SQL_INTEGER:	return "INTEGER";
      case SQL_SMALLINT:	return "SMALLINT";
      case SQL_FLOAT:	return "FLOAT";
      case SQL_REAL:	return "REAL";
      case SQL_DOUBLE:	return "DOUBLE";
      case SQL_VARCHAR:	return "VARCHAR";
      case SQL_DATE:	return "DATE";
      case SQL_TIME:	return "TIME";
      case SQL_TIMESTAMP:	return "TIMESTAMP";
      case SQL_BINARY:	return "BINARY";
      case SQL_VARBINARY: return "VARBINARY";
      case SQL_BIGINT:	return "BIGINT";
      case SQL_TINYINT:	return "TINYINT";
      case SQL_BIT:	return "BIT";
   }
   return "unknown";
}


static const char *
   S_SqlCTypeToString (SWORD sqltype)
{
   static char s_buf[100];
#define s_c(x) case x: return #x
   switch(sqltype) {
      s_c(SQL_C_CHAR);
      s_c(SQL_C_BIT);
      s_c(SQL_C_STINYINT);
      s_c(SQL_C_UTINYINT);
      s_c(SQL_C_SSHORT);
      s_c(SQL_C_USHORT);
      s_c(SQL_C_FLOAT);
      s_c(SQL_C_DOUBLE);
      s_c(SQL_C_BINARY);
      s_c(SQL_C_DATE);
      s_c(SQL_C_TIME);
      s_c(SQL_C_TIMESTAMP);
   }
#undef s_c
   sprintf(s_buf, "(unknown CType %d)", sqltype);
   return s_buf;
}


/*
 * describes the output variables of a query,
 * allocates buffers for result rows,
 * and binds this buffers to the statement.
 */
static int
   dbd_describe(h, imp_sth)
   SV *h;
imp_sth_t *imp_sth;

{
   dTHR;
   RETCODE rc;

   UCHAR *cbuf_ptr;		
   UCHAR *rbuf_ptr;		

   int t_cbufl=0;		/* length of all column names */
   int i;
   imp_fbh_t *fbh;
   int t_dbsize = 0;		/* size of native type */
   SWORD num_fields;
   struct imp_dbh_st *imp_dbh = NULL;
   imp_dbh = (struct imp_dbh_st *)(DBIc_PARENT_COM(imp_sth));

   if (imp_sth->done_desc)
      return 1;	/* success, already done it */

   if (DBIc_TRACE_LEVEL(imp_sth) >= 5) {
      PerlIO_printf(DBIc_LOGPIO(imp_sth), "    dbd_describe %d getting num fields\n",
		    imp_sth->hstmt);
      PerlIO_flush(DBIc_LOGPIO(imp_sth));
   }

   rc = SQLNumResultCols(imp_sth->hstmt, &num_fields);
   if (!SQL_ok(rc)) {
      dbd_error(h, rc, "dbd_describe/SQLNumResultCols");
      return 0;
   }

   /*
    * A little extra check to see if SQLMoreResults is supported
    * before trying to call it.  This is to work around some strange
    * behavior with SQLServer's driver and stored procedures which
    * insert data.
    * */
   imp_sth->done_desc = 1;	/* assume ok from here on */

   DBIc_NUM_FIELDS(imp_sth) = num_fields;

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    dbd_describe sql %d: num_fields=%d\n",
		    imp_sth->hstmt, DBIc_NUM_FIELDS(imp_sth));

   if (num_fields == 0) {
      if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		       "    dbd_describe skipped (no result cols) (sql f%d)\n",
		       imp_sth->hstmt);
      return 1;
   }

   /* allocate field buffers				*/
   Newz(42, imp_sth->fbh, num_fields, imp_fbh_t);

   /* Pass 1: Get space needed for field names, display buffer and dbuf */
   for (fbh=imp_sth->fbh, i=0; i < num_fields; i++, fbh++) {
      UCHAR ColName[256];

      fbh->imp_sth = imp_sth;
      memset(fbh->szDummyBuffer, 0, sizeof(fbh->szDummyBuffer));
      rc = SQLDescribeCol(imp_sth->hstmt, 
			  i+1, 
			  ColName, sizeof(ColName)-1,
			  &fbh->ColNameLen,
			  &fbh->ColSqlType,
			  &fbh->ColDef,
			  &fbh->ColScale,
			  &fbh->ColNullable);
      if (!SQL_ok(rc)) {	/* should never fail */
	 dbd_error(h, rc, "describe/SQLDescribeCol");
	 break;
      }

      ColName[fbh->ColNameLen] = '\0';

      t_cbufl += fbh->ColNameLen + 1;

      if (DBIc_TRACE_LEVEL(imp_sth) >= 8) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "   colname %d = %s len = %d (%d)\n", i+1, ColName, fbh->ColNameLen,
		       t_cbufl);
	 PerlIO_flush(DBIc_LOGPIO(imp_dbh));
      }


      rc = SQLColAttributes(imp_sth->hstmt,i+1,SQL_COLUMN_DISPLAY_SIZE,
			    NULL, 0, NULL ,&fbh->ColDisplaySize);
      if (!SQL_ok(rc)) {
	 dbd_error(h, rc, "describe/SQLColAttributes/SQL_COLUMN_DISPLAY_SIZE");
	 break;
      }
      /* TBD: should we only add a terminator if it's a char??? */
      fbh->ColDisplaySize += 1; /* add terminator */


      rc = SQLColAttributes(imp_sth->hstmt,i+1,SQL_COLUMN_LENGTH,
			    NULL, 0, NULL ,&fbh->ColLength);
      if (!SQL_ok(rc)) {
	 dbd_error(h, rc, "describe/SQLColAttributes/SQL_COLUMN_LENGTH");
	 break;
      }

      /* may want to ensure Display Size at least as large as column
       * length -- workaround for some drivers which report a shorter
       * display length 
       * */
      fbh->ColDisplaySize = fbh->ColDisplaySize > fbh->ColLength ? fbh->ColDisplaySize : fbh->ColLength;

      /* change fetched size for some types
       */
      fbh->ftype = SQL_C_CHAR;
      switch(fbh->ColSqlType)
      {
	 /* patch to allow binary types 3/24/99 courtesy of Jon
	  * Smirl
	  */
         case SQL_WCHAR:
         case SQL_WVARCHAR:
	 case SQL_VARBINARY:
	 case SQL_BINARY:
	    fbh->ftype = SQL_C_BINARY;
	    break;
	 case SQL_TIMESTAMP:
	    fbh->ftype = SQL_C_TIMESTAMP;
	    fbh->ColDisplaySize = sizeof(TIMESTAMP_STRUCT);
	    break;
      }

      /* make sure alignment is accounted for on all types, including
       * chars */
#if 0
      if (fbh->ftype != SQL_C_CHAR) { 
	 t_dbsize += t_dbsize % sizeof(int);     /* alignment (JLU incorrect!) */
      }
#endif
      t_dbsize += fbh->ColDisplaySize;
      t_dbsize += (sizeof(int) - (t_dbsize % sizeof(int))) % sizeof(int);     /* alignment -- always pad so the next column is aligned on a word boundary*/

      if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "      col %2d: %-8s (%d) len=%3d disp=%3d, prec=%3d scale=%d\n", 
		       i+1, S_SqlTypeToString(fbh->ColSqlType),
		       fbh->ColSqlType,
		       fbh->ColLength, fbh->ColDisplaySize,
		       fbh->ColDef, fbh->ColScale);
   }
   if (!SQL_ok(rc)) {
      /* dbd_error called above */
      Safefree(imp_sth->fbh);
      return 0;
   }

   /* allocate a buffer to hold all the column names	*/
   if (DBIc_TRACE_LEVEL(imp_sth) >= 8) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "  colname buffer size = %d\n", t_cbufl + num_fields);
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }

   /* quick fix for FoxPro: allocate extra 255 bytes as Foxpro seems
    * to clear out the buffer during SQLDescribeCol, as we are passing
    * the 255 there.  Probably need to fix this and call
    * SqlDescribeCol with the right size, if known!
    * */
   Newz(42, imp_sth->ColNames, t_cbufl + num_fields+255, UCHAR);
   /* allocate Row memory */
   Newz(42, imp_sth->RowBuffer, t_dbsize + num_fields, UCHAR);

   /* Second pass:
   - get column names
   - bind column output
*/

   cbuf_ptr = imp_sth->ColNames;
   rbuf_ptr = imp_sth->RowBuffer;

   for(i=0, fbh = imp_sth->fbh; i < num_fields && SQL_ok(rc); i++, fbh++)
   {
      /* not sure I need this anymore, since we are trying to align
       * the columns anyway
       * */
      switch(fbh->ftype)
      {
	 case SQL_C_BINARY:
	 case SQL_C_TIMESTAMP:
	    /* make sure pointer is on word boundary for Solaris */
	    rbuf_ptr += (sizeof(int) - ((rbuf_ptr - imp_sth->RowBuffer) % sizeof(int))) % sizeof(int);

	    break;
      }

      if (DBIc_TRACE_LEVEL(imp_sth) > 8) 
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "\t\t pre SQLDescribeCol %d: fbh[0]=%x fbh=%x, cbuf_ptr=%x\n",
		       i, &(imp_sth->fbh[0]), fbh, cbuf_ptr);
      rc = SQLDescribeCol(imp_sth->hstmt, 
			  i+1, 
			  cbuf_ptr, 255,
			  &(fbh->ColNameLen), &(fbh->ColSqlType),
			  &(fbh->ColDef), &(fbh->ColScale), &(fbh->ColNullable)
			 );
      if (!SQL_ok(rc)) {	/* should never fail */
	 dbd_error(h, rc, "describe/SQLDescribeCol");
	 break;
      }

      fbh->ColName = cbuf_ptr;
      cbuf_ptr[fbh->ColNameLen] = 0;
      cbuf_ptr[fbh->ColNameLen+1] = 0;

      if (DBIc_TRACE_LEVEL(imp_sth) > 8) 
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "\t\t post SQLDescribeCol     col %2d: '%s' (%x)\n",
		       0, imp_sth->fbh[0].ColName, imp_sth->fbh[0].ColName);

      if (DBIc_TRACE_LEVEL(imp_sth) >= 8) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "   colname %d = %s, len = %d (sp = %d)\n", i+1, fbh->ColName, fbh->ColNameLen,
		       cbuf_ptr - imp_sth->ColNames);
	 PerlIO_flush(DBIc_LOGPIO(imp_dbh));
      }

      cbuf_ptr += fbh->ColNameLen+1;

      fbh->data = rbuf_ptr;

      if (DBIc_TRACE_LEVEL(imp_sth) > 8) 
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "\t\t pre   SQLBindCol     col %2d: '%s', %x, %x, %x\n",
		       0, imp_sth->fbh[0].ColName, imp_sth->fbh[0].ColName, fbh->data, imp_sth->ColNames);

      rbuf_ptr += fbh->ColDisplaySize;
      rbuf_ptr += (sizeof(int) - ((rbuf_ptr - imp_sth->RowBuffer) % sizeof(int))) % sizeof(int);     /* alignment -- always pad so the next column is aligned on a word boundary*/

      /* Bind output column variables */
      rc = SQLBindCol(imp_sth->hstmt,
		      i+1,
		      fbh->ftype, fbh->data,
		      fbh->ColDisplaySize, &fbh->datalen);

      if (DBIc_TRACE_LEVEL(imp_sth) > 8) 
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "\t\t post  SQLBindCol     col %2d: '%s', %x, %x, %x\n",
		       0, imp_sth->fbh[0].ColName, imp_sth->fbh[0].ColName, fbh->data, imp_sth->ColNames);

      if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "      col %2d: '%s' sqltype=%s, ctype=%s, maxlen=%d, (dp = %d, cp = %d)\n",
		       i+1, fbh->ColName,
		       S_SqlTypeToString(fbh->ColSqlType),
		       S_SqlCTypeToString(fbh->ftype),
		       fbh->ColDisplaySize,
		       fbh->data - imp_sth->RowBuffer,
		       fbh->ColName - imp_sth->ColNames);
      if (!SQL_ok(rc)) {
	 dbd_error(h, rc, "describe/SQLBindCol");
	 break;
      }

      if (DBIc_TRACE_LEVEL(imp_sth) > 8) 
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "\t\t DEBUG     col %2d: '%s' \n",
		       0, imp_sth->fbh[0].ColName);
   } /* end pass 2 */


   if (!SQL_ok(rc)) {
      /* dbd_error called above */
      Safefree(imp_sth->fbh);
      return 0;
   }

   return 1;
}


int
   dbd_st_execute(sth, imp_sth)	/* <= -2:error, >=0:ok row count, (-1=unknown count) */
   SV *sth;
imp_sth_t *imp_sth;
{
   dTHR;
   RETCODE rc;
   int debug = DBIc_TRACE_LEVEL(imp_sth);
   D_imp_dbh_from_sth;
   int outparams = 0;
   /*
    * if the handle is active, we need to finish it here.
    * Note that dbd_st_finish already checks to see if it's active.
    */
   dbd_st_finish(sth, imp_sth);;

   /*
    * bind_param_inout support
    */
   outparams = (imp_sth->out_params_av) ? AvFILL(imp_sth->out_params_av)+1 : 0;
   if (debug >= 4) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "    dbd_st_execute (outparams = %d)...\n",
		    outparams);
   }

   if (outparams) {    /* check validity of bind_param_inout SV's      */
      int i = outparams;
      while(--i >= 0) {   
	 phs_t *phs = (phs_t*)(void*)SvPVX(AvARRAY(imp_sth->out_params_av)[i]);
	 /* Make sure we have the value in string format. Typically a number */
	 /* will be converted back into a string using the same bound buffer */
	 /* so the sv_buf test below will not trip.                   */

	 /* mutation check */
	 if (SvTYPE(phs->sv) != phs->sv_type        /* has the type changed? */
	     || (SvOK(phs->sv) && !SvPOK(phs->sv))  /* is there still a string? */
	     || SvPVX(phs->sv) != phs->sv_buf       /* has the string buffer moved? */
	      ) {
	    if (!_dbd_rebind_ph(sth, imp_sth, phs))
	       croak("Can't rebind placeholder %s", phs->name);
	 } else {
	    /* no mutation found */
	 }
      }
   }


   if (debug >= 2) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "    dbd_st_execute (for hstmt %d before)...\n",
		    imp_sth->hstmt);
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }

   if (imp_sth->ttExecDirect) {
      /* statement ready for SQLExecDirect */
      rc = SQLExecDirect(imp_sth->hstmt, imp_sth->statement, SQL_NTS);
   } else {
      rc = SQLExecute(imp_sth->hstmt);
   }
   if (debug >= 8) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "    dbd_st_execute (for hstmt %d after, rc = %d)...\n",
		    imp_sth->hstmt, rc);
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }
   /* patches to handle blobs better, via Jochen Wiedmann */
   while (rc == SQL_NEED_DATA) {
      phs_t* phs;
      STRLEN len;
      UCHAR* ptr;

      if (debug >= 5) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		       "    dbd_st_execute (NEED DATA)...\n",
		       imp_sth->hstmt);
	 PerlIO_flush(DBIc_LOGPIO(imp_dbh));
      }
      if ((rc = SQLParamData(imp_sth->hstmt, (PTR*) &phs))
	  !=  SQL_NEED_DATA) {
	 break;
      }

      /* phs->sv is already upgraded to a PV in _dbd_rebind_ph.
       * It is not NULL, because we otherwise won't be called here
       * (value_len = 0).
       */
      ptr = SvPV(phs->sv, len);
      rc = SQLPutData(imp_sth->hstmt, ptr, len);
      if (!SQL_ok(rc)) {
	 break;
      }
      rc = SQL_NEED_DATA;  /*  So the loop continues ...  */
   }

   /* 
    * Call dbd_error regardless of the value of rc so we can
    * get any status messages that are desired.
    */
   dbd_error(sth, rc, "st_execute/SQLExecute");
   if (!SQL_ok(rc) && rc != SQL_NO_DATA_FOUND) {
      return -2;
   }

   if (rc != SQL_NO_DATA_FOUND) {

      /* SWORD num_fields; */
      RETCODE rc2;
      if (debug >= 7)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		       "    dbd_st_execute getting row count\n");
      rc2 = SQLRowCount(imp_sth->hstmt, &imp_sth->RowCount);
      if (!SQL_ok(rc2)) {
	 dbd_error(sth, rc2, "st_execute/SQLRowCount");
	 imp_sth->RowCount = -1;
      }

      if (debug >= 7)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		       "    dbd_st_execute got row count %ld\n", imp_sth->RowCount);
   } else {
      /* SQL_NO_DATA returned, must have no rows :) */
      /* seem to need to reset the done_desc, but not sure if this is
       * what we want yet */
      if (debug >= 7) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		       "    dbd_st_execute SQL_NO_DATA...resetting done_desc!\n");
	 PerlIO_flush(DBIc_LOGPIO(imp_dbh));
      }
      imp_sth->done_desc = 0;
      imp_sth->RowCount = 0;
   }

   if (!imp_sth->done_desc) {
      /* This needs to be done after SQLExecute for some drivers!	*/
      /* Especially for order by and join queries.			*/
      /* See Microsoft Knowledge Base article (#Q124899)		*/
      /* describe and allocate storage for results (if any needed)	*/
      if (!dbd_describe(sth, imp_sth)) {
	 if (DBIc_TRACE_LEVEL(imp_sth) > 0) {
	    PerlIO_printf(DBIc_LOGPIO(imp_sth), "dbd_describe failed, dbd_st_execute...!\n");
	 }
	 return -2; /* dbd_describe already called dbd_error()	*/
      }
   }

   if (DBIc_NUM_FIELDS(imp_sth) > 0)
   {
      DBIc_ACTIVE_on(imp_sth);	/* only set for select (?)	*/
   }
   else
   {
      if (debug >= 2) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		       "    dbd_st_execute got no rows: resetting ACTIVE\n");
      }
      /* flag that we've done the describe to avoid a problem
       * where calling describe after execute returned no rows
       * caused SQLServer to provide a description of a query
       * that didn't quite apply. */

      /* imp_sth->done_desc = 1;  */
      DBIc_ACTIVE_off(imp_sth);
   }
   imp_sth->eod = SQL_SUCCESS;

   if (outparams) {	/* check validity of bound output SV's	*/
      timesten_handle_outparams(imp_sth, debug);
   }

   return imp_sth->RowCount;
}


/*----------------------------------------
 * running $sth->fetch()
 *----------------------------------------
 */
AV *
   dbd_st_fetch(sth, imp_sth)
   SV *	sth;
imp_sth_t *imp_sth;
{
   dTHR;
   D_imp_dbh_from_sth;
   int i;
   AV *av;
   RETCODE rc;
   int num_fields;
   char cvbuf[512];
   int ChopBlanks;

   /* Check that execute() was executed sucessfully. This also implies	*/
   /* that dbd_describe() executed sucessfuly so the memory buffers	*/
   /* are allocated and bound.						*/
   if ( !DBIc_ACTIVE(imp_sth) ) {
      dbd_error(sth, SQL_ERROR, "no select statement currently executing");
      return Nullav;
   }

   rc = SQLFetch(imp_sth->hstmt);
   if (DBIc_TRACE_LEVEL(imp_sth) >= 3)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "       SQLFetch rc %d\n", rc);
   imp_sth->eod = rc;
   if (!SQL_ok(rc)) {
      if (SQL_NO_DATA_FOUND == rc) {
	 /* XXX need to 'finish' here */
	 dbd_st_finish(sth, imp_sth);
	 return Nullav;
      } else {
	 dbd_error(sth, rc, "dbd_st_fetch/SQLFetch");
	 /* XXX need to 'finish' here */
	 dbd_st_finish(sth, imp_sth);
	 return Nullav;
      }
   }


   if (imp_sth->RowCount == -1)
      imp_sth->RowCount = 0;
   imp_sth->RowCount++;

   av = DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth);
   num_fields = AvFILL(av)+1;

   if (DBIc_TRACE_LEVEL(imp_sth) >= 3)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "fetch num_fields=%d\n", num_fields);

   ChopBlanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);

   for(i=0; i < num_fields; ++i) {
      imp_fbh_t *fbh = &imp_sth->fbh[i];
      SV *sv = AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/

      if (DBIc_TRACE_LEVEL(imp_sth) >= 4)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "fetch col#%d %s datalen=%d displ=%d\n",
		       i, fbh->ColName, fbh->datalen, fbh->ColDisplaySize);

      if (fbh->datalen == SQL_NULL_DATA) {	/* NULL value		*/
	 SvOK_off(sv);
	 continue;
      }

      if (fbh->datalen > fbh->ColDisplaySize || fbh->datalen < 0) { 
	 /* truncated LONG ??? DBIcf_LongTruncOk() */
	 /* DBIcf_LongTruncOk this should only apply to LONG type fields	*/
	 /* truncation of other fields should always be an error since it's	*/
	 /* a sign of an internal error */
	 if (!DBIc_has(imp_sth, DBIcf_LongTruncOk)
	     /*  && rc == SQL_SUCCESS_WITH_INFO */) {

	    /* 
	     * Since we've detected the problem locally via the datalen,
	     * we don't need to worry about the value of rc.
	     * 
	     * This used to make sure rc was set to SQL_SUCCESS_WITH_INFO 
	     * but since it's an error and not SUCCESS, call dbd_error() 
	     * with SQL_ERROR explicitly instead.
	     */

	    dbd_error(sth, SQL_ERROR, "dbd_st_fetch/SQLFetch (long truncated DBI attribute LongTruncOk not set and/or LongReadLen too small)");
	    return Nullav;
	 }
	 /* LongTruncOk true, just ensure perl has the right length
	  * for the truncated data.
	  */
	 sv_setpvn(sv, (char*)fbh->data, fbh->ColDisplaySize);
      }
      else switch(fbh->ftype) {
	 TIMESTAMP_STRUCT *ts;
	 case SQL_C_TIMESTAMP:
	    ts = (TIMESTAMP_STRUCT *)fbh->data;
	    sprintf(cvbuf, "%04d-%02d-%02d %02d:%02d:%02d",
		    ts->year, ts->month, ts->day, 
		    ts->hour, ts->minute, ts->second);
	    sv_setpv(sv, cvbuf);
	    break;
	 default:
	    if (ChopBlanks && fbh->ColSqlType == SQL_CHAR && fbh->datalen > 0) {
	       char *p = (char*)fbh->data;
	       while(fbh->datalen && p[fbh->datalen - 1]==' ')
		  --fbh->datalen;
	    }
	    sv_setpvn(sv, (char*)fbh->data, fbh->datalen);
      }
   }
   return av;
}


int
   dbd_st_rows(sth, imp_sth)
   SV *sth;
imp_sth_t *imp_sth;
{
   return imp_sth->RowCount;
}


int
   dbd_st_finish(sth, imp_sth)
   SV *sth;
imp_sth_t *imp_sth;
{
   dTHR;
   D_imp_dbh_from_sth;
   RETCODE rc;

   /* Cancel further fetches from this cursor.                 */
   /* We don't close the cursor till DESTROY (dbd_st_destroy). */
   /* The application may re execute(...) it.                  */
   if (DBIc_ACTIVE(imp_sth) && imp_dbh->hdbc != SQL_NULL_HDBC) {

      rc = SQLFreeStmt(imp_sth->hstmt, SQL_CLOSE);
      if (!SQL_ok(rc)) {
	 dbd_error(sth, rc, "dbd_st_finish/SQLFreeStmt(SQL_CLOSE)");
	 return 0;
      }
      if (DBIc_TRACE_LEVEL(imp_sth) > 5) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "dbd_st_finish closed query:\n");
      }
   }
   DBIc_ACTIVE_off(imp_sth);
   return 1;
}


void
   dbd_st_destroy(sth, imp_sth)
   SV *sth;
imp_sth_t *imp_sth;
{
   dTHR;
   D_imp_dbh_from_sth;
   RETCODE rc;

   /* Free contents of imp_sth	*/

   /* PerlIO_printf(DBIc_LOGPIO(imp_dbh), "  dbd_st_destroy\n"); */
   Safefree(imp_sth->fbh);
   Safefree(imp_sth->RowBuffer);
   Safefree(imp_sth->ColNames);
   Safefree(imp_sth->statement);

   if (imp_sth->out_params_av)
      sv_free((SV*)imp_sth->out_params_av);

   if (imp_sth->all_params_hv) {
      HV *hv = imp_sth->all_params_hv;
      SV *sv;
      char *key;
      I32 retlen;
      hv_iterinit(hv);
      while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
	 if (sv != &sv_undef) {
	    phs_t *phs_tpl = (phs_t*)(void*)SvPVX(sv);
	    sv_free(phs_tpl->sv);
	 }
      }
      sv_free((SV*)imp_sth->all_params_hv);
   }

   /* SQLxxx functions dump core when no connection exists. This happens
    * when the db was disconnected before perl ending.  Hence,
    * checking for the dirty flag.
    */
   if (imp_dbh->hdbc != SQL_NULL_HDBC && !dirty) {

      rc = SQLFreeStmt(imp_sth->hstmt, SQL_DROP);

      if (DBIc_TRACE_LEVEL(imp_sth) >= 5) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "   SQLFreeStmt called, returned %d.\n", rc);
	 PerlIO_flush(DBIc_LOGPIO(imp_dbh));
      }

      if (!SQL_ok(rc)) {
	 dbd_error(sth, rc, "dbd_st_destroy/SQLFreeStmt(SQL_DROP)");
	 /* return 0; */
      }
   }

   DBIc_IMPSET_off(imp_sth);		/* let DBI know we've done it	*/
}

/* XXX
 * This will fail (IM001) on drivers which don't support it.
 * We need to check for this and bind the param as varchars.
 * This will work on many drivers and databases.
 * If the database won't convert a varchar to an int (for example)
 * the user will get an error at execute time
 * but can add an explicit conversion to the SQL:
 * "... where num_field > int(?) ..."
 */
static void
   _dbd_get_param_type(SV *sth, imp_sth_t *imp_sth, phs_t *phs)
{
   SWORD fNullable;
   SWORD ibScale;
   UDWORD dp_cbColDef;
   D_imp_dbh_from_sth;
   RETCODE rc;
   SWORD fSqlType;

   if (phs->sql_type == 0) {

	 if (DBIc_TRACE_LEVEL(imp_sth) >= 3) {
	    PerlIO_printf(DBIc_LOGPIO(imp_dbh), "SQLDescribeParam idx = %d.\n", phs->idx);
	 }

	 rc = SQLDescribeParam(imp_sth->hstmt,
			       phs->idx, &fSqlType, &dp_cbColDef, &ibScale, &fNullable
			      );
	 if (!SQL_ok(rc)) {
	    /* SQLDescribeParam didn't work, assume it is a SQL_VARCHAR type */
	    phs->sql_type = SQL_VARCHAR;
	    /* dbd_error(sth, rc, "_rebind_ph/SQLDescribeParam");  */
	    if (DBIc_TRACE_LEVEL(imp_sth) > 0)
	       PerlIO_printf(DBIc_LOGPIO(imp_dbh), "SQLDescribeParam failed reverting to default type for this parameter: ");
            dbd_error_flush(sth);
	    /* fall through */
	    /* return 0; */
	 } else {
	    if (DBIc_TRACE_LEVEL(imp_sth) >=5) 
	       PerlIO_printf(DBIc_LOGPIO(imp_dbh),
			     "    SQLDescribeParam %s: SqlType=%s, ColDef=%d\n",
			     phs->name, S_SqlTypeToString(fSqlType), dp_cbColDef);


	    /* for non-integral numeric types, let the driver/database handle the
	     * conversion for us
	     */
	    switch(fSqlType) {
	       case SQL_NUMERIC:
	       case SQL_DECIMAL:
	       case SQL_FLOAT:
	       case SQL_REAL:
	       case SQL_DOUBLE:
		  if (DBIc_TRACE_LEVEL(imp_sth) >=5) 
		     PerlIO_printf(DBIc_LOGPIO(imp_dbh),
				   "    (DBD::TimesTen SQLDescribeParam NUMERIC FIXUP %s: SqlType=%s, ColDef=%d\n",
				   phs->name, S_SqlTypeToString(fSqlType), dp_cbColDef);
		  phs->sql_type = SQL_VARCHAR;
		  break;

	       default:
		  phs->sql_type = fSqlType;
	    }
	 }
   }
}

/* ====================================================================	*/


static int 
   _dbd_rebind_ph(SV *sth, imp_sth_t *imp_sth, phs_t *phs) 
{
   dTHR;
   D_imp_dbh_from_sth;
   RETCODE rc;
   /* args of SQLBindParameter() call */
   SWORD fParamType;
   SWORD fCType;
   UCHAR *rgbValue;
   UDWORD cbColDef;
   SWORD ibScale;
   SDWORD cbValueMax;

   STRLEN value_len = 0;

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2) {
      char *text = neatsvpv(phs->sv,value_len);
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "bind %s <== %s (size %d/%d/%ld, ptype %ld, otype %d, sqltype %d)\n",
		    phs->name, text, SvOK(phs->sv) ? SvCUR(phs->sv) : -1, SvOK(phs->sv) ? SvLEN(phs->sv) : -1 ,phs->maxlen,
		    SvTYPE(phs->sv), phs->ftype, phs->sql_type);
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }

   /* At the moment we always do sv_setsv() and rebind.        */
   /* Later we may optimise this so that more often we can     */
   /* just copy the value & length over and not rebind.        */

   if (phs->is_inout) {        /* XXX */
      if (SvREADONLY(phs->sv))
	 croak(no_modify);
      /* phs->sv _is_ the real live variable, it may 'mutate' later   */
      /* pre-upgrade high to reduce risk of SvPVX realloc/move        */
      (void)SvUPGRADE(phs->sv, SVt_PVNV);
      /* ensure room for result, 28 is magic number (see sv_2pv)      */
      SvGROW(phs->sv, (phs->maxlen < 28) ? 28 : phs->maxlen+1);
   }
   else {
      /* phs->sv is copy of real variable, upgrade to at least string */
      (void)SvUPGRADE(phs->sv, SVt_PV);
   }

   /* At this point phs->sv must be at least a PV with a valid buffer, */
   /* even if it's undef (null)                                        */
   /* Here we set phs->sv_buf, and value_len.                */
   if (SvOK(phs->sv)) {
      phs->sv_buf = SvPV(phs->sv, value_len);
   }
   else {
      /* it's undef but if it was inout param it would point to a
       * valid buffer, at least  */
      phs->sv_buf = SvPVX(phs->sv);
      value_len   = 0;
   }
   /* value_len has current value length now */
   phs->sv_type = SvTYPE(phs->sv);     /* part of mutation check       */
   phs->maxlen  = SvLEN(phs->sv)-1;    /* avail buffer space  		*/

   if (DBIc_TRACE_LEVEL(imp_sth) >= 3) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "bind %s <== '%.100s' (len %ld/%ld, null %d)\n",
		    phs->name, SvOK(phs->sv) ? phs->sv_buf : "(null)",
		    (long)value_len,(long)phs->maxlen, SvOK(phs->sv)?0:1);
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }

   /* ----------------------------------------------------------------	*/

   /* XXX
   This will fail (IM001) on drivers which don't support it.
   We need to check for this and bind the param as varchars.
   This will work on many drivers and databases.
   If the database won't convert a varchar to an int (for example)
   the user will get an error at execute time
   but can add an explicit conversion to the SQL:
   "... where num_field > int(?) ..."
*/

   _dbd_get_param_type(sth, imp_sth, phs);

   /*
    * JLU: was SQL_PARAM_OUTPUT only, but that caused a problem with
    * Oracle's drivers and in/out parameters.  Can't be output only
    * with Oracle.  Need to test on other platforms to ensure this
    * does not cause a problem.
    */
   fParamType = phs->is_inout ? SQL_PARAM_INPUT_OUTPUT : SQL_PARAM_INPUT; 
   fCType = phs->ftype;
   ibScale = value_len;
   cbColDef = phs->is_inout ? phs->maxlen : value_len;
   /* JLU
    * need to look at this.
    * was cbValueMax = value_len for some binding purposes
    */
   cbValueMax = phs->is_inout ? phs->maxlen : value_len;

   /* When we fill a LONGVARBINARY, the CTYPE must be set 
    * to SQL_C_BINARY.
    */
   if (fCType == SQL_C_CHAR) {	/* could be changed by bind_plh */
      switch(phs->sql_type) {
	 case SQL_WCHAR:
	 case SQL_WVARCHAR:
	 case SQL_BINARY:
	 case SQL_VARBINARY:
	    fCType = SQL_C_BINARY;
	    break;
	 case SQL_DATE:

	 case SQL_TIME:
	    /* fSqlType = SQL_VARCHAR;*/
	    break;
	 case SQL_TIMESTAMP:
	    /* fSqlType = SQL_VARCHAR; */
	    /* cbColDef = 23; */
	    ibScale = 0;		/* tbd: millisecondS?) */
	    /* bug fix! if phs->sv is not OK, then there's a chance
	     * we go through garbage data to determine the length */
	    if (SvOK(phs->sv)) {
	       char *cp;
	       if (phs->sv_buf && *phs->sv_buf) {
		  cp = strchr(phs->sv_buf, '.');
		  if (cp) {
		     ++cp;
		     while (*cp != '\0' && isdigit(*cp)) {
			cp++;
			ibScale++;
		     }
		  }
	       } else {
		  cbColDef = 23;	/* hard code for SQL Server when passing Undef to bound parameters */
	       }
	    }

	    break;
	 default:
	    break;
      }
   }
   /* per patch from Paul G. Weiss, who was experiencing re-preparing
    * of queries when the size of the bound string's were increasing
    * for example select * from tabtest where name = ?
    * then executing with 'paul' and then 'thomas' would cause
    * SQLServer to prepare the query twice, but if we ran 'thomas'
    * then 'paul', it would not re-prepare the query.  The key seems
    * to be allocating enough space for the largest parameter.
    * TBD: the default for this should be a DBD::ODBC specific option
    * or attribute.
    */
   if (phs->sql_type == SQL_VARCHAR && !phs->is_inout) {
      ibScale = 0;
      /* default to at least 80 if this is the first time through */
      if (phs->biggestparam == 0) {
	 phs->biggestparam = (value_len > 80) ? value_len : 80;
      } else {
	 /* bump up max, if needed */
	 if (value_len > phs->biggestparam) {
	    phs->biggestparam = value_len;
	 }
      }
      cbColDef = phs->biggestparam;
   }

   if (!SvOK(phs->sv)) {
      /* if is_inout, shouldn't we null terminate the buffer and send
       * it, instead?? */
      if (!phs->is_inout) {
	 cbColDef = 1;
      }
      if (phs->is_inout) {
	 if (!phs->sv_buf) {
	    croak("panic: DBD::TimesTen binding undef with bad buffer!!!!");
	 }
	 phs->sv_buf[0] = '\0'; /* just in case, we *know* we called SvGROW above */
	 rgbValue = phs->sv_buf;
	 /* patch for binding undef inout params on sql server */
	 ibScale = 1;
	 phs->cbValue = 1;
      } else {
	 rgbValue = NULL;
	 phs->cbValue = SQL_NULL_DATA;
      }
   }
   else {
      rgbValue = phs->sv_buf;
      phs->cbValue = (UDWORD) value_len;
      /* not undef, may be a blank string or something */
      if (!phs->is_inout && phs->cbValue == 0) {
	 cbColDef = 1;
      }
   }
   if (DBIc_TRACE_LEVEL(imp_sth) >=2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "    bind %s: CTy=%d, STy=%s, CD=%d, Sc=%d, VM=%d.\n",
		    phs->name, fCType, S_SqlTypeToString(phs->sql_type),
		    cbColDef, ibScale, cbValueMax);

   if (value_len < 32768) {
      /* already set and should be left alone JLU */
      /* ibScale = value_len; */
   } else {
      /* This exceeds the positive size of an SWORD, so we have to use
       * SQLPutData.
       */
      ibScale = 32767;
      phs->cbValue = SQL_LEN_DATA_AT_EXEC(value_len);
      /* This is lazyness!
       *
       * The ODBC docs declare rgbValue as a 32 bit value. May be this
       * breaks on 64 bit machines?
       */
      rgbValue = (UCHAR*) phs;
   }

   if (DBIc_TRACE_LEVEL(imp_sth) >=5) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh),
		    "    SQLBindParameter: idx = %d: fParamType=%d, name=%s, fCtype=%d, SQL_Type = %d, cbColDef=%d, scale=%d, rgbValue = %x, cbValueMax=%d, cbValue = %d\n",
		    phs->idx, fParamType, phs->name, fCType, phs->sql_type,
		    cbColDef, ibScale, rgbValue, cbValueMax, phs->cbValue);
      if (fCType == SQL_C_CHAR) {
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "    Param value = %s\n", rgbValue);
      }
   }

   rc = SQLBindParameter(imp_sth->hstmt,
			 phs->idx, fParamType, fCType, phs->sql_type,
			 cbColDef, ibScale,
			 rgbValue, cbValueMax, &phs->cbValue);

   if (!SQL_ok(rc)) {
      dbd_error(sth, rc, "_rebind_ph/SQLBindParameter");
      return 0;
   }

   return 1;
}


/*------------------------------------------------------------
 * bind placeholder.
 *  Is called from ODBC.xs execute()
 *  AND from ODBC.xs bind_param()
 */
int
   dbd_bind_ph(sth, imp_sth, ph_namesv, newvalue, sql_type, attribs, is_inout, maxlen)
   SV *sth;
imp_sth_t *imp_sth;
SV *ph_namesv;		/* index of execute() parameter 1..n */
SV *newvalue;
IV sql_type;
SV *attribs;		/* may be set by Solid.xs bind_param call */
int is_inout;		/* inout for procedure calls only */
IV maxlen;			/* ??? */
{
   dTHR;
   SV **phs_svp;
   STRLEN name_len;
   char *name;
   char namebuf[30];
   phs_t *phs;
   D_imp_dbh_from_sth;



   if (SvNIOK(ph_namesv) ) {	/* passed as a number	*/
      name = namebuf;
      sprintf(name, "%d", (int)SvIV(ph_namesv));
      name_len = strlen(name);
   } 
   else {
      name = SvPV(ph_namesv, name_len);
   }

   /* the problem with the code below is we are getting SVt_PVLV when
    * an "undef" value from a hash lookup that doesn't exist.  It's an
    * "undef" value, but it doesn't come in as a scalar.
    * from a hash is arriving.  Let's leave this out until we are
    * handling arrays. JLU 7/12/02
    */
#if 0
   if (SvTYPE(newvalue) > SVt_PVMG) {    /* hook for later array logic   */
      if (DBIc_TRACE_LEVEL(imp_sth) >= 2) 
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), "bind %s perl type = %d -- croaking!\n",
		       name, SvTYPE(newvalue));
      croak("Can't bind non-scalar value (currently)");
   }

#endif

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "bind %s <== '%.200s' (attribs: %s), type %d\n",
		    name, SvPV(newvalue,na), attribs ? SvPV(attribs,na) : "", sql_type );
      PerlIO_flush(DBIc_LOGPIO(imp_dbh));
   }

   phs_svp = hv_fetch(imp_sth->all_params_hv, name, name_len, 0);
   if (phs_svp == NULL)
      croak("Can't bind unknown placeholder '%s'", name);
   phs = (phs_t*)SvPVX(*phs_svp);	/* placeholder struct	*/

   if (phs->sv == &sv_undef) { /* first bind for this placeholder      */
      phs->ftype    = SQL_C_CHAR;     /* our default type VARCHAR2    */

      /* JLU: 1/29/2001: change to allow detection of column type
       * instead of assuming sql_varchar.  IF the user sets the
       * private attribute.
       */
      phs->sql_type = (sql_type) ? sql_type : imp_sth->ttDefaultBindType;


      phs->maxlen   = maxlen;         /* 0 if not inout               */
      phs->is_inout = is_inout;
      if (is_inout) {
	 phs->sv = SvREFCNT_inc(newvalue);   /* point to live var    */
	 ++imp_sth->has_inout_params;
	 /* build array of phs's so we can deal with out vars fast   */
	 if (!imp_sth->out_params_av)
	    imp_sth->out_params_av = newAV();
	 av_push(imp_sth->out_params_av, SvREFCNT_inc(*phs_svp));
	 /* croak("Can't bind output values (currently)");	XXX */
      }

      /* some types require the trailing null included in the length. */
      phs->alen_incnull = 0; /*Oracle:(phs->ftype==SQLT_STR || phs->ftype==SQLT_AVC);*/

   }
   /* check later rebinds for any changes */
   /*
    * else if (is_inout || phs->is_inout) {
    * croak("Can't rebind or change param %s in/out mode after first bind", phs->name);
    * }
    * */
   else if (is_inout != phs->is_inout) {
      croak("Can't rebind or change param %s in/out mode after first bind (%d => %d)",
	    phs->name, phs->is_inout, is_inout);
   }
   else if (maxlen && maxlen != phs->maxlen) {
      croak("Can't change param %s maxlen (%ld->%ld) after first bind",
	    phs->name, phs->maxlen, maxlen);
   }

   if (!is_inout) {    /* normal bind to take a (new) copy of current value    */
      if (phs->sv == &sv_undef)       /* (first time bind) */
	 phs->sv = newSV(0);
      sv_setsv(phs->sv, newvalue);
   } else if (newvalue != phs->sv) {
      if (phs->sv)
	 SvREFCNT_dec(phs->sv);
      phs->sv = SvREFCNT_inc(newvalue);       /* point to live var    */
   }

   return _dbd_rebind_ph(sth, imp_sth, phs);
}


/*------------------------------------------------------------
 * blob_read:
 * read part of a BLOB from a table.
 * XXX needs more thought
 */
int dbd_st_blob_read(sth, imp_sth, field, offset, len, destrv, destoffset)
SV *sth;
imp_sth_t *imp_sth;
int field;
long offset;
long len;
SV *destrv;
long destoffset;
{
   dTHR;
   SDWORD retl;
   SV *bufsv;
   RETCODE rc;

   bufsv = SvRV(destrv);
   sv_setpvn(bufsv,"",0);      /* ensure it's writable string  */
   SvGROW(bufsv, len+destoffset+1);    /* SvGROW doesn't do +1 */

   /* XXX for this to work be probably need to avoid calling SQLGetData in	*/
   /* fetch. The definition of SQLGetData doesn't work well with the DBI's	*/
   /* notion of how LongReadLen would work. Needs more thought.	*/

   rc = SQLGetData(imp_sth->hstmt, (UWORD)field+1,
		   SQL_C_BINARY,
		   ((UCHAR *)SvPVX(bufsv)) + destoffset, (SDWORD)len, &retl
		  );
   if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_sth),
		    "SQLGetData(...,off=%d, len=%d)->rc=%d,len=%d SvCUR=%d\n",
		    destoffset, len, rc, retl, SvCUR(bufsv));

   if (!SQL_ok(rc)) {
      dbd_error(sth, rc, "dbd_st_blob_read/SQLGetData");
      return 0;
   }
   if (rc == SQL_SUCCESS_WITH_INFO) {	/* XXX should check for 01004 */
      retl = len;
   }

   if (retl == SQL_NULL_DATA) {	/* field is null	*/
      (void)SvOK_off(bufsv);
      return 1;
   }
   if (retl == SQL_NO_TOTAL) {		/* unknown length!	*/
      (void)SvOK_off(bufsv);
      return 0;
   }

   SvCUR_set(bufsv, destoffset+retl);
   *SvEND(bufsv) = '\0'; /* consistent with perl sv_setpvn etc */

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_sth), "blob_read: SvCUR=%d\n",
		    SvCUR(bufsv));

   return 1;
}


/*----------------------------------------
 * db level interface
 * set connection attributes.
 *----------------------------------------
 */

typedef struct {
   const char *str;
   UWORD fOption;
   UDWORD true;
   UDWORD false;
} db_params;

static db_params S_db_storeOptions[] =  {
   { "AutoCommit", SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON, SQL_AUTOCOMMIT_OFF },
   { "ttIsolationLevel", SQL_TXN_ISOLATION },
   { "ttIgnoreNamedPlaceholders", TT_IGNORE_NAMED_PLACEHOLDERS },
   { "ttDefaultBindType", TT_DEFAULT_BIND_TYPE },
   { "ttExecDirect", TT_EXEC_DIRECT },
   { "ttQueryTimeout", TT_QUERY_TIMEOUT },
   { NULL },
};

static const db_params *
   S_dbOption(const db_params *pars, char *key, STRLEN len)
{
   /* search option to set */
   while (pars->str != NULL) {
      if (strncmp(pars->str, key, len) == 0
	  && len == strlen(pars->str))
	 break;
      pars++;
   }
   if (pars->str == NULL) {
      return NULL;
   }
   return pars;
}

int
   dbd_db_STORE_attrib(dbh, imp_dbh, keysv, valuesv)
   SV *dbh;
imp_dbh_t *imp_dbh;
SV *keysv;
SV *valuesv;
{
   dTHR;
   RETCODE rc;
   STRLEN kl;
   char *key = SvPV(keysv,kl);
   int on;
   UDWORD vParam;
   const db_params *pars;
   int bSetSQLConnectionOption;

   if ((pars = S_dbOption(S_db_storeOptions, key, kl)) == NULL) {
      if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
	 PerlIO_printf(DBIc_LOGPIO(imp_dbh), 
		       "DBD::TimesTen unsupported attribute passed (%s)\n", key);

      return FALSE;
   }

   bSetSQLConnectionOption = TRUE;
   switch(pars->fOption)
   {
      case SQL_TXN_ISOLATION:
	 vParam = SvIV(valuesv);
	 break;

      case TT_IGNORE_NAMED_PLACEHOLDERS:
	 bSetSQLConnectionOption = FALSE;
	 /*
	  * set value to ignore placeholders.  Will affect all
	  * statements from here on.
	  */
	 imp_dbh->ttIgnoreNamedPlaceholders = SvTRUE(valuesv);
	 break;

      case TT_DEFAULT_BIND_TYPE:
	 bSetSQLConnectionOption = FALSE;
	 /*
	  * set value of default bind type.  Default is SQL_VARCHAR,
	  * but setting to 0 will cause SQLDescribeParam to be used.
	  */
	 imp_dbh->ttDefaultBindType = SvIV(valuesv);

	 break;

      case TT_QUERY_TIMEOUT:
	 bSetSQLConnectionOption = FALSE;
	 imp_dbh->ttQueryTimeout = SvIV(valuesv);
	 break;

      case TT_EXEC_DIRECT:
	 bSetSQLConnectionOption = FALSE;
	 /*
	  * set value of ttExecDirect.  Non-zero will 
	  * make prepare, essentially a noop and make execute
	  * use SQLExecDirect.  This is to support drivers that
	  * _only_ support SQLExecDirect.
	  */
	 imp_dbh->ttExecDirect = SvIV(valuesv);

	 break;

      default:
	 on = SvTRUE(valuesv);
	 vParam = on ? pars->true : pars->false;
	 break;
   }

   if (bSetSQLConnectionOption) {
      rc = SQLSetConnectOption(imp_dbh->hdbc, pars->fOption, vParam);
      if (!SQL_ok(rc)) {
	 dbd_error(dbh, rc, "db_STORE/SQLSetConnectOption");
	 return FALSE;
      }
      /* keep our flags in sync */
      if (kl == 10 && strEQ(key, "AutoCommit"))
	 DBIc_set(imp_dbh, DBIcf_AutoCommit, SvTRUE(valuesv));
   }
   return TRUE;
}


static db_params S_db_fetchOptions[] =  {
   { "AutoCommit", SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON, SQL_AUTOCOMMIT_OFF },
   { "RowCacheSize", TT_ROWCACHESIZE },
   { "ttIsolationLevel", SQL_TXN_ISOLATION },
   { "ttIgnoreNamedPlaceholders", TT_IGNORE_NAMED_PLACEHOLDERS },
   { "ttDefaultBindType", TT_DEFAULT_BIND_TYPE },
   { "ttExecDirect", TT_EXEC_DIRECT },
   { "ttQueryTimeout", TT_QUERY_TIMEOUT },
   { NULL }
};

SV *
   dbd_db_FETCH_attrib(dbh, imp_dbh, keysv)
   SV *dbh;
imp_dbh_t *imp_dbh;
SV *keysv;
{
   dTHR;
   RETCODE rc;
   STRLEN kl;
   char *key = SvPV(keysv,kl);
   UDWORD vParam = 0;
   const db_params *pars;
   SV *retsv = NULL;

   /* checking pars we need FAST */

   if (DBIc_TRACE_LEVEL(imp_dbh) > 7) {
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), " FETCH %s\n", key);
   }

   if ((pars = S_dbOption(S_db_fetchOptions, key, kl)) == NULL)
      return Nullsv;

   switch (pars->fOption) {
      case TT_IGNORE_NAMED_PLACEHOLDERS:
	 /*
	  * fetch current value of named placeholders.
	  */
	 retsv = newSViv(imp_dbh->ttIgnoreNamedPlaceholders);
	 break;

      case TT_QUERY_TIMEOUT:
	 /*
	  * fetch current value of query timeout
	  */
	 retsv = newSViv(imp_dbh->ttQueryTimeout);
	 break;

      case TT_DEFAULT_BIND_TYPE:
	 /*
	  * fetch current value of default bind type.
	  */
	 retsv = newSViv(imp_dbh->ttDefaultBindType);
	 break;

      case TT_EXEC_DIRECT:
	 /*
	  * fetch current value of exec_direct.
	  */
	 retsv = newSViv(imp_dbh->ttExecDirect);
	 break;

      case TT_ROWCACHESIZE:
	 retsv = newSViv(imp_dbh->RowCacheSize);
	 break;
      default:
	 /*
	  * readonly, tracefile etc. isn't working yet. only AutoCommit supported.
	  */

	 rc = SQLGetConnectOption(imp_dbh->hdbc, pars->fOption, &vParam);
	 dbd_error(dbh, rc, "db_FETCH/SQLGetConnectOption");
	 if (!SQL_ok(rc)) {
	    if (DBIc_TRACE_LEVEL(imp_dbh) >= 1)
	       PerlIO_printf(DBIc_LOGPIO(imp_dbh),
			     "SQLGetConnectOption returned %d in dbd_db_FETCH\n", rc);
	    return Nullsv;
	 }
	 switch(pars->fOption) {
	    case SQL_TXN_ISOLATION:
	       retsv = newSViv(vParam);
	       break;
	    default:
	       if (vParam == pars->true)
		  retsv = newSViv(1);
	       else
		  retsv = newSViv(0);
	       break;
	 } /* inner switch */
   } /* outer switch */

   return sv_2mortal(retsv);
}

/*
 * added "need_describe" flag to handle the situation where you don't
 * have a result set yet to describe.  Certain attributes don't need
 * the result set to operate, hence don't do a describe unless you need
 * to do one.
 * DBD::ODBC 0.45_15
 * */
typedef struct {
   const char *str;
   unsigned len:8;
   unsigned array:1;
   unsigned need_describe:1;
   unsigned filler:22;
} T_st_params;

#define s_A(str,need_describe) { str, sizeof(str)-1,0,need_describe }
static T_st_params S_st_fetch_params[] = 
{
   s_A("NUM_OF_PARAMS",1),	/* 0 */
   s_A("NUM_OF_FIELDS",1),	/* 1 */
   s_A("NAME",1),		/* 2 */
   s_A("NULLABLE",1),		/* 3 */
   s_A("TYPE",1),		/* 4 */
   s_A("PRECISION",1),		/* 5 */
   s_A("SCALE",1),		/* 6 */
   s_A("CursorName",1),		/* 7 */
   s_A("ParamValues",1),	/* 8 */
   s_A("LongReadLen",0),	/* 9 */
   s_A("ttIgnoreNamedPlaceholders",0),	/* 10 */
   s_A("ttDefaultBindType",0),		/* 11 */
   s_A("ttQueryTimeout",0),	/* 12 */
   s_A("",0),			/* END */
};

static T_st_params S_st_store_params[] = 
{
   s_A("ttIgnoreNamedPlaceholders",0),	/* 0 */
   s_A("ttDefaultBindType",0),	/* 1 */
   s_A("ttQueryTimeout",0),	/* 2 */
   s_A("",0),			/* END */
};
#undef s_A

/*----------------------------------------
 * dummy routines st_XXXX
 *----------------------------------------
 */
SV *
   dbd_st_FETCH_attrib(sth, imp_sth, keysv)
   SV *sth;
imp_sth_t *imp_sth;
SV *keysv;
{
   dTHR;
   STRLEN kl;
   char *key = SvPV(keysv,kl);
   int i;
   SV *retsv = NULL;
   T_st_params *par;
   char cursor_name[256];
   SWORD cursor_name_len;
   RETCODE rc;

   for (par = S_st_fetch_params; par->len > 0; par++)
      if (par->len == kl && strEQ(key, par->str))
	 break;


   if (par->len <= 0)
      return Nullsv;

   if (par->need_describe && !imp_sth->done_desc && !dbd_describe(sth, imp_sth)) 
   {
      /* dbd_describe has already called dbd_error()          */
      /* we can't return Nullsv here because the xs code will */
      /* then just pass the attribute name to DBI for FETCH.  */
      if (DBIc_TRACE_LEVEL(imp_sth) > 3) {
	 PerlIO_printf(DBIc_LOGPIO(imp_sth), " dbd_st_FETCH_attrib (%s) needed query description, but failed\n", par->str);
      }
      if (DBIc_WARN(imp_sth)) {
	 warn("Describe failed during %s->FETCH(%s,%d)", SvPV(sth,na), key,imp_sth->done_desc);
      }
      return &sv_undef;
   }

   i = DBIc_NUM_FIELDS(imp_sth);


   switch(par - S_st_fetch_params)
   {
      AV *av;

      case 0:			/* NUM_OF_PARAMS */
	 return Nullsv;	/* handled by DBI */
      case 1:			/* NUM_OF_FIELDS */
	 if (DBIc_TRACE_LEVEL(imp_sth) > 8) {
	    PerlIO_printf(DBIc_LOGPIO(imp_sth), " dbd_st_FETCH_attrib NUM_OF_FIELDS %d\n", i);
	 }
	 retsv = newSViv(i);
	 break;
      case 2: 			/* NAME */
	 av = newAV();
	 retsv = newRV(sv_2mortal((SV*)av));
	 if (DBIc_TRACE_LEVEL(imp_sth) > 8) {
	    int j;
	    PerlIO_printf(DBIc_LOGPIO(imp_sth), " dbd_st_FETCH_attrib NAMES %d\n", i);

	    for (j = 0; j < i; j++) {
	       PerlIO_printf(DBIc_LOGPIO(imp_sth), "\t%s\n", imp_sth->fbh[j].ColName);
	       PerlIO_flush(DBIc_LOGPIO(imp_sth));
	    }
	    PerlIO_flush(DBIc_LOGPIO(imp_sth));
	 }
	 while(--i >= 0) {
	    if (DBIc_TRACE_LEVEL(imp_sth) > 8) {
	       PerlIO_printf(DBIc_LOGPIO(imp_sth), "    Colname %d => %s\n",
			     i, imp_sth->fbh[i].ColName);
	       PerlIO_flush(DBIc_LOGPIO(imp_sth));
	    }
	    av_store(av, i, newSVpv(imp_sth->fbh[i].ColName, 0));
	 }
	 break;
      case 3:			/* NULLABLE */
	 av = newAV();
	 retsv = newRV(sv_2mortal((SV*)av));
	 while(--i >= 0)
	    av_store(av, i,
		     (imp_sth->fbh[i].ColNullable == SQL_NO_NULLS)
		     ? &sv_no : &sv_yes);
	 break;
      case 4:			/* TYPE */
	 av = newAV();
	 retsv = newRV(sv_2mortal((SV*)av));
	 while(--i >= 0) 
	    av_store(av, i, newSViv(imp_sth->fbh[i].ColSqlType));
	 break;
      case 5:			/* PRECISION */
	 av = newAV();
	 retsv = newRV(sv_2mortal((SV*)av));
	 while(--i >= 0) 
	    av_store(av, i, newSViv(imp_sth->fbh[i].ColDef));
	 break;
      case 6:			/* SCALE */
	 av = newAV();
	 retsv = newRV(sv_2mortal((SV*)av));
	 while(--i >= 0) 
	    av_store(av, i, newSViv(imp_sth->fbh[i].ColScale));
	 break;
      case 7:			/* CursorName */
	 rc = SQLGetCursorName(imp_sth->hstmt,
			       cursor_name, sizeof(cursor_name), &cursor_name_len);
	 if (!SQL_ok(rc)) {
	    dbd_error(sth, rc, "st_FETCH/SQLGetCursorName");
	    return Nullsv;
	 }
	 retsv = newSVpv(cursor_name, cursor_name_len);
	 break;
      case 8:
      {
	 /* not sure if there's a memory leak here. */
	 HV *paramvalues = newHV();
	 if (imp_sth->all_params_hv) {
	    HV *hv = imp_sth->all_params_hv;
	    SV *sv;
	    char *key;
	    I32 retlen;
	    hv_iterinit(hv);
	    while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
	       if (sv != &sv_undef) {
		  phs_t *phs = (phs_t*)(void*)SvPVX(sv);
		  hv_store(paramvalues, phs->name, strlen(phs->name), newSVsv(phs->sv), 0);
	       }
	    }
	 }
	 /* ensure HV is freed when the ref is freed */
	 retsv = newRV_noinc((SV *)paramvalues);
      }
      break;
      case 9:
	 retsv = newSViv(DBIc_LongReadLen(imp_sth));
	 break;
      case 10:
	 retsv = newSViv(imp_sth->ttIgnoreNamedPlaceholders);
	 break;
      case 11:
	 retsv = newSViv(imp_sth->ttDefaultBindType);
	 break;
      case 12: /* query timeout */
	 retsv = newSViv(imp_sth->ttQueryTimeout);
	 break;
      default:
	 return Nullsv;
   }

   return sv_2mortal(retsv);
}


int
   dbd_st_STORE_attrib(sth, imp_sth, keysv, valuesv)
   SV *sth;
imp_sth_t *imp_sth;
SV *keysv;
SV *valuesv;
{
   dTHR;
   STRLEN kl;
   char *key = SvPV(keysv,kl);
   T_st_params *par;

   for (par = S_st_store_params; par->len > 0; par++)
      if (par->len == kl && strEQ(key, par->str))
	 break;

   if (par->len <= 0)
      return FALSE;

   switch(par - S_st_store_params)
   {
      case 0:
	 imp_sth->ttIgnoreNamedPlaceholders = SvTRUE(valuesv);
	 return TRUE;

      case 1:
	 imp_sth->ttDefaultBindType = SvIV(valuesv);
	 return TRUE;
	 break;

      case 2:/*  */
	 imp_sth->ttQueryTimeout = SvIV(valuesv);
	 return TRUE;
	 break;
   }
   return FALSE;
}


SV *
   dbd_db_get_info(dbh, ftype)
   SV *dbh;
int ftype;
{
   dTHR;
   D_imp_dbh(dbh);
   RETCODE rc;
   SV *retsv = NULL;
   int i;
   int size = 256;
   char *rgbInfoValue;
   SWORD cbInfoValue = -2;

   New(0, rgbInfoValue, size, char);

   /* See fancy logic below */
   for (i = 0; i < 6; i++)
      rgbInfoValue[i] = (char)0xFF;

   rc = SQLGetInfo(imp_dbh->hdbc, ftype,
		   rgbInfoValue, size-1, &cbInfoValue);
   if (cbInfoValue > size-1) {
      Renew(rgbInfoValue, cbInfoValue+1, char);
      rc = SQLGetInfo(imp_dbh->hdbc, ftype, rgbInfoValue, cbInfoValue, &cbInfoValue);
   }
   if (!SQL_ok(rc)) {
      dbd_error(dbh, rc, "dbd_db_get_info/SQLGetInfo");
      Safefree(rgbInfoValue);
      /* patched 2/12/02, thanks to Steffen Goldner */
      return &sv_undef;
      /* return Nullsv; */
   }

   /* Fancy logic here to determine if result is a string or int */
   if (cbInfoValue == -2)				/* is int */
      retsv = newSViv(*(int *)rgbInfoValue);	/* XXX cast */
   else if (cbInfoValue != 2 && cbInfoValue != 4)	/* must be string */
      retsv = newSVpv(rgbInfoValue, 0);
   else if (rgbInfoValue[cbInfoValue] == '\0')	/* must be string */ /* patch from Steffen Goldner 0.37 2/12/02 */
      retsv = newSVpv(rgbInfoValue, 0);
   else if (cbInfoValue == 2)			/* short */
      retsv = newSViv(*(short *)rgbInfoValue);	/* XXX cast */
   else if (cbInfoValue == 4)			/* int */
      retsv = newSViv(*(int *)rgbInfoValue);	/* XXX cast */
   else
      croak("panic: SQLGetInfo cbInfoValue == %d", cbInfoValue);

   if (DBIc_TRACE_LEVEL(imp_dbh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "SQLGetInfo: ftype %d, cbInfoValue %d: %s\n",
		    ftype, cbInfoValue, neatsvpv(retsv,0));

   Safefree(rgbInfoValue);
   return sv_2mortal(retsv);
}

int
   dbd_db_foreign_key_info(dbh, sth, PK_CatalogName, PK_SchemaName, PK_TableName, FK_CatalogName, FK_SchemaName, FK_TableName)
   SV *	 dbh;
SV *	 sth;
char * PK_CatalogName;
char * PK_SchemaName;
char * PK_TableName;
char * FK_CatalogName;
char * FK_SchemaName;
char * FK_TableName;
{
   dTHR;
   D_imp_dbh(dbh);
   D_imp_sth(sth);
   RETCODE rc;

   imp_sth->henv = imp_dbh->henv;	/* needed for dbd_error */
   imp_sth->hdbc = imp_dbh->hdbc;

   imp_sth->done_desc = 0;

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   rc = SQLAllocStmt(imp_dbh->hdbc, &imp_sth->hstmt);
   if (rc != SQL_SUCCESS) {
      dbd_error(sth, rc, "dbd_db_foreign_key_info/SQLAllocStmt");
      return 0;
   }


   /* just for sanity, later.  Any internals that may rely on this (including */
   /* debugging) will have valid data */
   imp_sth->statement = (char *)safemalloc(strlen(cSqlForeignKeys)+
					   strlen(XXSAFECHAR(PK_CatalogName))+
					   strlen(XXSAFECHAR(PK_SchemaName))+
					   strlen(XXSAFECHAR(PK_TableName))+
					   strlen(XXSAFECHAR(FK_CatalogName))+
					   strlen(XXSAFECHAR(FK_SchemaName))+
					   strlen(XXSAFECHAR(FK_TableName))+
					   1);

   sprintf(imp_sth->statement,
	   cSqlForeignKeys,
	   XXSAFECHAR(PK_CatalogName), XXSAFECHAR(PK_SchemaName),XXSAFECHAR(PK_TableName),
	   XXSAFECHAR(FK_CatalogName), XXSAFECHAR(FK_SchemaName),XXSAFECHAR(FK_TableName)
	  );
   /* fix to handle "" (undef) calls -- thanks to Kevin Shepherd */
   rc = SQLForeignKeys(imp_sth->hstmt, 
		       (PK_CatalogName && *PK_CatalogName) ? PK_CatalogName : 0, SQL_NTS,
		       (PK_SchemaName && *PK_SchemaName) ? PK_SchemaName : 0, SQL_NTS,
		       (PK_TableName && *PK_TableName) ? PK_TableName : 0, SQL_NTS,
		       (FK_CatalogName && *FK_CatalogName) ? FK_CatalogName : 0, SQL_NTS,
		       (FK_SchemaName && *FK_SchemaName) ? FK_SchemaName : 0, SQL_NTS,
		       (FK_TableName && *FK_TableName) ? FK_TableName : 0, SQL_NTS);
   if (!SQL_ok(rc)) {
      dbd_error(sth, rc, "dbd_db_foreign_key_info/SQLForeignKeys");
      return 0;
   }
   return build_results(sth,rc);
}


int
   dbd_db_type_info(dbh, sth, ftype)
   SV *dbh;
SV *sth;
int ftype;
{
   dTHR;
   D_imp_dbh(dbh);
   D_imp_sth(sth);
   RETCODE rc;

   imp_sth->henv = imp_dbh->henv;	/* needed for dbd_error */
   imp_sth->hdbc = imp_dbh->hdbc;

   imp_sth->done_desc = 0;

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   rc = SQLAllocStmt(imp_dbh->hdbc, &imp_sth->hstmt);
   if (rc != SQL_SUCCESS) {
      dbd_error(sth, rc, "dbd_db_type_info/SQLGetTypeInfo");
      return 0;
   }

   /* just for sanity, later. Any internals that may rely on this (including */
   /* debugging) will have valid data */
   imp_sth->statement = (char *)safemalloc(strlen(cSqlGetTypeInfo)+ftype/10+1);
   sprintf(imp_sth->statement, cSqlGetTypeInfo, ftype);

   rc = SQLGetTypeInfo(imp_sth->hstmt, ftype);

   dbd_error(sth, rc, "dbd_db_type_info/SQLGetTypeInfo");
   if (!SQL_ok(rc)) {
      SQLFreeStmt(imp_sth->hstmt, SQL_DROP);
      imp_sth->hstmt = SQL_NULL_HSTMT;
      return 0;
   }

   return build_results(sth,rc);
}


SV *
   dbd_st_cancel(sth)
   SV *sth;
{
   dTHR;
   D_imp_sth(sth);
   RETCODE rc;

   if ( !DBIc_ACTIVE(imp_sth) ) {
      dbd_error(sth, SQL_ERROR, "no statement executing");
      return Nullsv;
   }

   rc = SQLCancel(imp_sth->hstmt);
   if (!SQL_ok(rc)) {
      dbd_error(sth, rc, "dbd_st_cancel/SQLCancel");
      return Nullsv;
   }
   return newSViv(1);
}


int	
   dbd_db_column_info(dbh, sth, catalog, schema, table, column)
   SV *dbh;
SV *sth;
char *catalog;
char *schema;
char *table;
char *column;
{
   dTHR;
   D_imp_dbh(dbh);
   D_imp_sth(sth);
   RETCODE rc;
   imp_sth->henv = imp_dbh->henv;	/* needed for dbd_error */
   imp_sth->hdbc = imp_dbh->hdbc;

   imp_sth->done_desc = 0;

   if (!DBIc_ACTIVE(imp_dbh)) {
      dbd_error(sth, SQL_ERROR, "Can not allocate statement when disconnected from the database");
      return 0;
   }

   rc = SQLAllocStmt(imp_dbh->hdbc, &imp_sth->hstmt);
   if (rc != SQL_SUCCESS) {
      dbd_error(sth, rc, "dbd_db_column_info/SQLAllocStmt");
      return 0;
   }

   /* just for sanity, later.  Any internals that may rely on this (including */
   /* debugging) will have valid data */
   imp_sth->statement = (char *)safemalloc(strlen(cSqlColumns)+
					   strlen(XXSAFECHAR(catalog))+
					   strlen(XXSAFECHAR(schema))+
					   strlen(XXSAFECHAR(table))+
					   strlen(XXSAFECHAR(column))+1);

   sprintf(imp_sth->statement,
	   cSqlColumns, XXSAFECHAR(catalog), XXSAFECHAR(schema),
	   XXSAFECHAR(table), XXSAFECHAR(column));

   rc = SQLColumns(imp_sth->hstmt,
		   (catalog && *catalog) ? catalog : 0, SQL_NTS,
		   (schema && *schema) ? schema : 0, SQL_NTS,
		   (table && *table) ? table : 0, SQL_NTS,
		   (column && *column) ? column : 0, SQL_NTS);

   if (DBIc_TRACE_LEVEL(imp_sth) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_dbh), "SQLColumns call: cat = %s, schema = %s, table = %s, column = %s\n",
		    XXSAFECHAR(catalog), XXSAFECHAR(schema), XXSAFECHAR(table), XXSAFECHAR(column));
   dbd_error(sth, rc, "dbd_db_column_info/SQLColumns");

   if (!SQL_ok(rc)) {
      SQLFreeStmt(imp_sth->hstmt, SQL_DROP);
      imp_sth->hstmt = SQL_NULL_HSTMT;
      return 0;
   }

   return build_results(sth,rc);
}

static void dbd_error_flush(SV *h)
{
   D_imp_xxh(h);
   dTHR;

   struct imp_dbh_st *imp_dbh = NULL;
   struct imp_sth_st *imp_sth = NULL;
   HENV henv = SQL_NULL_HENV;
   HDBC hdbc = SQL_NULL_HDBC;
   HSTMT hstmt = SQL_NULL_HSTMT;

   switch(DBIc_TYPE(imp_xxh))
   {
      case DBIt_ST:
         imp_sth = (struct imp_sth_st *)(imp_xxh);
         imp_dbh = (struct imp_dbh_st *)(DBIc_PARENT_COM(imp_sth));
         hstmt = imp_sth->hstmt;
         break;
      case DBIt_DB:
         imp_dbh = (struct imp_dbh_st *)(imp_xxh);
         break;
      default:
         croak("panic: dbd_error_flush on bad handle type");
   }

   /* We will always have imp_dbh at this point. */
   henv = imp_dbh->henv;
   hdbc = imp_dbh->hdbc;

   while (henv != SQL_NULL_HENV)
   {
      RETCODE rc;
      do
      {
         UCHAR sqlstate[SQL_SQLSTATE_SIZE+1];
         UCHAR ErrorMsg[SQL_MAX_MESSAGE_LENGTH];
         SWORD ErrorMsgLen;
         SDWORD NativeError;

         rc=SQLError(henv, hdbc, hstmt,
		     sqlstate, &NativeError,
		     ErrorMsg, sizeof(ErrorMsg)-1, &ErrorMsgLen);

         if (SQL_ok(rc))
         {
            if (DBIc_TRACE_LEVEL(imp_dbh) >= 8)
            {
               PerlIO_printf(DBIc_LOGPIO(imp_dbh), "%s %s\n", sqlstate,
                  ErrorMsg);
            }
         }
      }
      while (SQL_ok(rc));

      if (hstmt != SQL_NULL_HSTMT)
         hstmt = SQL_NULL_HSTMT;
      else if (hdbc != SQL_NULL_HDBC)
         hdbc = SQL_NULL_HDBC;
      else
         henv = SQL_NULL_HENV;
   }

   return;
}

/* end */
