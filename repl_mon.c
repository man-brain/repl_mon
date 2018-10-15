/*-------------------------------------------------------------------------
 *
 * repl_mon.c
 *      Store replication related information of a Postgres instance
 *      once in a while.
 *
 * Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        repl_mon/repl_mon.c
 *
 *-------------------------------------------------------------------------
 */

/* Some general headers for custom bgworker facility */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
/* Needed for getting hostname of the host */
#include "unistd.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);

#if PG_VERSION_NUM >= 100000
void repl_mon_main(Datum) pg_attribute_noreturn();
#endif
/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* GUC variables */
static int interval = 1000;
static char *tablename = "repl_mon";

/* Worker name */
static char *worker_name = "repl_mon";

static char hostname[HOST_NAME_MAX];

#if PG_VERSION_NUM >= 100000
static char *get_current_lsn = "pg_current_wal_lsn()";
#else
static char *get_current_lsn = "pg_current_xlog_location()";
#endif

static void
repl_mon_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
repl_mon_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
repl_mon_prepare_queries()
{
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    SetCurrentStatementStartTimestamp();
}

static void
repl_mon_finish_queries()
{
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Send statistic about updated table to stats collector
     * to prevent bloating table. If we don't do this, we
     * will not know about number of dead tuples in table.
     * Consequently, autovacuum will not come.
     */
    pgstat_report_stat(false);
}

static void
repl_mon_init()
{
    int ret;
    StringInfoData buf;

    if (gethostname(hostname, sizeof hostname) != 0)
        elog(FATAL, "Error while trying to get hostname");

    repl_mon_prepare_queries();

    /* Creating table if it does not exist */
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT * FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'public' AND tablename = '%s'", tablename);
    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, true, 1);
    if (ret != SPI_OK_SELECT)
        elog(FATAL, "Error while trying to get info about table");

    if (SPI_processed == 0)
    {
        initStringInfo(&buf);
        appendStringInfo(&buf, "CREATE TABLE public.%s ("
                "ts timestamp with time zone,"
                "location text, replics int,"
                "master text);", tablename);
        pgstat_report_activity(STATE_RUNNING, buf.data);
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_UTILITY)
            elog(FATAL, "Error while creating table");
    }

    repl_mon_finish_queries();
}

static void
repl_mon_update_data()
{
    int ret;
    StringInfoData buf;

    repl_mon_prepare_queries();

    initStringInfo(&buf);
    appendStringInfo(&buf, "WITH repl AS ("
            "SELECT count(*) AS cnt FROM pg_catalog.pg_stat_replication "
            "WHERE state='streaming') UPDATE public.%s "
            "SET ts = current_timestamp, location = %s, "
            "replics = repl.cnt, master = '%s' FROM repl", tablename, get_current_lsn, hostname);
    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, false, 1);
    if (ret != SPI_OK_UPDATE)
        elog(FATAL, "Error while updating timestamp");

    if (SPI_processed == 0)
    {
        initStringInfo(&buf);
        appendStringInfo(&buf, "WITH repl AS ("
                "SELECT count(*) AS cnt FROM pg_catalog.pg_stat_replication "
                "WHERE state='streaming') INSERT INTO public.%s "
                "SELECT current_timestamp, %s, "
                "repl.cnt, '%s' FROM repl", tablename, get_current_lsn, hostname);
        pgstat_report_activity(STATE_RUNNING, buf.data);
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_INSERT)
            elog(FATAL, "Error while inserting timestamp");
    }

    repl_mon_finish_queries();
}

#if PG_VERSION_NUM < 100000
static void
#else
void
#endif
repl_mon_main(Datum main_arg)
{
    /* Register functions for SIGTERM/SIGHUP management */
    pqsignal(SIGHUP, repl_mon_sighup);
    pqsignal(SIGTERM, repl_mon_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to a database */
#if PG_VERSION_NUM < 110000
    BackgroundWorkerInitializeConnection("postgres", NULL);
#else
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#endif

    /* Creating table if it does not exist */
    repl_mon_init();

    while (!got_sigterm)
    {
        int rc;

        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
#if PG_VERSION_NUM < 100000
                       interval);
#else
                       interval, PG_WAIT_EXTENSION);
#endif
        ResetLatch(&MyProc->procLatch);

        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /* Process signals */
        if (got_sighup)
        {
            /* Process config file */
            ProcessConfigFile(PGC_SIGHUP);
            got_sighup = false;
            ereport(DEBUG1, (errmsg("bgworker repl_mon signal: processed SIGHUP")));
            /* Recreate table if needed */
            repl_mon_init();
        }

        if (got_sigterm)
        {
            /* Simply exit */
            ereport(DEBUG1, (errmsg("bgworker repl_mon signal: processed SIGTERM")));
            proc_exit(0);
        }

        /* Main work happens here */
        repl_mon_update_data();
    }

    /* No problems, so clean exit */
    proc_exit(0);
}

static void
repl_mon_load_params(void)
{
    DefineCustomIntVariable("repl_mon.interval",
                            "Time between writing timestamp (ms).",
                            "Default of 1s, max of 300s",
                            &interval,
                            1000,
                            1,
                            300000,
                            PGC_SIGHUP,
                            GUC_UNIT_MS,
                            NULL,
                            NULL,
                            NULL);
    DefineCustomStringVariable("repl_mon.table",
                               "Name of the table (in schema public).",
                               "Default is repl_mon",
                               &tablename,
                               "repl_mon",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);
}

/*
 * Entry point for worker loading
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    /* Add parameters */
    repl_mon_load_params();

    /* Worker parameter and registration */
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    /* Start only on master hosts after finishing crash recovery */
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
#if PG_VERSION_NUM < 100000
    worker.bgw_main = repl_mon_main;
#endif
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
#if PG_VERSION_NUM >= 100000
    sprintf(worker.bgw_library_name, "repl_mon");
    sprintf(worker.bgw_function_name, "repl_mon_main");
#endif
    /* Wait 10 seconds for restart after crash */
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = (Datum) 0;
#if PG_VERSION_NUM >= 90400
    /*
     * Notify PID is present since 9.4. If this is not initialized
     * a static background worker cannot start properly.
     */
    worker.bgw_notify_pid = 0;
#endif
    RegisterBackgroundWorker(&worker);
}
