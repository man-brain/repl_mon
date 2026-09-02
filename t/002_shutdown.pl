# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');

$node->init;
$node->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'repl_mon'
repl_mon.interval = 10
synchronous_commit = on
synchronous_standby_names = 'unavailable'
max_prepared_transactions = 10
});
$node->start;

ok(
	$node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1 FROM public.repl_mon}),
	'repl_mon commits monitoring updates without a synchronous standby');

$node->safe_psql(
	'postgres', q{
BEGIN;
SET LOCAL synchronous_commit = off;
UPDATE public.repl_mon SET ts = clock_timestamp();
PREPARE TRANSACTION 'block_repl_mon';
});

ok(
	$node->poll_query_until(
		'postgres',
		q{SELECT count(*) = 1
		  FROM pg_stat_activity
		 WHERE backend_type = 'repl_mon'
		   AND state = 'active'}),
	'repl_mon is blocked inside its monitoring update');

local $ENV{PGCTLTIMEOUT} = 5;
ok(
	$node->stop('fast', fail_ok => 1),
	'fast shutdown interrupts a blocked repl_mon worker');

done_testing();
