# Copyright (c) 2024, PostgreSQL Global Development Group

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
repl_mon.interval = 0
});
$node->start;

# create and drop database
$node->safe_psql('postgres', 'CREATE DATABASE db1');
$node->safe_psql('postgres', 'DROP DATABASE db1');

ok(1);

$node->stop;
done_testing();
