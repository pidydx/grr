#!/usr/bin/env python
"""Benchmark tests for MySQL advanced data store."""


from grr.lib import data_store_test
from grr.lib import flags
from grr.lib import test_lib
from grr.lib.data_stores import mysql_experimental_data_store_test


class MySQLExperimentalDataStoreBenchmarks(
    mysql_experimental_data_store_test.MySQLExperimentalTestMixin,
    data_store_test.DataStoreBenchmarks):
  """Benchmark the mysql data store abstraction."""


class MySQLExperimentalDataStoreCSVBenchmarks(
    mysql_experimental_data_store_test.MySQLExperimentalTestMixin,
    data_store_test.DataStoreCSVBenchmarks):
  """Benchmark the mysql data store abstraction."""


def main(args):
  test_lib.main(args)


if __name__ == "__main__":
  flags.StartMain(main)
