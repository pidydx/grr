#!/usr/bin/env python
# -*- mode: python; encoding: utf-8 -*-
"""An implementation of a data store based on mysql."""

import logging
import Queue
import thread
import threading
import time
import hashlib
from warnings import filterwarnings


import MySQLdb
from MySQLdb import cursors

from grr.config import contexts
from grr.lib import aff4
from grr.lib import config_lib
from grr.lib import data_store
from grr.lib import rdfvalue
from grr.lib import utils
from grr.lib.rdfvalues import flows as rdf_flows

# We use INSERT IGNOREs which generate useless duplicate entry warnings.
filterwarnings("ignore", category=MySQLdb.Warning, message=r"Duplicate entry.*")


# pylint: disable=nonstandard-exception
class Error(data_store.Error):
  """Base class for all exceptions in this module."""


# pylint: enable=nonstandard-exception


class SafeQueue(Queue.Queue):
  """Queue with RLock instead of Lock."""

  def __init__(self, maxsize=0):
    # Queue is an old-style class so we can't use super()
    Queue.Queue.__init__(self, maxsize=maxsize)
    # This code is far from ideal as the Queue implementation makes it difficult
    # to replace Lock with RLock. Here we override the variables that use
    # self.mutex in the super class __init__.  If Queue.Queue.__init__
    # implementation changes this code could break.
    self.mutex = threading.RLock()
    self.not_empty = threading.Condition(self.mutex)
    self.not_full = threading.Condition(self.mutex)
    self.all_tasks_done = threading.Condition(self.mutex)


class MySQLConnection(object):
  """A Class to manage MySQL database connections."""

  def __init__(self):
    database_name = config_lib.CONFIG["Mysql.database_name"]
    try:
      self.dbh = self._MakeConnection(database_name)
      self.cursor = self.dbh.cursor()
      self.cursor.connection.autocommit(True)
      self.cursor.execute("SET NAMES binary")
    except MySQLdb.OperationalError as e:
      # Database does not exist
      dbh = self._MakeConnection()
      cursor = dbh.cursor()
      cursor.connection.autocommit(True)
      cursor.execute("Create database `%s`" % database_name)
      cursor.close()
      dbh.close()
      data_store.DB._CreateTables()
      self.dbh = self._MakeConnection(database_name)
      self.cursor = self.dbh.cursor()
      self.cursor.connection.autocommit(True)
      self.cursor.execute("SET NAMES binary")

  def _MakeConnection(self, database_name = ""):
    """Repeat connection attempts to server until we get a valid connection."""
    while True:
      try:
        connection_args = dict(
            user=config_lib.CONFIG["Mysql.database_username"],
            db=database_name,
            charset="utf8",
            passwd=config_lib.CONFIG["Mysql.database_password"],
            cursorclass=cursors.DictCursor,
            host=config_lib.CONFIG["Mysql.host"],
            port=config_lib.CONFIG["Mysql.port"])

        dbh = MySQLdb.connect(**connection_args)
        return dbh
      except MySQLdb.OperationalError as e:
        # This is a fatal error, we just raise the top level exception here.
        if "Access denied" in str(e):
          raise Error(str(e))

        elif "Unknown database" in str(e):
          raise e

        else:
          logging.warning("Datastore connection retrying after failed with %s.",
                          str(e))
        time.sleep(1)



class ConnectionPool(object):
  """A pool of connections to the mysql server.

  Uses unfinished_tasks to track the number of open connections.
  """

  def __init__(self):
    self.connections = SafeQueue()

  def GetConnection(self):
    pool_max_size = int(config_lib.CONFIG["Mysql.conn_pool_max"])
    if self.connections.empty() and (
        self.connections.unfinished_tasks < pool_max_size):
      self.connections.put(MySQLConnection())
    if pool_max_size == 0:
      connection = MySQLConnection()
    else:
      connection = self.connections.get(block=True)
    return connection

  def PutConnection(self, connection):
    # If the pool is low on connections return this connection to the pool
    if int(config_lib.CONFIG["Mysql.conn_pool_max"]) == 0:
      pool_min_size = 0
    else:
      pool_min_size = int(config_lib.CONFIG["Mysql.conn_pool_min"])

    if self.connections.qsize() < pool_min_size:
      self.connections.put(connection)
    else:
      self.DropConnection(connection)

  def DropConnection(self, connection):
    """Attempt to cleanly drop the connection."""
    try:
      connection.cursor.close()
    except MySQLdb.Error:
      pass

    try:
      connection.dbh.close()
    except MySQLdb.Error:
      pass

  def QueryComplete(self):
    if int(config_lib.CONFIG["Mysql.conn_pool_max"]) > 0:
      self.connections.task_done()


class MySQLMutationPool(object):
  """A mutation pool.

  This is a pool to group a number of mutations together and apply
  them at the same time. Note that there are no guarantees about the
  atomicity of the mutations. Currently, no mutation will be applied
  before Flush() is called on the pool. If datastore errors occur
  during application, some mutations might be applied while others are
  not.
  """

  def __init__(self, token=None):
    self.token = token

    self.buffer_lock = threading.RLock()
    self.to_insert_aff4 = []
    self.to_replace_aff4 = []
    self.to_delete_aff4_subjects = []
    self.to_delete_aff4_attributes = {}
    self.query_buffer = []
    self.transaction_buffer = []

  def __enter__(self):
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    self.Flush()

  def Size(self):
    return (len(self.to_insert_aff4) + len(self.to_replace_aff4) +
            len(self.query_buffer) + len(self.transaction_buffer))

  def MultiSet(self, subject, values, timestamp=None, replace=True, sync=False, to_delete=None, token=None):
    """Set multiple attributes' values for this subject in one operation."""
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [subject], "w")
    to_delete = set(to_delete or [])

    # Prepare a bulk insert operation.
    subject = utils.SmartUnicode(subject)
    to_insert_aff4 = []
    to_replace_aff4 = []

    # Build a document for each unique timestamp.
    for attribute, sequence in values.items():
      for value in sequence:

        if isinstance(value, tuple):
          value, entry_timestamp = value
        else:
          entry_timestamp = timestamp

        if entry_timestamp is None:
          entry_timestamp = timestamp

        if entry_timestamp is not None:
          entry_timestamp = int(entry_timestamp)

        attribute = utils.SmartUnicode(attribute)
        data = self._Encode(value)

        # Replacing means to delete all versions of the attribute first.
        if replace or attribute in to_delete:
          existing = self._CountExistingAFF4Rows(subject, attribute)
          if existing == 1:
            # Ensure only the latest value is in the buffer.
            with self.buffer_lock:
              for buf_subject, buf_attribute, buf_data, buf_ts in self.to_replace_aff4:
                if subject == buf_subject and attribute == buf_attribute:
                    self.to_replace_aff4.remove([buf_subject, buf_attribute, buf_data, buf_ts])
            to_replace_aff4.append([subject, attribute, data, entry_timestamp])
          else:
            # Ensure only the latest value is in the buffer.
            with self.buffer_lock:
              for buf_subject, buf_attribute, buf_data, buf_ts in self.to_insert_aff4:
                if subject == buf_subject and attribute == buf_attribute:
                    self.to_insert_aff4.remove([buf_subject, buf_attribute, buf_data, buf_ts])
            to_insert_aff4.append([subject, attribute, data, entry_timestamp])
          if existing > 1:
            to_delete.add(attribute)
          elif attribute in to_delete:
            to_delete.remove(attribute)

        else:
          to_insert_aff4.append([subject, attribute, data, entry_timestamp])

    if to_delete:
      self.DeleteAttributes(subject, to_delete, sync=sync, token=token)

    if sync:
      if to_replace_aff4:
        for query in self._BuildAFF4Replaces(to_replace_aff4):
          data_store.DB.ExecuteQuery(query["query"], query["args"])
      if to_insert_aff4:
        for transaction in self._BuildAFF4Inserts(to_insert_aff4):
          data_store.DB.ExecuteTransaction(transaction)
    else:
      if to_replace_aff4:
        with self.buffer_lock:
          self.to_replace_aff4.extend(to_replace_aff4)
      if to_insert_aff4:
        with self.buffer_lock:
          self.to_insert_aff4.extend(to_insert_aff4)

  def Set(self, subject, attribute, value, timestamp=None, replace=True):
    self.MultiSet(
        subject, {attribute: [value]}, timestamp=timestamp, replace=replace)

  def DeleteAttributes(self, subject, attributes, start=None, end=None, sync=False, token=None):
    """Remove some attributes from a subject."""
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [subject], "w")
    if not attributes:
      return

    if isinstance(attributes, basestring):
      raise ValueError(
          "String passed to DeleteAttributes (non string iterable expected).")

    timestamp = self._MakeTimestamp(start, end)
    attributes = [utils.SmartUnicode(attribute) for attribute in attributes]

    if sync:
      transaction = self._BuildAFF4DeleteAttributes(subject, attributes, timestamp)
      data_store.DB.ExecuteTransaction(transaction)
    else:
      with self.buffer_lock:
        self.to_delete_aff4_attributes.setdefault(subject, {}).setdefault(timestamp, []).extend(attributes)

  def DeleteSubject(self, subject, sync=False, token=None):
    self.DeleteSubjects([subject], sync=sync, token=token)

  def DeleteSubjects(self, subjects, sync=False, token=None):
    if token is None:
      token = self.token

    data_store.DB.security_manager.CheckDataStoreAccess(token, subjects, "w")

    if sync:
      transaction = self._BuildAFF4DeleteSubjects(subjects)
      data_store.DB.ExecuteTransaction(transaction)
    else:
      with self.buffer_lock:
        self.to_delete_aff4_subjects.extend(subjects)

  def DispatchQuery(self, queries, sync):
      if sync:
        for query in queries:
          data_store.DB.ExecuteQuery(query["query"], query["args"])
      else:
        with self.buffer_lock:
          for query in queries:
            self.query_buffer.append(([query["query"], query["args"]]))

  def CreateNotifications(self, queue, notifications, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [queue], "w")
    query = "REPLACE INTO notifications (queue_hash, session_id_hash, session_id, timestamp, value) VALUES "
    args = []
    queue_hash = data_store.DB._Hash(queue)
    for session_id, notification_queue in notifications.items():
      session_id_hash = data_store.DB._Hash(session_id)
      for notification, timestamp in notification_queue:
        if timestamp is not None:
          timestamp = int(timestamp)
        elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
          timestamp = time.time() * 1000000
        args.extend([queue_hash, session_id_hash, session_id, timestamp, timestamp, self._Encode(notification.SerializeToString())])
    query += ", ".join(["(%s, %s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000), %s), unhex(%s))"] * (len(args) / 6))

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteNotifications(self, queue, session_ids, start=None, end=None, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [queue], "w")
    query_prefix = "DELETE FROM notifications WHERE "
    conditions = []
    args = []
    queue_hash = data_store.DB._Hash(queue)
    for session_id in session_ids:
      session_id_hash = data_store.DB._Hash(session_id)
      main_conditions = "queue_hash=%s AND session_id_hash=%s"
      args.append(queue_hash)
      args.append(session_id_hash)
      timestamp = self._MakeTimestamp(start, end)
      ts_conditions = ""
      if isinstance(timestamp, (tuple, list)):
        ts_conditions = " AND timestamp >= %s AND timestamp <= %s"
        args.append(int(timestamp[0]))
        args.append(int(timestamp[1]))
      conditions.append("("+main_conditions+ts_conditions+")")

    query = query_prefix + " OR ".join(conditions)
    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateTasks(self, queue, task_queue, timestamp, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [queue], "w")
    queue_hash = data_store.DB._Hash(queue)
    if timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
      timestamp = time.time() * 1000000

    query = "REPLACE INTO tasks (queue_hash, task_id, timestamp, value) VALUES"
    args = []
    for task_id, tasks in task_queue.items():
      for task in tasks:
        args.extend([queue_hash, int(task_id), timestamp, timestamp, self._Encode(task.SerializeToString())])
    query += ", ".join(["(%s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000),%s), unhex(%s))"] * (len(args) / 5))
    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteTasks(self, queue, task_ids, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [queue], "w")
    queue_hash = data_store.DB._Hash(queue)
    query = "DELETE FROM tasks WHERE queue_hash=%s AND task_id IN (" + ", ".join(["%s"] * len(task_ids)) + ")"
    args = [queue_hash]
    args.extend([int(task_id) for task_id in task_ids])
    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateResponses(self, session_id, responses, sync=False, token=None):
    if token is None:
      token = self.token

    query = "REPLACE INTO responses (session_id_hash, request_id, response_id, status, timestamp, value) VALUES"
    subjects = [session_id.Add("state")]
    args = []
    session_id_hash = data_store.DB._Hash(session_id)
    for response, timestamp in responses:
      if timestamp is not None:
        timestamp = int(timestamp)
      elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
        timestamp = time.time() * 1000000
      subjects.append(session_id.Add("state/request:%08X" % response.request_id))
      if response.type == rdf_flows.GrrMessage.Type.STATUS:
        status=True
      else:
        status=False
      args.extend([session_id_hash, int(response.request_id), int(response.response_id), status, timestamp, timestamp, self._Encode(response.SerializeToString())])
    query += ", ".join(["(%s, %s, %s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000),%s), unhex(%s))"] * (len(args) / 7))

    data_store.DB.security_manager.CheckDataStoreAccess(token, subjects, "w")
    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteResponses(self, session_id, request_id, responses=None, sync=False, token=None):
    if token is None:
      token = self.token
    response_subject = session_id.Add("state/request:%08X" % request_id)
    status_subject = session_id.Add("state")
    data_store.DB.security_manager.CheckDataStoreAccess(token, [response_subject, status_subject], "w")
    session_id_hash = data_store.DB._Hash(session_id)
    query = "DELETE FROM responses WHERE session_id_hash=%s AND request_id=%s"
    args = [session_id_hash, int(request_id)]
    if responses:
      query += " AND response_id IN (" + ", ".join(["%s"] * len(responses)) + ")"
      args.extend([int(response.response_id) for response in responses])

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateRequests(self, session_id, requests, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [session_id.Add("state")], "w")
    session_id_hash = data_store.DB._Hash(session_id)
    query = "REPLACE INTO requests (session_id_hash, request_id, timestamp, value) VALUES "
    args = []
    for request, timestamp in requests:
      if timestamp is not None:
        timestamp = int(timestamp)
      elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
        timestamp = time.time() * 1000000
      args.extend([session_id_hash, int(request.id), timestamp, timestamp, self._Encode(request.SerializeToString())])
    query += ", ".join(["(%s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000),%s), unhex(%s))"] * (len(args) / 5))

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteRequests(self, session_id, requests, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [session_id.Add("state")], "w")
    session_id_hash = data_store.DB._Hash(session_id)
    query = "DELETE FROM requests WHERE session_id_hash=%s AND request_id IN (" + ", ".join(["%s"] * len(requests)) + ")"
    args = [session_id_hash]
    args.extend([int(request.id) for request in requests])

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateCollectionItem(self, collection_id, rdf_value, timestamp, suffix, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [collection_id.Add("Results").Add("%016x.%06x" % (timestamp, suffix))], "w")
    collection_id_hash = data_store.DB._Hash(collection_id)
    query = "REPLACE INTO collections (collection_id_hash, timestamp, suffix, value) VALUES (%s, %s, %s, unhex(%s))"
    args = [collection_id_hash, int(timestamp), int(suffix), self._Encode(rdf_value.SerializeToString())]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteCollection(self, collection_id, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [collection_id], "w")
    collection_id_hash = data_store.DB._Hash(collection_id)
    items_query = "DELETE FROM collections WHERE collection_id_hash=%s"
    indexes_query = "DELETE FROM collection_indexes WHERE collection_id_hash=%s"
    types_query = "DELETE FROM collection_types WHERE collection_id_hash=%s"
    args = [collection_id_hash]

    self.DispatchQuery([{"query": items_query, "args": args}], sync)
    self.DispatchQuery([{"query": indexes_query, "args": args}], sync)
    self.DispatchQuery([{"query": types_query, "args": args}], sync)

  def CreateMultiTypeCollectionEntry(self, collection_id, collection_type, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [collection_id], "w")
    collection_id_hash = data_store.DB._Hash(collection_id)
    collection_type_hash = data_store.DB._Hash(collection_type)
    query = "INSERT IGNORE INTO collection_types (collection_id_hash, type_hash, type) VALUES (%s, %s, %s)"
    args = [collection_id_hash, collection_type_hash, collection_type]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateCollectionIndexEntry(self, collection_id, idx, item_timestamp, item_suffix, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [collection_id], "w")
    collection_id_hash = data_store.DB._Hash(collection_id)
    query = "INSERT INTO collection_indexes (collection_id_hash, idx, item_timestamp, item_suffix) VALUES (%s, %s, %s, %s)"
    args = [collection_id_hash, int(idx), int(item_timestamp), int(item_suffix)]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateQueueItem(self, queue, rdf_value, timestamp, suffix=None, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [queue.Add("Records").Add("%016x.%06x" % (timestamp, suffix))], "w")
    queue_hash = data_store.DB._Hash(queue)
    query = "REPLACE INTO queues (queue_hash, timestamp, suffix, value) VALUES (%s, %s, %s, unhex(%s))"
    args = [queue_hash, int(timestamp), int(suffix), self._Encode(rdf_value.SerializeToString())]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def UpdateQueueItemLeases(self, records, lease_expiration, sync=False, token=None):
    if token is None:
      token = self.token
    subjects = [queue.Add("Records").Add("%016x.%06x" % (timestamp, suffix)) for queue, timestamp, suffix in records]
    data_store.DB.security_manager.CheckDataStoreAccess(token, subjects, "w")
    queries = []

    for queue, timestamp, suffix in records:
      queue_hash = data_store.DB._Hash(queue)
      query = "UPDATE IGNORE queues SET lease_expiration=%s WHERE queue_hash=%s AND timestamp=%s AND suffix=%s"
      args = [int(lease_expiration), queue_hash, int(timestamp), int(suffix)]
      queries.append({"query": query, "args": args})

    self.DispatchQuery(queries, sync)

  def DeleteQueueItems(self, records, sync=False, token=None):
    if token is None:
      token = self.token
    subjects = [queue.Add("Records").Add("%016x.%06x" % (timestamp, suffix)) for queue, timestamp, suffix in records]
    data_store.DB.security_manager.CheckDataStoreAccess(token, subjects, "w")

    conditions = []
    args = []
    query_prefix = "DELETE FROM queues WHERE "
    for queue, timestamp, suffix in records:
      queue_hash = data_store.DB._Hash(queue)
      conditions.append("(queue_hash=%s AND timestamp=%s AND suffix=%s)")
      args.append(queue_hash)
      args.append(timestamp)
      args.append(suffix)

    query = query_prefix + " OR ".join(conditions)
    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateStats(self, process_stats_store, stats, timestamp, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [process_stats_store], "w")
    process_stats_store_hash = data_store.DB._Hash(process_stats_store)
    if timestamp is not None:
      timestamp = int(timestamp)
    elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
      timestamp = time.time() * 1000000

    query = "INSERT INTO stats_store (process_stats_store_hash, process_stats_store, metric_name_hash, metric_name, timestamp, metric_value) VALUES"
    args = []
    for metric_name, metric_values in stats.items():
      metric_name_hash = data_store.DB._Hash(metric_name)
      for metric_value in metric_values:
        args.extend([process_stats_store_hash, process_stats_store, metric_name_hash, metric_name, timestamp, timestamp, self._Encode(metric_value)])
    query += ", ".join(
      ["(%s, %s, %s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000),%s), unhex(%s))"] * (len(args) / 7))

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteStats(self, process_stats_store, metric_names, start=None, end=None, sync=False, token=None):
    _ = metric_names
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [process_stats_store], "w")
    process_stats_store_hash = data_store.DB._Hash(process_stats_store)
    query = "DELETE FROM stats_store WHERE process_stats_store_hash=%s"
    args = [process_stats_store_hash]

    timestamp = self._MakeTimestamp(start, end)
    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

      self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateVersionedCollectionNotification(self, urn, timestamp=None, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [data_store.DB.VERSIONED_COLLECTION_NOTIFICATION_QUEUE], "w")
    urn_hash = data_store.DB._Hash(urn)
    if timestamp is not None:
      timestamp = int(timestamp)
    elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
      timestamp = time.time() * 1000000

    query = "REPLACE INTO pvc_notifications (urn_hash, urn, timestamp) VALUES (%s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000), %s))"
    args = [urn_hash, urn, timestamp, timestamp]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteVersionedCollectionNotifications(self, urn, end=None, sync=True, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [data_store.DB.VERSIONED_COLLECTION_NOTIFICATION_QUEUE], "w")
    urn_hash = data_store.DB._Hash(urn)
    query = "DELETE FROM pvc_notifications WHERE urn_hash=%s AND timestamp <= %s"
    args = [urn_hash, int(end)]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateLabels(self, urn, labels, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [urn], "w")
    urn_hash = data_store.DB._Hash(urn)
    query = "REPLACE INTO labels (urn_hash, label_hash, label) VALUES "
    args = []

    for label in labels:
      label_hash = data_store.DB._Hash(label)
      args.extend([urn_hash, label_hash, label])

    query += ", ".join(
      ["(%s, %s, %s)"] * (len(args) / 3))

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteLabels(self, urn, labels, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [urn], "w")
    urn_hash = data_store.DB._Hash(urn)
    query = "DELETE FROM labels WHERE urn_hash=%s AND label_hash IN (" + ", ".join(
      ["%s"] * len(labels)) + ")"
    args = [urn_hash]
    args.extend([data_store.DB._Hash(label) for label in labels])

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateHashIndex(self, index, file_urn, timestamp=None, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [index], "w")
    hash_urn_hash = data_store.DB._Hash(index)
    file_urn_hash = data_store.DB._Hash(file_urn)

    if timestamp is not None:
      timestamp = int(timestamp)
    elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
      timestamp = time.time() * 1000000

    query = "REPLACE INTO hash_index (hash_urn_hash, hash_urn, file_urn_hash, file_urn, timestamp) VALUES (%s, %s, %s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000), %s))"
    args = [hash_urn_hash, index, file_urn_hash, file_urn, timestamp, timestamp]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateAFF4Index(self, parent_urn, child_urn, timestamp=None, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [parent_urn], "w")
    parent_urn_hash = data_store.DB._Hash(parent_urn)
    child_urn_hash = data_store.DB._Hash(child_urn)
    if timestamp is not None:
      timestamp = int(timestamp)
    elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
      timestamp = time.time() * 1000000

    query = "REPLACE INTO aff4_index (urn_hash, urn, child_urn_hash, child_urn, timestamp) VALUES (%s, %s, %s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000), %s))"
    args = [parent_urn_hash, parent_urn, child_urn_hash, child_urn, timestamp, timestamp]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteAFF4Index(self, parent_urn, child_urn, sync=False, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [parent_urn], "w")
    parent_urn_hash = data_store.DB._Hash(parent_urn)
    child_urn_hash = data_store.DB._Hash(child_urn)
    query = "DELETE FROM aff4_index WHERE urn_hash=%s AND child_urn_hash=%s"
    args = [parent_urn_hash, child_urn_hash]

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def CreateKeywordsIndex(self, name, keywords, timestamp=None, sync=True, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [data_store.DB.CLIENT_INDEX_URN.Add(keyword) for keyword in keywords], "w")
    name_hash = data_store.DB._Hash(name)
    query = "REPLACE INTO keywords (keyword_hash, keyword, name_hash, name, timestamp) VALUES "
    args = []

    if timestamp is not None:
      timestamp = int(timestamp)
    elif timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
      timestamp = time.time() * 1000000

    for keyword in keywords:
      keyword_hash = data_store.DB._Hash(keyword)
      args.extend([keyword_hash, keyword, name_hash, name, timestamp, timestamp])

    query += ", ".join(
      ["(%s, %s, %s, %s, if(%s is NULL,floor(unix_timestamp(now(6))*1000000), %s))"] * (len(args) / 6))

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def DeleteKeywordsIndex(self, name, keywords, sync=True, token=None):
    if token is None:
      token = self.token
    data_store.DB.security_manager.CheckDataStoreAccess(token, [data_store.DB.CLIENT_INDEX_URN.Add(keyword) for keyword in keywords], "w")
    name_hash = data_store.DB._Hash(name)
    query = "DELETE FROM keywords WHERE name_hash=%s AND keyword_hash IN ("
    args = [name_hash]

    query += ", ".join(["%s"] * len(keywords)) + ")"
    args.extend([data_store.DB._Hash(keyword) for keyword in keywords])

    self.DispatchQuery([{"query": query, "args": args}], sync)

  def Flush(self):
    data_store.DB.Flush()
    self.FlushOnce()

  def FlushOnce(self):
    with self.buffer_lock:
      query_buffer = self.query_buffer
      to_insert_aff4 = self.to_insert_aff4
      to_replace_aff4 = self.to_replace_aff4
      to_delete_aff4_subjects = self.to_delete_aff4_subjects
      to_delete_aff4_attributes = self.to_delete_aff4_attributes
      self.to_replace_aff4 = []
      self.to_insert_aff4 = []
      self.to_delete_aff4_subjects = []
      self.to_delete_aff4_attributes = {}
      self.query_buffer = []

    for query, args in query_buffer:
      data_store.DB.ExecuteQuery(query, args)

    for subject, timestamps in to_delete_aff4_attributes.items():
      for timestamp, attributes in timestamps.items():
        transaction = self._BuildAFF4DeleteAttributes(subject, attributes, timestamp)
        data_store.DB.ExecuteTransaction(transaction)

    if to_delete_aff4_subjects:
      transaction = self._BuildAFF4DeleteSubjects(to_delete_aff4_subjects)
      data_store.DB.ExecuteTransaction(transaction)

    if to_insert_aff4:
      for transaction in self._BuildAFF4Inserts(to_insert_aff4):
        data_store.DB.ExecuteTransaction(transaction)

    if to_replace_aff4:
      for query in self._BuildAFF4Replaces(to_replace_aff4):
        data_store.DB.ExecuteQuery(query["query"], query["args"])

  def _BuildAFF4Replaces(self, values):
    queries = []

    for (subject, attribute, data, timestamp) in values:
      # This is a hack to allow for testing with FakeTime while letting MySQL set timestamps in production to avoid races
      if timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
        timestamp = time.time() * 1000000
      table, attribute = attribute.split(":", 1)
      urn_hash = data_store.DB._Hash(subject)
      attribute_hash = data_store.DB._Hash(attribute)
      query = {"query": "UPDATE %s" % table + " SET value=unhex(%s), timestamp=if(%s is NULL,floor(unix_timestamp(now(6))*1000000),%s) WHERE urn_hash=%s AND attribute_hash=%s",
               "args": [data, timestamp, timestamp, urn_hash, attribute_hash]}

      queries.append(query)
    return queries

  def _BuildAFF4Inserts(self, values):
    subjects = {}
    for (subject, attribute, value, timestamp) in values:
      # This is a hack to allow for testing with FakeTime while letting MySQL set timestamps in production to avoid races
      if timestamp is None and contexts.TEST_CONTEXT in config_lib.CONFIG.context:
        timestamp = time.time() * 1000000

      table, attribute = attribute.split(":", 1)
      urn_hash = data_store.DB._Hash(subject)
      attribute_hash = data_store.DB._Hash(attribute)
      subjects.setdefault(subject, {}).setdefault(table, []).extend([urn_hash, attribute_hash, attribute, timestamp, timestamp, value])

    for _, attribute_tables in subjects.items():
      queries = []

      for table, values in attribute_tables.items():
        query = {"query": "INSERT INTO %s (urn_hash, attribute_hash, attribute, timestamp, value) VALUES " % table,
                 "args": values}

        query["query"] += ", ".join([
            "(%s, %s, %s, "
            "if(%s is NULL,floor(unix_timestamp(now(6))*1000000),%s), "
            "unhex(%s))"
        ] * (len(query["args"]) / 6))
        queries.append(query)
      yield queries

  def _BuildAFF4DeleteSubjects(self, subjects):
    transaction = []

    for table in ["aff4", "metadata", "aff4_index", "locks"]:
      query = {"query": "DELETE FROM %s WHERE urn_hash IN (" % table,
               "args": [data_store.DB._Hash(subject) for subject in subjects]}
      query["query"] += ", ".join(["%s"] * len(subjects)) + ")"
      transaction.append(query)
    return transaction

  def _BuildAFF4DeleteAttributes(self, subject, attributes, timestamp=None):
    """Build the DELETE query to be executed."""
    attribute_tables = {}
    urn_hash = data_store.DB._Hash(subject)
    for attribute in attributes:
      table, attribute = attribute.split(":", 1)
      attribute_tables.setdefault(table, []).append(attribute)

    transaction = []
    for table, attributes in attribute_tables.items():
      query = {"query": "DELETE FROM %s " % table,
               "args": [urn_hash] + [data_store.DB._Hash(attribute) for attribute in attributes]}
      query["query"] += "WHERE urn_hash=%s AND attribute_hash IN ("
      query["query"] += ", ".join(["%s"] * len(attributes)) + ")"
      if isinstance(timestamp, (tuple, list)):
        query["query"] += " AND timestamp >= %s AND timestamp <= %s"
        query["args"].append(int(timestamp[0]))
        query["args"].append(int(timestamp[1]))

      transaction.append(query)
    return transaction

  def _CountExistingAFF4Rows(self, subject, attribute):
    table, attribute = attribute.split(":", 1)
    urn_hash = data_store.DB._Hash(subject)
    attribute_hash = data_store.DB._Hash(attribute)
    query = ("SELECT count(*) AS total FROM %s " % table +
             "WHERE urn_hash=%s "
             "AND attribute_hash=%s")
    args = [urn_hash, attribute_hash]
    result, _ = data_store.DB.ExecuteQuery(query, args)
    return int(result[0]["total"])

  def _Encode(self, value):
    """Encode the value for the attribute."""
    try:
      return value.SerializeToString().encode("hex")
    except AttributeError:
      if isinstance(value, (int, long)):
        return str(value).encode("hex")
      else:
        # Types "string" and "bytes" are stored as strings here.
        return utils.SmartStr(value).encode("hex")

  def _MakeTimestamp(self, start=None, end=None):
    """Create a timestamp using a start and end time.

    Args:
      start: Start timestamp.
      end: End timestamp.
    Returns:
      A tuple (start, end) of converted timestamps or None for all time.
    """
    mysql_unsigned_bigint_max = 18446744073709551615
    ts_start = int(start or 0)
    if end is None:
      ts_end = mysql_unsigned_bigint_max
    else:
      ts_end = int(end)
    if ts_start == 0 and ts_end == mysql_unsigned_bigint_max:
      return None
    else:
      return (ts_start, ts_end)


class MySQLExperimentalDataStore(data_store.DataStore):
  """A mysql based data store."""

  CONNECTION_POOL = None
  MUTATION_POOL = None
  mutation_pool_cls = MySQLMutationPool


  def __init__(self):
    # Use the global connection pool.
    if MySQLExperimentalDataStore.CONNECTION_POOL is None:
      MySQLExperimentalDataStore.CONNECTION_POOL = ConnectionPool()
    self.con_pool = self.CONNECTION_POOL
    self.mutation_pool = self.GetMutationPool()

    # Build a mapping between column names and types.
    self.attribute_types = {attribute.predicate: attribute.attribute_type.data_store_type for attribute in aff4.Attribute.PREDICATES.values()}


    super(MySQLExperimentalDataStore, self).__init__()

  def Initialize(self):
    super(MySQLExperimentalDataStore, self).Initialize()
    # This should only raise in ExecuteQuery if the table doesn't exist.
    try:
      self.ExecuteQuery("desc `aff4`")
    except MySQLdb.Error:
      logging.debug("Recreating Tables")
      self.Clear()

  def Destroy(self):
    self.ExecuteQuery("DROP DATABASE `%s`" % config_lib.CONFIG["Mysql.database_name"])

  def DropTables(self):
    """Drop all existing tables."""

    rows, _ = self.ExecuteQuery(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='%s'" % config_lib.CONFIG["Mysql.database_name"])
    for row in rows:
      self.ExecuteQuery("DROP TABLE `%s`" % row["table_name"])

  def Clear(self):
    """Drops the tables and creates a new ones."""
    self.DropTables()
    self._CreateTables()

  def _CreateTables(self):
    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `aff4` (
      id BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
      urn_hash BINARY(16) NOT NULL,
      attribute_hash BINARY(16) NOT NULL,
      attribute TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      KEY `master` (`urn_hash`, `attribute_hash`, `timestamp`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing AFF4 objects';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `metadata` (
      id BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
      urn_hash BINARY(16) NOT NULL,
      attribute_hash BINARY(16) NOT NULL,
      attribute TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      KEY `master` (`urn_hash`, `attribute_hash`, `timestamp`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing AFF4 objects';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `locks` (
      urn_hash BINARY(16) PRIMARY KEY NOT NULL,
      lock_owner BIGINT UNSIGNED DEFAULT NULL,
      lock_expiration BIGINT UNSIGNED DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing locks on subjects';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `notifications` (
      queue_hash BINARY(16) NOT NULL,
      session_id_hash BINARY(16) NOT NULL,
      session_id TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      PRIMARY KEY `master` (`queue_hash`, `session_id_hash`, `timestamp`),
      KEY `secondary` (`queue_hash`, `timestamp`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing notifications';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `requests` (
      session_id_hash BINARY(16) NOT NULL,
      request_id BIGINT UNSIGNED NOT NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      PRIMARY KEY `master` (`session_id_hash`, `request_id`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing requests';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `responses` (
      session_id_hash BINARY(16) NOT NULL,
      request_id BIGINT UNSIGNED NOT NULL,
      response_id BIGINT UNSIGNED NOT NULL,
      status BOOL NOT NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      PRIMARY KEY `master` (`session_id_hash`, `request_id`, `response_id`),
      KEY `status` (`session_id_hash`, `status`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing responses';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `tasks` (
      queue_hash BINARY(16) NOT NULL,
      task_id BIGINT UNSIGNED NOT NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      PRIMARY KEY `master` (`queue_hash`, `task_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing tasks';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `collections` (
      collection_id_hash BINARY(16) NOT NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      suffix BIGINT UNSIGNED NOT NULL,
      value MEDIUMBLOB NULL,
      PRIMARY KEY `master` (`collection_id_hash`, `timestamp`, `suffix`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing collection items';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `collection_indexes` (
      collection_id_hash BINARY(16) NOT NULL,
      idx BIGINT UNSIGNED NOT NULL,
      item_timestamp BIGINT UNSIGNED NOT NULL,
      item_suffix BIGINT UNSIGNED NOT NULL,
      PRIMARY KEY `master` (`collection_id_hash`, `idx`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing collection indexes';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `collection_types` (
      collection_id_hash BINARY(16) NOT NULL,
      type_hash BINARY(16) NOT NULL,
      type TEXT CHARACTER SET utf8 NULL,
      PRIMARY KEY `master` (`collection_id_hash`, `type_hash`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing collection types';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `queues` (
      queue_hash BINARY(16) NOT NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      suffix BIGINT UNSIGNED NOT NULL,
      lease_expiration BIGINT UNSIGNED DEFAULT 0,
      value MEDIUMBLOB NULL,
      PRIMARY KEY `master` (`queue_hash`, `timestamp`, `suffix`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing queue items';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `stats_store` (
      id BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
      process_stats_store_hash BINARY(16) NOT NULL,
      process_stats_store TEXT CHARACTER SET utf8 NULL,
      metric_name_hash BINARY(16) NOT NULL,
      metric_name TEXT CHARACTER SET utf8 NULL,
      metric_value MEDIUMBLOB NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      KEY `master` (`process_stats_store_hash`, `metric_name_hash`, `timestamp`),
      KEY `secondary` (`process_stats_store_hash`, `timestamp`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing queue items';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `keywords` (
      keyword_hash BINARY(16) NOT NULL,
      keyword TEXT CHARACTER SET utf8 NULL,
      name_hash BINARY(16) NOT NULL,
      name TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      PRIMARY KEY `master` (`keyword_hash`, `name_hash`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing keywords';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `aff4_index` (
      urn_hash BINARY(16) NOT NULL,
      urn TEXT CHARACTER SET utf8 NULL,
      child_urn_hash BINARY(16) NOT NULL,
      child_urn TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      PRIMARY KEY `master` (`urn_hash`, `child_urn_hash`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing the AFF4 index';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `labels` (
      urn_hash BINARY(16) NOT NULL,
      label_hash BINARY(16) NOT NULL,
      label TEXT CHARACTER SET utf8 NULL,
      PRIMARY KEY `master` (`urn_hash`, `label_hash`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing labels';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `hash_index` (
      hash_urn_hash BINARY(16) NOT NULL,
      hash_urn TEXT CHARACTER SET utf8 NULL,
      file_urn_hash BINARY(16) NOT NULL,
      file_urn TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      PRIMARY KEY `master` (`hash_urn_hash`, `file_urn_hash`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing the hash index';
    """)

    self.ExecuteQuery("""
    CREATE TABLE IF NOT EXISTS `pvc_notifications` (
      urn_hash BINARY(16) NOT NULL,
      urn TEXT CHARACTER SET utf8 NULL,
      timestamp BIGINT UNSIGNED NOT NULL,
      PRIMARY KEY `master` (`urn_hash`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    COMMENT ='Table representing pvc notifications';
    """)

  def DBSubjectLock(self, subject, lease_time=None, token=None):
    return MySQLDBSubjectLock(self, subject, lease_time=lease_time, token=token)

  def Size(self):
    query = ("SELECT table_schema, Sum(data_length + index_length) `size` "
             "FROM information_schema.tables "
             "WHERE table_schema = \"%s\" GROUP by table_schema" %
             config_lib.CONFIG["Mysql.database_name"])

    result, _ = self.ExecuteQuery(query, [])
    if len(result) != 1:
      return -1
    return int(result[0]["size"])

  def ExecuteQuery(self, query, args=None):
    """Get connection from pool and execute query."""
    while True:
      # Connectivity issues and deadlocks should not cause threads to die and
      # create inconsistency.  Any MySQL errors here should be temporary in
      # nature and GRR should be able to recover when the server is available or
      # deadlocks have been resolved.
      connection = self.con_pool.GetConnection()
      try:
        connection.cursor.execute(query, args)
        rowcount = connection.cursor.rowcount
        results = connection.cursor.fetchall()
        self.con_pool.PutConnection(connection)
        return results, rowcount
      except MySQLdb.Error as e:
        # If there was an error attempt to clean up this connection and let it
        # drop
        self.con_pool.DropConnection(connection)
        if "doesn't exist" in str(e):
          # This should indicate missing tables and raise immediately
          raise e
        else:
          logging.warning("Datastore query retrying after failed with %s.",
                          str(e))
          # Most errors encountered here need a reasonable backoff time to
          # resolve.
          time.sleep(1)
      finally:
        # Reduce the open connection count by calling QueryComplete. This will
        # increment again if the connection is returned to the pool.
        self.con_pool.QueryComplete()

  def ExecuteTransaction(self, transaction):
    """Get connection from pool and execute transaction."""
    while True:
      # Connectivity issues and deadlocks should not cause threads to die and
      # create inconsistency.  Any MySQL errors here should be temporary in
      # nature and GRR should be able to recover when the server is available or
      # deadlocks have been resolved.
      connection = self.con_pool.GetConnection()
      try:
        connection.cursor.execute("START TRANSACTION")
        for query in transaction:
          connection.cursor.execute(query["query"], query["args"])
        connection.cursor.execute("COMMIT")
        results = connection.cursor.fetchall()
        self.con_pool.PutConnection(connection)
        return results
      except MySQLdb.Error as e:
        # If there was an error attempt to clean up this connection and let it
        # drop
        self.con_pool.DropConnection(connection)
        if "doesn't exist" in str(e):
          # This should indicate missing tables and raise immediately
          raise e
        else:
          logging.warning("Datastore query retrying after failed with %s.",
                          str(e))
          # Most errors encountered here need a reasonable backoff time to
          # resolve.
          time.sleep(1)
      finally:
        # Reduce the open connection count by calling QueryComplete. This will
        # increment again if the connection is returned to the pool.
        self.con_pool.QueryComplete()

  def ResolveMulti(self, subject, attributes, timestamp=None, limit=None, token=None):
    """Resolves multiple attributes at once for one subject."""
    self.security_manager.CheckDataStoreAccess(
        token, [subject], self.GetRequiredResolveAccess(attributes))

    results = []
    query = self._BuildAFF4Selects(subject, attributes, timestamp, limit)
    result, _ = self.ExecuteQuery(query["query"], query["args"])

    for row in result:
      attribute = row["attribute"]
      value = self._Decode(attribute, row["value"])
      results.append((attribute, value, row["timestamp"]))

    # Ensure we return the most recent of all rows returned up to the limit
    return sorted(results, reverse=True, key=lambda value: value[2])[:limit]

  def MultiResolvePrefix(self, subjects, attribute_prefix, timestamp=None, limit=None, token=None):
    """Result multiple subjects using one or more attribute regexps."""
    results = {}
    unsorted_results = []
    for subject in subjects:
      for attribute, value, ts in self.ResolvePrefix(
          subject,
          attribute_prefix,
          token=token,
          timestamp=timestamp,
          limit=limit):
        unsorted_results.append((subject, attribute, value, ts))

    # Ensure we return the most recent of all rows returned up to the limit
    for subject, attribute, value, ts in sorted(unsorted_results, reverse=True, key=lambda value: value[3])[:limit]:
      results.setdefault(subject, []).append((attribute, value, ts))
    return results.iteritems()

  def ResolvePrefix(self, subject, attribute_prefix, timestamp=None, limit=None, token=None):
    """ResolvePrefix."""
    self.security_manager.CheckDataStoreAccess(
        token, [subject], self.GetRequiredResolveAccess(attribute_prefix))

    if isinstance(attribute_prefix, basestring):
      attribute_prefix = [attribute_prefix]

    results = []

    query = self._BuildAFF4Selects(subject, attribute_prefix, timestamp, limit)
    result, _ = self.ExecuteQuery(query["query"], query["args"])

    for row in result:
      attribute = row["attribute"]
      value = self._Decode(attribute, row["value"])
      results.append((attribute, value, row["timestamp"]))

    return results

  def ResolveRow(self, subject, **kw):
    return self.ResolvePrefix(subject, ["aff4:", "metadata:"], **kw)

  def ReadNotifications(self, queue, timestamp, limit=None, token=None):
    self.security_manager.CheckDataStoreAccess(token, [queue], "r")
    queue_hash = self._Hash(queue)
    query = "SELECT session_id, value, timestamp FROM notifications WHERE queue_hash=%s"
    args = [queue_hash]
    # Limit to time range if specified
    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

    # Always order results.
    query += " ORDER BY timestamp DESC"
    # Add limit if set.
    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      session_id = row["session_id"]
      serialized_notification = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      try:
        notification = rdf_flows.GrrNotification.FromSerializedString(serialized_notification)
      except Exception:  # pylint: disable=broad-except
        logging.exception("Can't unserialize notification, deleting it: "
                          "session_id=%s, ts=%d", session_id, ts)
        self.DeleteNotifications(
            queue,
            [session_id],
            token=token,
            # Make the time range narrow, but be sure to include the needed
            # notification.
            start=ts,
            end=ts,
            sync=True)
        continue
      yield (session_id, notification, ts)

  def ReadTasks(self, queue, task_id=None, timestamp=None, limit=None, token=None):
    self.security_manager.CheckDataStoreAccess(token, [queue], "r")
    queue_hash = self._Hash(queue)
    query = "SELECT value, timestamp FROM tasks WHERE queue_hash=%s"
    args = [queue_hash]

    tasks = []
    if task_id:
      query += " AND task_id=%s"
      args.append(int(task_id))

    # Limit to time range if specified
    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      serialized_task = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      tasks.append((rdf_flows.GrrMessage.FromSerializedString(serialized_task), ts))
    return sorted(tasks, reverse=True, key=lambda task: task[0].priority)

  def ReadStatuses(self, session_id, timestamp=None, limit=None, token=None):
    subject = session_id.Add("state")
    self.security_manager.CheckDataStoreAccess(token, [subject], "r")
    session_id_hash = self._Hash(session_id)
    query = "SELECT value, timestamp FROM responses WHERE session_id_hash=%s AND status=%s"
    args = [session_id_hash, True]

    # Limit to time range if specified
    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

    query += " ORDER BY timestamp DESC"

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      serialized_status = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      yield (rdf_flows.GrrMessage.FromSerializedString(serialized_status), ts)

  def ReadResponses(self, session_id, request_id, timestamp=None, limit=None, token=None):
    subject = session_id.Add("state/request:%08X" % request_id)
    self.security_manager.CheckDataStoreAccess(token, [subject], "r")
    session_id_hash = self._Hash(session_id)
    query = "SELECT value, timestamp FROM responses WHERE session_id_hash=%s AND request_id=%s"
    args = [session_id_hash, int(request_id)]

    # Limit to time range if specified
    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

    query += " ORDER BY timestamp DESC"

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      serialized_response = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      yield (rdf_flows.GrrMessage.FromSerializedString(serialized_response), ts)

  def ReadRequests(self, session_id, timestamp=None, limit=None, token=None):
    subject = session_id.Add("state")
    self.security_manager.CheckDataStoreAccess(token, [subject], "r")
    session_id_hash = self._Hash(session_id)
    query = "SELECT value, timestamp FROM requests WHERE session_id_hash=%s"
    args = [session_id_hash]

    # Limit to time range if specified
    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

    query += " ORDER BY timestamp DESC"

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      serialized_request = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      yield (rdf_flows.RequestState.FromSerializedString(serialized_request), ts)

  def ReadCollectionItems(self, collection_id, timestamps, rdf_type, token=None):
    subjects = [collection_id.Add("Results").Add("%016x.%06x" % (timestamp, suffix)) for timestamp, suffix in timestamps]
    self.security_manager.CheckDataStoreAccess(token, subjects, "r")
    collection_id_hash = self._Hash(collection_id)
    query = "SELECT value, timestamp FROM collections WHERE collection_id_hash=%s AND ("
    args = [collection_id_hash]

    query += " OR ".join(["(timestamp=%s AND suffix=%s)"] * len(timestamps))
    query += ") ORDER BY timestamp DESC"

    for timestamp, suffix in timestamps:
      args.extend([int(timestamp), int(suffix)])

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      serialized_rdf_value = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      yield (rdf_type.FromSerializedString(serialized_rdf_value), ts)

  def ScanCollectionItems(self, collection_id, rdf_type, after_timestamp=None, after_suffix=None, limit=None, token=None):
    self.security_manager.CheckDataStoreAccess(token, [collection_id], "r")
    collection_id_hash = self._Hash(collection_id)
    query = "SELECT value, timestamp, suffix FROM collections WHERE collection_id_hash=%s"
    args = [collection_id_hash]

    if after_timestamp:
      query += " AND timestamp > %s"
      args.append(int(after_timestamp))

    if after_suffix:
      query += " OR (timestamp=%s AND suffix > %s)"
      args.append(int(after_timestamp))
      args.append(int(after_suffix))

    query += " ORDER BY timestamp, suffix DESC"

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)
    for row in rows:
      serialized_rdf_value = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      suffix = row["suffix"]
      yield (rdf_type.FromSerializedString(serialized_rdf_value), ts, suffix)

  def ReadMultiTypeCollectionEntries(self, collection_id, token=None):
    self.security_manager.CheckDataStoreAccess(token, [collection_id], "r")
    collection_id_hash = self._Hash(collection_id)
    query = "SELECT type FROM collection_types WHERE collection_id_hash=%s"
    args = [collection_id_hash]

    rows, _ = self.ExecuteQuery(query, args)

    return [row["type"] for row in rows]

  def ReadCollectionIndexEntries(self, collection_id, token=None):
    self.security_manager.CheckDataStoreAccess(token, [collection_id], "rq")
    collection_id_hash = self._Hash(collection_id)
    query = "SELECT idx, item_timestamp, item_suffix FROM collection_indexes WHERE collection_id_hash=%s"
    args = [collection_id_hash]

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      idx = row["idx"]
      item_timestamp = row["item_timestamp"]
      item_suffix = row["item_suffix"]
      yield (idx, item_timestamp, item_suffix)

  def ScanQueueItems(self, queue, rdf_type, start_time, limit, token=None):
    self.security_manager.CheckDataStoreAccess(token, [queue.Add("Records")], "r")
    queue_hash = self._Hash(queue)
    query = "SELECT value, timestamp, suffix, lease_expiration FROM queues WHERE queue_hash=%s"
    args = [queue_hash]

    if start_time:
      query += " AND timestamp > %s"
      args.append(int(start_time.AsMicroSecondsFromEpoch()))

    query += " ORDER BY timestamp ASC"

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      serialized_rdf_value = self._Decode("bytes", row["value"])
      ts = row["timestamp"]
      suffix = row["suffix"]
      lease_expiration = row["lease_expiration"]
      yield (rdf_type.FromSerializedString(serialized_rdf_value), ts, suffix, lease_expiration)

  def ReadStats(self, process_stats_stores, metric_name=None, timestamp=None, limit=None, token=None):
    self.security_manager.CheckDataStoreAccess(token, process_stats_stores, "r")
    query = "SELECT process_stats_store, metric_name, metric_value, timestamp FROM stats_store WHERE process_stats_store_hash IN ("
    query += ", ".join(["%s"] * len(process_stats_stores))
    query += ")"
    args = [self._Hash(process_stats_store) for process_stats_store in process_stats_stores]

    if metric_name:
      metric_name_hash = self._Hash(metric_name)
      query += " AND metric_name_hash=%s"
      args.append(metric_name_hash)

    if isinstance(timestamp, (tuple, list)):
      query += " AND timestamp >= %s AND timestamp <= %s"
      args.append(int(timestamp[0]))
      args.append(int(timestamp[1]))

    query += " ORDER BY timestamp DESC"

    if limit:
      query += " LIMIT %s"
      args.append(int(limit))
    rows, _ = self.ExecuteQuery(query, args)

    results = {}
    for row in rows:
      process_stats_store = row["process_stats_store"]
      metric_name = row["metric_name"]
      metric_value = self._Decode("bytes", row["metric_value"])
      timestamp = row["timestamp"]
      process_stats_store = rdfvalue.RDFURN(process_stats_store)
      results.setdefault(process_stats_store.Basename(), []).append((metric_name, metric_value, timestamp))
    for process_stats_store in results:
      results[process_stats_store] = sorted(results[process_stats_store], key=lambda x: x[2])
    return results

  def ReadVersionedCollectionNotifications(self, timestamp, token=None):
    self.security_manager.CheckDataStoreAccess(token, self.VERSIONED_COLLECTION_NOTIFICATION_QUEUE, "rq")
    query = "SELECT urn, timestamp FROM pvc_notifications WHERE timestamp >= %s AND timestamp <= %s"
    args = [int(timestamp[0]), int(timestamp[1])]
    query += " ORDER BY timestamp DESC"

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      yield rdfvalue.RDFURN(row["urn"], age=row["timestamp"])

  def ReadLabels(self, urn, token=None):
    self.security_manager.CheckDataStoreAccess(token, [urn], "rq")
    urn_hash = self._Hash(urn)
    query = "SELECT label FROM labels WHERE urn_hash=%s"
    args = [urn_hash]

    rows, _ = self.ExecuteQuery(query, args)

    result = []
    for row in rows:
      result.append(row["label"])
    return sorted(result)

  def ReadHashIndex(self, hash_urn, timestamp=None, limit=None, token=None):
    for hash_urn, file_urns in self.MultiReadHashIndexes([hash_urn], timestamp, limit=limit, token=token):
      for file_urn in file_urns:
        yield file_urn

  def MultiReadHashIndexes(self, hash_urns, timestamp, limit=None, token=None):
    self.security_manager.CheckDataStoreAccess(token, hash_urns, "rq")
    queries = []
    args = []
    results = {}

    for hash_urn in hash_urns:
      hash_urn_hash = self._Hash(hash_urn)
      tables = "FROM hash_index"
      criteria = "WHERE hash_urn_hash=%s"
      sorting = ""
      args.append(hash_urn_hash)

      if isinstance(timestamp, (tuple, list)):
        criteria += " AND timestamp >= %s AND timestamp <= %s"
        args.append(int(timestamp[0]))
        args.append(int(timestamp[1]))

      if timestamp == self.NEWEST_TIMESTAMP:
        tables += (" JOIN (SELECT file_urn_hash, MAX(timestamp) timestamp "
                   "%s %s GROUP BY file_urn_hash) maxtime ON "
                   "hash_index.file_urn_hash=maxtime.file_urn_hash AND "
                   "hash_index.timestamp=maxtime.timestamp") % (tables, criteria)
        args.append(hash_urn_hash)
      else:
        sorting += "ORDER BY hash_index.timestamp DESC"

      if limit:
        sorting += " LIMIT %s"
        args.append(int(limit))

      queries.append(" ".join(["(SELECT hash_index.hash_urn, hash_index.file_urn", tables, criteria, sorting, ")"]))

    query = " UNION ".join(queries)

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      hash_urn = row["hash_urn"]
      file_urn = row["file_urn"]
      results.setdefault(hash_urn, []).append(file_urn)

    for hash_urn, file_urns in results.items():
      yield hash_urn, file_urns

  def ReadAFF4Index(self, parent_urn, timestamp, limit=None, token=None):
    for parent_urn, child_urns in self.MultiReadAFF4Indexes([parent_urn], timestamp, limit=limit, token=token):
      for child_urn in child_urns:
        yield child_urn

  def MultiReadAFF4Indexes(self, parent_urns, timestamp, limit=None, token=None):
    self.security_manager.CheckDataStoreAccess(token, parent_urns, "rq")
    queries = []
    args = []
    results = {}

    for parent_urn in parent_urns:
      parent_urn_hash = self._Hash(parent_urn)
      tables = "FROM aff4_index"
      criteria = "WHERE urn_hash=%s"
      sorting = ""
      args.append(parent_urn_hash)

      if isinstance(timestamp, (tuple, list)):
        criteria += " AND timestamp >= %s AND timestamp <= %s"
        args.append(int(timestamp[0]))
        args.append(int(timestamp[1]))

      if timestamp == self.NEWEST_TIMESTAMP:
        tables += (" JOIN (SELECT child_urn_hash, MAX(timestamp) timestamp "
                   "%s %s GROUP BY child_urn_hash) maxtime ON "
                   "aff4_index.child_urn_hash=maxtime.child_urn_hash AND "
                   "aff4_index.timestamp=maxtime.timestamp") % (tables, criteria)
        args.append(parent_urn_hash)
      else:
        sorting += "ORDER BY aff4_index.timestamp DESC"

      if limit:
        sorting += " LIMIT %s"
        args.append(int(limit))

      queries.append(" ".join(["(SELECT aff4_index.urn, aff4_index.child_urn, aff4_index.timestamp", tables, criteria, sorting, ")"]))

    query = " UNION ".join(queries)

    rows, _ = self.ExecuteQuery(query, args)

    for row in rows:
      parent_urn = row["urn"]
      child_urn = rdfvalue.RDFURN(parent_urn).Add(row["child_urn"])
      child_urn.age = rdfvalue.RDFDatetime(row["timestamp"])
      results.setdefault(parent_urn, []).append(child_urn)

    for parent_urn, child_urns in results.items():
      yield parent_urn, child_urns

  def MultiReadKeywordsIndex(self, keywords, timestamp, token=None):
    self.security_manager.CheckDataStoreAccess(token, keywords, "r")

    query = "SELECT keyword, name, timestamp FROM keywords WHERE keyword_hash IN ("
    query += ", ".join(["%s"] * len(keywords))
    query += ")"
    args = [self._Hash(keyword) for keyword in keywords]

    query += " AND timestamp >= %s AND timestamp <= %s"
    args.append(int(timestamp[0]))
    args.append(int(timestamp[1]))

    query += " ORDER BY timestamp DESC"

    rows, _ = self.ExecuteQuery(query, args)

    results = {}
    for row in rows:
      results.setdefault(row["keyword"], []).append((row["name"], row["timestamp"]))

    for keyword, names in results.items():
      yield keyword, names

  def ScanAttributes(self, subject_prefix, attributes, after_urn=None, max_records=None, token=None, relaxed_order=False):
    """
    This method is no longer used or supported by the MySQLExperimental datastore and the required tables no longer exist.
    This should simply raise NotImplemented if called.
    """
    raise NotImplemented

  def _BuildAFF4Selects(self, subject, attributes, timestamp=None, limit=None):
    """Build the SELECT query to be executed."""
    subject = utils.SmartUnicode(subject)
    urn_hash = self._Hash(subject)
    attribute_tables = {}
    queries = []
    args = []

    for attribute in attributes:
      table, attribute = attribute.split(":", 1)
      attribute_tables.setdefault(table, [])
      if attribute:
        attribute_tables[table].append(attribute)

    for table, attributes in attribute_tables.items():
      tables = "FROM %s" % table
      fields = "CONCAT('%s:', %s.attribute) as attribute, %s.value, %s.timestamp" % (table, table, table, table)
      sorting = ""

      criteria = "WHERE %s.urn_hash=" % table + "%s"
      args.append(urn_hash)
      if attributes:
        criteria += " AND attribute_hash IN ("
        criteria += ", ".join(["%s"] * len(attributes))
        criteria += ")"
        args.extend([self._Hash(attribute) for attribute in attributes])

      # Limit to time range if specified
      if isinstance(timestamp, (tuple, list)):
        criteria += " AND %s.timestamp" % table + " >= %s" + " AND %s.timestamp" % table + " <= %s"
        args.append(int(timestamp[0]))
        args.append(int(timestamp[1]))

      # Modify fields and sorting for timestamps.
      if timestamp == self.NEWEST_TIMESTAMP:
        tables += (" JOIN (SELECT attribute_hash, MAX(timestamp) timestamp "
                   "%s %s GROUP BY attribute_hash) maxtime ON "
                   "%s.attribute_hash=maxtime.attribute_hash AND "
                   "%s.timestamp=maxtime.timestamp") % (tables, criteria, table, table)
        criteria = "WHERE %s.urn_hash" % table + "=%s"
        args.append(urn_hash)
      else:
        # Always order results.
        sorting = "ORDER BY %s.timestamp DESC" % table
      # Add limit if set.
      if limit:
        sorting += " LIMIT %s" % int(limit)

      queries.append(" ".join(["(SELECT", fields, tables, criteria, sorting, ")"]))

    query = " UNION ".join(queries)

    return {"query": query, "args": args}

  def _Decode(self, attribute, value):
    required_type = self.attribute_types.get(attribute, "bytes")
    if isinstance(value, buffer):
      value = str(value)
    if required_type in ("integer", "unsigned_integer"):
      return int(value)
    elif required_type == "string":
      return utils.SmartUnicode(value)
    else:
      return value

  def _Hash(self, value):
    value = utils.SmartUnicode(value)
    return hashlib.md5(value.encode('utf-8')).digest()

  def MultiSet(self, subject, values, timestamp=None, replace=True, sync=True, to_delete=None, token=None):
    self.mutation_pool.MultiSet(subject, values, timestamp=timestamp, replace=replace, to_delete=to_delete, sync=sync, token=token)

  def DeleteAttributes(self, subject, attributes, start=None, end=None, sync=True, token=None):
    self.mutation_pool.DeleteAttributes(subject, attributes, start=start, end=end, sync=sync, token=token)

  def DeleteSubject(self, subject, sync=False, token=None):
    self.mutation_pool.DeleteSubjects([subject], sync=sync, token=token)

  def CreateNotifications(self, queue, notifications, sync=True, token=None):
    self.mutation_pool.CreateNotifications(queue, notifications, sync=sync, token=token)

  def DeleteNotifications(self, queue, session_ids, start=None, end=None, sync=True, token=None):
    self.mutation_pool.DeleteNotifications(queue, session_ids, start=start, end=end, sync=sync, token=token)

  def CreateTasks(self, queue, task_queue, timestamp, sync=True, token=None):
    self.mutation_pool.CreateTasks(queue, task_queue, timestamp=timestamp, sync=sync, token=token)

  def DeleteTasks(self, queue, task_ids, sync=True, token=None):
    self.mutation_pool.DeleteTasks(queue, task_ids, sync=sync, token=token)

  def CreateResponses(self, session_id, responses, sync=True, token=None):
    self.mutation_pool.CreateResponses(session_id, responses, sync=sync, token=token)

  def DeleteResponses(self, session_id, request_id, responses=None, sync=False, token=None):
    self.mutation_pool.DeleteResponses(session_id, request_id, responses=responses, sync=sync, token=token)

  def CreateRequests(self, session_id, requests, sync=True, token=None):
    self.mutation_pool.CreateRequests(session_id, requests, sync=sync, token=token)

  def DeleteRequests(self, session_id, requests, sync=False, token=None):
    self.mutation_pool.DeleteRequests(session_id, requests, sync=sync, token=token)

  def CreateCollectionItem(self, collection_id, rdf_value, timestamp, suffix, sync=True, token=None):
    self.mutation_pool.CreateCollectionItem(collection_id, rdf_value, timestamp, suffix=suffix, sync=sync, token=token)

  def DeleteCollection(self, collection_id, sync=True, token=None):
    self.mutation_pool.DeleteCollection(collection_id, sync=sync, token=token)

  def CreateMultiTypeCollectionEntry(self, collection_id, collection_type, sync=True, token=None):
    self.mutation_pool.CreateMultiTypeCollectionEntry(collection_id, collection_type, sync=sync, token=token)

  def CreateCollectionIndexEntry(self, collection_id, idx, item_timestamp, item_suffix, sync=False, token=None):
    self.mutation_pool.CreateCollectionIndexEntry(collection_id, idx, item_timestamp, item_suffix, sync=sync, token=token)

  def CreateQueueItem(self, queue, rdf_value, timestamp, suffix=None, sync=True, token=None):
    self.mutation_pool.CreateQueueItem(queue, rdf_value, timestamp, suffix=suffix, sync=sync, token=token)

  def UpdateQueueItemLeases(self, records, lease_expiration, sync=False, token=None):
    self.mutation_pool.UpdateQueueItemLeases(records, lease_expiration, sync=sync, token=token)

  def DeleteQueueItems(self, records, sync=True, token=None):
    self.mutation_pool.DeleteQueueItems(records, sync=sync, token=token)

  def CreateStats(self, process_stats_store, stats, timestamp, sync=False, token=None):
    self.mutation_pool.CreateStats(process_stats_store, stats, timestamp=timestamp, sync=sync, token=token)

  def DeleteStats(self, process_stats_store, metric_names, start=None, end=None, sync=False, token=None):
    self.mutation_pool.DeleteStats(process_stats_store, metric_names, start=start, end=end, sync=sync, token=token)

  def CreateVersionedCollectionNotification(self, urn, timestamp=None, sync=False, token=None):
    self.mutation_pool.CreateVersionedCollectionNotification(urn, timestamp=timestamp, sync=sync, token=token)

  def DeleteVersionedCollectionNotifications(self, urn, end=None, sync=True, token=None):
    self.mutation_pool.DeleteVersionedCollectionNotifications(urn, end=end, sync=sync, token=token)

  def CreateLabels(self, urn, labels, sync=False, token=None):
    self.mutation_pool.CreateLabels(urn, labels, sync=sync, token=token)

  def DeleteLabels(self, urn, labels, sync=False, token=None):
    self.mutation_pool.DeleteLabels(urn, labels, sync=sync, token=token)

  def CreateHashIndex(self, hash_urn, file_urn, timestamp=None, sync=False, token=None):
    self.mutation_pool.CreateHashIndex(hash_urn, file_urn, timestamp=timestamp, sync=sync, token=token)

  def CreateAFF4Index(self, parent_urn, child_urn, timestamp=None, sync=False, token=None):
    self.mutation_pool.CreateAFF4Index(parent_urn, child_urn, timestamp=timestamp, sync=sync, token=token)

  def DeleteAFF4Index(self, parent_urn, child_urn, sync=False, token=None):
    self.DeleteAttributes(parent_urn, [self.AFF4_INDEX_TEMPLATE % child_urn], sync=sync, token=token)

  def CreateKeywordsIndex(self, name, keywords, timestamp=None, sync=True, token=None):
    self.mutation_pool.CreateKeywordsIndex(name, keywords, timestamp=timestamp, sync=sync, token=token)

  def DeleteKeywordsIndex(self, name, keywords, sync=True, token=None):
    self.mutation_pool.DeleteKeywordsIndex(name, keywords, sync=sync, token=token)

  def Flush(self):
    self.mutation_pool.FlushOnce()


class MySQLDBSubjectLock(data_store.DBSubjectLock):
  """The Mysql data store lock object.

  This object does not aim to ensure ACID like consistently. We only ensure that
  two simultaneous locks can not be held on the same AFF4 subject.

  This means that the first thread which grabs the lock is considered the owner
  of the lock. Any subsequent locks on the same subject will fail
  immediately with data_store.DBSubjectLockError.

  A lock is considered expired after a certain time.
  """

  def _Acquire(self, lease_time):
    self.lock_token = thread.get_ident()
    self.expires = int((time.time() + lease_time) * 1e6)

    # This single query will create a new entry if one doesn't exist, and update
    # the lock value if there is a lock but it's expired.  The SELECT 1
    # statement checks if there is a current lock. The select from dual is
    # essentially a way to get the numbers into the query conditional on there
    # not being an existing lock.
    self.urn_hash = data_store.DB._Hash(self.subject)
    query = (
        "REPLACE INTO locks(lock_expiration, lock_owner, urn_hash) "
        "SELECT %s, %s, %s FROM dual WHERE NOT EXISTS (SELECT 1 "
        "FROM locks WHERE urn_hash=%s AND (lock_expiration > %s))")
    args = [
        self.expires, self.lock_token, self.urn_hash, self.urn_hash,
        time.time() * 1e6
    ]
    unused_results, rowcount = self.store.ExecuteQuery(query, args)

    # New row rowcount == 1, updating expired lock rowcount == 2.
    if rowcount == 0:
      raise data_store.DBSubjectLockError("Subject %s is locked" % self.subject)
    self.locked = True

  def UpdateLease(self, lease_time):
    self.expires = int((time.time() + lease_time) * 1e6)
    query = ("UPDATE locks SET lock_expiration=%s, lock_owner=%s "
             "WHERE urn_hash=%s")
    args = [self.expires, self.lock_token, self.urn_hash]
    self.store.ExecuteQuery(query, args)

  def Release(self):
    """Remove the lock.

    Note that this only resets the lock if we actually hold it since
    lock_expiration == self.expires and lock_owner = self.lock_token.
    """
    if self.locked:
      query = ("UPDATE locks SET lock_expiration=0, lock_owner=0 "
               "WHERE lock_expiration=%s "
               "AND lock_owner=%s "
               "AND urn_hash=%s")
      args = [self.expires, self.lock_token, self.urn_hash]
      self.store.ExecuteQuery(query, args)
      self.locked = False
