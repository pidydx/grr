#!/usr/bin/env python
"""This is the manager for the various queues."""



import collections
import os
import random
import socket
import time

import logging

from grr.lib import config_lib
from grr.lib import data_store
from grr.lib import rdfvalue
from grr.lib import registry
from grr.lib import stats
from grr.lib import utils
from grr.lib.rdfvalues import client as rdf_client
from grr.lib.rdfvalues import flows as rdf_flows


class Error(Exception):
  """Base class for errors in this module."""


class MoreDataException(Error):
  """Raised when there is more data available."""


class QueueManager(object):
  """This class manages the representation of the flow within the data store.

  The workflow for client task scheduling is as follows:

  1) Create a bunch of tasks (rdf_flows.GrrMessage()). Tasks must
  be assigned to queues and contain arbitrary values.

  2) Call QueueManager.Schedule(task) to add the tasks to their queues.

  3) In another thread, call QueueManager.QueryAndOwn(queue) to
  obtain a list of tasks leased for a particular time.

  4) If the lease time expires, the tasks automatically become
  available for consumption. When done with the task we can remove it
  from the scheduler using QueueManager.Delete(tasks).

  5) Tasks can be re-leased by calling QueueManager.Schedule(task)
  repeatedly. Each call will extend the lease by the specified amount.

  Important QueueManager's feature is the ability to freeze the timestamp used
  for time-limiting Resolve and Delete queries to the datastore. "with"
  statement should be used to freeze the timestamp, like:
    with queue_manager.QueueManager(token=self.token) as manager:
      ...

  Another option is to use FreezeTimestamp()/UnfreezeTimestamp() methods:
    queue_manager.FreezeTimestamp()
    ...
    queue_manager.UnfreezeTimestamp()
  """

  # These attributes are related to a flow's internal data structures Requests
  # are protobufs of type RequestState. They have a constant prefix followed by
  # the request number:

  STUCK_PRIORITY = "Flow stuck"

  request_limit = 1000000
  response_limit = 1000000

  notification_shard_counters = {}

  def __init__(self, store=None, token=None):
    self.token = token
    if store is None:
      store = data_store.DB

    self.data_store = store

    # We cache all these and write/delete in one operation.
    self.to_write_requests = {}
    self.to_write_responses = {}
    self.to_delete_requests = {}

    # A queue of client messages to remove. Keys are client ids, values are
    # lists of task ids.
    self.client_messages_to_delete = {}
    self.new_client_messages = []
    self.notifications = {}

    self.prev_frozen_timestamps = []
    self.frozen_timestamp = None

    self.num_notification_shards = config_lib.CONFIG["Worker.queue_shards"]

  def GetNotificationShard(self, queue):
    queue_name = str(queue)
    QueueManager.notification_shard_counters.setdefault(queue_name, 0)
    QueueManager.notification_shard_counters[queue_name] += 1
    notification_shard_index = (
        QueueManager.notification_shard_counters[queue_name] %
        self.num_notification_shards)
    if notification_shard_index > 0:
      return queue.Add(str(notification_shard_index))
    else:
      return queue

  def GetAllNotificationShards(self, queue):
    result = [queue]
    for i in range(1, self.num_notification_shards):
      result.append(queue.Add(str(i)))
    return result

  def Copy(self):
    """Return a copy of the queue manager.

    Returns:
    Copy of the QueueManager object.
    NOTE: pending writes/deletions are not copied. On the other hand, if the
    original object has a frozen timestamp, a copy will have it as well.
    """
    result = QueueManager(store=self.data_store, token=self.token)
    result.prev_frozen_timestamps = self.prev_frozen_timestamps
    result.frozen_timestamp = self.frozen_timestamp
    return result

  def FreezeTimestamp(self):
    """Freezes the timestamp used for resolve/delete database queries.

    Frozen timestamp is used to consistently limit the datastore resolve and
    delete queries by time range: from 0 to self.frozen_timestamp. This is
    done to avoid possible race conditions, like accidentally deleting
    notifications that were written by another process while we were
    processing requests.
    """
    self.prev_frozen_timestamps.append(self.frozen_timestamp)
    self.frozen_timestamp = rdfvalue.RDFDatetime.Now()

  def UnfreezeTimestamp(self):
    """Unfreezes the timestamp used for resolve/delete database queries."""
    if not self.prev_frozen_timestamps:
      raise RuntimeError("Unbalanced UnfreezeTimestamp call.")
    self.frozen_timestamp = self.prev_frozen_timestamps.pop()

  def __enter__(self):
    """Supports 'with' protocol."""
    self.FreezeTimestamp()
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    """Supports 'with' protocol."""
    # Flush() uses the frozen timestamp so needs to go first.
    self.Flush()
    self.UnfreezeTimestamp()

  def DeQueueClientRequest(self, client_id, task_id):
    """Remove the message from the client queue that this request forms."""
    # Check this request was actually bound for a client.
    if client_id:
      client_id = rdf_client.ClientURN(client_id)

      self.client_messages_to_delete.setdefault(client_id, []).append(task_id)

  def MultiCheckStatus(self, messages):
    """Checks if there is a client status queued for a number of requests."""
    status_available = set()

    for message in messages:
      for status, _ in self.data_store.ReadStatuses(message.session_id, token=self.token):
        if message.request_id == status.request_id:
          status_available.add(message)
    return status_available

  def FetchCompletedRequests(self, session_id, timestamp=None):
    """Fetch all the requests with a status message queued for them."""
    requests = {}
    status_ids = []

    if timestamp is None:
      timestamp = (0, self.frozen_timestamp or rdfvalue.RDFDatetime.Now())

    for status, _ in self.data_store.ReadStatuses(session_id, timestamp=timestamp, token=self.token):
      status_ids.append(status.request_id)

    for request, _ in self.data_store.ReadRequests(session_id, timestamp=timestamp, limit=self.request_limit, token=self.token):
      requests[request.id] = request

    for request_id, request in sorted(requests.items()):
      if request_id in status_ids:
        yield request

  def FetchCompletedResponses(self, session_id, timestamp=None, limit=10000):
    """Fetch only completed requests and responses up to a limit."""
    if timestamp is None:
      timestamp = (0, self.frozen_timestamp or rdfvalue.RDFDatetime.Now())

    completed_requests = collections.deque(
        self.FetchCompletedRequests(
            session_id, timestamp=timestamp))

    total_size = 0
    while completed_requests:
      request = completed_requests.popleft()
      responses = [response for response, _ in self.data_store.ReadResponses(session_id, request.id, timestamp=timestamp, token=self.token)]

      yield (request, sorted(responses, key=lambda msg: msg.response_id))
      total_size += len(responses)
      if total_size > limit:
        raise MoreDataException()

  def FetchRequestsAndResponses(self, session_id, timestamp=None):
    """Fetches all outstanding requests and responses for this flow.

    We first cache all requests and responses for this flow in memory to
    prevent round trips.

    Args:
      session_id: The session_id to get the requests/responses for.
      timestamp: Tupe (start, end) with a time range. Fetched requests and
                 responses will have timestamp in this range.

    Yields:
      an tuple (request protobufs, list of responses messages) in ascending
      order of request ids.

    Raises:
      MoreDataException: When there is more data available than read by the
                         limited query.
    """
    requests = {}

    if timestamp is None:
      timestamp = (0, self.frozen_timestamp or rdfvalue.RDFDatetime.Now())

    for request, _ in self.data_store.ReadRequests(session_id, timestamp=timestamp, limit=self.request_limit, token=self.token):
      requests[request.id] = request

    for request_id, request in sorted(requests.items()):
      responses = [response for response, _ in
                   self.data_store.ReadResponses(session_id, request.id, limit=self.response_limit, timestamp=timestamp, token=self.token)]
      yield (request, sorted(responses, key=lambda msg: msg.response_id))

    if len(requests) >= self.request_limit:
      raise MoreDataException()

  def DeleteFlowRequestStates(self, session_id, request_state):
    """Deletes the request and all its responses from the flow state queue."""
    request_queue = self.to_delete_requests.setdefault(session_id, [])
    request_queue.append(request_state)

    if request_state and request_state.HasField("request"):
      self.DeQueueClientRequest(request_state.client_id,
                                request_state.request.task_id)

    # Efficiently drop all responses to this request.
    self.data_store.DeleteResponses(session_id, request_state.id, token=self.token)

  def DestroyFlowStates(self, session_id):
    """Deletes all states in this flow and dequeues all client messages."""
    self.MultiDestroyFlowStates([session_id])

  def MultiDestroyFlowStates(self, session_ids):
    """Deletes all states in multiple flows and dequeues all client messages."""
    for session_id in session_ids:
      to_delete_requests = []
      for request, _ in self.data_store.ReadRequests(session_id, token=self.token):
        to_delete_requests.append(request)
        if request.HasField("request"):
          # Client request dequeueing is cached so we can call it directly.
          self.DeQueueClientRequest(request.client_id, request.request.task_id)
        # Drop all responses to this request.
        self.data_store.DeleteResponses(request.session_id, request.id, token=self.token)
      if to_delete_requests:
        self.data_store.DeleteRequests(session_id, requests=to_delete_requests, token=self.token)

  def Flush(self):
    """Writes the changes in this object to the datastore."""

    # We need to make sure that notifications are written after the requests so
    # we flush after writing all requests and only notify afterwards.
    mutation_pool = self.data_store.GetMutationPool(token=self.token)
    with mutation_pool:
      for session_id, responses in self.to_write_responses.items():
        mutation_pool.CreateResponses(session_id, responses)

      for session_id, requests in self.to_write_requests.items():
        mutation_pool.CreateRequests(session_id, requests)

      for session_id, requests in self.to_delete_requests.items():
        mutation_pool.DeleteRequests(session_id, requests)

      for client_id, messages in self.client_messages_to_delete.iteritems():
        self.Delete(client_id.Queue(), messages, mutation_pool=mutation_pool)

      if self.new_client_messages:
        for timestamp, messages in utils.GroupBy(self.new_client_messages,
                                                 lambda x: x[1]).iteritems():

          self.Schedule(
              [x[0] for x in messages],
              timestamp=timestamp,
              mutation_pool=mutation_pool)

    if self.notifications:
      for notification, timestamp in self.notifications.itervalues():
        self.NotifyQueue(
            notification, timestamp=timestamp, mutation_pool=mutation_pool)

      mutation_pool.Flush()
    data_store.DB.Flush()
    self.to_write_requests = {}
    self.to_write_responses = {}
    self.to_write_statuses = {}
    self.to_delete_requests = {}
    self.client_messages_to_delete = {}
    self.notifications = {}
    self.new_client_messages = []

  def QueueResponse(self, session_id, response, timestamp=None):
    """Queues the message on the flow's state."""
    if timestamp is None:
      timestamp = self.frozen_timestamp
    status_queue = self.to_write_responses.setdefault(session_id, [])
    status_queue.append((response, timestamp))

  def QueueRequest(self, session_id, request, timestamp=None):
    if timestamp is None:
      timestamp = self.frozen_timestamp
    request_queue = self.to_write_requests.setdefault(session_id, [])
    request_queue.append((request, timestamp))

  def QueueClientMessage(self, msg, timestamp=None):
    if timestamp is None:
      timestamp = self.frozen_timestamp

    self.new_client_messages.append((msg, timestamp))

  def QueueNotification(self, notification=None, timestamp=None, **kw):
    """Queues a notification for a flow."""

    if notification is None:
      notification = rdf_flows.GrrNotification(**kw)

    session_id = notification.session_id
    if session_id:
      if timestamp is None:
        timestamp = self.frozen_timestamp

      # We must not store more than one notification per session id and
      # timestamp or there will be race conditions. We therefore only keep
      # the one with the highest request number (indicated by last_status).
      # Note that timestamp might be None. In that case we also only want
      # to keep the latest.
      if timestamp is None:
        ts_str = "None"
      else:
        ts_str = int(timestamp)
      key = "%s!%s" % (session_id, ts_str)
      existing = self.notifications.get(key)
      if existing is not None:
        if existing[0].last_status < notification.last_status:
          self.notifications[key] = (notification, timestamp)
      else:
        self.notifications[key] = (notification, timestamp)

  def Delete(self, queue, tasks, mutation_pool=None):
    """Removes the tasks from the queue.

    Note that tasks can already have been removed. It is not an error
    to re-delete an already deleted task.

    Args:
     queue: A queue to clear.
     tasks: A list of tasks to remove. Tasks may be Task() instances
          or integers representing the task_id.
     mutation_pool: An optional MutationPool object to schedule deletions on.
                    If not given, self.data_store is used directly.
    """
    if queue:
      task_ids = []
      for task in tasks:
        try:
          task_id = task.task_id
        except AttributeError:
          task_id = int(task)
        task_ids.append(task_id)

      if mutation_pool:
        mutation_pool.DeleteTasks(queue, task_ids)
      else:
        self.data_store.DeleteTasks(
            queue, task_ids, token=self.token, sync=False)

  def Schedule(self, tasks, sync=False, timestamp=None, mutation_pool=None):
    """Schedule a set of Task() instances."""
    if timestamp is None:
      timestamp = self.frozen_timestamp

    for queue, queued_tasks in utils.GroupBy(tasks, lambda x: x.queue).iteritems():
      if queue:
        to_schedule = {task.task_id: [task] for task in queued_tasks}

        if mutation_pool:
          mutation_pool.CreateTasks(queue, to_schedule, timestamp=timestamp)
        else:
          self.data_store.CreateTasks(
              queue,
              to_schedule,
              timestamp=timestamp,
              sync=sync,
              token=self.token)

  def _SortByPriority(self, notifications, queue, output_dict=None):
    """Sort notifications by priority into output_dict."""
    if output_dict is None:
      output_dict = {}

    for notification in notifications:
      priority = notification.priority
      if notification.in_progress:
        priority = self.STUCK_PRIORITY

      output_dict.setdefault(priority, []).append(notification)

    for priority in output_dict:
      stats.STATS.SetGaugeValue(
          "notification_queue_count",
          len(output_dict[priority]),
          fields=[queue.Basename(), str(priority)])
      random.shuffle(output_dict[priority])

    return output_dict

  def GetNotificationsByPriority(self, queue):
    """Retrieves session ids for processing grouped by priority."""
    # Check which sessions have new data.
    # Read all the sessions that have notifications.
    queue_shard = self.GetNotificationShard(queue)
    return self._SortByPriority(
        self._GetUnsortedNotifications(queue_shard).values(), queue)

  def GetNotificationsByPriorityForAllShards(self, queue):
    """Same as GetNotificationsByPriority but for all shards.

    Used by worker_test to cover all shards with a single worker.

    Args:
      queue: usually rdfvalue.RDFURN("aff4:/W")
    Returns:
      dict of notifications objects keyed by priority.
    """
    output_dict = {}
    for queue_shard in self.GetAllNotificationShards(queue):
      self._GetUnsortedNotifications(
          queue_shard, notifications_by_session_id=output_dict)

    return self._SortByPriority(output_dict.values(), queue)

  def GetNotifications(self, queue):
    """Returns all queue notifications sorted by priority."""
    queue_shard = self.GetNotificationShard(queue)
    notifications = self._GetUnsortedNotifications(queue_shard).values()
    notifications.sort(
        key=lambda notification: notification.priority, reverse=True)
    return notifications

  def GetNotificationsForAllShards(self, queue):
    """Returns notifications for all shards of a queue at once.

    Used by test_lib.MockWorker to cover all shards with a single worker.

    Args:
      queue: usually rdfvalue.RDFURN("aff4:/W")
    Returns:
      List of rdf_flows.GrrNotification objects
    """
    notifications_by_session_id = {}
    for queue_shard in self.GetAllNotificationShards(queue):
      notifications_by_session_id = self._GetUnsortedNotifications(
          queue_shard, notifications_by_session_id=notifications_by_session_id)

    notifications = notifications_by_session_id.values()
    notifications.sort(
        key=lambda notification: notification.priority, reverse=True)
    return notifications

  def _GetUnsortedNotifications(self, queue_shard, notifications_by_session_id=None):
    """Returns all the available notifications for a queue_shard.

    Args:
      queue_shard: urn of queue shard
      notifications_by_session_id: store notifications in this dict rather than
        creating a new one

    Returns:
      dict of notifications. keys are session ids.
    """
    if notifications_by_session_id is None:
      notifications_by_session_id = {}
    end_time = self.frozen_timestamp or rdfvalue.RDFDatetime.Now()
    for session_id, notification, ts in self.data_store.ReadNotifications(
        queue_shard,
        timestamp=(0, end_time),
        token=self.token,
        limit=config_lib.CONFIG["Worker.max_notifications"]):

      # Strip the prefix from the predicate to get the session_id.
      notification.session_id = session_id
      notification.timestamp = ts

      existing = notifications_by_session_id.get(notification.session_id)
      if existing:
        # If we have a notification for this session_id already, we only store
        # the one that was scheduled last.
        if notification.first_queued > existing.first_queued:
          notifications_by_session_id[notification.session_id] = notification
        elif notification.first_queued == existing.first_queued and (
            notification.last_status > existing.last_status):
          # Multiple notifications with the same timestamp should not happen.
          # We can still do the correct thing and use the latest one.
          logging.warn(
              "Notifications with equal first_queued fields detected: %s %s",
              notification, existing)
          notifications_by_session_id[notification.session_id] = notification
      else:
        notifications_by_session_id[notification.session_id] = notification

    return notifications_by_session_id

  def NotifyQueue(self, notification, **kwargs):
    """This signals that there are new messages available in a queue."""
    self._MultiNotifyQueue(notification.session_id.Queue(), [notification],
                           **kwargs)

  def MultiNotifyQueue(self, notifications, timestamp=None, sync=True, mutation_pool=None):
    """This is the same as NotifyQueue but for several session_ids at once.

    Args:
      notifications: A list of notifications.
      timestamp: An optional timestamp for this notification.
      sync: If True, sync to the data_store immediately.
      mutation_pool: An optional MutationPool object to schedule Notifications
                     on. If not given, self.data_store is used directly.

    Raises:
      RuntimeError: An invalid session_id was passed.
    """
    extract_queue = lambda notification: notification.session_id.Queue()
    for queue, notifications in utils.GroupBy(notifications,
                                              extract_queue).iteritems():
      self._MultiNotifyQueue(
          queue,
          notifications,
          timestamp=timestamp,
          sync=sync,
          mutation_pool=mutation_pool)

  def _MultiNotifyQueue(self, queue, notifications, timestamp=None, sync=True, mutation_pool=None):
    """Does the actual queuing."""
    notification_queue = {}
    now = rdfvalue.RDFDatetime.Now()
    expiry_time = config_lib.CONFIG["Worker.notification_expiry_time"]
    for notification in notifications:
      if not notification.first_queued:
        notification.first_queued = (self.frozen_timestamp or
                                     rdfvalue.RDFDatetime.Now())
      else:
        diff = now - notification.first_queued
        if diff.seconds >= expiry_time:
          # This notification has been around for too long, we drop it.
          logging.debug("Dropping notification: %s", str(notification))
          continue
      session_id = notification.session_id
      # Don't serialize session ids to save some bytes.
      notification.session_id = None
      notification.timestamp = None
      notification_queue[session_id] = [(notification, timestamp)]

    if notification_queue:
      if mutation_pool:
        mutation_pool.CreateNotifications(
            self.GetNotificationShard(queue), notification_queue)
      else:
        self.data_store.CreateNotifications(
            self.GetNotificationShard(queue),
            notification_queue,
            sync=sync,
            token=self.token)

  def DeleteNotification(self, session_id, start=None, end=None):
    self.DeleteNotifications([session_id], start=start, end=end)

  def DeleteNotifications(self, session_ids, start=None, end=None):
    """This deletes the notification when all messages have been processed."""
    if not session_ids:
      return

    for session_id in session_ids:
      if not isinstance(session_id, rdfvalue.SessionID):
        raise RuntimeError(
            "Can only delete notifications for rdfvalue.SessionIDs.")

    if start is None:
      start = 0
    else:
      start = int(start)

    if end is None:
      end = self.frozen_timestamp or rdfvalue.RDFDatetime.Now()

    for queue, ids in utils.GroupBy(
        session_ids, lambda session_id: session_id.Queue()).iteritems():
      for queue_shard in self.GetAllNotificationShards(queue):
        self.data_store.DeleteNotifications(
            queue_shard,
            ids,
            token=self.token,
            start=start,
            end=end,
            sync=True)

  def Query(self, queue, limit=1, task_id=None):
    """Retrieves tasks from a queue without leasing them.

    This is good for a read only snapshot of the tasks.

    Args:
       queue: The task queue that this task belongs to, usually client.Queue()
              where client is the ClientURN object you want to schedule msgs on.
       limit: Number of values to fetch.
       task_id: If an id is provided we only query for this id.

    Returns:
        A list of Task() objects.
    """
    # This function is usually used for manual testing so we also accept client
    # ids and get the queue from it.
    if isinstance(queue, rdf_client.ClientURN):
      queue = queue.Queue()

    all_tasks = []

    for task, ts in self.data_store.ReadTasks(
        queue,
        task_id,
        timestamp=self.data_store.ALL_TIMESTAMPS,
        token=self.token):
      task.eta = ts
      all_tasks.append(task)

    # Sort the tasks in order of priority.
    all_tasks.sort(key=lambda task: task.priority, reverse=True)

    return all_tasks[:limit]

  def QueryAndOwn(self, queue, lease_seconds=10, limit=1):
    """Returns a list of Tasks leased for a certain time.

    Args:
      queue: The queue to query from.
      lease_seconds: The tasks will be leased for this long.
      limit: Number of values to fetch.
    Returns:
        A list of GrrMessage() objects leased.
    """
    user = ""
    if self.token:
      user = self.token.username
    # Do the real work in a transaction
    try:
      lock = self.data_store.LockRetryWrapper(queue, token=self.token)
      return self._QueryAndOwn(
          lock.subject, lease_seconds=lease_seconds, limit=limit, user=user)
    except data_store.DBSubjectLockError:
      # This exception just means that we could not obtain the lock on the queue
      # so we just return an empty list, let the worker sleep and come back to
      # fetch more tasks.
      return []
    except data_store.Error as e:
      logging.warning("Datastore exception: %s", e)
      return []

  def _QueryAndOwn(self, queue, lease_seconds=100, limit=1, user=""):
    """Does the real work of self.QueryAndOwn()."""
    tasks = []

    lease = long(lease_seconds * 1e6)

    # Only grab attributes with timestamps in the past.
    delete_task_ids = set()
    task_queue = {}
    for task, timestamp in data_store.DB.ReadTasks(
        queue,
        timestamp=(0, self.frozen_timestamp or rdfvalue.RDFDatetime.Now()),
        token=self.token):
      task.eta = timestamp
      task.last_lease = "%s@%s:%d" % (user, socket.gethostname(), os.getpid())
      # Decrement the ttl
      task.task_ttl -= 1
      if task.task_ttl <= 0:
        # Remove the task if ttl is exhausted.
        delete_task_ids.add(task.task_id)
        stats.STATS.IncrementCounter("grr_task_ttl_expired_count")
      else:
        if task.task_ttl != rdf_flows.GrrMessage.max_ttl - 1:
          stats.STATS.IncrementCounter("grr_task_retransmission_count")

        task_queue.setdefault(task.task_id, []).append(task)
        tasks.append(task)
        if len(tasks) >= limit:
          break

    if delete_task_ids:
      self.data_store.DeleteTasks(
        queue, delete_task_ids, token=self.token, sync=False)

    if task_queue:
      # Update the timestamp on claimed tasks to be in the future and decrement
      # their TTLs, delete tasks with expired ttls.
      self.data_store.CreateTasks(
          queue,
          task_queue,
          timestamp=long(time.time() * 1e6) + lease,
          sync=True,
          token=self.token)

    if delete_task_ids:
      logging.info("TTL exceeded for %d messages on queue %s",
                   len(delete_task_ids), queue)
    return tasks


class WellKnownQueueManager(QueueManager):
  """A flow manager for well known flows."""

  response_limit = 10000

  def DeleteWellKnownFlowResponses(self, session_id, responses):
    """Deletes given responses from the flow state queue."""
    to_delete_responses = {}
    for response in responses:
      to_delete_responses.setdefault(response.request_id, []).append(response)

    for request_id in to_delete_responses:
      self.data_store.DeleteResponses(session_id, request_id, responses=to_delete_responses[request_id], sync=True, token=self.token)

  def FetchRequestsAndResponses(self, session_id):
    """Well known flows do not have real requests.

    This manages retrieving all the responses without requiring corresponding
    requests.

    Args:
      session_id: The session_id to get the requests/responses for.

    Yields:
      A tuple of request (None) and responses.
    """
    timestamp = (0, self.frozen_timestamp or rdfvalue.RDFDatetime.Now())

    for response, _ in self.data_store.ReadResponses(session_id, 0, limit=self.response_limit, timestamp=timestamp, token=self.token):
      yield rdf_flows.RequestState(id=0), [response]



class QueueManagerInit(registry.InitHook):
  """Registers vars used by the QueueManager."""

  def Run(self):
    # Counters used by the QueueManager.
    stats.STATS.RegisterCounterMetric("grr_task_retransmission_count")
    stats.STATS.RegisterCounterMetric("grr_task_ttl_expired_count")
    stats.STATS.RegisterGaugeMetric(
        "notification_queue_count",
        int,
        fields=[("queue_name", str), ("priority", str)])
