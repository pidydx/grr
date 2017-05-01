#!/usr/bin/env python
"""A simple message queue synchronized through data store locks.
"""
import random

from grr.lib import aff4
from grr.lib import data_store
from grr.lib import rdfvalue


class Queue(aff4.AFF4Object):
  """A queue of messages which can be polled, locked and deleted in bulk."""

  # The type which we store, subclasses must set this to a subclass of RDFValue
  rdf_type = None

  # The largest possible suffix - maximum value expressible by 6 hex digits.
  MAX_SUFFIX = 0xffffff

  @classmethod
  def StaticAdd(cls, queue_urn, token, rdf_value):
    """Adds an rdf value the queue.

    Adds an rdf value to a queue. Does not require that the queue be locked, or
    even open. NOTE: The caller is responsible for ensuring that the queue
    exists and is of the correct type.

    Args:
      queue_urn: The urn of the queue to add to.

      token: The database access token to write with.

      rdf_value: The rdf value to add to the queue.

    Raises:
      ValueError: rdf_value has unexpected type.

    """
    if not isinstance(rdf_value, cls.rdf_type):
      raise ValueError("This collection only accepts values of type %s." %
                       cls.rdf_type.__name__)

    timestamp = rdfvalue.RDFDatetime.Now().AsMicroSecondsFromEpoch()

    if not isinstance(queue_urn, rdfvalue.RDFURN):
      queue_urn = rdfvalue.RDFURN(queue_urn)

    suffix = random.randint(1, cls.MAX_SUFFIX)

    data_store.DB.CreateQueueItem(queue_urn, rdf_value, timestamp, suffix, token=token)

  def Add(self, rdf_value):
    """Adds an rdf value to the queue.

    Adds an rdf value to the queue. Does not require that the queue be locked.

    Args:
      rdf_value: The rdf value to add to the queue.

    Raises:
      ValueError: rdf_value has unexpected type.

    """
    self.StaticAdd(self.urn, self.token, rdf_value)

  def ClaimRecords(self,
                   limit=10000,
                   timeout="30m",
                   start_time=None,
                   record_filter=lambda x: False,
                   max_filtered=1000):
    """Returns and claims up to limit unclaimed records for timeout seconds.

    Returns a list of records which are now "claimed", a claimed record will
    generally be unavailable to be claimed until the claim times out. Note
    however that in case of an unexpected timeout or other error a record might
    be claimed twice at the same time. For this reason it should be considered
    weaker than a true lock.

    Args:
      limit: The number of records to claim.

      timeout: The duration of the claim.

      start_time: The time to start claiming records at. Only records with a
        timestamp after this point will be claimed.

      record_filter: A filter method to determine if the record should be
        returned. It will be called serially on each record and the record will
        be filtered (not returned or locked) if it returns True.

      max_filtered: If non-zero, limits the number of results read when
        filtered. Specifically, if max_filtered filtered results are read
        sequentially without any unfiltered results, we stop looking for
        results.

    Returns:
      A list (id, record) where record is a self.rdf_type and id is a record
      identifier which can be used to delete or release the record.

    Raises:
      LockError: If the queue is not locked.

    """
    if not self.locked:
      raise aff4.LockError("Queue must be locked to claim records.")

    results = []
    filtered_count = 0
    now = rdfvalue.RDFDatetime.Now()

    for rdf_value, timestamp, suffix, lease_expiration in data_store.DB.ScanQueueItems(self.urn, self.rdf_type, start_time, limit=limit, token=self.token):
      if lease_expiration > now:
        continue

      if record_filter(rdf_value):
        filtered_count += 1
        if max_filtered and filtered_count >= max_filtered:
          break
        continue

      results.append(((self.urn, timestamp, suffix), rdf_value))
      filtered_count = 0
      if len(results) >= limit:
        break

    self.RefreshClaims([record for record, _ in results], timeout=timeout)
    return results

  def RefreshClaims(self, records, timeout="30m"):
    """Refreshes claims on records identified by ids.

    Args:
      ids: A list of ids provided by ClaimRecords

      timeout: The new timeout for these claims.

    Raises:
      LockError: If the queue is not locked.

    """
    expiration = rdfvalue.RDFDatetime.Now() + rdfvalue.Duration(timeout)
    with data_store.DB.GetMutationPool(token=self.token) as mutation_pool:
      mutation_pool.UpdateQueueItemLeases(records, expiration)

  @classmethod
  def DeleteRecords(cls, records, token):
    """Delete records identified by ids.

    Args:
      ids: A list of ids provided by ClaimRecords.
      token: The database access token to delete with.

    Raises:
      LockError: If the queue is not locked.
    """
    data_store.DB.DeleteQueueItems(records, token=token)

  @classmethod
  def DeleteRecord(cls, record, token):
    """Delete a single record."""
    cls.DeleteRecords([record], token=token)

  @classmethod
  def ReleaseRecords(cls, records, token):
    """Release records identified by subjects.

    Releases any claim on the records identified by ids.

    Args:
      ids: A list of ids provided by ClaimRecords.
      token: The database access token to write with.

    Raises:
      LockError: If the queue is not locked.
    """

    with data_store.DB.GetMutationPool(token=token) as mutation_pool:
      mutation_pool.UpdateQueueItemLeases(records, 0)

  @classmethod
  def ReleaseRecord(cls, record, token):
    """Release a single record."""
    cls.ReleaseRecords([record], token=token)
