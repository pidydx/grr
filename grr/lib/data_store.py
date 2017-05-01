#!/usr/bin/env python
"""The main data store abstraction.

The data store is responsible for storing AFF4 objects permanently. This file
defines the basic interface of the data store, but there is no specific
implementation. Concrete implementations should extend the DataStore class and
provide non-abstract methods.

The data store is essentially an object store. Objects have a subject (a unique
identifying name) and a series of arbitrary attributes. Attributes also have a
name and can only store a number of well defined types.

Some data stores have internal capability to filter and search for objects based
on attribute conditions. Due to the variability of this capability in
implementations, the Filter() class is defined inside the DataStore class
itself. This allows callers to create a data store specific filter
implementation, with no prior knowledge of the concrete implementation.

In order to accommodate for the data store's basic filtering capabilities it is
important to allow the data store to store attribute values using the most
appropriate types.

The currently supported data store storage types are:
  - Integer
  - Bytes
  - String (unicode object).

This means that if one stores an attribute containing an integer, and then
retrieves this attribute, the data store guarantees that an integer is
returned (although it may be stored internally as something else).

More complex types should be encoded into bytes and stored in the data store as
bytes. The data store can then treat the type as an opaque type (and will not be
able to filter it directly).
"""


import abc
import atexit
import sys
import time
import logging

from grr.lib import access_control
from grr.lib import blob_store
from grr.lib import config_lib
from grr.lib import flags
from grr.lib import rdfvalue
from grr.lib import registry
from grr.lib import stats
from grr.lib import utils
from grr.lib.rdfvalues import flows as rdf_flows

flags.DEFINE_bool("list_storage", False, "List all storage subsystems present.")

# A global data store handle
DB = None

# There are stub methods that don't return/yield as indicated by the docstring.
# pylint: disable=g-doc-return-or-yield


class Error(stats.CountingExceptionMixin, Exception):
  """Base class for all exceptions in this module."""
  pass


class TimeoutError(Exception):
  """Raised when an access times out."""
  pass


class DBSubjectLockError(Error):
  """Raised when a lock fails to commit."""
  counter = "grr_commit_failure"


# This token will be used by default if no token was provided.
default_token = None


def GetDefaultToken(token):
  """Returns the provided token or the default token.

  Args:
    token: A token or None.

  Raises:
    access_control.UnauthorizedAccess, if no token was provided.
  """
  if token is None:
    token = default_token

  if not isinstance(token, access_control.ACLToken):
    raise access_control.UnauthorizedAccess(
        "Token is not properly specified. It should be an "
        "instance of grr.lib.access_control.ACLToken()")

  return token


class MutationPool(object):
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
    self.delete_subject_requests = []
    self.set_requests = []
    self.to_write_notifications = []
    self.to_write_tasks = []
    self.to_delete_tasks = []
    self.to_write_responses = []
    self.to_write_requests = []
    self.to_delete_requests = []
    self.to_write_collection_items = []
    self.to_write_collection_types = []
    self.to_write_collection_indexes = []
    self.delete_attributes_requests = []
    self.to_update_queue_item_leases = []
    self.to_write_aff4_indexes = []
    self.to_delete_aff4_indexes = []

  def DeleteSubjects(self, subjects):
    self.delete_subject_requests.extend(subjects)

  def DeleteSubject(self, subject):
    self.delete_subject_requests.append(subject)

  def MultiSet(self,
               subject,
               values,
               timestamp=None,
               replace=True,
               to_delete=None):
    self.set_requests.append((subject, values, timestamp, replace, to_delete))

  def Set(self, subject, attribute, value, timestamp=None, replace=True):
    self.MultiSet(
        subject, {attribute: [value]}, timestamp=timestamp, replace=replace)

  def DeleteAttributes(self, subject, attributes, start=None, end=None):
    self.delete_attributes_requests.append((subject, attributes, start, end))

  def Flush(self):
    """Flushing actually applies all the operations in the pool."""
    DB.DeleteSubjects(
        self.delete_subject_requests, token=self.token, sync=False)

    for req in self.delete_attributes_requests:
      subject, attributes, start, end = req
      DB.DeleteAttributes(
          subject,
          attributes,
          start=start,
          end=end,
          token=self.token,
          sync=False)

    for req in self.set_requests:
      subject, values, timestamp, replace, to_delete = req
      DB.MultiSet(
          subject,
          values,
          timestamp=timestamp,
          replace=replace,
          to_delete=to_delete,
          token=self.token,
          sync=False)

    for req in self.to_write_notifications:
      queue, values = req
      DB.CreateNotifications(
          queue,
          values,
          token=self.token,
          sync=False)

    for req in self.to_write_tasks:
      queue, values, timestamp = req
      DB.CreateTasks(
          queue,
          values,
          timestamp=timestamp,
          token=self.token,
          sync=False)

    for req in self.to_delete_tasks:
      queue, values = req
      DB.DeleteTasks(
          queue,
          values,
          token=self.token,
          sync=False)

    for req in self.to_write_responses:
      session_id, values = req
      DB.CreateResponses(
          session_id,
          values,
          token=self.token,
          sync=False)

    for req in self.to_write_requests:
      session_id, values = req
      DB.CreateRequests(
          session_id,
          values,
          token=self.token,
          sync=False)

    for req in self.to_delete_requests:
      session_id, values = req
      DB.DeleteRequests(
          session_id,
          values,
          token=self.token,
          sync=False)

    for req in self.to_write_collection_items:
      collection_id, rdf_value, timestamp, suffix = req
      DB.CreateCollectionItem(collection_id,
                              rdf_value,
                              timestamp,
                              suffix,
                              token=self.token,
                              sync=False)

    for req in self.to_write_collection_types:
      collection_id, collection_type = req
      DB.CreateMultiTypeCollectionEntry(collection_id, collection_type, token=self.token, sync=False)

    for req in self.to_write_collection_indexes:
      collection_id, idx, timestamp, suffix = req
      DB.CreateCollectionIndexEntry(collection_id, idx, timestamp, suffix, token=self.token, sync=False)

    for req in self.to_update_queue_item_leases:
      records, lease_expiration = req
      DB.UpdateQueueItemLeases(records, lease_expiration, token=self.token, sync=False)
      
    for req in self.to_write_aff4_indexes:
      parent_urn, child_urn = req
      DB.CreateAFF4Index(parent_urn, child_urn, token=self.token, sync=False)

    for req in self.to_delete_aff4_indexes:
      dirname, child_dir = req
      DB.DeleteAFF4Index(dirname, child_dir, token=self.token, sync=False)

    DB.Flush()

    self.delete_subject_requests = []
    self.set_requests = []
    self.to_write_notifications = []
    self.to_write_tasks = []
    self.to_delete_tasks = []
    self.to_write_responses = []
    self.to_write_requests = []
    self.to_delete_requests = []
    self.to_write_collection_items = []
    self.to_write_collection_types = []
    self.to_write_collection_indexes = []
    self.delete_attributes_requests = []
    self.to_update_queue_item_leases = []
    self.to_write_aff4_indexes = []
    self.to_delete_aff4_indexes = []

  def __enter__(self):
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    self.Flush()

  def Size(self):
    return (len(self.delete_subject_requests) +
            len(self.delete_attributes_requests) +
            len(self.set_requests) +
            len(self.to_write_notifications) +
            len(self.to_write_tasks) +
            len(self.to_delete_tasks) +
            len(self.to_write_responses) +
            len(self.to_write_requests) +
            len(self.to_delete_requests) +
            len(self.to_write_collection_items) +
            len(self.to_write_collection_types) +
            len(self.to_write_collection_indexes) +
            len(self.to_update_queue_item_leases) +
            len(self.to_write_aff4_indexes)+
            len(self.to_delete_aff4_indexes))

  def CreateNotifications(self,
               queue,
               values):
    self.to_write_notifications.append((queue, values))

  def CreateTasks(self,
               queue,
               values,
               timestamp):
    self.to_write_tasks.append((queue, values, timestamp))

  def CreateResponses(self,
               session_id,
               values):
    self.to_write_responses.append((session_id, values))

  def CreateRequests(self,
               session_id,
               values):
    self.to_write_requests.append((session_id, values))

  def DeleteTasks(self,
               queue,
               values):
    self.to_delete_tasks.append((queue, values))

  def DeleteRequests(self,
                  session_id,
                  values):
    self.to_delete_requests.append((session_id, values))

  def CreateCollectionItem(self, collection_id, rdf_value, timestamp, suffix=None, replace=True):
    self.to_write_collection_items.append((collection_id, rdf_value, timestamp, suffix))

  def CreateMultiTypeCollectionEntry(self, collection_id, collection_type):
    self.to_write_collection_types.append((collection_id, collection_type))

  def CreateCollectionIndexEntry(self, collection_id, idx, item_timestamp, item_suffix):
    self.to_write_collection_indexes.append((collection_id, idx, item_timestamp, item_suffix))

  def UpdateQueueItemLeases(self, records, lease_expiration):
    self.to_update_queue_item_leases.append((records, lease_expiration))

  def CreateAFF4Index(self, parent_urn, child_urn):
    self.to_write_aff4_indexes.append((parent_urn, child_urn))

  def DeleteAFF4Index(self, parent_urn, child_urn):
    self.to_delete_aff4_indexes.append((parent_urn, child_urn))

class DataStore(object):
  """Abstract database access."""

  __metaclass__ = registry.MetaclassRegistry

  # Constants relating to timestamps.
  EMPTY_DATA = "X"
  ALL_TIMESTAMPS = "ALL_TIMESTAMPS"
  NEWEST_TIMESTAMP = "NEWEST_TIMESTAMP"
  TIMESTAMPS = [ALL_TIMESTAMPS, NEWEST_TIMESTAMP]
  LEASE_ATTRIBUTE = "aff4:lease"
  COLLECTION_ITEM_ATTRIBUTE = "aff4:sequential_value"
  COLLECTION_VALUE_TYPE_PREFIX = "aff4:value_type_"
  COLLECTION_VALUE_TYPE_TEMPLATE = COLLECTION_VALUE_TYPE_PREFIX + "%s"
  NOTIFY_PREDICATE_PREFIX = "notify:"
  NOTIFY_PREDICATE_TEMPLATE = NOTIFY_PREDICATE_PREFIX + "%s"
  TASK_PREDICATE_PREFIX = "task:"
  TASK_PREDICATE_TEMPLATE = TASK_PREDICATE_PREFIX + "%s"
  COLLECTION_INDEX_ATTRIBUTE_PREFIX = "index:sc_"
  COLLECTION_INDEX_ATTRIBUTE_TEMPLATE = COLLECTION_INDEX_ATTRIBUTE_PREFIX + "%08x"
  QUEUE_ITEM_ATTRIBUTE = "aff4:sequential_value"
  QUEUE_ITEM_LEASE_ATTRIBUTE = "aff4:lease"

  # These attributes are related to a flow's internal data structures Requests
  # are protobufs of type RequestState. They have a constant prefix followed by
  # the request number:
  FLOW_REQUEST_PREFIX = "flow:request:"
  FLOW_REQUEST_TEMPLATE = FLOW_REQUEST_PREFIX + "%08X"

  # When a status message is received from the client, we write it with the
  # request using the following template.
  FLOW_STATUS_PREFIX = "flow:status:"
  FLOW_STATUS_TEMPLATE = FLOW_STATUS_PREFIX + "%08X"

  # Each request may have any number of responses. Responses are kept in their
  # own subject object. The subject name is derived from the session id.
  FLOW_RESPONSE_PREFIX = "flow:response:"
  FLOW_RESPONSE_TEMPLATE = FLOW_RESPONSE_PREFIX + "%08X:%08X"

  STATS_STORE_PREFIX = "aff4:stats_store/"

  VERSIONED_COLLECTION_NOTIFICATION_QUEUE = "aff4:/cron/versioned_collection_compactor"
  VERSIONED_COLLECTION_NOTIFICATION_PREFIX = "index:changed/"
  VERSIONED_COLLECTION_NOTIFICATION_TEMPLATE = VERSIONED_COLLECTION_NOTIFICATION_PREFIX + "%s"

  HASH_INDEX_PREFIX = "index:target:"
  HASH_INDEX_TEMPLATE = HASH_INDEX_PREFIX + "%s"

  AFF4_INDEX_PREFIX = "index:dir/"
  AFF4_INDEX_TEMPLATE = AFF4_INDEX_PREFIX + "%s"

  LABEL_PREFIX = "index:label_"
  LABEL_TEMPLATE = LABEL_PREFIX + "%s"

  CLIENT_INDEX_URN = rdfvalue.RDFURN("aff4:/client_index")
  KEYWORD_INDEX_PREFIX = "kw_index:"
  KEYWORD_INDEX_TEMPLATE = KEYWORD_INDEX_PREFIX + "%s"

  mutation_pool_cls = MutationPool

  flusher_thread = None
  monitor_thread = None

  def __init__(self):
    security_manager = access_control.AccessControlManager.GetPlugin(
        config_lib.CONFIG["Datastore.security_manager"])()
    self.security_manager = security_manager
    logging.info("Using security manager %s", security_manager)
    # Start the flusher thread.
    self.flusher_thread = utils.InterruptableThread(
        name="DataStore flusher thread", target=self.Flush, sleep_time=0.5)
    self.flusher_thread.start()
    self.monitor_thread = None

  def GetRequiredResolveAccess(self, attribute_prefix):
    """Returns required level of access for resolve operations.

    Args:
      attribute_prefix: A string (single attribute) or a list of
                        strings (multiple attributes).

    Returns:
      "r" when only read access is needed for resolve operation to succeed.
      Read operation allows reading the object when its URN is known.
      "rq" when both read and query access is needed for resolve operation to
      succeed. Query access allows reading indices, and thus traversing
      trees of objects (see AFF4Volume.ListChildren for details).
    """

    if isinstance(attribute_prefix, basestring):
      attribute_prefix = [utils.SmartStr(attribute_prefix)]
    else:
      attribute_prefix = [utils.SmartStr(x) for x in attribute_prefix]

    for prefix in attribute_prefix:
      if not prefix:
        return "rq"

      # Extract the column family
      try:
        column_family, _ = prefix.split(":", 1)
      except ValueError:
        raise RuntimeError("The attribute prefix must contain the column "
                           "family: %s" % prefix)

      # Columns with index require the query permission.
      if column_family.startswith("index"):
        return "rq"

    return "r"

  def InitializeBlobstore(self):
    blobstore_name = config_lib.CONFIG.Get("Blobstore.implementation")
    try:
      cls = blob_store.Blobstore.GetPlugin(blobstore_name)
    except KeyError:
      raise RuntimeError("No blob store %s found." % blobstore_name)

    self.blobstore = cls()

  def InitializeMonitorThread(self):
    """Start the thread that registers the size of the DataStore."""
    if self.monitor_thread:
      return
    self.monitor_thread = utils.InterruptableThread(
        name="DataStore monitoring thread",
        target=self._RegisterSize,
        sleep_time=60)
    self.monitor_thread.start()

  def _RegisterSize(self):
    """Measures size of DataStore."""
    stats.STATS.SetGaugeValue("datastore_size", self.Size())

  def Initialize(self):
    """Initialization of the datastore."""
    self.InitializeBlobstore()

  def Clear(self):
    """Resets datastore to empty state"""
    pass

  def Destroy(self):
    """More complete teardown of datastore resources than Clear"""
    pass

  @abc.abstractmethod
  def DeleteSubject(self, subject, sync=False, token=None):
    """Completely deletes all information about this subject."""

  def DeleteSubjects(self, subjects, sync=False, token=None):
    """Delete multiple subjects at once."""
    for subject in subjects:
      self.DeleteSubject(subject, sync=sync, token=token)

  def Set(self,
          subject,
          attribute,
          value,
          timestamp=None,
          token=None,
          replace=True,
          sync=True):
    """Set a single value for this subject's attribute.

    Args:
      subject: The subject this applies to.
      attribute: Attribute name.
      value: serialized value into one of the supported types.
      timestamp: The timestamp for this entry in microseconds since the
              epoch. If None means now.
      token: An ACL token.
      replace: Bool whether or not to overwrite current records.
      sync: If true we ensure the new values are committed before returning.
    """
    # TODO(user): don't allow subject = None
    self.MultiSet(
        subject, {attribute: [value]},
        timestamp=timestamp,
        token=token,
        replace=replace,
        sync=sync)

  def LockRetryWrapper(self,
                       subject,
                       retrywrap_timeout=1,
                       token=None,
                       retrywrap_max_timeout=10,
                       blocking=True,
                       lease_time=None):
    """Retry a DBSubjectLock until it succeeds.

    Args:
      subject: The subject which the lock applies to.
      retrywrap_timeout: How long to wait before retrying the lock.
      token: An ACL token.
      retrywrap_max_timeout: The maximum time to wait for a retry until we
         raise.
      blocking: If False, raise on first lock failure.
      lease_time: lock lease time in seconds.

    Returns:
      The DBSubjectLock object

    Raises:
      DBSubjectLockError: If the maximum retry count has been reached.
    """
    timeout = 0
    while timeout < retrywrap_max_timeout:
      try:
        return self.DBSubjectLock(subject, token=token, lease_time=lease_time)
      except DBSubjectLockError:
        if not blocking:
          raise
        stats.STATS.IncrementCounter("datastore_retries")
        time.sleep(retrywrap_timeout)
        timeout += retrywrap_timeout

    raise DBSubjectLockError("Retry number exceeded.")

  @abc.abstractmethod
  def DBSubjectLock(self, subject, lease_time=None, token=None):
    """Returns a DBSubjectLock object for a subject.

    This opens a read/write lock to the subject. Any read access to the subject
    will have a consistent view between threads. Any attempts to write to the
    subject must be performed under lock. DBSubjectLocks may fail and raise the
    DBSubjectLockError() exception.

    Users should almost always call LockRetryWrapper() to retry if the lock
    isn't obtained on the first try.

    Args:
        subject: The subject which the lock applies to. Only a
          single subject may be locked in a lock.
        lease_time: The minimum amount of time the lock should remain
          alive.
        token: An ACL token.

    Returns:
        A lock object.
    """

  @abc.abstractmethod
  def MultiSet(self,
               subject,
               values,
               timestamp=None,
               replace=True,
               sync=True,
               to_delete=None,
               token=None):
    """Set multiple attributes' values for this subject in one operation.

    Args:
      subject: The subject this applies to.
      values: A dict with keys containing attributes and values, serializations
              to be set. values can be a tuple of (value, timestamp). Value must
              be one of the supported types.
      timestamp: The timestamp for this entry in microseconds since the
              epoch. None means now.
      replace: Bool whether or not to overwrite current records.
      sync: If true we block until the operation completes.
      to_delete: An array of attributes to clear prior to setting.
      token: An ACL token.
    """

  def MultiDeleteAttributes(self,
                            subjects,
                            attributes,
                            start=None,
                            end=None,
                            sync=True,
                            token=None):
    """Remove all specified attributes from a list of subjects.

    Args:
      subjects: The list of subjects that will have these attributes removed.
      attributes: A list of attributes.
      start: A timestamp, attributes older than start will not be deleted.
      end: A timestamp, attributes newer than end will not be deleted.
      sync: If true we block until the operation completes.
      token: An ACL token.
    """
    for subject in subjects:
      self.DeleteAttributes(
          subject, attributes, start=start, end=end, sync=sync, token=token)

  @abc.abstractmethod
  def DeleteAttributes(self,
                       subject,
                       attributes,
                       start=None,
                       end=None,
                       sync=True,
                       token=None):
    """Remove all specified attributes.

    Args:
      subject: The subject that will have these attributes removed.
      attributes: A list of attributes.
      start: A timestamp, attributes older than start will not be deleted.
      end: A timestamp, attributes newer than end will not be deleted.
      sync: If true we block until the operation completes.
      token: An ACL token.
    """

  def Resolve(self, subject, attribute, token=None):
    """Retrieve a value set for a subject's attribute.

    This method is easy to use but always gets the latest version of the
    attribute. It is more flexible and efficient to use the other Resolve
    methods.

    Args:
      subject: The subject URN.
      attribute: The attribute.
      token: An ACL token.

    Returns:
      A (value, timestamp in microseconds) stored in the datastore cell, or
      (None, 0). Value will be the same type as originally stored with Set().

    Raises:
      AccessError: if anything goes wrong.
    """
    for _, value, timestamp in self.ResolveMulti(
        subject, [attribute], token=token, timestamp=self.NEWEST_TIMESTAMP):

      # Just return the first one.
      return value, timestamp

    return (None, 0)

  @abc.abstractmethod
  def MultiResolvePrefix(self,
                         subjects,
                         attribute_prefix,
                         timestamp=None,
                         limit=None,
                         token=None):
    """Generate a set of values matching for subjects' attribute.

    This method provides backwards compatibility for the old method of
    specifying regexes. Each datastore can move to prefix matching by
    overriding this method and ResolvePrefix below.

    Args:
      subjects: A list of subjects.
      attribute_prefix: The attribute prefix.

      timestamp: A range of times for consideration (In
          microseconds). Can be a constant such as ALL_TIMESTAMPS or
          NEWEST_TIMESTAMP or a tuple of ints (start, end). Inclusive of both
          lower and upper bounds.
      limit: The total number of result values to return.
      token: An ACL token.

    Returns:
       A dict keyed by subjects, with values being a list of (attribute, value
       string, timestamp).

       Values with the same attribute (happens when timestamp is not
       NEWEST_TIMESTAMP, but ALL_TIMESTAMPS or time range) are guaranteed
       to be ordered in the decreasing timestamp order.

    Raises:
      AccessError: if anything goes wrong.
    """

  def ResolvePrefix(self,
                    subject,
                    attribute_prefix,
                    timestamp=None,
                    limit=None,
                    token=None):
    """Retrieve a set of value matching for this subject's attribute.

    Args:
      subject: The subject that we will search.
      attribute_prefix: The attribute prefix.

      timestamp: A range of times for consideration (In
          microseconds). Can be a constant such as ALL_TIMESTAMPS or
          NEWEST_TIMESTAMP or a tuple of ints (start, end).

      limit: The number of results to fetch.
      token: An ACL token.

    Returns:
       A list of (attribute, value string, timestamp).

       Values with the same attribute (happens when timestamp is not
       NEWEST_TIMESTAMP, but ALL_TIMESTAMPS or time range) are guaranteed
       to be ordered in the decreasing timestamp order.

    Raises:
      AccessError: if anything goes wrong.
    """
    for _, values in self.MultiResolvePrefix(
        [subject],
        attribute_prefix,
        timestamp=timestamp,
        token=token,
        limit=limit):
      values.sort(key=lambda a: a[0])
      return values

    return []

  def ResolveMulti(self,
                   subject,
                   attributes,
                   timestamp=None,
                   limit=None,
                   token=None):
    """Resolve multiple attributes for a subject.

    Results may be in unsorted order.

    Args:
      subject: The subject to resolve.
      attributes: The attribute string or list of strings to match. Note this is
          an exact match, not a regex.
      timestamp: A range of times for consideration (In
          microseconds). Can be a constant such as ALL_TIMESTAMPS or
          NEWEST_TIMESTAMP or a tuple of ints (start, end).
      limit: The maximum total number of results we return.
      token: The security token used in this call.
    """

  def ResolveRow(self, subject, **kw):
    return self.ResolvePrefix(subject, "", **kw)

  @abc.abstractmethod
  def Flush(self):
    """Flushes the DataStore."""

  def Size(self):
    """DataStore size in bytes."""
    return -1

  def __del__(self):
    if self.flusher_thread:
      self.flusher_thread.Stop()
    if self.monitor_thread:
      self.monitor_thread.Stop()
    try:
      self.Flush()
    except Exception:  # pylint: disable=broad-except
      pass

  def _CleanSubjectPrefix(self, subject_prefix):
    subject_prefix = utils.SmartStr(rdfvalue.RDFURN(subject_prefix))
    if subject_prefix[-1] != "/":
      subject_prefix += "/"
    return subject_prefix

  def _CleanAfterURN(self, after_urn, subject_prefix):
    if after_urn:
      after_urn = utils.SmartStr(after_urn)
      if not after_urn.startswith(subject_prefix):
        raise RuntimeError("after_urn \"%s\" does not begin with prefix \"%s\""
                           % (after_urn, subject_prefix))
    return after_urn

  @abc.abstractmethod
  def ScanAttributes(self,
                     subject_prefix,
                     attributes,
                     after_urn=None,
                     max_records=None,
                     token=None,
                     relaxed_order=False):
    """Scan for values of multiple attributes across a range of rows.

    Scans rows for values of attribute. Reads the most recent value stored in
    each row.

    Args:
      subject_prefix: Subject beginning with this prefix can be scanned. Must
        be an aff4 object and a directory - "/" will be appended if necessary.
        User must have read and query permissions on this directory.

      attributes: A list of attribute names to scan.

      after_urn: If set, only scan records which come after this urn.

      max_records: The maximum number of records to scan.

      token: The security token to authenticate with.

      relaxed_order: By default, ScanAttribute yields results in lexographic
        order. If this is set, a datastore may yield results in a more
        convenient order. For certain datastores this might greatly increase
        the performance of large scans.


    Yields: Pairs (subject, result_dict) where result_dict maps attribute to
      (timestamp, value) pairs.

    """

  def ScanAttribute(self,
                    subject_prefix,
                    attribute,
                    after_urn=None,
                    max_records=None,
                    token=None,
                    relaxed_order=False):
    for s, r in self.ScanAttributes(
        subject_prefix, [attribute],
        after_urn=after_urn,
        max_records=max_records,
        token=token,
        relaxed_order=relaxed_order):
      ts, v = r[attribute]
      yield (s, ts, v)

  def ReadBlob(self, identifier, token=None):
    return self.ReadBlobs([identifier], token=token).values()[0]

  def ReadBlobs(self, identifiers, token=None):
    return self.blobstore.ReadBlobs(identifiers, token=token)

  def StoreBlob(self, content, token=None):
    return self.blobstore.StoreBlob(content, token=token)

  def StoreBlobs(self, contents, token=None):
    return self.blobstore.StoreBlobs(contents, token=token)

  def BlobExists(self, identifier, token=None):
    return self.BlobsExist([identifier], token=token).values()[0]

  def BlobsExist(self, identifiers, token=None):
    return self.blobstore.BlobsExist(identifiers, token=token)

  def DeleteBlob(self, identifier, token=None):
    return self.DeleteBlobs([identifier], token=token)

  def DeleteBlobs(self, identifiers, token=None):
    return self.blobstore.DeleteBlobs(identifiers, token=token)

  def GetMutationPool(self, token=None):
    return self.mutation_pool_cls(token=token)

  def CreateNotifications(self, queue, notifications, sync=True, token=None):
    values = {}
    for session_id, notification_queue in notifications.items():
      values[self.NOTIFY_PREDICATE_TEMPLATE % session_id] = [(notification.SerializeToString(), timestamp) for notification, timestamp in notification_queue]
    self.MultiSet(queue, values, sync=sync, replace=False, token=token)

  def ReadNotifications(self, queue, timestamp, limit=None, token=None):
    for predicate, serialized_notification, ts in self.ResolvePrefix(queue, self.NOTIFY_PREDICATE_PREFIX, timestamp=timestamp, limit=limit, token=token):
      session_id = predicate[len(self.NOTIFY_PREDICATE_PREFIX):]
      # Parse the notification.
      try:
        notification = rdf_flows.GrrNotification.FromSerializedString(serialized_notification)
      except Exception:  # pylint: disable=broad-except
        logging.exception("Can't unserialize notification, deleting it: "
                          "session_id=%s, ts=%d", session_id, ts)
        self.data_store.DeleteNotifications(
            queue,
            [session_id],
            token=self.token,
            # Make the time range narrow, but be sure to include the needed
            # notification.
            start=ts,
            end=ts,
            sync=True)
        continue
      yield (session_id, notification, ts)

  def DeleteNotifications(self, queue, session_ids, start=None, end=None, sync=True, token=None):
    self.DeleteAttributes(queue, [self.NOTIFY_PREDICATE_TEMPLATE % session_id for session_id in session_ids], start=start, end=end, sync=sync, token=token)

  def CreateTasks(self, queue, task_queue, timestamp, sync=True, token=None):
    values = {}
    for task_id, tasks in task_queue.items():
      values[self.TASK_PREDICATE_TEMPLATE % task_id] = [task.SerializeToString() for task in tasks]
    self.MultiSet(queue, values, timestamp=timestamp, sync=sync, replace=True, token=token)

  def ReadTasks(self, queue, task_id=None, timestamp=None, limit=None, token=None):
    tasks = []
    if task_id is None:
      prefix = self.TASK_PREDICATE_PREFIX
    else:
      prefix = self.TASK_PREDICATE_TEMPLATE % utils.SmartStr(task_id)

    for _, serialized_task, ts in self.ResolvePrefix(queue, prefix, timestamp=timestamp, limit=limit, token=token):
      tasks.append((rdf_flows.GrrMessage.FromSerializedString(serialized_task), ts))
    return sorted(tasks, reverse=True, key=lambda task: task[0].priority)

  def DeleteTasks(self, queue, task_ids, sync=True, token=None):
    self.DeleteAttributes(queue, [self.TASK_PREDICATE_TEMPLATE % task_id for task_id in task_ids], sync=sync, token=token)

  def CreateResponses(self, session_id, responses, replace=True, sync=True, token=None):
    status_values = {}
    response_values = {}
    for response, timestamp in responses:
      serialized_response = response.SerializeToString()
      if response.type == rdf_flows.GrrMessage.Type.STATUS:
        status_values.setdefault(self.FLOW_STATUS_TEMPLATE % response.request_id, []).append((serialized_response, timestamp))
      response_subject = session_id.Add("state/request:%08X" % response.request_id)
      response_values.setdefault(response_subject, {}).setdefault(self.FLOW_RESPONSE_TEMPLATE % (response.request_id, response.response_id), []).append((serialized_response, timestamp))
    status_subject = session_id.Add("state")
    for response_subject, values in response_values.items():
      self.MultiSet(response_subject, values, sync=sync, replace=replace, token=token)
    self.MultiSet(status_subject, status_values, sync=sync, replace=replace, token=token)

  def ReadStatuses(self, session_id, timestamp=None, limit=None, token=None):
    subject = session_id.Add("state")
    for _, serialized_status, ts in self.ResolvePrefix(subject, self.FLOW_STATUS_PREFIX, timestamp=timestamp, limit=limit, token=token):
      yield (rdf_flows.GrrMessage.FromSerializedString(serialized_status), ts)

  def ReadResponses(self, session_id, request_id, timestamp=None, limit=None, token=None):
    subject = session_id.Add("state/request:%08X" % request_id)
    for _, serialized_response, ts in self.ResolvePrefix(subject, self.FLOW_RESPONSE_PREFIX, timestamp=timestamp, limit=limit, token=token):
      yield (rdf_flows.GrrMessage.FromSerializedString(serialized_response), ts)

  def DeleteResponses(self, session_id, request_id, responses=None, sync=False, token=None):
    response_subject = session_id.Add("state/request:%08X" % request_id)
    status_subject = session_id.Add("state")
    if responses:
      self.DeleteAttributes(response_subject, [self.FLOW_RESPONSE_TEMPLATE % (request_id, response.response_id) for response in responses], sync=sync, token=token)
      self.DeleteAttributes(status_subject, [self.FLOW_STATUS_TEMPLATE % response.request_id for response in responses if response.type == rdf_flows.GrrMessage.Type.STATUS], sync=sync, token=token)
    else:
      self.DeleteSubject(response_subject, sync=sync, token=token)
      self.DeleteAttributes(status_subject, [self.FLOW_STATUS_TEMPLATE % request_id], sync=sync, token=token)

  def CreateRequests(self, session_id, requests, replace=True, sync=True, token=None):
    subject = session_id.Add("state")
    request_values = {}
    for request, timestamp in requests:
      serialized_request = request.SerializeToString()
      request_values.setdefault(self.FLOW_REQUEST_TEMPLATE % request.id, []).append((serialized_request, timestamp))
    self.MultiSet(subject, request_values, sync=sync, replace=replace, token=token)

  def ReadRequests(self, session_id, timestamp=None, limit=None, token=None):
    subject = session_id.Add("state")
    for _, serialized_request, ts in self.ResolvePrefix(subject, [self.FLOW_REQUEST_PREFIX], limit=limit, timestamp=timestamp, token=token):
      yield (rdf_flows.RequestState.FromSerializedString(serialized_request), ts)

  def DeleteRequests(self, session_id, requests, sync=False, token=None):
    subject = session_id.Add("state")
    self.DeleteAttributes(subject, [self.FLOW_REQUEST_TEMPLATE % request.id for request in requests], sync=sync, token=token)

  def CreateCollectionItem(self, collection_id, rdf_value, timestamp, suffix=None, token=None, replace=True, sync=True):
    item_subject = collection_id.Add("Results").Add("%016x.%06x" % (timestamp, suffix))

    self.Set(item_subject, self.COLLECTION_ITEM_ATTRIBUTE, rdf_value.SerializeToString(), timestamp=timestamp, token=token, replace=replace, sync=sync)

  def ReadCollectionItems(self, collection_id, timestamps, rdf_type, token=None):
    subjects = [collection_id.Add("Results").Add("%016x.%06x" % (timestamp, suffix)) for timestamp, suffix in timestamps]
    for _, results in self.MultiResolvePrefix(subjects, self.COLLECTION_ITEM_ATTRIBUTE, token=token):
      for _, serialized_rdf_value, timestamp in results:
        yield (rdf_type.FromSerializedString(serialized_rdf_value), timestamp)

  def DeleteCollection(self, collection_id, token=None):
    mutation_pool = self.GetMutationPool(token)
    mutation_pool.DeleteSubject(collection_id)
    for subject, _, _ in self.ScanAttribute(collection_id, self.COLLECTION_ITEM_ATTRIBUTE, token=token):
      mutation_pool.DeleteSubject(subject)
      if mutation_pool.Size() > 50000:
        mutation_pool.Flush()
    mutation_pool.Flush()

  def ScanCollectionItems(self, collection_id, rdf_type, after_timestamp=None, after_suffix=None, limit=None, token=None):
    if after_timestamp and after_suffix:
      after_urn = utils.SmartStr(collection_id.Add("Results").Add("%016x.%06x" % (after_timestamp, after_suffix)))
    else:
      after_urn = None

    for subject, timestamp, serialized_rdf_value in self.ScanAttribute(
        collection_id.Add("Results"),
        self.COLLECTION_ITEM_ATTRIBUTE,
        after_urn=after_urn,
        max_records=limit,
        token=token):
      suffix = int(subject[-6:], 16)
      yield (rdf_type.FromSerializedString(serialized_rdf_value), timestamp, suffix)

  def CreateMultiTypeCollectionEntry(self, collection_id, collection_type, sync=True, token=None):
    self.Set(collection_id, self.COLLECTION_VALUE_TYPE_TEMPLATE % collection_type, 1, timestamp=0, token=token, sync=sync, replace=True)

  def ReadMultiTypeCollectionEntries(self, collection_id, token=None):
    return [attribute[len(self.COLLECTION_VALUE_TYPE_PREFIX):] for attribute, _, _ in self.ResolvePrefix(collection_id, self.COLLECTION_VALUE_TYPE_PREFIX, token=token)]

  def CreateCollectionIndexEntry(self, collection_id, idx, item_timestamp, item_suffix, sync=False, token=None):
    self.Set(collection_id, self.COLLECTION_INDEX_ATTRIBUTE_TEMPLATE % idx, "%06x" % item_suffix, timestamp=item_timestamp, token=token, sync=sync, replace=True)

  def ReadCollectionIndexEntries(self, collection_id, token=None):
    for (attribute, value, timestamp) in self.ResolvePrefix(collection_id, self.COLLECTION_INDEX_ATTRIBUTE_PREFIX, token=token):
      yield (int(attribute[len(self.COLLECTION_INDEX_ATTRIBUTE_PREFIX):], 16), timestamp, int(value, 16))

  def CreateQueueItem(self, queue, rdf_value, timestamp, suffix=None, token=None, replace=True, sync=True):
    item_subject = queue.Add("Records").Add("%016x.%06x" % (timestamp, suffix))

    self.Set(item_subject, self.QUEUE_ITEM_ATTRIBUTE, rdf_value.SerializeToString(), timestamp=timestamp, token=token, replace=replace, sync=sync)

  def ScanQueueItems(self, queue, rdf_type, start_time, limit, token=None):
    after_urn = None
    if start_time:
      after_urn = queue.Add("Records").Add("%016x.%06x" % (start_time.AsMicroSecondsFromEpoch(), 0))

    for subject, values in self.ScanAttributes(
            queue.Add("Records"), [self.QUEUE_ITEM_ATTRIBUTE, self.QUEUE_ITEM_LEASE_ATTRIBUTE],
            max_records=4 * limit,
            after_urn=after_urn,
            token=token):
      if self.QUEUE_ITEM_ATTRIBUTE not in values:
        # Unlikely case, but could happen if, say, a thread called RefreshClaims
        # so late that another thread already deleted the record. Go ahead and
        # clean this up.
        self.DeleteAttributes(
          subject, [self.QUEUE_ITEM_LEASE_ATTRIBUTE], token=token)
        continue
      if self.QUEUE_ITEM_LEASE_ATTRIBUTE in values:
        lease_expiration = rdfvalue.RDFDatetime.FromSerializedString(values[self.QUEUE_ITEM_LEASE_ATTRIBUTE][1])
      else:
        lease_expiration = 0
      suffix = int(subject[-6:], 16)
      yield (rdf_type.FromSerializedString(values[self.QUEUE_ITEM_ATTRIBUTE][1]), values[self.QUEUE_ITEM_ATTRIBUTE][0], suffix, lease_expiration)

  def UpdateQueueItemLeases(self, records, lease_expiration, sync=False, token=None):
      for queue, timestamp, suffix in records:
        subject = queue.Add("Records").Add("%016x.%06x" % (timestamp, suffix))
        self.Set(subject, self.QUEUE_ITEM_LEASE_ATTRIBUTE, lease_expiration, token=token)

  def DeleteQueueItems(self, records, sync=True, token=None):
    subjects = [queue.Add("Records").Add("%016x.%06x" % (timestamp, suffix)) for queue, timestamp, suffix in records]
    self.MultiDeleteAttributes(
      subjects, [self.QUEUE_ITEM_LEASE_ATTRIBUTE, self.QUEUE_ITEM_ATTRIBUTE], sync=sync, token=token)

  def CreateStats(self, process_stats_store, stats, timestamp, sync=False, token=None):
    to_set = { self.STATS_STORE_PREFIX + name: value for name, value in stats.items()}
    self.MultiSet(process_stats_store, to_set, replace=False, token=token, timestamp=timestamp, sync=sync)

  def ReadStats(self, process_stats_stores, metric_name=None, timestamp=None, limit=None, token=None):
    multi_query_results = self.MultiResolvePrefix(
        process_stats_stores,
        self.STATS_STORE_PREFIX + (metric_name or ""),
        token=token,
        timestamp=timestamp,
        limit=limit)
    results = {}
    for process_stats_store, subject_results in multi_query_results:
      process_stats_store = rdfvalue.RDFURN(process_stats_store)
      subject_results = sorted(subject_results, key=lambda x: x[2])
      for predicate, value_string, timestamp in subject_results:
        metric_name = predicate[len(self.STATS_STORE_PREFIX):]
        results.setdefault(process_stats_store.Basename(), []).append((metric_name, value_string, timestamp))
    return results

  def DeleteStats(self, process_stats_store, metric_names, start=None, end=None, sync=False, token=None):
      predicates = [self.STATS_STORE_PREFIX + metric_name for metric_name in metric_names]
      self.DeleteAttributes(process_stats_store, predicates, start=start, end=end, token=token, sync=sync)

  def ReadVersionedCollectionNotifications(self, timestamp, token=None):
    for _, urn, urn_timestamp in self.ResolvePrefix(
        self.VERSIONED_COLLECTION_NOTIFICATION_QUEUE,
        self.VERSIONED_COLLECTION_NOTIFICATION_PREFIX,
        timestamp=timestamp,
        token=token):
      yield rdfvalue.RDFURN(urn, age=urn_timestamp)

  def CreateVersionedCollectionNotification(self, urn, timestamp=None, sync=False, token=None):
    self.Set(self.VERSIONED_COLLECTION_NOTIFICATION_QUEUE,
             self.VERSIONED_COLLECTION_NOTIFICATION_TEMPLATE % urn,
             urn,
             timestamp=timestamp,
             replace=True,
             token=token,
             sync=sync)

  def DeleteVersionedCollectionNotifications(self, urn, end=None, sync=True, token=None):
    self.DeleteAttributes(
        self.VERSIONED_COLLECTION_NOTIFICATION_QUEUE, [self.VERSIONED_COLLECTION_NOTIFICATION_TEMPLATE % urn], end=end, token=token, sync=sync)

  def CreateLabels(self, urn, labels, sync=False, token=None):
    attributes = [self.LABEL_TEMPLATE % label for label in labels]
    to_set = dict(zip(attributes, self.EMPTY_DATA * len(attributes)))
    self.MultiSet(urn, to_set, timestamp=0, token=token, replace=True, sync=sync)

  def ReadLabels(self, urn, token=None):
    result = []
    for attribute, _, _ in self.ResolvePrefix(urn, self.LABEL_PREFIX, token=token):
      result.append(attribute[len(self.LABEL_PREFIX):])
    return sorted(result)

  def DeleteLabels(self, urn, labels, sync=False, token=None):
    attributes = [self.LABEL_TEMPLATE % label for label in labels]
    self.DeleteAttributes(urn, attributes, sync=sync, token=token)

  def CreateHashIndex(self, hash_urn, file_urn, timestamp=None, sync=False, token=None):
    predicate = (self.HASH_INDEX_TEMPLATE % file_urn).lower()
    self.MultiSet(hash_urn, {predicate: [file_urn]}, timestamp=timestamp, token=token, replace=True, sync=sync)

  def ReadHashIndex(self, hash_urn, timestamp=None, limit=None, token=None):
    for _, file_urn, ts in self.ResolvePrefix(hash_urn, self.HASH_INDEX_PREFIX, timestamp=timestamp, limit=limit, token=token):
      yield file_urn

  def MultiReadHashIndexes(self, hash_urns, timestamp, limit=None, token=None):
    for index_urn, file_urns in self.MultiResolvePrefix(hash_urns, self.HASH_INDEX_PREFIX, timestamp=timestamp, limit=limit, token=token):
      yield index_urn, [file_urn for _, file_urn, _ in file_urns]

  def CreateAFF4Index(self, parent_urn, child_urn, timestamp=None, sync=False, token=None):
    attributes = {self.AFF4_INDEX_TEMPLATE % child_urn: [self.EMPTY_DATA]}
    self.MultiSet(parent_urn, attributes, timestamp=timestamp, token=token, replace=True, sync=sync)

  def DeleteAFF4Index(self, parent_urn, child_urn, sync=False, token=None):
    self.DeleteAttributes(parent_urn, [self.AFF4_INDEX_TEMPLATE % child_urn], sync=sync, token=token)

  def ReadAFF4Index(self, parent_urn, timestamp, limit=None, token=None):
    for predicate, _, timestamp in self.ResolvePrefix(
            parent_urn,
        self.AFF4_INDEX_PREFIX,
        token=token,
        timestamp=timestamp,
        limit=limit):
      child_urn = rdfvalue.RDFURN(parent_urn).Add(predicate[len(self.AFF4_INDEX_PREFIX):])
      child_urn.age = rdfvalue.RDFDatetime(timestamp)
      yield child_urn

  def MultiReadAFF4Indexes(self, parent_urns, timestamp, limit=None, token=None):
    for parent_urn, values in self.MultiResolvePrefix(
        parent_urns,
        self.AFF4_INDEX_PREFIX,
        token=token,
        timestamp=timestamp,
        limit=limit):

      child_urns = []
      for predicate, _, timestamp in values:
        child_urn = rdfvalue.RDFURN(parent_urn).Add(predicate[len(self.AFF4_INDEX_PREFIX):])
        child_urn.age = rdfvalue.RDFDatetime(timestamp)
        child_urns.append(child_urn)

      yield parent_urn, child_urns

  def CreateKeywordsIndex(self, name, keywords, timestamp=None, sync=True, token=None):
    for keyword in set(keywords):
      self.Set(self.CLIENT_INDEX_URN.Add(keyword),
               self.KEYWORD_INDEX_TEMPLATE % name,
               self.EMPTY_DATA,
               token=token,
               sync=sync,
               timestamp=timestamp)

  def DeleteKeywordsIndex(self, name, keywords, sync=True, token=None):
    for keyword in set(keywords):
      self.DeleteAttributes(self.CLIENT_INDEX_URN.Add(keyword), [self.KEYWORD_INDEX_TEMPLATE % name],
            token=token,
            sync=sync)

  def MultiReadKeywordsIndex(self, keywords, timestamp, token=None):
    keyword_urns = {self.CLIENT_INDEX_URN.Add(keyword): keyword for keyword in keywords}
    for keyword_urn, values in self.MultiResolvePrefix(
            keyword_urns.keys(),
            self.KEYWORD_INDEX_PREFIX,
            timestamp=timestamp,
            token=token):
      names = []
      for predicate, _, ts in values:
        names.append((predicate[len(self.KEYWORD_INDEX_PREFIX):], ts))
      yield keyword_urns[keyword_urn], names


class DBSubjectLock(object):
  """Provide a simple subject lock using the database.

  This class should not be used directly. Its only safe to use via the
  DataStore.LockRetryWrapper() above which implements correct backoff and
  retry behavior.
  """

  __metaclass__ = registry.MetaclassRegistry

  def __init__(self, data_store, subject, lease_time=None, token=None):
    """Obtain the subject lock for lease_time seconds.

    This is never called directly but produced from the
    DataStore.LockedSubject() factory.

    Args:
      data_store: A data_store handler.
      subject: The name of a subject to lock.
      lease_time: The minimum length of time the lock will remain valid in
        seconds. Note this will be converted to usec for storage.
      token: An ACL token which applies to all methods in this lock.
    """
    self.subject = utils.SmartStr(subject)
    self.store = data_store
    self.token = token
    # expires should be stored as usec
    self.expires = None
    self.locked = False
    if lease_time is None:
      lease_time = config_lib.CONFIG["Datastore.transaction_timeout"]
    self._Acquire(lease_time)

  def __enter__(self):
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    self.Release()

  def _Acquire(self, lease_time):
    raise NotImplementedError

  def Release(self):
    raise NotImplementedError

  def UpdateLease(self, duration):
    """Update the lock lease time by at least the number of seconds.

    Note that not all data stores implement timed locks. This method is
    only useful for data stores which expire a lock after some time.

    Args:
      duration: The number of seconds to extend the lock lease.
    """
    raise NotImplementedError

  def CheckLease(self):
    """Return the time remaining on the lock in seconds."""
    if not self.expires:
      return 0
    return max(0, self.expires / 1e6 - time.time())

  def __del__(self):
    try:
      self.Release()
    except Exception:  # This can raise on cleanup pylint: disable=broad-except
      pass


class ResultSet(object):
  """A class returned from Query which contains all the result."""
  # Total number of results that could have been returned. The results returned
  # may have been limited in some way.
  total_count = 0

  def __init__(self, results=None):
    if results is None:
      results = []

    self.results = results

  def __iter__(self):
    return iter(self.results)

  def __getitem__(self, item):
    return self.results[item]

  def __len__(self):
    return len(self.results)

  def __iadd__(self, other):
    self.results = list(self.results) + list(other)
    return self

  def Append(self, item):
    self.results.append(item)


class DataStoreInit(registry.InitHook):
  """Initialize the data store.

  Depends on the stats module being initialized.
  """

  pre = ["UserManagersInit"]

  def _ListStorageOptions(self):
    for name, cls in DataStore.classes.items():
      print "%s\t\t%s" % (name, cls.__doc__)

  def Run(self):
    """Initialize the data_store."""
    global DB  # pylint: disable=global-statement

    if flags.FLAGS.list_storage:
      self._ListStorageOptions()
      sys.exit(0)

    try:
      cls = DataStore.GetPlugin(config_lib.CONFIG["Datastore.implementation"])
    except KeyError:
      msg = ("No Storage System %s found." %
             config_lib.CONFIG["Datastore.implementation"])
      print msg
      print "Available options:"
      self._ListStorageOptions()
      raise RuntimeError(msg)

    DB = cls()  # pylint: disable=g-bad-name
    DB.Initialize()
    atexit.register(DB.Flush)
    monitor_port = config_lib.CONFIG["Monitoring.http_port"]
    if monitor_port != 0:
      stats.STATS.RegisterGaugeMetric(
          "datastore_size",
          int,
          docstring="Size of data store in bytes",
          units="BYTES")
      DB.InitializeMonitorThread()

  def RunOnce(self):
    """Initialize some Varz."""
    stats.STATS.RegisterCounterMetric("grr_commit_failure")
    stats.STATS.RegisterCounterMetric("datastore_retries")
