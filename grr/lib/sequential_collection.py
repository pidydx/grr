#!/usr/bin/env python
"""A collection of records stored sequentially.
"""

import collections
import random
import threading
import time

from grr.lib import access_control
from grr.lib import data_store
from grr.lib import rdfvalue
from grr.lib import registry

from grr.lib.rdfvalues import flows as rdf_flows
from grr.lib.rdfvalues import protodict as rdf_protodict


class SequentialCollection(object):
  """A sequential collection of RDFValues.

  This class supports the writing of individual RDFValues and the sequential
  reading of them.

  """

  # The type which we store, subclasses must set this to a subclass of RDFValue.
  RDF_TYPE = None


  # The largest possible suffix - maximum value expressible by 6 hex digits.
  MAX_SUFFIX = 2**24 - 1

  def __init__(self, collection_id, token=None):
    super(SequentialCollection, self).__init__()
    # collection_id for this collection is a RDFURN.
    self.collection_id = collection_id
    self.token = token

  @classmethod
  def StaticAdd(cls,
                collection_urn,
                token,
                rdf_value,
                timestamp=None,
                suffix=None,
                mutation_pool=None,
                **kwargs):
    """Adds an rdf value to a collection.

    Adds an rdf value to a collection. Does not require that the collection be
    open. NOTE: The caller is responsible for ensuring that the collection
    exists and is of the correct type.

    Args:
      collection_urn: The urn of the collection to add to.

      token: The database access token to write with.

      rdf_value: The rdf value to add to the collection.

      timestamp: The timestamp (in microseconds) to store the rdf value
          at. Defaults to the current time.

      suffix: A 'fractional timestamp' suffix to reduce the chance of
          collisions. Defaults to a random number.

      mutation_pool: An optional MutationPool object to write to. If not given,
                     the data_store is used directly.

      **kwargs: Keyword arguments to pass through to the underlying database
        call.

    Returns:
      The pair (timestamp, suffix) which identifies the value within the
      collection.

    Raises:
      ValueError: rdf_value has unexpected type.

    """
    if not isinstance(rdf_value, cls.RDF_TYPE):
      raise ValueError("This collection only accepts values of type %s." %
                       cls.RDF_TYPE.__name__)

    if timestamp is None:
      timestamp = rdfvalue.RDFDatetime.Now()
    if isinstance(timestamp, rdfvalue.RDFDatetime):
      timestamp = timestamp.AsMicroSecondsFromEpoch()

    if not rdf_value.age:
      rdf_value.age = rdfvalue.RDFDatetime.Now()

    if not isinstance(collection_urn, rdfvalue.RDFURN):
      collection_urn = rdfvalue.RDFURN(collection_urn)

    if suffix is None:
      suffix = random.randint(1, cls.MAX_SUFFIX)

    if mutation_pool:
      mutation_pool.CreateCollectionItem(collection_urn, rdf_value, timestamp, suffix)
    else:
      data_store.DB.CreateCollectionItem(collection_urn, rdf_value, timestamp, suffix, token=token)
    return (timestamp, suffix)

  def Add(self, rdf_value, timestamp=None, suffix=None, **kwargs):
    """Adds an rdf value to the collection.

    Adds an rdf value to the collection. Does not require that the collection
    be locked.

    Args:
      rdf_value: The rdf value to add to the collection.

      timestamp: The timestamp (in microseconds) to store the rdf value
          at. Defaults to the current time.

      suffix: A 'fractional timestamp' suffix to reduce the chance of
          collisions. Defaults to a random number.

      **kwargs: Keyword arguments to pass through to the underlying database
        call.

    Returns:
      The pair (timestamp, suffix) which identifies the value within the
      collection.

    Raises:
      ValueError: rdf_value has unexpected type.

    """
    return self.StaticAdd(
        self.collection_id,
        self.token,
        rdf_value,
        timestamp=timestamp,
        suffix=suffix,
        **kwargs)

  def Scan(self, after_timestamp=None, include_suffix=False, max_records=None):
    """Scans for stored records.

    Scans through the collection, returning stored values ordered by timestamp.

    Args:

      after_timestamp: If set, only returns values recorded after timestamp.

      include_suffix: If true, the timestamps returned are pairs of the form
        (micros_since_epoc, suffix) where suffix is a 24 bit random refinement
        to avoid collisions. Otherwise only micros_since_epoc is returned.

      max_records: The maximum number of records to return. Defaults to
        unlimited.

    Yields:
      Pairs (timestamp, rdf_value), indicating that rdf_value was stored at
      timestamp.

    """

    if after_timestamp is not None:
      if isinstance(after_timestamp, tuple):
        after_suffix = after_timestamp[1]
        after_timestamp = after_timestamp[0]
      else:
        after_suffix = self.MAX_SUFFIX
    else:
      after_suffix=None

    for rdf_value, timestamp, suffix in data_store.DB.ScanCollectionItems(self.collection_id, self.RDF_TYPE, after_timestamp=after_timestamp, after_suffix=after_suffix, limit=max_records, token=self.token):
      rdf_value.age = timestamp
      if include_suffix:
        yield ((timestamp, suffix), rdf_value)
      else:
        yield (timestamp, rdf_value)

  def MultiResolve(self, timestamps):
    """Lookup multiple values by (timestamp, suffix) pairs."""
    for rdf_value, timestamp in data_store.DB.ReadCollectionItems(self.collection_id, timestamps, rdf_type=self.RDF_TYPE, token=self.token):
      rdf_value.age = timestamp
      yield rdf_value

  def __iter__(self):
    for _, item in self.Scan():
      yield item

  def Delete(self):
    data_store.DB.DeleteCollection(self.collection_id, token=self.token)


class BackgroundIndexUpdater(object):
  """Updates IndexedSequentialCollection objects in the background."""
  INDEX_DELAY = 240

  exit_now = False

  def __init__(self):
    self.to_process = collections.deque()
    self.cv = threading.Condition()

  def ExitNow(self):
    with self.cv:
      self.exit_now = True
      self.to_process.append(None)
      self.cv.notify()

  def AddIndexToUpdate(self, collection_cls, index_urn):
    with self.cv:
      self.to_process.append((collection_cls, index_urn,
                              time.time() + self.INDEX_DELAY))
      self.cv.notify()

  def ProcessCollection(self, collection_cls, collection_id, token):
    collection_cls(collection_id, token=token).UpdateIndex()

  def UpdateLoop(self):
    token = access_control.ACLToken(
        username="Background Index Updater", reason="Updating An Index")
    while not self.exit_now:
      with self.cv:
        while not self.to_process:
          self.cv.wait()
        next_update = self.to_process.popleft()
        if next_update is None:
          return

      now = time.time()
      next_cls = next_update[0]
      next_urn = next_update[1]
      next_time = next_update[2]
      while now < next_time:
        time.sleep(next_time - now)
        now = time.time()

      self.ProcessCollection(next_cls, next_urn, token)


BACKGROUND_INDEX_UPDATER = BackgroundIndexUpdater()


class UpdaterStartHook(registry.InitHook):

  def RunOnce(self):
    t = threading.Thread(
        None,
        BACKGROUND_INDEX_UPDATER.UpdateLoop,
        name="SequentialCollectionIndexUpdater")
    t.daemon = True
    t.start()


class IndexedSequentialCollection(SequentialCollection):
  """An indexed sequential collection of RDFValues.

  Adds an index to SequentialCollection, making it efficient to find the number
  of records present, and to find a particular record number.

  IMPLEMENTATION NOTE: The index is created lazily, and for records older than
    INDEX_WRITE_DELAY.
  """

  # How many records between index entries. Subclasses may change this.  The
  # full index must fit comfortably in RAM, default is meant to be reasonable
  # for collections of up to ~1b small records. (Assumes we can have ~1m index
  # points in ram, and that reading 1k records is reasonably fast.)

  INDEX_SPACING = 1024

  # An attribute name of the form "index:sc_<i>" at timestamp <t> indicates that
  # the item with record number i was stored at timestamp t. The timestamp
  # suffix is stored as the value.

  INDEX_ATTRIBUTE_PREFIX = "index:sc_"

  # The time to wait before creating an index for a record - hacky defense
  # against the correct index changing due to a late write.

  INDEX_WRITE_DELAY = rdfvalue.Duration("3m")

  def __init__(self, *args, **kwargs):
    super(IndexedSequentialCollection, self).__init__(*args, **kwargs)
    self._index = None

  def _ReadIndex(self):
    if self._index:
      return
    self._index = {0: (0, 0)}
    self._max_indexed = 0
    for idx, timestamp, suffix in data_store.DB.ReadCollectionIndexEntries(self.collection_id, token=self.token):
      self._index[idx] = (timestamp, suffix)
      self._max_indexed = max(idx, self._max_indexed)

  def _MaybeWriteIndex(self, i, ts, mutation_pool):
    """Write index marker i."""
    if i > self._max_indexed and i % self.INDEX_SPACING == 0:
      # We only write the index if the timestamp is more than 5 minutes in the
      # past: hacky defense against a late write changing the count.
      if ts[0] < (rdfvalue.RDFDatetime.Now() - self.INDEX_WRITE_DELAY
                 ).AsMicroSecondsFromEpoch():
        # We may be used in contexts were we don't have write access, so simply
        # give up in that case. TODO(user): Remove this when the ACL
        # system allows.
        try:
          mutation_pool.CreateCollectionIndexEntry(self.collection_id, i, item_timestamp=ts[0], item_suffix=ts[1])
          self._index[i] = ts
          self._max_indexed = max(i, self._max_indexed)
        except access_control.UnauthorizedAccess:
          pass

  def _IndexedScan(self, i, max_records=None):
    """Scan records starting with index i."""
    self._ReadIndex()

    # The record number that we will read next.
    idx = 0
    # The timestamp that we will start reading from.
    start_ts = 0
    if i >= self._max_indexed:
      start_ts = max((0, 0), (self._index[self._max_indexed][0],
                              self._index[self._max_indexed][1] - 1))
      idx = self._max_indexed
    else:
      try:
        possible_idx = i - i % self.INDEX_SPACING
        start_ts = (max(0, self._index[possible_idx][0]),
                    self._index[possible_idx][1] - 1)
        idx = possible_idx
      except KeyError:
        pass

    if max_records is not None:
      max_records += i - idx

    with data_store.DB.GetMutationPool(token=self.token) as mutation_pool:
      for (ts, value) in self.Scan(
          after_timestamp=start_ts,
          max_records=max_records,
          include_suffix=True):
        self._MaybeWriteIndex(idx, ts, mutation_pool)
        if idx >= i:
          yield (idx, ts, value)
        idx += 1

  def GenerateItems(self, offset=0):
    for (_, _, value) in self._IndexedScan(offset):
      yield value

  def __getitem__(self, index):
    if index >= 0:
      for (_, _, value) in self._IndexedScan(index, max_records=1):
        return value
      raise IndexError("collection index out of range")
    else:
      raise RuntimeError("Index must be >= 0")

  def CalculateLength(self):
    self._ReadIndex()
    highest_index = None
    for (i, _, _) in self._IndexedScan(self._max_indexed):
      highest_index = i
    if highest_index is None:
      return 0
    return highest_index + 1

  def __len__(self):
    return self.CalculateLength()

  def UpdateIndex(self):
    self._ReadIndex()
    for _ in self._IndexedScan(self._max_indexed):
      pass

  @classmethod
  def StaticAdd(cls,
                collection_urn,
                token,
                rdf_value,
                timestamp=None,
                suffix=None,
                **kwargs):
    r = super(IndexedSequentialCollection, cls).StaticAdd(
        collection_urn, token, rdf_value, timestamp, suffix, **kwargs)
    if random.randint(0, cls.INDEX_SPACING) == 0:
      BACKGROUND_INDEX_UPDATER.AddIndexToUpdate(cls, collection_urn)
    return r


class GeneralIndexedCollection(IndexedSequentialCollection):
  """An indexed sequential collection of RDFValues with different types."""
  RDF_TYPE = rdf_protodict.EmbeddedRDFValue

  @classmethod
  def StaticAdd(cls, collection_urn, token, rdf_value, **kwargs):
    if not rdf_value.age:
      rdf_value.age = rdfvalue.RDFDatetime.Now()

    super(GeneralIndexedCollection, cls).StaticAdd(
        collection_urn,
        token,
        rdf_protodict.EmbeddedRDFValue(payload=rdf_value),
        **kwargs)

  def Scan(self, **kwargs):
    for (timestamp, rdf_value) in super(GeneralIndexedCollection, self).Scan(
        **kwargs):
      yield (timestamp, rdf_value.payload)


class GrrMessageCollection(IndexedSequentialCollection):
  """Sequential HuntResultCollection."""
  RDF_TYPE = rdf_flows.GrrMessage

  def AddAsMessage(self, rdfvalue_in, source):
    """Helper method to add rdfvalues as GrrMessages for testing."""
    self.Add(rdf_flows.GrrMessage(payload=rdfvalue_in, source=source))
