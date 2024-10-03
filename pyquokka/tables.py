"""
lists of redis client wrappers that prepend keys with schema
can't use different Redis DBs because that's not best practice, and you can't do transactions across different DBs.
to do a transaction here just take out r.pipeline on the main redis client that's passed in to construct these tables.
"""

import polars
from pyquokka.target_info import TargetInfo
from pyquokka.types import (
    ChannelId,
    ChannelSeqId,
    IpAddress,
    StageId,
    StateSeq,
    TaskManagerId,
    TaskType,
    SourceDataStreamIndex,
    TaskGraphNodeId,
)
import ray.cloudpickle as pickle
from typing import Dict, Generic, Iterable, Optional, Sequence, Tuple, TypeVar

import redis

K = TypeVar("K", str, bytes, int)
V = TypeVar("V")

TaskGraphNodeIdChannelIdTupleBytes = bytes


class ClientWrapper(Generic[K, V]):
    def __init__(self, key_prefix) -> None:
        self.key_prefix = key_prefix.encode("utf-8")

    def wrap_key(self, key):
        assert type(key) == str or type(key) == bytes or type(key) == int, (
            key,
            type(key),
        )
        if type(key) == str:
            key = key.encode("utf-8")
        elif type(key) == int:
            key = str(key).encode("utf-8")
        return self.key_prefix + b"-" + key

    def srem(self, redis_client: redis.Redis, key: K, fields):
        key = self.wrap_key(key)
        return redis_client.srem(key, *fields)

    def sadd(self, redis_client: redis.Redis, key: K, field):
        key = self.wrap_key(key)
        return redis_client.sadd(key, field)

    def scard(self, redis_client: redis.Redis, key: K):
        key = self.wrap_key(key)
        return redis_client.scard(key)

    def set(self, redis_client: redis.Redis, key: K, value: V):
        key = self.wrap_key(key)
        return redis_client.set(key, value)

    def get(self, redis_client: redis.Redis, key: K) -> Optional[V]:
        key = self.wrap_key(key)
        return redis_client.get(key)

    def mget(self, redis_client: redis.Redis, keys: Sequence[K]) -> Sequence[V]:
        keys = [self.wrap_key(key) for key in keys]
        return redis_client.mget(keys)

    def mset(self, redis_client: redis.Redis, vals: Dict[K, V]):
        vals = {self.wrap_key(key): vals[key] for key in vals}
        return redis_client.mset(vals)

    def delete(self, redis_client: redis.Redis, key: K):
        key = self.wrap_key(key)
        return redis_client.delete(key)

    def smembers(self, redis_client: redis.Redis, key: K):
        key = self.wrap_key(key)
        return redis_client.smembers(key)

    def sismember(self, redis_client: redis.Redis, key: K, value):
        key = self.wrap_key(key)
        return redis_client.sismember(key, value)

    def srandmember(self, redis_client: redis.Redis, key: K):
        key = self.wrap_key(key)
        return redis_client.srandmember(key)

    def lrem(self, redis_client: redis.Redis, key: K, count, element):
        key = self.wrap_key(key)
        return redis_client.lrem(key, count, element)

    def lpush(self, redis_client: redis.Redis, key: K, value):
        key = self.wrap_key(key)
        return redis_client.lpush(key, value)

    def rpush(self, redis_client: redis.Redis, key: K, value):
        key = self.wrap_key(key)
        return redis_client.rpush(key, value)

    def lpop(self, redis_client: redis.Redis, key: K, count=1):
        key = self.wrap_key(key)
        return redis_client.lpop(key, count)

    def llen(self, redis_client: redis.Redis, key: K):
        key = self.wrap_key(key)
        return redis_client.llen(key)

    def lindex(self, redis_client: redis.Redis, key: K, index):
        key = self.wrap_key(key)
        return redis_client.lindex(key, index)

    def lrange(self, redis_client: redis.Redis, key: K, start, end) -> list[V]:
        key = self.wrap_key(key)
        return redis_client.lrange(key, start, end)

    def keys(self, redis_client: redis.Redis) -> list[K]:
        key = self.key_prefix + b"*"
        return [i.replace(self.key_prefix + b"-", b"") for i in redis_client.keys(key)]


"""
Cemetary Table (CT): track if an object is considered alive, i.e. should be present or will be generated.
The key is (actor-id, channel, seq). The value is another set of values with form (target-actor-id, partition_fn, target-channel-id)
During garbage collection, we garbage collect all the objects with prefix actor-id, channel, seq together when all of them are no longer needed.
Objects are stored in the HBQ together by prefix. As a result, we have to delete all objects with the same prefix all at once or not at all.
"""


class CemetaryTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("CT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[pickle.loads(key)] = [
                pickle.loads(k) for k in self.smembers(redis_client, key)
            ]
        return result


"""
Node Object Table (NOT): track the objects held by each node. Persistent storage like S3 is treated like a node.
    Key is node_id, value is a set of object name prefixes. We just need to store source-actor-id, source-channel-id and seq
    because all objects generated for different target channels are all present or not together. This saves order of magnitude storage.
"""


class NodeObjectTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("NOT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[key] = [pickle.loads(k) for k in self.smembers(redis_client, key)]
        return result


"""
- Present Objects Set (POT): This ikeeps track of all the objects that are present across the cluster.
  This is like an inverted index of NOT, maintained transactionally to save time.
    Key: object_name, value is where it is. The key is again just the prefix.
"""


class PresentObjectTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("POT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


"""
- Node Task Table (NTT): this keeps track of all the tasks on a node.
    Key: node_id, value is a list of tasks. Poor man's priority queue.
"""

TaskTypeAndTaskGraphNodeIdChannelIdAndRestTupleBytes = bytes


class NodeTaskTable(
    ClientWrapper[TaskManagerId, TaskTypeAndTaskGraphNodeIdChannelIdAndRestTupleBytes]
):
    def __init__(self) -> None:
        super().__init__("NTT")

    def to_dict(self, redis_client) -> Dict[TaskManagerId, tuple[TaskType, tuple]]:
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[key] = [
                pickle.loads(k) for k in self.lrange(redis_client, key, 0, -1)
            ]
        return result


"""
- Generated Input Table (GIT): no tasks in the system are running to generate those objects. This could be figured out
 by just reading through all the node tasks, but that can be expensive.
    Key: (source_actor_id, source_channel_id)
    Value: set of seq numbers in this NOTT.
"""


class GeneratedInputTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("GIT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[pickle.loads(key)] = self.smembers(redis_client, key)
        return result


"""
- Lineage Table (LT): this tracks the inputs of each output. This dynamically tells you what is IN(x)
- The key is simply (actor_id, channel_id, seq). Since you know what partition_fn to apply to get the objects.
"""

TaskGraphNodeIdChannelIdChannelSeqIdTupleBytes = bytes
SeqInfoBytes = bytes


class LineageTable(
    ClientWrapper[TaskGraphNodeIdChannelIdChannelSeqIdTupleBytes, SeqInfoBytes]
):
    def __init__(self) -> None:
        super().__init__("LT")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, ChannelId, ChannelSeqId], SeqInfoBytes]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


"""
- Done Seq Table (DST): this tracks the last sequence number of each actor_id, channel_id. There can only be one value
"""


class DoneSeqTable(ClientWrapper[TaskGraphNodeIdChannelIdTupleBytes, ChannelSeqId]):
    def __init__(self) -> None:
        super().__init__("DST")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, ChannelId], ChannelSeqId]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


"""
- Last Checkpoint Table (LCT): this tracks the last state_seq number that has been checkpointed for actor_id, channel_id
"""


class LastCheckpointTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("LCT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[key] = [
                pickle.loads(k) for k in self.lrange(redis_client, key, 0, -1)
            ]
        return result


"""
- Executor State Table (EST): this tracks what state_seq each actor_id and channel_id are on (last committed)
"""


class ExecutorStateTable(ClientWrapper[TaskGraphNodeIdChannelIdTupleBytes, StateSeq]):
    def __init__(self) -> None:
        super().__init__("EST")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, ChannelId], StateSeq]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


"""
- Channel Location Table (CLT): this tracks where each channel is scheduled
"""


class ChannelLocationTable(
    ClientWrapper[TaskGraphNodeIdChannelIdTupleBytes, IpAddress]
):
    def __init__(self) -> None:
        super().__init__("CLT")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, ChannelId], IpAddress]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


"""
- Function Object Table (FOT): this stores the function objects
    key: actor_id, value: serialized Python object
"""

ReaderOrExecutorBytes = bytes


class FunctionObjectTable(ClientWrapper[TaskGraphNodeId, ReaderOrExecutorBytes]):
    def __init__(self) -> None:
        super().__init__("FOT")


"""
- Input Requirements Table (IRT): this stores the new input requirements
    key: actor_id, channel_id, seq (only seqs will be ckpt seqs), value: new_input_reqs df
"""


class InputRequirementsTable(
    ClientWrapper[TaskGraphNodeIdChannelIdChannelSeqIdTupleBytes, bytes]
):
    def __init__(self) -> None:
        super().__init__("IRT")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, ChannelId, StateSeq], list[polars.DataFrame]]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {
            pickle.loads(key): pickle.loads(value) for key, value in zip(keys, values)
        }


"""
- Sorted Actors Table (SAT): this stores the information corresponding to actors that are supposedly sorted
"""


class SortedActorsTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("SAT")

    def to_dict(
        self, redis_client
    ) -> Dict[TaskGraphNodeId, Dict[SourceDataStreamIndex, bool]]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {int(key): pickle.loads(value) for key, value in zip(keys, values)}


"""
- Partition Function Table (PFT): this stores the partition functions for source - target pairs
"""

SourceToTargetTaskGraphNodeIdsBytes = bytes


# stores lowered target infos (with a function partitioner etc)
class PartitionFunctionTable(
    ClientWrapper[SourceToTargetTaskGraphNodeIdsBytes, TargetInfo]
):
    def __init__(self) -> None:
        super().__init__("PFT")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, TaskGraphNodeId], TargetInfo]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


"""
- Actor Stage Table (AST): this stores the stage of each actor
"""


class ActorStageTable(ClientWrapper[TaskGraphNodeId, StageId]):
    def __init__(self) -> None:
        super().__init__("AST")

    def to_dict(self, redis_client) -> Dict[TaskGraphNodeId, StageId]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {int(key): int(value) for key, value in zip(keys, values)}


"""
- Last Input Table (LIT): this tracks the last input of each actor_id, channel_id
"""


class LastInputTable(ClientWrapper[TaskGraphNodeIdChannelIdTupleBytes, ChannelSeqId]):
    def __init__(self) -> None:
        super().__init__("LIT")

    def to_dict(
        self, redis_client
    ) -> Dict[Tuple[TaskGraphNodeId, ChannelId], ChannelSeqId]:
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): int(value) for key, value in zip(keys, values)}


"""
- Executor Watermark Table (EWT): this keeps track of the latest processed seq for each actor_id, channel_id to affect
   backpressure similar to Storm
"""


class ExecutorWatermarkTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("EWT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): int(value) for key, value in zip(keys, values)}
