from typing import (
    Any,
    Iterable,
    Literal,
    Optional,
    Sequence,
    TypedDict,
    Union,
    Tuple,
    Set,
    Dict,
    List,
    Protocol,
)

import polars
import pyarrow
import sqlglot

NodeId = int
StageId = int
CatalogTableId = int
TaskManagerId = int
IpAddress = str
ColumnName = str
Schema = Sequence[ColumnName]
SourceDataStreamIndex = int
NodeType = Literal["input", "exec"]
ExplainMode = Literal["graph", "text"]
ChannelId = int
ChannelSeqId = int
SeqInfo = Any
NumTotalChannels = int
TaskGraphNodeId = int

# Tasks
TaskType = Literal["input", "inputtape", "exec", "exectape", "replay"]
StateSeq = Any
OutSeq = Any
# List[DF[source_actor_id, source_channel_id, min_seq]]
InputReqs = List[polars.DataFrame]
LastStateSeq = Any
ReplaySpecs = Any
InputObject = Any
InputTaskTuple = Tuple[TaskGraphNodeId, ChannelId, ChannelSeqId, InputObject]
TapedInputTaskTuple = Tuple[TaskGraphNodeId, ChannelId, list[ChannelSeqId]]
ExecutorTaskTuple = Tuple[TaskGraphNodeId, ChannelId, StateSeq, OutSeq, InputReqs]
TapedExecutorTaskTuple = Tuple[
    TaskGraphNodeId, ChannelId, StateSeq, OutSeq, LastStateSeq
]
ReplayTaskTuple = Tuple[TaskGraphNodeId, ChannelId, ReplaySpecs]

TaskGraphNodeType = Literal["input", "exec"]
SortOrder = Literal["stride", "range"]
JoinType = Literal["inner", "left", "semi", "anti"]
UnresolvedJoinSpec = Tuple[JoinType, Dict[SourceDataStreamIndex, ColumnName]]
ResolvedJoinSpec = Tuple[JoinType, List[Tuple[SourceDataStreamIndex, ColumnName]]]
JoinSpec = Union[UnresolvedJoinSpec, ResolvedJoinSpec]


class INode: ...


class IDataStream: ...


class ICoordinator: ...


class ICatalog:
    def estimate_cardinality(
        self,
        table_id: CatalogTableId,
        predicate: sqlglot.exp.Expression,
        filters_list: Optional[Any] = None,
    ) -> float: ...

    def register_s3_csv_source(
        self, bucket: str, key: str, schema: Schema, sep: str, total_size: int
    ) -> CatalogTableId: ...


class IDatasetManager: ...


class ITaskManager: ...


class IPartitioner(Protocol): ...


class Cluster:
    leader_public_ip: IpAddress
    leader_private_ip: IpAddress


class IDataSet:
    quokka_context: "IQuokkaContext"
    schema: Schema
    source_node_id: NodeId


DatasetId = int


class IDataset: ...


class IQuokkaContext:
    latest_node_id: NodeId
    nodes: Dict[NodeId, INode]

    cluster: Cluster

    io_per_node: int
    exec_per_node: int

    coordinator: ICoordinator
    catalog: ICatalog
    dataset_manager: IDatasetManager

    task_managers: Dict[TaskManagerId, ITaskManager]
    node_locs: Dict[TaskManagerId, IpAddress]

    io_nodes: Set[TaskManagerId]
    compute_nodes: Set[TaskManagerId]
    replay_nodes: Set[TaskManagerId]

    leader_compute_nodes: List[TaskManagerId]
    leader_io_nodes: List[TaskManagerId]

    def read_csv(
        self, table_location: str, schema: Optional[Schema], has_header: bool, sep=str
    ) -> IDataStream: ...

    def new_stream(
        self,
        sources: Dict[SourceDataStreamIndex, IDataStream],
        partitioners: Dict[SourceDataStreamIndex, IPartitioner],
        node: INode,
        schema: Schema,
        sorted: dict,
        materialized: bool,
    ) -> IDataStream: ...

    def new_dataset(self, source: IDataStream, schema: Schema) -> IDataSet: ...

    """
  If explain is True, returns None.
  If collect is True, returns a DataFrame or Series.
  Otherwise, returns a Dataset.
  """

    def execute_node(
        self, node_id: NodeId, explain: bool, mode: ExplainMode, collect: bool
    ) -> polars.DataFrame | polars.Series | IDataset | None: ...


NextTask = Any


class Reader(Protocol):

    def get_own_state(self, num_channels: int) -> Dict[ChannelId, List[SeqInfo]]: ...

    def execute(
        self, channel_id: ChannelId, SeqInfo
    ) -> Tuple[NextTask, pyarrow.Table]: ...
