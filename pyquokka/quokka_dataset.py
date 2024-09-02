from typing import Any, cast
import ray
import polars
import pandas as pd
import pyarrow as pa

from pyquokka.types import DatasetId, IDataset, IDatasetManager, IpAddress, Schema


class Dataset(IDataset):

    def __init__(
        self, schema: Schema, wrapped_dataset: IDatasetManager, dataset_id: DatasetId
    ) -> None:
        self.schema = schema
        self.wrapped_dataset = wrapped_dataset
        self.dataset_id = dataset_id

    def __str__(self):
        return "DataSet[" + ",".join(self.schema) + "]"

    def __repr__(self):
        return "DataSet[" + ",".join(self.schema) + "]"

    def __copy__(self):
        return Dataset(self.schema, self.wrapped_dataset, self.dataset_id)

    def __deepcopy__(self, memo):
        return Dataset(self.schema, self.wrapped_dataset, self.dataset_id)

    def to_df(self) -> polars.DataFrame:
        """
        This is a blocking call. It will collect all the data from the cluster and return a Polars DataFrame to the calling Python session (could be your local machine, be careful of OOM!).

        Return:
            Polars DataFrame
        """

        return ray.get(self.wrapped_dataset.to_df.remote(self.dataset_id))

    def to_dict(self):
        return ray.get(self.wrapped_dataset.to_dict.remote(self.dataset_id))

    def to_arrow_refs(self):
        """
        This will return a list of Ray ObjectRefs to Arrow Tables. This is a blocking call. It will NOT move data to your local machine.

        Return:
            List of Ray ObjectRefs to Arrow Tables
        """
        return ray.get(self.wrapped_dataset.to_arrow_refs.remote(self.dataset_id))

    def to_ray_dataset(self):
        """
        This will convert the Quokka Dataset to a Ray Dataset. This is a blocking call. It will NOT move data to your local machine.

        Return:
            Ray Dataset
        """
        return ray.data.from_arrow_refs(self.to_arrow_refs())

    def length(self):
        return ray.get(self.wrapped_dataset.length.remote(self.dataset_id))


# we need to figure out how to clean up dead objects on dead nodes.
# not a big problem right now since Arrow Datasets are not fault tolerant anyway


@ray.remote
class ArrowDataset(IDatasetManager):

    def __init__(self) -> None:
        self.latest_dataset: DatasetId = 0
        self.objects: dict[DatasetId, dict[IpAddress, list[Any]]] = {}
        self.done: dict[DatasetId, bool] = {}
        self.length: dict[DatasetId, int] = {}

    def length(self, dataset: DatasetId) -> int:
        return self.length[dataset]

    def to_dict(self, dataset: DatasetId) -> dict[IpAddress, list[bytes]]:
        return {
            ip: [ray.cloudpickle.dumps(object) for object in self.objects[dataset][ip]]
            for ip in self.objects[dataset]
        }

    def create_dataset(self):
        self.latest_dataset += 1
        self.done[self.latest_dataset] = False
        self.length[self.latest_dataset] = 0
        self.objects[self.latest_dataset] = {}
        return self.latest_dataset

    def delete_dataset(self):
        del self.done[self.latest_dataset]
        del self.length[self.latest_dataset]
        del self.objects[self.latest_dataset]

    def added_object(self, dataset: DatasetId, ip: IpAddress, object_handle: list):
        assert dataset in self.objects, "must create the dataset first!"
        if ip not in self.objects[dataset]:
            self.objects[dataset][ip] = []
        self.objects[dataset][ip].append(cast(ray.ObjectRef, object_handle[0]))
        self.length[dataset] += int(object_handle[1])

    def to_arrow_refs(self, dataset):
        results = []
        for ip in self.objects[dataset]:
            results.extend(self.objects[dataset][ip])
        return results

    def to_df(self, dataset: DatasetId) -> polars.DataFrame | polars.Series | None:
        dfs = []
        for ip in self.objects[dataset]:
            for object in self.objects[dataset][ip]:
                dfs.append(ray.get(object))
        if len(dfs) > 0:
            arrow_table = pa.concat_tables(dfs)
            return polars.from_arrow(arrow_table)
        else:
            return None

    def ping(self):
        return True
