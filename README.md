# dune-aws

Basic AWS Components for syncing off-chain data with Dune Community Sources

## Installation & Usage


```sh
pip install dune-aws
```


### Usage Option 1; Direct via AWSClient

This option provides the end user with many facets of functionality (delete, upload, download, view files). 
One can mix and match the required components for their use case (utilizing also `aws.last_sync_block`). 
For a very simple example utilizing this, see Option 2.

```py
import os
from dune_aws.aws import AWSClient

aws_client = AWSClient(
    internal_role=os.environ["AWS_INTERNAL_ROLE"],
    # Info below is provided by Dune team.
    external_role=os.environ["AWS_EXTERNAL_ROLE"],
    external_id=os.environ["AWS_EXTERNAL_ID"],  
    bucket=os.environ["AWS_BUCKET"],
)

data_set = [{"x": 1, "y": 2}, {"z": 3}]

aws_client.put_object(
    data_set,
    object_key="table_name/must_contain_dot_json_then_number.json",
)
```


### Usage Option 2; Simpler via RecordHandler

```py
from dune_aws.record_handler import RecordHandler

handler = RecordHandler(
    file="table_name/moo_123",
    data_set=[{"x": 1, "y": 2}, {"z": 3}]  # should be list[dict[str, Any]],
    # If AWSClient is not supplied to the handler, it will be loaded from environment.
)

handler.upload_content(delete_first=True)
```

Note that, one must first coordinate with Dune to 
1. gain access to the AWS bucket (i.e. external credentials) and 
2. define the table schema of your dataset.