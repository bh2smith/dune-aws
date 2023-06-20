# dune-aws

Basic AWS Components for syncing off-chain data with Dune Community Sources

## Installation & Usage


```sh
pip install dune-aws
```

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

Note that, one must first coordinate with Dune to 
1. gain access to the AWS bucket (i.e. external credentials) and 
2. define the table schema of your dataset.