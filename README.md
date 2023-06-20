# dune-aws

Basic AWS Components for syncing off-chain data with Dune Community Sources

## Installation & Usage


```sh
pip install dune-aws
```

```py
from dune_aws import AWSClient

aws_client = AWSClient(
    internal_role=os.environ["AWS_INTERNAL_ROLE"],
    external_role=os.environ["AWS_EXTERNAL_ROLE"],
    external_id=os.environ["AWS_EXTERNAL_ID"],
    bucket=os.environ["AWS_BUCKET"],
)

data_set: list[dict[str, Any]] = [{"x": 1, "y": 2}, {"z": 3}]

aws_client.put_object(
    data_set,
    object_key="table_name/must_contain_dot_json_then_number.json",
)
```
