"""
Abstraction for New Content Handling
provides a framework for writing new content to disk and posting to AWS
"""
import sys
from typing import Any, Optional

from s3transfer import S3UploadFailedError
from dune_aws.aws import AWSClient, BucketFileObject

from dune_aws.logger import set_log

log = set_log(__name__)


class RecordHandler:

    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        file: BucketFileObject | str,
        data_set: list[dict[str, Any]],
        aws: Optional[AWSClient] = None,
    ):
        if isinstance(file, str):
            self.file = BucketFileObject.from_key(file)
        else:
            self.file = file
        self.data_set = data_set

        # Lazy load the aws client (or provide your own)
        self.aws = AWSClient.new_from_environment() if not aws else aws

    def num_records(self) -> int:
        """Returns number of records to handle"""
        return len(self.data_set)

    def upload_content(self, delete_first: bool = False) -> None:
        """uploads `self.data_set` to aws bucket under `self.object_key`"""
        count = self.num_records()
        object_key = self.file.object_key
        if count > 0:
            log.info(f"posting {count} new records to {object_key}")
            try:
                if delete_first:
                    self.aws.delete_file(object_key)

                self.aws.put_object(
                    data_set=self.data_set,
                    object_key=object_key,
                )
                log.info(f"{object_key} post complete: added {count} records")
                return
            except S3UploadFailedError as err:
                log.error(err)
                sys.exit(1)

        else:
            log.info(f"No new records for {self.file.path} - sync not necessary")
