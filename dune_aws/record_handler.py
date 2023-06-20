"""
Abstraction for New Content Handling
provides a framework for writing new content to disk and posting to AWS
"""
import sys
from typing import Any

from s3transfer import S3UploadFailedError
from dune_aws.aws import AWSClient

from dune_aws.logger import set_log

log = set_log(__name__)


class RecordHandler:

    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        file_index: int,
        file_prefix: str,
        table: str,
        data_set: list[dict[str, Any]],
    ):
        self.file_index = file_index
        self.file_prefix = file_prefix
        self.table = table
        self.data_set = data_set

    def num_records(self) -> int:
        """Returns number of records to handle"""
        return len(self.data_set)

    @property
    def content_filename(self) -> str:
        """returns filename"""
        return f"{self.file_prefix}_{self.file_index}.json"

    @property
    def object_key(self) -> str:
        """returns object key"""
        return f"{self.table}/{self.content_filename}"

    def upload_content(self, aws: AWSClient, delete_first: bool = False) -> None:
        """
        - Writes record handlers content to persistent volume,
        - attempts to upload to AWS and
        - records last sync block on volume.
        """
        count = self.num_records()
        if count > 0:
            log.info(f"posting {count} new records to {self.object_key}")
            try:
                if delete_first:
                    aws.delete_file(self.object_key)

                aws.put_object(
                    data_set=self.data_set,
                    object_key=self.object_key,
                )
                log.info(f"{self.object_key} post complete: added {count} records")
                return
            except S3UploadFailedError as err:
                log.error(err)
                sys.exit(1)

        else:
            log.info(f"No new records for {self.table} - sync not necessary")
