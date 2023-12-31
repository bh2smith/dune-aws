"""Aws S3 Bucket functionality (namely upload_file)"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import boto3
from boto3.resources.base import ServiceResource
from boto3.s3.transfer import S3Transfer
from botocore.client import BaseClient
from dotenv import load_dotenv

from dune_aws.logger import set_log
from dune_aws.text_io import BytesIteratorIO

log = set_log(__name__)


@dataclass
class BucketFileObject:
    """
    Basic Structure describing a file's location and associated index in AWS bucket
    Files are structured as `table_name/prefix_N.json`
    where N is an increasing sequence of integers.
    """

    path: str
    prefix: str
    index: Optional[int]

    @classmethod
    def from_key(cls, object_key: str) -> BucketFileObject:
        """
        Decompose the unique identifier `object_key` into
        more meaningful parts from which it can be reconstructed
        """
        path, name = object_key.split("/")

        name.replace(".json", "")
        try:
            split_name = name.split("_")
            block = int(split_name[-1])
            name = "_".join(split_name[:-1])
        except ValueError:
            # File structure does not satisfy block indexing!
            block = None
        return cls(
            path,
            name,  # Keep the full reference (for delete)
            block,
        )

    @property
    def object_key(self) -> str:
        """
        Original object key
        used to operate on these elements within the S3 bucket (e.g. delete)
        """
        return "/".join([self.path, self.content_filename])

    @property
    def content_filename(self) -> str:
        """
        Object Filename (i.e. without the table)
        """
        return self.prefix if not self.index else f"{self.prefix}_{self.index}.json"


@dataclass
class BucketStructure:
    """Representation of the bucket directory structure"""

    files: dict[str, list[BucketFileObject]]

    @classmethod
    def from_bucket_collection(cls, bucket_objects: Any) -> BucketStructure:
        """
        Constructor from results of ServiceResource.Buckets
        """
        # Initialize empty lists (incase the directories contain nothing)
        grouped_files: dict[str, list[BucketFileObject]] = defaultdict(
            list[BucketFileObject]
        )
        for bucket_obj in bucket_objects:
            object_key = bucket_obj.key
            path, _ = object_key.split("/")
            grouped_files[path].append(BucketFileObject.from_key(object_key))

        log.debug(f"loaded bucket filesystem: {grouped_files.keys()}")

        return cls(files=grouped_files)

    def get(self, table: str) -> list[BucketFileObject]:
        """
        Returns the list of files under `table`
            - returns empty list if none available.
        """
        return self.files.get(table, [])


class AWSClient:
    """
    Class managing the roles required to do file operations on our S3 bucket
    """

    def __init__(
        self, internal_role: str, external_role: str, external_id: str, bucket: str
    ):
        self.internal_role = internal_role
        self.external_role = external_role
        self.external_id = external_id
        self.bucket = bucket

    @classmethod
    def new_from_environment(cls) -> AWSClient:
        """Constructs an instance of AWSClient from environment variables"""
        load_dotenv()
        return cls(
            internal_role=os.environ["AWS_INTERNAL_ROLE"],
            external_role=os.environ["AWS_EXTERNAL_ROLE"],
            external_id=os.environ["AWS_EXTERNAL_ID"],
            bucket=os.environ["AWS_BUCKET"],
        )

    @staticmethod
    def _get_s3_client(s3_resource: ServiceResource) -> BaseClient:
        """Constructs a client session for S3 Bucket upload."""
        return s3_resource.meta.client

    def _assume_role(self) -> ServiceResource:
        """
        Borrowed from AWS documentation
        https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html
        """
        sts_client = boto3.client("sts")

        # TODO - assume that the internal role is already assumed. and use get session_token
        # sts_client.get_session_token()
        internal_assumed_role_object = sts_client.assume_role(
            RoleArn=self.internal_role,
            RoleSessionName="InternalSession",
        )
        credentials = internal_assumed_role_object["Credentials"]
        # sts_client.get_session_token()

        sts_client = boto3.client(
            "sts",
            aws_access_key_id=credentials["AccessKeyId"],  # AWS_ACCESS_KEY_ID
            aws_secret_access_key=credentials[
                "SecretAccessKey"
            ],  # AWS_SECRET_ACCESS_KEY
            aws_session_token=credentials["SessionToken"],  # AWS_SESSION_TOKEN
        )

        external_assumed_role_object = sts_client.assume_role(
            RoleArn=self.external_role,
            RoleSessionName="ExternalSession",
            ExternalId=self.external_id,
        )
        credentials = external_assumed_role_object["Credentials"]

        s3_resource: ServiceResource = boto3.resource(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
        return s3_resource

    def upload_file(self, filename: str, table: str) -> bool:
        """Upload a file to an S3 bucket
        :return: True if file was uploaded, else raises
        """
        s3_client = self._get_s3_client(self._assume_role())
        S3Transfer(s3_client).upload_file(
            filename=filename,
            bucket=self.bucket,
            key=f"{table}/{filename}",
            extra_args={"ACL": "bucket-owner-full-control"},
        )
        log.debug(f"uploaded {filename} to {self.bucket}")
        return True

    def put_object(self, data_set: list[dict[str, Any]], object_key: str) -> bool:
        """Upload a file to an S3 bucket

        :param data_set: Data to upload. Should be a full path to file.
        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was uploaded, else raises
        """

        file_object = BytesIteratorIO(
            f"{json.dumps(row)}\n".encode("utf-8") for row in data_set
        )

        s3_client = self._get_s3_client(self._assume_role())

        s3_client.upload_fileobj(  # type: ignore
            file_object,
            Bucket=self.bucket,
            Key=object_key,
            ExtraArgs={"ACL": "bucket-owner-full-control"},
        )
        return True

    def delete_file(self, object_key: str) -> bool:
        """Delete a file from an S3 bucket

        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was deleted, else raises
        """
        s3_client = self._get_s3_client(self._assume_role())
        s3_client.delete_object(  # type: ignore
            Bucket=self.bucket,
            Key=object_key,
        )
        log.debug(f"deleted {object_key} from {self.bucket}")
        return True

    def download_file(self, filename: str, table: str) -> bool:
        """
        Download a file, by name, from an S3 bucket
        :return: True if file was downloaded, else raises
        """
        s3_client = self._get_s3_client(self._assume_role())
        S3Transfer(s3_client).download_file(
            filename=filename,
            bucket=self.bucket,
            key=f"{table}/{filename}",
        )
        log.debug(f"downloaded {filename} from {self.bucket}")
        return True

    def existing_files(self) -> BucketStructure:
        """
        Returns an object representing the bucket file
        structure with sync block metadata
        """
        service_resource = self._assume_role()
        bucket = service_resource.Bucket(self.bucket)  # type: ignore

        bucket_objects = bucket.objects.all()
        return BucketStructure.from_bucket_collection(bucket_objects)

    def last_sync_block(self, table: str) -> int:
        """
        Based on the existing bucket files,
        the last sync block is uniquely determined from the file names.
        """
        try:
            table_files = self.existing_files().get(table)
            return max(file_obj.index for file_obj in table_files if file_obj.index)
        except ValueError as err:
            # Raised when table_files = []
            raise FileNotFoundError(
                f"Could not determine last sync block for {table} files. No files."
            ) from err

    def delete_all(self, table: str) -> None:
        """Deletes all files within the supported tables directory"""
        log.info(f"Emptying Bucket {table}")
        try:
            table_files = self.existing_files().get(table)
            log.info(f"Found {len(table_files)} files to be removed.")
            for file_data in table_files:
                log.info(f"Deleting file {file_data.object_key}")
                self.delete_file(file_data.object_key)
        except KeyError as err:
            raise ValueError(f"invalid table name {table}") from err
