from collections import namedtuple
import os
import time
from typing import Dict, List

from data.model import db_transaction

from peewee import JOIN, fn
from data.database import (
    ImageStorage,
    ManifestBlob,
    ManifestChild,
    QuotaNamespaceSize,
    Repository,
    QuotaRepositorySize,
    Tag,
    User,
)
from data.model.oci.tag import delete_tag, get_tag
import features

get_epoch_timestamp_ms = lambda: int(time.time() * 1000)


# storage_sizes: [(id, image_size),...]
def add_blob_size(repository_id: int, tag_id: int, storage_sizes):
    update_sizes(repository_id, tag_id, storage_sizes, "add")


# storage_sizes: [(id, image_size),...]
def subtract_blob_size(repository_id: int, tag_id: int, storage_sizes):
    update_sizes(repository_id, tag_id, storage_sizes, "subtract")


def update_sizes(repository_id: int, tag_id: int, storage_sizes, operation: str):
    namespace_id = get_namespace_id_from_repository(repository_id)

    # Addition - if the blob already referenced it's already been counted
    # Subtraction - should only happen on the deletion of the last blob, if another exists
    # don't subtract
    namespace_total = 0
    repository_total = 0
    for blob_id, blob_size in storage_sizes.items():
        # To account for schema 1, which doesn't include the compressed_size field
        blob_size = blob_size if blob_size is not None else 0

        # If the blob doesn't exist in the namespace it doesn't exist in the repo either
        # so add the total to both. If it exists in the namespace we need to check
        # if it exists in the repository.
        if not blob_exists_in_namespace(namespace_id, tag_id, blob_id):
            namespace_total = namespace_total + blob_size
            repository_total = repository_total + blob_size
        elif not blob_exists_in_repository(repository_id, tag_id, blob_id):
            repository_total = repository_total + blob_size

    write_namespace_total(namespace_id, tag_id, namespace_total, operation)
    write_repository_total(repository_id, tag_id, repository_total, operation)


def blob_exists_in_namespace(namespace_id: int, tag_id: int, blob_id: int):
    # Check if the blob is being referenced by an alive, non-hidden tag that isn't the
    # tag we're currently creating/deleting within the namespace.
    # Since sub-manifests are only considered alive if their parent tag is alive,
    # check the parent tag as well.
    # The where statements create an if ... else ... statement creating the logic:
    # if ParentTag is None:
    #     check that Tag is not hidden, alive, and in the namespace
    # elif ParentTag is not None:
    #     check that ParentTag is not hidden, alive, and in the namespace
    ParentTag = Tag.alias()
    return (
        ManifestBlob.select(1)
        .join(Repository, on=(ManifestBlob.repository == Repository.id))
        .join(Tag, on=(Tag.manifest == ManifestBlob.manifest))
        .join(
            ManifestChild,
            on=(ManifestBlob.manifest == ManifestChild.child_manifest),
            join_type=JOIN.LEFT_OUTER,
        )
        .join(
            ParentTag, on=(ManifestChild.manifest == ParentTag.manifest), join_type=JOIN.LEFT_OUTER
        )
        .where(
            (
                ParentTag.id.is_null(True)
                & ~Tag.hidden
                & (Repository.namespace_user == namespace_id)
                & (ManifestBlob.blob == blob_id)
                & (Tag.id != tag_id)
                & (
                    Tag.lifetime_end_ms.is_null(True)
                    | (Tag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
            | (
                ParentTag.id.is_null(False)
                & ~ParentTag.hidden
                & (Repository.namespace_user == namespace_id)
                & (ParentTag.id != tag_id)
                & (ManifestBlob.blob == blob_id)
                & (
                    ParentTag.lifetime_end_ms.is_null(True)
                    | (ParentTag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
        )
        .exists()
    )


def blob_exists_in_repository(repository_id: int, tag_id: int, blob_id: int):
    # Check if the blob is being referenced by an alive, non-hidden tag that isn't the
    # tag we're currently creating/deleting within the repository.
    # Since sub-manifests are only considered alive if their parent tag is alive,
    # check the parent tag as well.
    # The where statements create an if ... else ... statement creating the logic:
    # if ParentTag is None:
    #     check that Tag is not hidden, alive, and in the repository
    # elif ParentTag is not None:
    #     check that ParentTag is not hidden, alive, and in the repository
    ParentTag = Tag.alias()
    return (
        ManifestBlob.select(1)
        .join(Tag, on=(Tag.manifest == ManifestBlob.manifest))
        .join(
            ManifestChild,
            on=(ManifestBlob.manifest == ManifestChild.child_manifest),
            join_type=JOIN.LEFT_OUTER,
        )
        .join(
            ParentTag, on=(ManifestChild.manifest == ParentTag.manifest), join_type=JOIN.LEFT_OUTER
        )
        .where(
            (
                ParentTag.id.is_null(True)
                & ~Tag.hidden
                & (ManifestBlob.repository == repository_id)
                & (ManifestBlob.blob == blob_id)
                & (Tag.id != tag_id)
                & (
                    Tag.lifetime_end_ms.is_null(True)
                    | (Tag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
            | (
                ParentTag.id.is_null(False)
                & ~ParentTag.hidden
                & (ManifestBlob.repository == repository_id)
                & (ParentTag.id != tag_id)
                & (ManifestBlob.blob == blob_id)
                & (
                    ParentTag.lifetime_end_ms.is_null(True)
                    | (ParentTag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
        )
        .exists()
    )


def write_namespace_total(namespace_id: int, tag_id: int, namespace_total: int, operation: str):
    namespace_size = get_namespace_size(namespace_id)
    namespace_size_exists = namespace_size is not None

    # If backfill hasn't ran yet for this namespace don't do anything
    if namespace_size_exists and (
        namespace_size.backfill_start_ms is None
        or namespace_size.backfill_start_ms > get_epoch_timestamp_ms()
    ):
        return

    # If the namespacesize entry doesn't exist and this is the only manifest in the namespace
    # we can assume this is the first push to the namespace and there is no blobs to be
    # backfilled, so let the entry be created. Otherwise it still needs to be handled by the
    # backfill worker so let's exit
    params = {}
    if (
        operation == "add"
        and not namespace_size_exists
        and only_tag_in_namespace(namespace_id, tag_id)
    ):
        params["backfill_start_ms"] = 0
        params["backfill_complete"] = True
    elif operation == "add" and not namespace_size_exists:
        return

    increment_namespacesize(namespace_id, namespace_total, operation, namespace_size_exists, params)


def write_repository_total(repository_id: int, tag_id: int, repository_total: int, operation: str):
    repository_size = get_repository_size(repository_id)
    repository_size_exists = repository_size is not None

    # If backfill hasn't ran yet for this repository don't do anything
    if repository_size_exists and (
        repository_size.backfill_start_ms is None
        or repository_size.backfill_start_ms > get_epoch_timestamp_ms()
    ):
        return

    # If the repositorysize entry doesn't exist and this is the only manifest in the repository
    # we can assume this is the first push to the repository and there is no blobs to be
    # backfilled, so let the entry be created. Otherwise it still needs to be handled by the
    # backfill worker so let's exit
    params = {}
    if (
        operation == "add"
        and not repository_size_exists
        and only_tag_in_repository(repository_id, tag_id)
    ):
        params["backfill_start_ms"] = 0
        params["backfill_complete"] = True
    elif operation == "add" and not repository_size_exists:
        return

    increment_repositorysize(
        repository_id, repository_total, operation, repository_size_exists, params
    )


def get_namespace_id_from_repository(repository: int):
    try:
        repo = Repository.select(Repository.namespace_user).where(Repository.id == repository).get()
        return repo.namespace_user_id
    except Repository.DoesNotExist:
        # TODO: should not happen
        return None


def get_namespace_size(namespace_id: int):
    try:
        namespace_size = (
            QuotaNamespaceSize.select()
            .where(QuotaNamespaceSize.namespace_user_id == namespace_id)
            .get()
        )
        return namespace_size
    except QuotaNamespaceSize.DoesNotExist:
        return None


def increment_namespacesize(
    namespace_id: int, size: int, operation: str, exists: bool, params=None
):
    params = params if params is not None else {}

    if exists:
        if operation == "add":
            params["size_bytes"] = QuotaNamespaceSize.size_bytes + size
        elif operation == "subtract":
            params["size_bytes"] = QuotaNamespaceSize.size_bytes - size
        QuotaNamespaceSize.update(**params).where(
            QuotaNamespaceSize.namespace_user_id == namespace_id
        ).execute()
    else:
        params["size_bytes"] = size
        params["backfill_start_ms"] = 0
        params["backfill_complete"] = True
        # pylint: disable-next=no-value-for-parameter
        QuotaNamespaceSize.insert(namespace_user_id=namespace_id, **params).execute()


def get_repository_size(repository_id: int):
    try:
        repository_size = (
            QuotaRepositorySize.select()
            .where(QuotaRepositorySize.repository_id == repository_id)
            .get()
        )
        return repository_size
    except QuotaRepositorySize.DoesNotExist:
        return None


def increment_repositorysize(
    repository_id: int, size: int, operation: str, exists: bool, params=None
):
    params = params if params is not None else {}

    if exists:
        if operation == "add":
            params["size_bytes"] = QuotaRepositorySize.size_bytes + size
        elif operation == "subtract":
            params["size_bytes"] = QuotaRepositorySize.size_bytes - size
        QuotaRepositorySize.update(**params).where(
            QuotaRepositorySize.repository == repository_id
        ).execute()
    else:
        params["size_bytes"] = size
        params["backfill_start_ms"] = 0
        params["backfill_complete"] = True
        # pylint: disable-next=no-value-for-parameter
        QuotaRepositorySize.insert(repository_id=repository_id, **params).execute()


def only_tag_in_namespace(namespace_id: int, tag_id: int):
    return not (
        Tag.select(1)
        .join(Repository, on=(Repository.id == Tag.repository))
        .where(
            Repository.namespace_user == namespace_id,
            Tag.id != tag_id,
            Tag.hidden == False,
            (Tag.lifetime_end_ms >> None) | (Tag.lifetime_end_ms > get_epoch_timestamp_ms()),
        )
        .exists()
    )


def only_tag_in_repository(repository_id: int, tag_id: int):
    return not (
        Tag.select(1)
        .where(
            Tag.repository == repository_id,
            Tag.id != tag_id,
            Tag.hidden == False,
            (Tag.lifetime_end_ms >> None) | (Tag.lifetime_end_ms > get_epoch_timestamp_ms()),
        )
        .exists()
    )


def get_all_blob_sizes(manifest_id: int):
    blob_sizes = {}
    for blob in (
        ImageStorage.select(ImageStorage.id, ImageStorage.image_size)
        .join(ManifestBlob, on=(ManifestBlob.blob == ImageStorage.id))
        .where(ManifestBlob.manifest == manifest_id)
    ):
        blob_sizes[blob.id] = blob.image_size

    # Get blobs under the child manifests
    # pylint: disable-next=not-an-iterable
    for blob in (
        ImageStorage.select(ImageStorage.id, ImageStorage.image_size)
        .join(ManifestBlob, on=(ManifestBlob.blob == ImageStorage.id))
        .join(
            ManifestChild,
            on=(ManifestBlob.manifest == ManifestChild.child_manifest),
        )
        .where(ManifestChild.manifest == manifest_id)
    ):
        blob_sizes[blob.id] = blob.image_size
    return blob_sizes


# Backfill of existing manifests
def run_backfill(namespace_id: int):
    namespace_size = get_namespace_size(namespace_id)
    namespace_size_exists = namespace_size is not None

    if not namespace_size_exists or (
        namespace_size_exists
        and not namespace_size.backfill_complete
        and namespace_size.backfill_start_ms is None
    ):
        params = {
            "size_bytes": 0,
            "backfill_start_ms": get_epoch_timestamp_ms(),
            "backfill_complete": False,
        }
        update_namespacesize(namespace_id, params, namespace_size_exists)

        params = {"size_bytes": get_namespace_total(namespace_id), "backfill_complete": True}
        update_namespacesize(namespace_id, params, True)

    # pylint: disable-next=not-an-iterable
    for repository in repositories_in_namespace(namespace_id):
        repository_size = get_repository_size(repository.id)
        repository_size_exists = repository_size is not None
        if not repository_size_exists or (
            repository_size_exists
            and not repository_size.backfill_complete
            and repository_size.backfill_start_ms is None
        ):
            params = {
                "size_bytes": 0,
                "backfill_start_ms": get_epoch_timestamp_ms(),
                "backfill_complete": False,
            }
            update_repositorysize(repository.id, params, repository_size_exists)

            params = {"size_bytes": get_repository_total(repository.id), "backfill_complete": True}
            update_repositorysize(repository.id, params, True)


def get_namespace_total(namespace_id: int):
    # Get the total of all blobs being referenced by an alive, non-hidden tags
    # within the namespace. Since sub-manifests are considered alive if their parent tag is alive,
    # include blobs that that are referenced by non-hidden, alive parent tags as well.
    # The where statements create an if ... else ... statement creating the logic:
    # if ParentTag is None:
    #     check that Tag is not hidden, alive, and in the namespace
    # elif ParentTag is not None:
    #     check that ParentTag is not hidden, alive, and in the namespace
    ParentTag = Tag.alias()
    derived_ns = (
        ImageStorage.select(ImageStorage.image_size)
        .join(ManifestBlob, on=(ImageStorage.id == ManifestBlob.blob))
        .join(Repository, on=(Repository.id == ManifestBlob.repository))
        .join(Tag, on=(Tag.manifest == ManifestBlob.manifest))
        .join(
            ManifestChild,
            on=(ManifestChild.child_manifest == ManifestBlob.manifest),
            join_type=JOIN.LEFT_OUTER,
        )
        .join(
            ParentTag, on=(ManifestChild.manifest == ParentTag.manifest), join_type=JOIN.LEFT_OUTER
        )
        .where(
            (
                ParentTag.id.is_null(True)
                & ~Tag.hidden
                & (Repository.namespace_user == namespace_id)
                & (
                    Tag.lifetime_end_ms.is_null(True)
                    | (Tag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
            | (
                ParentTag.id.is_null(False)
                & ~ParentTag.hidden
                & (Repository.namespace_user == namespace_id)
                & (
                    ParentTag.lifetime_end_ms.is_null(True)
                    | (ParentTag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
        )
        .group_by(ImageStorage.id)
    )
    total = ImageStorage.select(fn.Sum(derived_ns.c.image_size)).from_(derived_ns).scalar()
    return total if total is not None else 0


def get_repository_total(repository_id: int):
    # Get the total of all blobs being referenced by an alive, non-hidden tags
    # within the repository. Since sub-manifests are considered alive if their parent tag is alive,
    # include blobs that that are referenced by non-hidden, alive parent tags as well.
    # The where statements create an if ... else ... statement creating the logic:
    # if ParentTag is None:
    #     check that Tag is not hidden, alive, and in the repository
    # elif ParentTag is not None:
    #     check that ParentTag is not hidden, alive, and in the repository
    ParentTag = Tag.alias()
    derived_ns = (
        ImageStorage.select(ImageStorage.image_size)
        .join(ManifestBlob, on=(ImageStorage.id == ManifestBlob.blob))
        .join(Tag, on=(Tag.manifest == ManifestBlob.manifest))
        .join(
            ManifestChild,
            on=(ManifestChild.child_manifest == ManifestBlob.manifest),
            join_type=JOIN.LEFT_OUTER,
        )
        .join(
            ParentTag, on=(ManifestChild.manifest == ParentTag.manifest), join_type=JOIN.LEFT_OUTER
        )
        .where(
            (
                ParentTag.id.is_null(True)
                & ~Tag.hidden
                & (ManifestBlob.repository == repository_id)
                & (
                    Tag.lifetime_end_ms.is_null(True)
                    | (Tag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
            | (
                ParentTag.id.is_null(False)
                & ~ParentTag.hidden
                & (ManifestBlob.repository == repository_id)
                & (
                    ParentTag.lifetime_end_ms.is_null(True)
                    | (ParentTag.lifetime_end_ms > get_epoch_timestamp_ms())
                )
            )
        )
        .group_by(ImageStorage.id)
    )
    total = ImageStorage.select(fn.Sum(derived_ns.c.image_size)).from_(derived_ns).scalar()
    return total if total is not None else 0


def repositories_in_namespace(namespace_id: int):
    return Repository.select().where(Repository.namespace_user == namespace_id)


def update_namespacesize(namespace_id: int, params, exists=False):
    if exists:
        QuotaNamespaceSize.update(**params).where(
            QuotaNamespaceSize.namespace_user_id == namespace_id
        ).execute()
    else:
        # pylint: disable-next=no-value-for-parameter
        QuotaNamespaceSize.insert(namespace_user_id=namespace_id, **params).execute()


def update_repositorysize(repository_id: int, params, exists: bool):
    if exists:
        QuotaRepositorySize.update(**params).where(
            QuotaRepositorySize.repository == repository_id
        ).execute()
    else:
        # pylint: disable-next=no-value-for-parameter
        QuotaRepositorySize.insert(repository_id=repository_id, **params).execute()


def reset_backfill(repository_id: int):
    try:
        QuotaRepositorySize.update({"backfill_start_ms": None, "backfill_complete": False}).where(
            QuotaRepositorySize.repository == repository_id
        ).execute()
        namespace_id = get_namespace_id_from_repository(repository_id)
        reset_namespace_backfill(namespace_id)
    except QuotaRepositorySize.DoesNotExist:
        pass


def reset_namespace_backfill(namespace_id: int):
    try:
        QuotaNamespaceSize.update({"backfill_start_ms": None, "backfill_complete": False}).where(
            QuotaNamespaceSize.namespace_user_id == namespace_id
        ).execute()
    except QuotaNamespaceSize.DoesNotExist:
        pass


def delete_tag_with_quota(repository_id, tag_name):
    tag = get_tag(repository_id, tag_name)
    if tag is None:
        return None

    if features.QUOTA_MANAGEMENT:
        blob_sizes = get_all_blob_sizes(tag.manifest)

    deleted_tag = delete_tag(repository_id, tag_name)

    if features.QUOTA_MANAGEMENT:
        subtract_blob_size(deleted_tag.repository, deleted_tag.id, blob_sizes)
    else:
        reset_backfill(deleted_tag.repository)

    return deleted_tag
