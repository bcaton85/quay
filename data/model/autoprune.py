import json
import datetime
import logging.config
from data.database import (
    AutoPruneTaskStatus,
    NamespaceAutoPrunePolicy as NamespaceAutoPrunePolicyTable,
    User,
    Repository,
    RepositoryState,
    get_epoch_timestamp_ms,
)
from data.model import db_transaction
from data.model import oci
from enum import Enum
from peewee import SQL

from util.timedeltastring import convert_to_timedelta

logger = logging.getLogger(__name__)
REPOS_BATCH_SIZE = 100


class AutoPruneMethod(Enum):
    NUMBER_OF_TAGS = "number_of_tags"
    CREATION_DATE = "creation_date"


class NamespaceAutoPrunePolicy:
    def __init__(self, db_row):
        config = json.loads(db_row.policy)
        self._db_row = db_row
        self.uuid = db_row.uuid
        self.method = config.get("method")
        self.config = config

    def get_row(self):
        return self._db_row

    def get_view(self):
        return {"uuid": self.uuid, "method": self.method, "value": self.config.get("value")}


def valid_value(method, value):
    if method == AutoPruneMethod.NUMBER_OF_TAGS and not isinstance(value, int):
        return False
    elif method == AutoPruneMethod.CREATION_DATE:
        if not isinstance(value, str):
            return False

        try:
            convert_to_timedelta(value)
        except ValueError:
            return False

    return True


def get_namespace_autoprune_policies_by_orgname(orgname):
    """
    Get the autopruning policies for the specified namespace.
    """
    try:
        query = (
            NamespaceAutoPrunePolicyTable.select(NamespaceAutoPrunePolicyTable)
            .join(User)
            .where(
                User.username == orgname,
            )
        )
        return [NamespaceAutoPrunePolicy(row) for row in query]
    except NamespaceAutoPrunePolicyTable.DoesNotExist:
        return []


def get_namespace_autoprune_policies_by_id(namespace_id):
    """
    Get the autopruning policies for the namespace by id.
    """
    try:
        query = NamespaceAutoPrunePolicyTable.select().where(
            NamespaceAutoPrunePolicyTable.namespace == namespace_id,
        )
        return [NamespaceAutoPrunePolicy(row) for row in query]
    except NamespaceAutoPrunePolicyTable.DoesNotExist:
        return []


def get_namespace_autoprune_policy(orgname, uuid):
    try:
        row = (
            NamespaceAutoPrunePolicyTable.select(NamespaceAutoPrunePolicyTable)
            .join(User)
            .where(NamespaceAutoPrunePolicyTable.uuid == uuid, User.username == orgname)
            .get()
        )
        return NamespaceAutoPrunePolicy(row)
    except NamespaceAutoPrunePolicyTable.DoesNotExist:
        return None


def create_namespace_autoprune_policy(orgname, policy_config, create_task=False):
    with db_transaction():
        try:
            namespace_id = User.select().where(User.username == orgname).get().id
        except User.DoesNotExist:
            pass
            # TODO: throw unknown user error

        if namespace_has_autoprune_policy(namespace_id):
            # TODO: throw namespace already has policy error
            return

        new_policy = NamespaceAutoPrunePolicyTable.create(
            namespace=namespace_id, policy=json.dumps(policy_config)
        )

        # Add task if it doesn't already exist
        if create_task and not namespace_has_autoprune_task(namespace_id):
            AutoPruneTaskStatus.create(namespace=namespace_id, status="queued", last_ran_ms=None)

        return new_policy


def update_namespace_autoprune_policy(orgname, uuid, policy_config):
    policy = get_namespace_autoprune_policy(orgname, uuid)
    if policy is None:
        # TODO: throw 404 here
        return None

    try:
        namespace_id = User.select().where(User.username == orgname).get().id
    except User.DoesNotExist:
        pass
        # TODO: throw unknown user error

    (
        NamespaceAutoPrunePolicyTable.update(policy=json.dumps(policy_config))
        .where(
            NamespaceAutoPrunePolicyTable.uuid == uuid,
            NamespaceAutoPrunePolicyTable.namespace == namespace_id,
        )
        .execute()
    )
    return True


def delete_namespace_autoprune_policy(orgname, uuid):
    with db_transaction():
        try:
            namespace_id = User.select().where(User.username == orgname).get().id
        except User.DoesNotExist:
            pass
            # TODO: throw unknown user error

        try:
            (
                NamespaceAutoPrunePolicyTable.delete()
                .where(
                    NamespaceAutoPrunePolicyTable.uuid == uuid,
                    NamespaceAutoPrunePolicyTable.namespace == namespace_id,
                )
                .execute()
            )
            return True
        except NamespaceAutoPrunePolicyTable.DoesNotExist:
            return None


def namespace_has_autoprune_policy(namespace_id):
    return (
        NamespaceAutoPrunePolicyTable.select(1)
        .where(NamespaceAutoPrunePolicyTable.namespace == namespace_id)
        .exists()
    )


def namespace_has_autoprune_task(namespace_id):
    return (
        AutoPruneTaskStatus.select(1).where(AutoPruneTaskStatus.namespace == namespace_id).exists()
    )


def update_autoprune_task_to_in_progress(task):
    """
    Using optimistic locking to ensure the task is not picked by another worker
    https://docs.peewee-orm.com/en/latest/peewee/hacks.html#optimistic-locking
    """
    # TODO: need to use skip locked here
    query = AutoPruneTaskStatus.update(
        status="in progress", last_ran_ms=get_epoch_timestamp_ms()
    ).where(AutoPruneTaskStatus.id == task.id)

    if query.execute() == 0:
        print("another worker picked up the task")
        return False
    return True


def fetch_ordered_autoprune_tasks_for_batchsize(batch_size):
    """
    Get the auto prune task prioritized by last_ran_ms = None followed by asc order of last_ran_ms
    """
    try:
        query = (
            AutoPruneTaskStatus.select()
            .order_by(AutoPruneTaskStatus.last_ran_ms.asc(nulls="first"), AutoPruneTaskStatus.id)
            .limit(batch_size)
        )
        return [row for row in query]
    except AutoPruneTaskStatus.DoesNotExist:
        return []


def fetch_batched_autoprune_tasks(batch_size):
    batched_tasks = fetch_ordered_autoprune_tasks_for_batchsize(batch_size)
    if not len(batched_tasks):
        return None

    list(map(lambda x: update_autoprune_task_to_in_progress(x), batched_tasks))
    return batched_tasks


def delete_autoprune_task(task):
    with db_transaction():
        try:
            (
                AutoPruneTaskStatus.delete()
                .where(
                    AutoPruneTaskStatus.id == task.id,
                    AutoPruneTaskStatus.namespace_id == task.namespace_id,
                )
                .execute()
            )
            return True
        except AutoPruneTaskStatus.DoesNotExist:
            return None


def prune_repo_by_number_of_tags(repo_id, policy_config):
    if policy_config.get("method", None) != "number_of_tags" or not policy_config.get("value", ""):
        return

    tags = oci.tag.fetch_autoprune_repo_tags_by_number(repo_id, int(policy_config["value"]))
    for tag in tags:
        oci.tag.delete_tag(repo_id, tag.name)


def prune_repo_by_creation_date(repo_id, policy_config):
    if (
        policy_config.get("method", None) != "creation_date"
        or not policy_config.get("value", "").strip()
    ):
        return

    val = int(policy_config["value"].replace("d", ""))
    td = datetime.timedelta(days=val)
    days_ms = int(td.total_seconds() * 1000)
    tags = oci.tag.fetch_autoprune_repo_tags_older_than_ms(repo_id, days_ms)
    for tag in tags:
        oci.tag.delete_tag(repo_id, tag.name)


def execute_poilcy_on_repo(policy, repo):
    poilicy_to_func_map = {
        "number_of_tags": prune_repo_by_number_of_tags,
        "creation_date": prune_repo_by_creation_date,
    }

    if poilicy_to_func_map.get(policy.method, None) is None:
        raise KeyError("Unsupported policy provided", policy.method)

    poilicy_to_func_map[policy.method](repo, policy.config)


def execute_policies_for_repo(policies, repo):
    list(map(lambda policy: execute_poilcy_on_repo(policy, repo), policies))


def get_repositories_for_namespace(namespace_id):
    query = Repository.select(
        Repository.name,
        Repository.id,
        Repository.visibility,
        Repository.kind,
        Repository.state,
    ).where(
        Repository.state != RepositoryState.MARKED_FOR_DELETION,
        Repository.namespace_user == namespace_id,
    )
    # is there a use to limit repositories here?
    # .limit(REPOS_BATCH_SIZE).order_by(SQL("rid"))
    return [row for row in query]


def execute_namespace_polices(policies, namespace_id):
    if not policies:
        return
    repo_list = get_repositories_for_namespace(namespace_id)

    # When implementing repo policies, fetch repo policies and add it to the policies list here
    list(map(lambda repo: execute_policies_for_repo(policies, repo), repo_list))
