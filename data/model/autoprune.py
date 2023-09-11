
import json
import time
import logging.config
from data.database import AutoPruneTaskStatus, NamespaceAutoPrunePolicy as NamespaceAutoPrunePolicyTable, User
from data.model import db_transaction
from enum import Enum

from util.timedeltastring import convert_to_timedelta
logger = logging.getLogger(__name__)

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
        query = (NamespaceAutoPrunePolicyTable.select(NamespaceAutoPrunePolicyTable)
            .join(User)
            .where(
                User.username == orgname,
            ))
        return [ NamespaceAutoPrunePolicy(row) for row in query ]
    except NamespaceAutoPrunePolicyTable.DoesNotExist:
        return []


def get_namespace_autoprune_policies_by_id(namespace_id):
    """
    Get the autopruning policies for the namespace by id.
    """
    try:
        query = (NamespaceAutoPrunePolicyTable.select(NamespaceAutoPrunePolicyTable)
            .join(User)
            .where(
                User.id == namespace_id,
            ))
        return [ NamespaceAutoPrunePolicy(row) for row in query ]
    except NamespaceAutoPrunePolicyTable.DoesNotExist:
        return []


def get_namespace_autoprune_policy(orgname, uuid):
    try:
        row = (
            NamespaceAutoPrunePolicyTable.select(NamespaceAutoPrunePolicyTable)
            .join(User)
            .where(NamespaceAutoPrunePolicyTable.uuid==uuid, User.username==orgname)
            .get()
        )
        return NamespaceAutoPrunePolicy(row)
    except NamespaceAutoPrunePolicyTable.DoesNotExist:
        return None
    

def create_namespace_autoprune_policy(orgname, policy_config, create_task=False):
    with db_transaction():
        try:
            namespace_id = User.select().where(User.username==orgname).get().id
        except User.DoesNotExist:
            pass
            # TODO: throw unknown user error

        if namespace_has_autoprune_policy(namespace_id):
            # TODO: throw namespace already has policy error
            return
        
        new_policy = NamespaceAutoPrunePolicyTable.create(namespace=namespace_id, policy=json.dumps(policy_config))

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
        namespace_id = User.select().where(User.username==orgname).get().id
    except User.DoesNotExist:
        pass
        # TODO: throw unknown user error

    (NamespaceAutoPrunePolicyTable.update(policy=json.dumps(policy_config))
    .where(NamespaceAutoPrunePolicyTable.uuid==uuid, NamespaceAutoPrunePolicyTable.namespace==namespace_id)
    .execute())
    return True


def delete_namespace_autoprune_policy(orgname, uuid):
    with db_transaction():
        try:
            namespace_id = User.select().where(User.username==orgname).get().id
        except User.DoesNotExist:
            pass
            # TODO: throw unknown user error

        try:
            (NamespaceAutoPrunePolicyTable.delete()
             .where(NamespaceAutoPrunePolicyTable.uuid==uuid, NamespaceAutoPrunePolicyTable.namespace==namespace_id)
             .execute())
            return True
        except NamespaceAutoPrunePolicyTable.DoesNotExist:
            return None

def namespace_has_autoprune_policy(namespace_id):
    return (NamespaceAutoPrunePolicyTable.select(1)
            .where(NamespaceAutoPrunePolicyTable.namespace==namespace_id)
            .exists())

def namespace_has_autoprune_task(namespace_id):
    return (AutoPruneTaskStatus.select(1)
            .where(AutoPruneTaskStatus.namespace==namespace_id)
            .exists())


def fetch_ordered_autoprune_task_by_last_ran():
    try:
        return (AutoPruneTaskStatus.select()
                .where(AutoPruneTaskStatus.status == "queued")
                .order_by(AutoPruneTaskStatus.last_ran_ms.asc(), AutoPruneTaskStatus.id)
                .get())
    except AutoPruneTaskStatus.DoesNotExist:
        print("Returning no task that is currently queued")
        return None

def fetch_never_ran_ordered_autoprune_task():
    try:
        return (AutoPruneTaskStatus.select()
                .where(AutoPruneTaskStatus.last_ran_ms.is_null(True))
                .order_by(AutoPruneTaskStatus.id)
                .get())
    except AutoPruneTaskStatus.DoesNotExist:
        print("Returning no task that was never ran")
        return None


def update_autoprune_task_to_in_progress(task):
    """
        Using optimistic locking to ensure the task is not picked by another worker
        https://docs.peewee-orm.com/en/latest/peewee/hacks.html#optimistic-locking
    """
    query = AutoPruneTaskStatus.update(status='in progress', last_ran_ms=time.time_ns()).where(
            (AutoPruneTaskStatus.status == "queued") &(AutoPruneTaskStatus.id == task.id))

    if query.execute() == 0:
        print("another worker picked up the task")
        return False
    return True


def fetch_namespaceid_autoprune_task():
    """
    Get the auto prune task prioritized by last_ran_ms = None followed by desc order of last_ran_ms
    """
    logger.debug("in fetch_autoprune_task")
    next_task = fetch_never_ran_ordered_autoprune_task() or fetch_ordered_autoprune_task_by_last_ran()
    if not next_task:
        return None

    if update_autoprune_task_to_in_progress(next_task):
        return next_task

    return None

def delete_autoprune_task(task):
    with db_transaction():
        try:
            (AutoPruneTaskStatus.delete()
             .where(AutoPruneTaskStatus.id==task.id, AutoPruneTaskStatus.namespace_id==task.namespace_id, AutoPruneTaskStatus.status=="in progess")
             .execute())
            return True
        except AutoPruneTaskStatus.DoesNotExist:
            return None


def execute_namespace_polices(policies):
    if not policies:
        return
    print("in execute_namespace_polices", policies)
    # fetch all repos
