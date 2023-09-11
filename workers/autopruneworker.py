import logging.config
import time

import features

from app import app
from data.database import UseThenDisconnect
from data.model.autoprune import *
from workers.gunicorn_worker import GunicornWorker
from workers.worker import Worker
from util.log import logfile_path


logger = logging.getLogger(__name__)
POLL_PERIOD = app.config.get("AUTO_PRUNING_POLL_PERIOD", 30)


class AutoPruneWorker(Worker):
    def __init__(self):
        super(AutoPruneWorker, self).__init__()
        self.add_operation(self.prune, POLL_PERIOD)

    def prune(self):
        # TODO: add prune logic here
        logger.info("starting auto prune logic")
        with UseThenDisconnect(app.config):
            logger.debug("in UseThenDisconnect")
            # TODO: investigate SKIP LOCK FOR UPDATE
            autoprune_task = fetch_namespaceid_autoprune_task()
            while autoprune_task is not None:
                print("autoprune_task id", autoprune_task.id)

                policies = get_namespace_autoprune_policies_by_id(3234)
                print("got autoprune policies as", policies)
                if not policies:
                    # delete_autoprune_task() --> ?
                    # print("Delete autoprune task")
                    continue
                # delete entry from autoprunetask table

                execute_namespace_polices(policies)
                # execute namespace policies

                # fetch repository policies from repositoryautoprunepolicy table

                # execute repository policies

                # delete entry from autoprune table

                # if not policies:
                #     # fetch repository policies from repositoryautoprunepolicy table
                #     pass
                print("policies", policies)
                autoprune_task = fetch_namespaceid_autoprune_task()

            logger.infor("Got no autoprune task. Exiting autoprune worker")
            return


def create_gunicorn_worker():
    worker = GunicornWorker(__name__, app, AutoPruneWorker(), features.AUTO_PRUNE)
    return worker


if __name__ == "__main__":
    if app.config.get("ACCOUNT_RECOVERY_MODE", False):
        logger.debug("Quay running in account recovery mode")
        while True:
            time.sleep(100000)

    if not features.AUTO_PRUNE:
        logger.debug("Auto-prune disabled; skipping autopruneworker")
        while True:
            time.sleep(100000)

    logging.config.fileConfig(logfile_path(debug=False), disable_existing_loggers=False)
    worker = AutoPruneWorker()
    worker.start()
