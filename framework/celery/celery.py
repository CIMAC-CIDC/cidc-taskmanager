#!/usr/bin/env python
"""Configures and runs a celery app
"""
import logging
from cidc_utils.loghandler import StackdriverJsonFormatter
from celery import Celery

LOGGER = logging.getLogger()
LOGGER.setLevel("INFO")
LOGHANDLER = logging.StreamHandler()
FORMATTER = StackdriverJsonFormatter()
LOGHANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(LOGHANDLER)
LOGGER.info({"message": "Testing logging", "category": "INFO-CELERY"})
APP = Celery(
    "taskmanager",
    include=[
        "framework.tasks.storage_tasks",
        "framework.tasks.analysis_tasks",
        "framework.tasks.processing_tasks",
        "framework.tasks.administrative_tasks",
        "framework.tasks.hugo_tasks",
        "framework.tasks.snakemake_tasks"
    ],
)

APP.config_from_object("celeryconfig")


@APP.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
    Adds periodic tasks to the application.

    Arguments:
        sender {[type]} -- [description]
    """
    # This is needed for celery to actually see what modules are registered to it.
    APP.loader.import_default_modules()
    check_last_login = APP.tasks[
        "framework.tasks.administrative_tasks.check_last_login"
    ]
    poll_auth0_logs = APP.tasks["framework.tasks.administrative_tasks.poll_auth0_logs"]
    update_gene_symbols = APP.tasks["framework.tasks.hugo_tasks.refresh_hugo_defs"]

    # Check for user expirey once per day, check for auth0 logs once per day
    sender.add_periodic_task(86400, check_last_login.s())
    sender.add_periodic_task(86400, poll_auth0_logs.s())

    # Update hugo definitions once per week.
    sender.add_periodic_task(604800, update_gene_symbols.s())


if __name__ == "__main__":
    APP.start()
