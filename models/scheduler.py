# -*- coding: utf-8 -*-
from gluon.scheduler import Scheduler
scheduler = Scheduler(db)

def __schedule_daemon_tasks():
    for t in DAEMON_TASKS:
        __schedule_daemon_task(t)

def __schedule_daemon_task(task_tuple):
    task_name=task_tuple[0]
    task_period=task_tuple[1]
    tasks = db(db.scheduler_task.function_name == task_name).count()
    if not tasks:
        session.flash = scheduler.queue_task(task_name,
                pvars={},
                period = task_period,
                timeout = task_period - 1,
                repeats = 0, # 0 = unlimited
                retry_failed = -1, # -1  = unlimited
                group_name = WGRP_DAEMONS
                )
    db.commit()

__schedule_daemon_tasks()
