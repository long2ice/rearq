from enum import Enum


class JobStatus(str, Enum):
    """
    Enum of job statuses.
    """

    # job is in the queue, time it should be run not yet reached
    deferred = "deferred"
    # job is in the queue, time it should run has been reached
    queued = "queued"
    # job is in progress
    in_progress = "in_progress"
    # job is complete, result is available
    complete = "complete"
