from enum import Enum


class JobStatus(str, Enum):
    """
    Enum of job statuses.
    """

    deferred = "deferred"
    queued = "queued"
    in_progress = "in_progress"
    success = "success"
    failed = "failed"
    expired = "expired"


class TaskStatus(str, Enum):
    enabled = "enabled"
    disabled = "disabled"

    def __str__(self):
        return self.value
