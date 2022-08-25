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
    canceled = "canceled"


class TaskStatus(str, Enum):
    enabled = "enabled"
    disabled = "disabled"

    def __str__(self):
        return self.value


class ChannelType(str, Enum):
    cancel_task = "cancel_task"
    delay_changed = "delay_changed"
