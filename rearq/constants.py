job_key_prefix = "rearq:job:"
in_progress_key_prefix = "rearq:in-progress:"
result_key_prefix = "rearq:result:"
retry_key_prefix = "rearq:retry:"
queue_key_prefix = "rearq:queue:"

health_check_key_suffix = "rearq:health-check"

default_queue: str = f"{queue_key_prefix}default"
delay_queue = f"{queue_key_prefix}delay"
