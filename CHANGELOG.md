# ChangeLog

## 0.2

### 0.2.4

- Allow split delay queues.
- Remove `tortoise_config` and add `db_url`.

### 0.2.3

- Add `run_at_start` in cron task.

### 0.2.2

- Add job_retry_after.
- Improve web ui.

### 0.2.1

- Add web interface.

### 0.2.0

- Add api module.

## 0.1

### 0.1.4

- Add `bind` param in `rearq.task()`.
- Fix parse error with result_info().
- Add log for registered tasks.

### 0.1.3

- Fix timezone in cron task.
- Add `rearq.cancel()` to cancel delay task.

### 0.1.2

- Add check_pending_msgs.

### 0.1.1

- Update cron.
- Update task api inject task instead of worker.
