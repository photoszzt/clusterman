import os

LOGGER_FORMAT = (
    "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s")
LOGGER_FORMAT_HELP = f"The logging format. default='{LOGGER_FORMAT}'"
LOGGER_LEVEL = "info"
LOGGER_LEVEL_CHOICES = ["debug", "info", "warning", "error", "critical"]
LOGGER_LEVEL_HELP = ("The logging level threshold, choices=['debug', 'info',"
                     " 'warning', 'error', 'critical'], default='info'")

LOGGING_ROTATE_BYTES = 512 * 1024 * 1024  # 512MB.
LOGGING_ROTATE_BACKUP_COUNT = 5  # 5 Backup files at max.


def env_integer(key, default):
    if key in os.environ:
        return int(os.environ[key])
    return default


# Whether event logging to driver is enabled. Set to 0 to disable.
AUTOSCALER_EVENTS = env_integer("AUTOSCALER_EVENTS", 1)

# Whether to avoid launching GPU nodes for CPU only tasks.
AUTOSCALER_CONSERVE_GPU_NODES = env_integer("AUTOSCALER_CONSERVE_GPU_NODES", 1)

# How long to wait for a node to start, in seconds
AUTOSCALER_NODE_START_WAIT_S = env_integer("AUTOSCALER_NODE_START_WAIT_S", 900)

# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
AUTOSCALER_MAX_NUM_FAILURES = env_integer("AUTOSCALER_MAX_NUM_FAILURES", 5)

# The maximum number of nodes to launch in a single request.
# Multiple requests may be made for this batch size, up to
# the limit of AUTOSCALER_MAX_CONCURRENT_LAUNCHES.
AUTOSCALER_MAX_LAUNCH_BATCH = env_integer("AUTOSCALER_MAX_LAUNCH_BATCH", 5)

# Max number of nodes to launch at a time.
AUTOSCALER_MAX_CONCURRENT_LAUNCHES = env_integer(
    "AUTOSCALER_MAX_CONCURRENT_LAUNCHES", 10)

# Interval at which to perform autoscaling updates.
AUTOSCALER_UPDATE_INTERVAL_S = env_integer("AUTOSCALER_UPDATE_INTERVAL_S", 5)

# The autoscaler will attempt to restart Ray on nodes it hasn't heard from
# in more than this interval.
AUTOSCALER_HEARTBEAT_TIMEOUT_S = env_integer("AUTOSCALER_HEARTBEAT_TIMEOUT_S",
                                             30)

# The maximum allowed resource demand vector size to guarantee the resource
# demand scheduler bin packing algorithm takes a reasonable amount of time
# to run.
AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE = 1000

# Max number of retries to AWS (default is 5, time increases exponentially)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 12)
# Max number of retries to create an EC2 node (retry different subnet)
BOTO_CREATE_MAX_RETRIES = env_integer("BOTO_CREATE_MAX_RETRIES", 5)
