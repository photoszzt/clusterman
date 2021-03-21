"""The autoscaler uses tags/labels to associate metadata with instances."""

PREFIX = "cls"

# Tag for the name of the node
TAG_NODE_NAME = f"{PREFIX}-node-name"

# Tag for the kind of node (e.g. Head, Worker). For legacy reasons, the tag
# value says 'type' instead of 'kind'.
TAG_NODE_KIND = f"{PREFIX}-node-type"
NODE_KIND_WORKER = f"{PREFIX}-worker"

NODE_TYPE_LEGACY_WORKER = f"{PREFIX}-legacy-worker-node-type"

# Tag that reports the current state of the node (e.g. Updating, Up-to-date)
TAG_NODE_STATUS = f"{PREFIX}-node-status"
STATUS_UNINITIALIZED = "uninitialized"
STATUS_WAITING_FOR_SSH = "waiting-for-ssh"
STATUS_SYNCING_FILES = "syncing-files"
STATUS_SETTING_UP = "setting-up"
STATUS_UPDATE_FAILED = "update-failed"
STATUS_UP_TO_DATE = "up-to-date"

# Tag uniquely identifying all nodes of a cluster
TAG_CLUSTER_NAME = f"{PREFIX}-cluster-name"

# Hash of the node launch config, used to identify out-of-date nodes
TAG_LAUNCH_CONFIG = f"{PREFIX}-launch-config"

# Hash of the node runtime config, used to determine if updates are needed
TAG_RUNTIME_CONFIG = f"{PREFIX}-runtime-config"
# Hash of the contents of the directories specified by the file_mounts config
# if the node is a worker, this also hashes content of the directories
# specified by the cluster_synced_files config
TAG_FILE_MOUNTS_CONTENTS = f"{PREFIX}-file-mounts-contents"
