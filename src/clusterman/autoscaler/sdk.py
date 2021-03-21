def get_docker_host_mount_location(cluster_name: str) -> str:
    """Return host path that Docker mounts attach to."""
    docker_mount_prefix = "/tmp/cls_tmp_mount/{cluster_name}"
    return docker_mount_prefix.format(cluster_name=cluster_name)
