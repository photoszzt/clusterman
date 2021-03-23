import copy
import hashlib
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from types import ModuleType
from typing import Any, Dict, List, Optional

import yaml

import clusterman.autoscaler._private.subprocess_output_util as cmd_output_util
from clusterman.autoscaler._private.cli_logger import cf, cli_logger
from clusterman.autoscaler._private.command_runner import set_rsync_silent, set_using_login_shells
from clusterman.autoscaler._private.event_system import CreateClusterEvent, global_event_system
from clusterman.autoscaler._private.log_timer import LogTimer
from clusterman.autoscaler._private.providers import _NODE_PROVIDERS, _PROVIDER_PRETTY_NAMES, _get_node_provider
from clusterman.autoscaler._private.updater import NodeUpdaterThread
from clusterman.autoscaler._private.util import hash_launch_conf, hash_runtime_conf, prepare_config, validate_config
from clusterman.autoscaler.node_provider import NodeProvider
from clusterman.autoscaler.tags import (
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    TAG_LAUNCH_CONFIG,
    TAG_NODE_KIND,
    TAG_NODE_NAME,
    TAG_NODE_STATUS
)
from clusterman.util.debug import log_once

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5


def try_logging_config(config: Dict[str, Any]) -> None:
    if config["provider"]["type"] == "aws":
        from clusterman.autoscaler._private.aws.config import log_to_cli
        log_to_cli(config)


def try_get_log_state(provider_config: Dict[str, Any]) -> Optional[dict]:
    if provider_config["type"] == "aws":
        from clusterman.autoscaler._private.aws.config import get_log_state
        return get_log_state()
    return None


def try_reload_log_state(provider_config: Dict[str, Any],
                         log_state: dict) -> None:
    if not log_state:
        return
    if provider_config["type"] == "aws":
        from clusterman.autoscaler._private.aws.config import reload_log_state
        return reload_log_state(log_state)


def create_nodes(config: Dict[str, Any],
                 yes: bool,
                 _provider: Optional[NodeProvider] = None,
                 _runner: ModuleType = subprocess) -> None:
    provider = (_provider or _get_node_provider(config["provider"],
                                                config["cluster_name"]))

    worker_filter = {TAG_NODE_KIND: NODE_KIND_WORKER}
    launch_config = copy.deepcopy(config["worker_nodes"])
    launch_hash = hash_launch_conf(launch_config, config["auth"])
    count = int(config["num_workers"])
    cli_logger.print("Launching {} nodes.".format(count))
    node_config = copy.deepcopy(config["worker_nodes"])
    node_tags = {
        TAG_NODE_NAME: "cls-{}-worker".format(config["cluster_name"]),
        TAG_NODE_KIND: NODE_KIND_WORKER,
        TAG_NODE_STATUS: STATUS_UNINITIALIZED,
        TAG_LAUNCH_CONFIG: launch_hash,
    }
    provider.create_node(node_config, node_tags, count)
    start = time.time()
    workers = []
    prev = start
    with cli_logger.group("Fetching the new worker node"):
        while True:
            nodes = provider.non_terminated_nodes(worker_filter)
            cur = time.time()
            if cur - prev > 50:
                prev = cur
            if len(nodes) >= count:
                workers = nodes
                break
            time.sleep(POLL_INTERVAL)
    cli_logger.newline()
    updaters = []
    (runtime_hash, file_mounts_contents_hash) = hash_runtime_conf(
        config["file_mounts"], None, config)
    for worker in workers:
        updater = NodeUpdaterThread(
            node_id=worker,
            provider_config=config["provider"],
            provider=provider,
            auth_config=config['auth'],
            cluster_name=config['cluster_name'],
            file_mounts=config['file_mounts'],
            initialization_commands=config["initialization_commands"],
            setup_commands=config['worker_setup_commands'],
            process_runner=_runner,
            runtime_hash=runtime_hash,
            is_head_node=False,
            file_mounts_contents_hash=file_mounts_contents_hash,
            rsync_options={
                "rsync_exclude": config.get("rsync_exclude"),
                "rsync_filter": config.get("rsync_filter")
            },
        )
        updater.start()
        updaters.append(updater)
    for up in updaters:
        up.join()
        provider.non_terminated_nodes(worker_filter)
        if up.exitcode != 0:
            cli_logger.abort("Fail to setup worker node. ")
            sys.exit(1)


def create_or_update_cluster(
    config_file: str,
    yes: bool,
    override_num_workers: Optional[int],
    override_cluster_name: Optional[str] = None,
    no_config_cache: bool = False,
    redirect_command_output: Optional[bool] = False,
    use_login_shells: bool = True,
):
    set_using_login_shells(use_login_shells)
    if not use_login_shells:
        cmd_output_util.set_allow_interactive(False)
    if redirect_command_output is None:
        # Do not redirect by default.
        cmd_output_util.set_output_redirected(False)
    else:
        cmd_output_util.set_output_redirected(redirect_command_output)

    def handle_yaml_error(e):
        cli_logger.error("Cluster config invalid")
        cli_logger.newline()
        cli_logger.error("Failed to load YAML file " + cf.bold("{}"),
                         config_file)
        cli_logger.newline()
        with cli_logger.verbatim_error_ctx("PyYAML error:"):
            cli_logger.error(e)
        cli_logger.abort()

    try:
        config = yaml.safe_load(open(config_file).read())
    except FileNotFoundError:
        cli_logger.abort(
            "Provided cluster configuration file ({}) does not exist",
            cf.bold(config_file))
        raise
    except yaml.parser.ParserError as e:
        handle_yaml_error(e)
        raise
    except yaml.scanner.ScannerError as e:
        handle_yaml_error(e)
        raise
    global_event_system.execute_callback(CreateClusterEvent.up_started,
                                         {"cluster_config": config})
    importer = _NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        cli_logger.abort(
            "Unknown provider type " + cf.bold("{}") + "\n"
            "Available providers are: {}", config["provider"]["type"],
            cli_logger.render_list([
                k for k in _NODE_PROVIDERS.keys()
                if _NODE_PROVIDERS[k] is not None
            ]))
        raise NotImplementedError("Unsupported provider {}".format(
            config["provider"]))

    printed_overrides = False

    def handle_cli_override(key, override):
        if override is not None:
            if key in config:
                nonlocal printed_overrides
                printed_overrides = True
                cli_logger.warning(
                    "`{}` override provided on the command line.\n"
                    "  Using " + cf.bold("{}") + cf.dimmed(
                        " [configuration file has " + cf.bold("{}") + "]"),
                    key, override, config[key])
            config[key] = override

    handle_cli_override("num_workers", override_num_workers)
    handle_cli_override("cluster_name", override_cluster_name)
    if printed_overrides:
        cli_logger.newline()

    cli_logger.labeled_value("Cluster", config["cluster_name"])

    cli_logger.newline()
    config = _bootstrap_config(config, no_config_cache=no_config_cache)

    try_logging_config(config)
    create_nodes(config, yes)
    return config


CONFIG_CACHE_VERSION = 1


def _bootstrap_config(config: Dict[str, Any],
                      no_config_cache: bool = False) -> Dict[str, Any]:
    config = prepare_config(config)

    hasher = hashlib.sha1()
    hasher.update(json.dumps([config], sort_keys=True).encode("utf-8"))
    cache_key = os.path.join(tempfile.gettempdir(),
                             "ray-config-{}".format(hasher.hexdigest()))

    if os.path.exists(cache_key) and not no_config_cache:
        config_cache = json.loads(open(cache_key).read())
        if config_cache.get("_version", -1) == CONFIG_CACHE_VERSION:
            # todo: is it fine to re-resolve? afaik it should be.
            # we can have migrations otherwise or something
            # but this seems overcomplicated given that resolving is
            # relatively cheap
            try_reload_log_state(config_cache["config"]["provider"],
                                 config_cache.get("provider_log_info"))

            if log_once("_printed_cached_config_warning"):
                cli_logger.verbose_warning(
                    "Loaded cached provider configuration "
                    "from " + cf.bold("{}"), cache_key)
                if cli_logger.verbosity == 0:
                    cli_logger.warning("Loaded cached provider configuration")
                cli_logger.warning(
                    "If you experience issues with "
                    "the cloud provider, try re-running "
                    "the command with {}.", cf.bold("--no-config-cache"))

            return config_cache["config"]
        else:
            cli_logger.warning(
                "Found cached cluster config "
                "but the version " + cf.bold("{}") + " "
                "(expected " + cf.bold("{}") + ") does not match.\n"
                "This is normal if cluster launcher was updated.\n"
                "Config will be re-resolved.",
                config_cache.get("_version", "none"), CONFIG_CACHE_VERSION)

    importer = _NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        raise NotImplementedError("Unsupported provider {}".format(
            config["provider"]))

    provider_cls = importer(config["provider"])

    cli_logger.print("Checking {} environment settings",
                     _PROVIDER_PRETTY_NAMES.get(config["provider"]["type"]))
    try:
        config = provider_cls.fillout_available_node_types_resources(config)
    except Exception as exc:
        if cli_logger.verbosity > 2:
            logger.exception("Failed to autodetect node resources.")
        else:
            cli_logger.warning(
                f"Failed to autodetect node resources: {str(exc)}. "
                "You can see full stack trace with higher verbosity.")

    # NOTE: if `resources` field is missing, validate_config for providers
    # other than AWS and Kubernetes will fail (the schema error will ask the
    # user to manually fill the resources) as we currently support autofilling
    # resources for AWS and Kubernetes only.
    validate_config(config)
    resolved_config = provider_cls.bootstrap_config(config)

    if not no_config_cache:
        with open(cache_key, "w") as f:
            config_cache = {
                "_version": CONFIG_CACHE_VERSION,
                "provider_log_info": try_get_log_state(config["provider"]),
                "config": resolved_config
            }
            f.write(json.dumps(config_cache))
    return resolved_config


def teardown_cluster(config_file: str, yes: bool,
                     override_cluster_name: Optional[str]) -> None:
    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    config = _bootstrap_config(config)

    cli_logger.confirm(yes, "Destroying cluster.", _abort=True)

    provider = _get_node_provider(config["provider"], config["cluster_name"])

    A = provider.non_terminated_nodes({
        TAG_NODE_KIND: NODE_KIND_WORKER
    })
    with LogTimer("teardown_cluster: done."):
        while A:
            provider.terminate_nodes(A)

            cli_logger.print(
                "Requested {} nodes to shut down.",
                cf.bold(len(A)),
                _tags=dict(interval="1s"))

            time.sleep(POLL_INTERVAL)  # todo: interval should be a variable
            A = provider.non_terminated_nodes({
                TAG_NODE_KIND: NODE_KIND_WORKER
            })
            cli_logger.print("{} nodes remaining after {} second(s).",
                             cf.bold(len(A)), POLL_INTERVAL)
        cli_logger.success("No nodes remaining.")


def rsync(config_file: str,
          source: Optional[str],
          target: Optional[str],
          override_cluster_name: Optional[str],
          down: bool,
          ip_address: Optional[str] = None,
          use_internal_ip: bool = False,
          no_config_cache: bool = False,
          all_nodes: bool = False,
          _runner: ModuleType = subprocess) -> None:
    """Rsyncs files.

    Arguments:
        config_file: path to the cluster yaml
        source: source dir
        target: target dir
        override_cluster_name: set the name of the cluster
        down: whether we're syncing remote -> local
        ip_address (str): Address of node. Raise Exception
            if both ip_address and 'all_nodes' are provided.
        use_internal_ip (bool): Whether the provided ip_address is
            public or private.
        all_nodes: whether to sync worker nodes in addition to the head node
    """
    if bool(source) != bool(target):
        cli_logger.abort(
            "Expected either both a source and a target, or neither.")

    assert bool(source) == bool(target), (
        "Must either provide both or neither source and target.")

    if ip_address and all_nodes:
        cli_logger.abort("Cannot provide both ip_address and 'all_nodes'.")

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config, no_config_cache=no_config_cache)

    is_file_mount = False
    if source and target:
        for remote_mount in config.get("file_mounts", {}).keys():
            if (source if down else target).startswith(remote_mount):
                is_file_mount = True
                break

    provider = _get_node_provider(config["provider"], config["cluster_name"])

    def rsync_to_node(node_id, is_head_node):
        updater = NodeUpdaterThread(
            node_id=node_id,
            provider_config=config["provider"],
            provider=provider,
            auth_config=config["auth"],
            cluster_name=config["cluster_name"],
            file_mounts=config["file_mounts"],
            initialization_commands=[],
            setup_commands=[],
            ray_start_commands=[],
            runtime_hash="",
            use_internal_ip=use_internal_ip,
            process_runner=_runner,
            file_mounts_contents_hash="",
            is_head_node=is_head_node,
            rsync_options={
                "rsync_exclude": config.get("rsync_exclude"),
                "rsync_filter": config.get("rsync_filter")
            },
            docker_config=config.get("docker"))
        if down:
            rsync = updater.rsync_down
        else:
            rsync = updater.rsync_up

        if source and target:
            # print rsync progress for single file rsync
            if cli_logger.verbosity > 0:
                cmd_output_util.set_output_redirected(False)
                set_rsync_silent(False)
            rsync(source, target, is_file_mount)
        else:
            updater.sync_file_mounts(rsync)

    nodes = _get_worker_nodes(config, override_cluster_name)

    for node_id in nodes:
        rsync_to_node(node_id, is_head_node=False)


def get_worker_node_ips(config_file: str,
                        override_cluster_name: Optional[str] = None
                        ) -> List[str]:
    """Returns worker node IPs for given configuration file."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    nodes = provider.non_terminated_nodes({
        TAG_NODE_KIND: NODE_KIND_WORKER
    })

    if config.get("provider", {}).get("use_internal_ips", False) is True:
        return [provider.internal_ip(node) for node in nodes]
    else:
        return [provider.external_ip(node) for node in nodes]


def _get_worker_nodes(config: Dict[str, Any],
                      override_cluster_name: Optional[str]) -> List[str]:
    """Returns worker node ids for given configuration."""
    # todo: technically could be reused in get_worker_node_ips
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    return provider.non_terminated_nodes({TAG_NODE_KIND: NODE_KIND_WORKER})
