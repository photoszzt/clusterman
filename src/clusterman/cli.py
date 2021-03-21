"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mclusterman` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``clusterman.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``clusterman.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import copy
import logging
import urllib

import click

from clusterman.autoscaler._private.cli_logger import cli_logger
from clusterman.autoscaler._private.commands import create_or_update_cluster, teardown_cluster
from clusterman.autoscaler._private.constants import LOGGER_FORMAT, LOGGER_FORMAT_HELP, LOGGER_LEVEL, LOGGER_LEVEL_HELP
from clusterman.cluster_logging import setup_logger

logger = logging.getLogger(__name__)

logging_options = [
    click.option(
        "--log-style",
        required=False,
        type=click.Choice(cli_logger.VALID_LOG_STYLES, case_sensitive=False),
        default="auto",
        help=("If 'pretty', outputs with formatting and color. If 'record', "
              "outputs record-style without formatting. "
              "'auto' defaults to 'pretty', and disables pretty logging "
              "if stdin is *not* a TTY.")),
    click.option(
        "--log-color",
        required=False,
        type=click.Choice(["auto", "false", "true"], case_sensitive=False),
        default="auto",
        help=("Use color logging. "
              "Auto enables color logging if stdout is a TTY.")),
    click.option("-v", "--verbose", default=None, count=True)
]


def add_click_options(options):
    def wrapper(f):
        for option in reversed(logging_options):
            f = option(f)
        return f

    return wrapper


@click.group()
@click.option(
    "--logging-level",
    required=False,
    default=LOGGER_LEVEL,
    type=str,
    help=LOGGER_LEVEL_HELP)
@click.option(
    "--logging-format",
    required=False,
    default=LOGGER_FORMAT,
    type=str,
    help=LOGGER_FORMAT_HELP)
@click.version_option()
def cli(logging_level, logging_format):
    level = logging.getLevelName(logging_level.upper())
    setup_logger(level, logging_format)
    cli_logger.set_format(format_tmpl=logging_format)


def add_command_alias(command, name, hidden):
    new_command = copy.deepcopy(command)
    new_command.hidden = hidden
    cli.add_command(new_command, name=name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--num-workers",
    required=False,
    type=int,
    help="Override the configured min worker node count for the cluster.")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--redirect-command-output",
    is_flag=True,
    default=False,
    help="Whether to redirect command output to a file.")
@click.option(
    "--use-login-shells/--use-normal-shells",
    is_flag=True,
    default=True,
    help=("Clusterman uses login shells (bash --login -i) to run cluster commands "
          "by default. If your workflow is compatible with normal shells, "
          "this can be disabled for a better user experience."))
@add_click_options(logging_options)
def up(cluster_config_file, num_workers,
       yes, cluster_name, no_config_cache, redirect_command_output,
       use_login_shells, log_style, log_color, verbose):
    """Create or update a cluster."""
    cli_logger.configure(log_style, log_color, verbose)

    if urllib.parse.urlparse(cluster_config_file).scheme in ("http", "https"):
        try:
            response = urllib.request.urlopen(cluster_config_file, timeout=5)
            content = response.read()
            file_name = cluster_config_file.split("/")[-1]
            with open(file_name, "wb") as f:
                f.write(content)
            cluster_config_file = file_name
        except urllib.error.HTTPError as e:
            cli_logger.warning("{}", str(e))
            cli_logger.warning(
                "Could not download remote cluster configuration file.")
    create_or_update_cluster(
        config_file=cluster_config_file,
        override_num_workers=num_workers,
        yes=yes,
        override_cluster_name=cluster_name,
        no_config_cache=no_config_cache,
        redirect_command_output=redirect_command_output,
        use_login_shells=use_login_shells)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@add_click_options(logging_options)
def down(cluster_config_file, yes, cluster_name,
         log_style, log_color, verbose):
    """Tear down a Ray cluster."""
    cli_logger.configure(log_style, log_color, verbose)

    teardown_cluster(cluster_config_file, yes, cluster_name)


cli.add_command(up)
cli.add_command(down)


def main():
    return cli()
