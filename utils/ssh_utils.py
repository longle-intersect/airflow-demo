from typing import Optional, Any, Dict

from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from select import select

def execute_command_for_operator(operator: BaseOperator,
                                 ssh_hook: Optional[SSHHook] = None,
                                 command: Optional[str] = None) -> Any:
    """
    Utility method that runs a command over SSH
    for an operator.

    Although operator can be of any type if ssh_hook and
    command params are not provided, it is assumed to be
    an SSHOperator and the said params are read from it

    :param operator: An operator for which command is to
                     to be run remotely over SSHHook
    :type operator:  BaseOperator or SSHOperator
    :param ssh_hook: Optional SSHHook over which command
                     is to be run remotely
    :type ssh_hook:  Optional[SSHHook]
    :param command:  Optional command to be run remotely
    :type command:   Optional[str]
    :return:         Aggregated stdout messages of running command
    :type:           Optional[str] (byte-string)
    """

    if not ssh_hook:
        ssh_hook: SSHHook = operator.ssh_hook
    if not command:
        command: str = operator.command

    operator.log.info(f"Executing command:-\n{command}")
    with ssh_hook.get_conn() as ssh_client:
        """
        Code borrowed from
         - airflow.contrib.operators.SSHOperator.execute() method
         - airflow.contrib.operators.SFTPOperator.execute() method
        """

        # execute command over SSH
        stdin, stdout, stderr = ssh_client.exec_command(command=command)

        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        # byte-strings to hold aggregated output / error
        agg_stdout = b""
        agg_stderr = b""

        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        # read from both stdout and stderr
        while not channel.closed or \
                channel.recv_ready() or \
                channel.recv_stderr_ready():
            timeout: Optional[int] = operator.timeout if hasattr(operator, "timeout") else None
            readq, _, _ = select([channel], [], [], timeout)
            for c in readq:
                if c.recv_ready():
                    line = stdout.channel.recv(len(c.in_buffer))
                    line = line
                    agg_stdout += line
                    operator.log.info(line.decode("utf-8").strip("\n"))
                if c.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                    line = line
                    agg_stderr += line
                    operator.log.warning(line.decode("utf-8").strip("\n"))
            if stdout.channel.exit_status_ready() \
                    and not stderr.channel.recv_stderr_ready() \
                    and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()

        exit_status = stdout.channel.recv_exit_status()
        if exit_status is 0:
            # returning output if do_xcom_push is set
            if hasattr(operator, "do_xcom_push") and operator.do_xcom_push:
                # pickle the output (assume core.xcom_pickling is enabled)
                return agg_stdout

        else:
            error_msg = agg_stderr.decode("utf-8")
            raise AirflowException("error running cmd: {0}, error: {1}"
                                   .format(command, error_msg))
