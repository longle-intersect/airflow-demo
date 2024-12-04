from typing import List, Optional, Dict, Any

from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.exceptions import AirflowException
from airflow.utils import apply_defaults

import ssh_utils


class MultiCmdSSHOperator(SSHOperator):
    """
    An extension of SSHOperator that executes a list of commands
    (rather that just a single command)

    If optional list of commands is not provided in __init__
    it must be supplied somehow before execute(..) method
    (for example in pre_execute(..) method) otherwise
    exception will be thrown

    :param commands:      Optional list of commands to be executed
    :type commands:       Optional[List[str]]
    :param ssh_hook_args: Optional dictionary of arguments for SSHHook
    :type ssh_hook_args:  Optional[SSHHook]
    """

    @apply_defaults
    def __init__(self,
                 commands: Optional[List[str]] = None,
                 ssh_hook_args: Optional[Dict[str, Any]] = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands: Optional[List[str]] = (
            commands if isinstance(commands, List) else list(commands)) if commands else None
        self.ssh_hook_args: Optional[Dict[str, Any]] = ssh_hook_args

    def execute(self, context) -> Optional[List[Dict[str, Optional[str]]]]:
        if not self.commands:
            raise AirflowException("no commands specified so nothing to execute here.")
        else:
            # instantiate SSHHook
            self.ssh_hook: SSHHook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            # create a list for holding all return values of commands run over SSH
            xcoms_list: List[Dict[str, Optional[str]]] = []
            
            for cmd in self.commands:
                # just for consistency (not required)
                self.command: str = cmd

                # run command and gather its output in dictionary
                output: Optional[str] = ssh_utils.execute_command_for_operator(operator=self, command=cmd)
                xcoms_list.append({
                    "command": cmd,
                    "output": output
                })

            if self.do_xcom_push:
                return xcoms_list