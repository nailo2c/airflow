# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import types
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from airflow.exceptions import AirflowException
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions._internal.setup_teardown import SetupTeardownContext
from airflow.sdk.definitions.decorators.task_group import _TaskGroupFactory

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import _TaskDecorator
    from airflow.sdk.definitions.xcom_arg import XComArg

try:
    from airflow.providers.standard.decorators.python import python_task
except (ImportError, AttributeError):
    from airflow.decorators import python_task  # type: ignore


def setup_task(func: Callable) -> Callable:
    """
    Decorate a function to mark it as a setup task.

    A setup task runs before all other tasks in its DAG or TaskGroup context
    and can perform initialization or resource preparation.

    Example::

        @setup
        def initialize_context(...):
            ...
    """
    # Using FunctionType here since _TaskDecorator is also a callable
    if isinstance(func, types.FunctionType):
        func = python_task(func)
    if isinstance(func, _TaskGroupFactory):
        raise AirflowException("Task groups cannot be marked as setup or teardown.")
    func = cast("_TaskDecorator", func)
    func.is_setup = True
    return func


def teardown_task(_func=None, *, on_failure_fail_dagrun: bool = False) -> Callable:
    """
    Decorate a function to mark it as a teardown task.

    A teardown task runs after all main tasks in its DAG or TaskGroup context.
    If ``on_failure_fail_dagrun=True``, a failure in teardown will mark the DAG run as failed.

    Example::

        @teardown(on_failure_fail_dagrun=True)
        def cleanup(...):
            ...
    """

    def teardown(func: Callable) -> Callable:
        # Using FunctionType here since _TaskDecorator is also a callable
        if isinstance(func, types.FunctionType):
            func = python_task(func)
        if isinstance(func, _TaskGroupFactory):
            raise AirflowException("Task groups cannot be marked as setup or teardown.")
        func = cast("_TaskDecorator", func)

        func.is_teardown = True
        func.on_failure_fail_dagrun = on_failure_fail_dagrun
        return func

    if _func is None:
        return teardown
    return teardown(_func)


class ContextWrapper(list):
    """A list subclass that has a context manager that pushes setup/teardown tasks to the context."""

    def __init__(self, tasks: list[BaseOperator | XComArg]):
        self.tasks = tasks
        super().__init__(tasks)

    def __enter__(self):
        operators = []
        for task in self.tasks:
            if isinstance(task, BaseOperator):
                operators.append(task)
                if not task.is_setup and not task.is_teardown:
                    raise AirflowException("Only setup/teardown tasks can be used as context managers.")
            elif not task.operator.is_setup and not task.operator.is_teardown:
                raise AirflowException("Only setup/teardown tasks can be used as context managers.")
        if not operators:
            # means we have XComArgs
            operators = [task.operator for task in self.tasks]
        SetupTeardownContext.push_setup_teardown_task(operators)
        return SetupTeardownContext

    def __exit__(self, exc_type, exc_val, exc_tb):
        SetupTeardownContext.set_work_task_roots_and_leaves()


context_wrapper = ContextWrapper
