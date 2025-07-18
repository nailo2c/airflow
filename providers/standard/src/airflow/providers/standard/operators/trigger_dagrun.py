#
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

import datetime
import json
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound

from airflow.api.common.trigger_dag import trigger_dag
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowSkipException,
    DagNotFound,
    DagRunAlreadyExists,
)
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.providers.standard.triggers.external_task import DagStateTrigger
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS, BaseOperator, BaseOperatorLink
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import NOTSET, ArgNotSet, DagRunType

XCOM_LOGICAL_DATE_ISO = "trigger_logical_date_iso"
XCOM_RUN_ID = "trigger_run_id"


if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.taskinstancekey import TaskInstanceKey

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.xcom import XCom
else:
    from airflow.models import XCom


class DagIsPaused(AirflowException):
    """Raise when a dag is paused and something tries to run it."""

    def __init__(self, dag_id: str) -> None:
        super().__init__(dag_id)
        self.dag_id = dag_id

    def __str__(self) -> str:
        return f"Dag {self.dag_id} is paused"


class TriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for TriggerDagRunOperator.

    It allows users to access DAG triggered by task using TriggerDagRunOperator.
    """

    name = "Triggered DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        if TYPE_CHECKING:
            assert isinstance(operator, TriggerDagRunOperator)

        trigger_dag_id = operator.trigger_dag_id
        if not AIRFLOW_V_3_0_PLUS:
            from airflow.models.renderedtifields import RenderedTaskInstanceFields

            if template_fields := RenderedTaskInstanceFields.get_templated_fields(ti_key):
                trigger_dag_id: str = template_fields.get("trigger_dag_id", operator.trigger_dag_id)  # type: ignore[no-redef]

        # Fetch the correct dag_run_id for the triggerED dag which is
        # stored in xcom during execution of the triggerING task.
        triggered_dag_run_id = XCom.get_value(ti_key=ti_key, key=XCOM_RUN_ID)

        if AIRFLOW_V_3_0_PLUS:
            from airflow.utils.helpers import build_airflow_dagrun_url

            return build_airflow_dagrun_url(dag_id=trigger_dag_id, run_id=triggered_dag_run_id)
        from airflow.utils.helpers import build_airflow_url_with_query  # type:ignore[attr-defined]

        query = {"dag_id": trigger_dag_id, "dag_run_id": triggered_dag_run_id}
        return build_airflow_url_with_query(query)


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified DAG ID.

    Note that if database isolation mode is enabled, not all features are supported.

    :param trigger_dag_id: The ``dag_id`` of the DAG to trigger (templated).
    :param trigger_run_id: The run ID to use for the triggered DAG run (templated).
        If not provided, a run ID will be automatically generated.
    :param conf: Configuration for the DAG run (templated).
    :param logical_date: Logical date for the triggered DAG (templated).
    :param reset_dag_run: Whether clear existing DAG run if already exists.
        This is useful when backfill or rerun an existing DAG run.
        This only resets (not recreates) the DAG run.
        DAG run conf is immutable and will not be reset on rerun of an existing DAG run.
        When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
        When reset_dag_run=True and dag run exists, existing DAG run will be cleared to rerun.
    :param wait_for_completion: Whether or not wait for DAG run completion. (default: False)
    :param poke_interval: Poke interval to check DAG run status when wait_for_completion=True.
        (default: 60)
    :param allowed_states: Optional list of allowed DAG run states of the triggered DAG. This is useful when
        setting ``wait_for_completion`` to True. Must be a valid DagRunState.
        Default is ``[DagRunState.SUCCESS]``.
    :param failed_states: Optional list of failed or disallowed DAG run states of the triggered DAG. This is
        useful when setting ``wait_for_completion`` to True. Must be a valid DagRunState.
        Default is ``[DagRunState.FAILED]``.
    :param skip_when_already_exists: Set to true to mark the task as SKIPPED if a DAG run of the triggered
        DAG for the same logical date already exists.
    :param fail_when_dag_is_paused: If the dag to trigger is paused, DagIsPaused will be raised.
    :param deferrable: If waiting for completion, whether or not to defer the task until done,
        default is ``False``.
    """

    template_fields: Sequence[str] = (
        "trigger_dag_id",
        "trigger_run_id",
        "logical_date",
        "conf",
        "wait_for_completion",
        "skip_when_already_exists",
    )
    template_fields_renderers = {"conf": "py"}
    ui_color = "#ffefeb"
    operator_extra_links = [TriggerDagRunLink()]

    def __init__(
        self,
        *,
        trigger_dag_id: str,
        trigger_run_id: str | None = None,
        conf: dict | None = None,
        logical_date: str | datetime.datetime | None | ArgNotSet = NOTSET,
        reset_dag_run: bool = False,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        allowed_states: list[str | DagRunState] | None = None,
        failed_states: list[str | DagRunState] | None = None,
        skip_when_already_exists: bool = False,
        fail_when_dag_is_paused: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.trigger_run_id = trigger_run_id
        self.conf = conf
        self.reset_dag_run = reset_dag_run
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        if allowed_states:
            self.allowed_states = [DagRunState(s) for s in allowed_states]
        else:
            self.allowed_states = [DagRunState.SUCCESS]
        if failed_states is not None:
            self.failed_states = [DagRunState(s) for s in failed_states]
        else:
            self.failed_states = [DagRunState.FAILED]
        self.skip_when_already_exists = skip_when_already_exists
        self.fail_when_dag_is_paused = fail_when_dag_is_paused
        self._defer = deferrable
        self.logical_date = logical_date
        if logical_date is NOTSET:
            self.logical_date = NOTSET
        elif logical_date is None or isinstance(logical_date, (str, datetime.datetime)):
            self.logical_date = logical_date
        else:
            raise TypeError(
                f"Expected str, datetime.datetime, or None for parameter 'logical_date'. Got {type(logical_date).__name__}"
            )

    def execute(self, context: Context):
        if self.logical_date is NOTSET:
            # If no logical_date is provided we will set utcnow()
            parsed_logical_date = timezone.utcnow()
        elif self.logical_date is None or isinstance(self.logical_date, datetime.datetime):
            parsed_logical_date = self.logical_date  # type: ignore
        elif isinstance(self.logical_date, str):
            parsed_logical_date = timezone.parse(self.logical_date)

        try:
            json.dumps(self.conf)
        except TypeError:
            raise ValueError("conf parameter should be JSON Serializable")

        if self.trigger_run_id:
            run_id = str(self.trigger_run_id)
        else:
            if AIRFLOW_V_3_0_PLUS:
                run_id = DagRun.generate_run_id(
                    run_type=DagRunType.MANUAL,
                    logical_date=parsed_logical_date,
                    run_after=parsed_logical_date or timezone.utcnow(),
                )
            else:
                run_id = DagRun.generate_run_id(DagRunType.MANUAL, parsed_logical_date or timezone.utcnow())  # type: ignore[misc,call-arg]

        if self.fail_when_dag_is_paused:
            dag_model = DagModel.get_current(self.trigger_dag_id)
            if dag_model.is_paused:
                if AIRFLOW_V_3_0_PLUS:
                    raise DagIsPaused(dag_id=self.trigger_dag_id)
                raise AirflowException(f"Dag {self.trigger_dag_id} is paused")

        if AIRFLOW_V_3_0_PLUS:
            self._trigger_dag_af_3(context=context, run_id=run_id, parsed_logical_date=parsed_logical_date)
        else:
            self._trigger_dag_af_2(context=context, run_id=run_id, parsed_logical_date=parsed_logical_date)

    def _trigger_dag_af_3(self, context, run_id, parsed_logical_date):
        from airflow.exceptions import DagRunTriggerException

        raise DagRunTriggerException(
            trigger_dag_id=self.trigger_dag_id,
            dag_run_id=run_id,
            conf=self.conf,
            logical_date=parsed_logical_date,
            reset_dag_run=self.reset_dag_run,
            skip_when_already_exists=self.skip_when_already_exists,
            wait_for_completion=self.wait_for_completion,
            allowed_states=self.allowed_states,
            failed_states=self.failed_states,
            poke_interval=self.poke_interval,
            deferrable=self._defer,
        )

    def _trigger_dag_af_2(self, context, run_id, parsed_logical_date):
        try:
            dag_run = trigger_dag(
                dag_id=self.trigger_dag_id,
                run_id=run_id,
                conf=self.conf,
                execution_date=parsed_logical_date,
                replace_microseconds=False,
            )

        except DagRunAlreadyExists as e:
            if self.reset_dag_run:
                dag_run = e.dag_run
                self.log.info("Clearing %s on %s", self.trigger_dag_id, dag_run.run_id)

                # Get target dag object and call clear()
                dag_model = DagModel.get_current(self.trigger_dag_id)
                if dag_model is None:
                    raise DagNotFound(f"Dag id {self.trigger_dag_id} not found in DagModel")

                # Note: here execution fails on database isolation mode. Needs structural changes for AIP-72
                dag_bag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
                dag = dag_bag.get_dag(self.trigger_dag_id)
                dag.clear(start_date=dag_run.logical_date, end_date=dag_run.logical_date)
            else:
                if self.skip_when_already_exists:
                    raise AirflowSkipException(
                        "Skipping due to skip_when_already_exists is set to True and DagRunAlreadyExists"
                    )
                raise e
        if dag_run is None:
            raise RuntimeError("The dag_run should be set here!")
        # Store the run id from the dag run (either created or found above) to
        # be used when creating the extra link on the webserver.
        ti = context["task_instance"]
        ti.xcom_push(key=XCOM_RUN_ID, value=dag_run.run_id)

        if self.wait_for_completion:
            # Kick off the deferral process
            if self._defer:
                self.defer(
                    trigger=DagStateTrigger(
                        dag_id=self.trigger_dag_id,
                        states=self.allowed_states + self.failed_states,
                        execution_dates=[dag_run.logical_date],
                        run_ids=[run_id],
                        poll_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )
            # wait for dag to complete
            while True:
                self.log.info(
                    "Waiting for %s on %s to become allowed state %s ...",
                    self.trigger_dag_id,
                    run_id,
                    self.allowed_states,
                )
                time.sleep(self.poke_interval)

                # Note: here execution fails on database isolation mode. Needs structural changes for AIP-72
                dag_run.refresh_from_db()
                state = dag_run.state
                if state in self.failed_states:
                    raise AirflowException(f"{self.trigger_dag_id} failed with failed states {state}")
                if state in self.allowed_states:
                    self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
                    return

    def execute_complete(self, context: Context, event: tuple[str, dict[str, Any]]):
        if AIRFLOW_V_3_0_PLUS:
            self._trigger_dag_run_af_3_execute_complete(event=event)
        else:
            self._trigger_dag_run_af_2_execute_complete(event=event)

    def _trigger_dag_run_af_3_execute_complete(self, event: tuple[str, dict[str, Any]]):
        run_ids = event[1]["run_ids"]
        event_data = event[1]
        failed_run_id_conditions = []

        for run_id in run_ids:
            state = event_data.get(run_id)
            if state in self.failed_states:
                failed_run_id_conditions.append(run_id)
                continue
            if state in self.allowed_states:
                self.log.info(
                    "%s finished with allowed state %s for run_id %s",
                    self.trigger_dag_id,
                    state,
                    run_id,
                )

        if failed_run_id_conditions:
            raise AirflowException(
                f"{self.trigger_dag_id} failed with failed states {self.failed_states} for run_ids"
                f" {failed_run_id_conditions}"
            )

    if not AIRFLOW_V_3_0_PLUS:
        from airflow.utils.session import NEW_SESSION, provide_session  # type: ignore[misc]

        @provide_session
        def _trigger_dag_run_af_2_execute_complete(
            self, event: tuple[str, dict[str, Any]], session: Session = NEW_SESSION
        ):
            # This logical_date is parsed from the return trigger event
            provided_logical_date = event[1]["execution_dates"][0]
            try:
                # Note: here execution fails on database isolation mode. Needs structural changes for AIP-72
                dag_run = session.execute(
                    select(DagRun).where(
                        DagRun.dag_id == self.trigger_dag_id, DagRun.execution_date == provided_logical_date
                    )
                ).scalar_one()
            except NoResultFound:
                raise AirflowException(
                    f"No DAG run found for DAG {self.trigger_dag_id} and logical date {self.logical_date}"
                )

            state = dag_run.state

            if state in self.failed_states:
                raise AirflowException(f"{self.trigger_dag_id} failed with failed state {state}")
            if state in self.allowed_states:
                self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
                return

            raise AirflowException(
                f"{self.trigger_dag_id} return {state} which is not in {self.failed_states}"
                f" or {self.allowed_states}"
            )
