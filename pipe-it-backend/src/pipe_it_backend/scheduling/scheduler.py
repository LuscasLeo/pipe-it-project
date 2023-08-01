from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
from typing import Generic, NoReturn, TypeVar
import croniter


@dataclass
class Task:
    name: str
    schedule: str


TASK_T = TypeVar("TASK_T")


class ScheduleProvider(Generic[TASK_T], ABC):
    @abstractmethod
    def run_task(self, task: TASK_T) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_pending_tasks(
        self, last_tick: datetime, current_tick: datetime
    ) -> list[TASK_T]:
        raise NotImplementedError


def on_tick(
    last_tick: datetime,
    current_tick: datetime,
    schedule_provider: ScheduleProvider[TASK_T],
):
    for task in schedule_provider.get_pending_tasks(
        last_tick=last_tick, current_tick=current_tick
    ):
        schedule_provider.run_task(task)


class ScheduleProviderImpl(ScheduleProvider):
    def __init__(self) -> None:
        self.tasks: dict[str, Task] = {
            # "task1": Task(name="task1", schedule="* * * * * *"),
            "task2": Task(name="task2", schedule="* * * * * 30-59"),
        }

        self.last_tasks_tick: dict[str, datetime] = {}

    def run_task(self, task: Task) -> None:
        print(f"Running task {task.name} at {datetime.now().strftime('%H:%M:%S')}")
        self.last_tasks_tick[task.name] = datetime.now()

    def get_pending_tasks(
        self, last_tick: datetime, current_tick: datetime
    ) -> list[Task]:
        return [
            task
            for task in self.tasks.values()
            if self._is_task_pending(task, last_tick, current_tick)
        ]

    def _is_task_pending(
        self, task: Task, last_tick: datetime, current_tick: datetime
    ) -> bool:
        last_task_tick = self.last_tasks_tick.get(task.name)
        if last_task_tick is None:
            last_task_tick = last_tick
            self.last_tasks_tick[task.name] = last_task_tick

        cron = croniter.croniter(task.schedule, last_task_tick)
        task_next_tick: datetime = cron.get_next(datetime)

        return task_next_tick < current_tick


def run_forever() -> NoReturn:
    schedule_provider = ScheduleProviderImpl()

    last_tick = datetime.now().replace(microsecond=0)
    current_tick = last_tick + timedelta(seconds=1)

    while True:
        on_tick(
            last_tick=last_tick,
            current_tick=current_tick,
            schedule_provider=schedule_provider,
        )
        last_tick = current_tick
        current_tick = (datetime.now()).replace(microsecond=0) + timedelta(seconds=2)
        time.sleep(1)
