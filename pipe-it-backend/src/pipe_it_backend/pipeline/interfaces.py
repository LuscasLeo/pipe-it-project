import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)

PAYLOAD_T = TypeVar("PAYLOAD_T")
PIPELINE_T = TypeVar("PIPELINE_T")
EVENT_T = TypeVar("EVENT_T")


class Executor(Generic[PIPELINE_T, PAYLOAD_T, EVENT_T], ABC):
    @abstractmethod
    def execute(self, pipeline_item: PIPELINE_T, payload: PAYLOAD_T) -> list[EVENT_T]:
        raise NotImplementedError


class EventHandler(Generic[EVENT_T], ABC):
    @abstractmethod
    def handle(self, event: EVENT_T) -> None:
        raise NotImplementedError


class PipelineUsecase(Generic[PIPELINE_T, PAYLOAD_T, EVENT_T]):
    def __init__(
        self,
        executor: Executor[PIPELINE_T, PAYLOAD_T, EVENT_T],
        event_handler: EventHandler[EVENT_T],
    ):
        self.executor = executor
        self.event_handler = event_handler

    def execute_pipeline_item(
        self,
        pipeline_item: PIPELINE_T,
        payload: PAYLOAD_T,
    ) -> None:
        for event in self.executor.execute(pipeline_item, payload):
            self.event_handler.handle(event)
