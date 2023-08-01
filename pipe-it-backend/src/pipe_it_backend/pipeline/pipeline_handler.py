from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Optional, TypeVar
from uuid import UUID, uuid4

from pipe_it_backend.pipeline.interfaces import (
    EventHandler,
    Executor,
    PipelineUsecase,
)


JSON_OBJ = dict[str, Any]

EVENT_REFERENCE = str

ACTION = str


@dataclass
class PipelineNodeEventData:
    name: str
    payload: JSON_OBJ
    pipeline_name: str
    pipeline_node_identifier: str


@dataclass
class PipelineNode:
    identifier: str
    action: ACTION
    fork_events: dict[EVENT_REFERENCE, list["PipelineNode"]] = field(
        default_factory=dict
    )
    static_params: JSON_OBJ = field(default_factory=dict)


@dataclass
class Pipeline:
    name: str
    trigger_event: str
    dispatch_events: list[PipelineNode]

    def __post_init__(self) -> None:
        self.nodes_map: dict[str, PipelineNode] = self.__recursive_nodes_map()

    def __recursive_nodes_map(self) -> dict[str, PipelineNode]:
        nodes_map = {}

        def _recursive_nodes_map(node: PipelineNode):
            nodes_map[node.identifier] = node
            for _, nodes in node.fork_events.items():
                for node in nodes:
                    _recursive_nodes_map(node)

        for node in self.dispatch_events:
            _recursive_nodes_map(node)

        return nodes_map

    def get_node(self, identifier: str) -> PipelineNode:
        if identifier not in self.nodes_map:
            raise Exception(f"Node {identifier} not found in pipeline {self.name}")

        return self.nodes_map[identifier]


update_weather_every_morning = Pipeline(
    name="update_weather_every_morning",
    trigger_event="cron_timer",
    dispatch_events=[
        PipelineNode(
            identifier="update_weather_node",
            action="update_weather",
            static_params={"time": "morning"},
            fork_events={
                "weather_updated": [
                    PipelineNode(
                        identifier="send_weather_to_subscribers",
                        action="send_weather",
                        fork_events={
                            "discord_message_sent": [
                                PipelineNode(
                                    identifier="send_weather_update_success_to_subscribers",
                                    action="print_message",
                                    static_params={"message": "Wather update success"},
                                )
                            ]
                        },
                    )
                ],
                "weather_update_failed": [
                    PipelineNode(
                        identifier="send_weather_update_failed_to_subscribers",
                        action="send_weather_update_failed",
                        fork_events={},
                    )
                ],
            },
        )
    ],
)


# region Event Collector
@dataclass
class EventCollector:
    staging_events: list[PipelineNodeEventData] = field(
        default_factory=list, init=False
    )
    pipeline: Pipeline
    pipeline_node: PipelineNode


events_collector_ctxvar = ContextVar[Optional[EventCollector]](
    "events_collector_ctxvar", default=None
)


@contextmanager
def open_events_collector_scope(collector: EventCollector):
    token = events_collector_ctxvar.set(collector)
    try:
        yield collector
    except:
        raise
    finally:
        events_collector_ctxvar.reset(token)


def dispatch_event(event: str, payload: JSON_OBJ):
    current_collector = events_collector_ctxvar.get()
    if current_collector is None:
        raise Exception("No event collector")

    current_collector.staging_events.append(
        PipelineNodeEventData(
            name=event,
            payload=payload,
            pipeline_name=current_collector.pipeline.name,
            pipeline_node_identifier=current_collector.pipeline_node.identifier,
        )
    )


# endregion


# region PipelineContext


@dataclass
class PipelineContext:
    pipeline: Pipeline
    pipeline_node: PipelineNode


pipeline_ctxvar = ContextVar[Optional[PipelineContext]]("pipeline_ctxvar", default=None)


@contextmanager
def open_pipeline_context_scope(pipeline: Pipeline, pipeline_node: PipelineNode):
    token = pipeline_ctxvar.set(
        PipelineContext(pipeline=pipeline, pipeline_node=pipeline_node)
    )
    try:
        yield
    except:
        raise
    finally:
        pipeline_ctxvar.reset(token)


def get_pipeline_context() -> PipelineContext:
    current_ctx = pipeline_ctxvar.get()
    if current_ctx is None:
        raise Exception("No pipeline context")

    return current_ctx

# endregion


def update_weather(event: PipelineNodeEventData) -> None:
    print("Updating weather")
    dispatch_event("weather_updated", payload={"weather": "sunny"})


def send_weather(event: PipelineNodeEventData) -> None:
    print("Sending weather")
    dispatch_event("discord_message_sent", payload={"message": "Weather is sunny"})


def send_discord_message(event: PipelineNodeEventData) -> None:
    print("Sending discord message")
    dispatch_event(
        "discord_message_sent", payload={"message": event.payload["message"]}
    )


def print_message(event: PipelineNodeEventData) -> None:
    param = get_pipeline_context().pipeline_node.static_params["message"]

    print("Printing, message:", param)


actions: dict[ACTION, Callable[[PipelineNodeEventData], None]] = {
    "update_weather": update_weather,
    "send_weather": send_weather,
    "send_discord_message": send_discord_message,
    "print_message": print_message,
}


class PipelineNodeExecutor(
    Executor[PipelineNode, PipelineNodeEventData, PipelineNodeEventData]
):
    def execute(
        self, pipeline_item: PipelineNode, event: PipelineNodeEventData
    ) -> list[PipelineNodeEventData]:
        action_identifier = pipeline_item.action

        pipeline = update_weather_every_morning

        action = actions[action_identifier]
        with open_events_collector_scope(
            EventCollector(
                pipeline=pipeline,
                pipeline_node=pipeline_item,
            )
        ) as collector, open_pipeline_context_scope(
            pipeline=pipeline, pipeline_node=pipeline_item
        ):
            action(event)
        return collector.staging_events


class PipelineNodeEventHandler(EventHandler[PipelineNodeEventData]):
    def __init__(self, executor: PipelineNodeExecutor):
        self.executor = executor

    def handle(self, event: PipelineNodeEventData) -> None:
        pipeline = update_weather_every_morning
        node = pipeline.get_node(event.pipeline_node_identifier)

        if event.name not in node.fork_events:
            return

        for node in node.fork_events[event.name]:
            for new_event in self.executor.execute(node, event):
                self.handle(new_event)


executor = PipelineNodeExecutor()
event_handler = PipelineNodeEventHandler(
    executor=executor,
)
usecase = PipelineUsecase(
    executor=executor,
    event_handler=event_handler,
)


def initialize() -> None:
    found_pipeline = update_weather_every_morning

    for node in found_pipeline.dispatch_events:
        usecase.execute_pipeline_item(
            payload=PipelineNodeEventData(
                name=found_pipeline.trigger_event,
                payload={},
                pipeline_name=found_pipeline.name,
                pipeline_node_identifier=node.identifier,
            ),
            pipeline_item=node,
        )


if __name__ == "__main__":
    initialize()
