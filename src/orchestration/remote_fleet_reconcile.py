"""Explicit bounded convergence loop for durable Fleet projections.

The reconciler never discovers authority from Worker or Fleet snapshots and
never advances Domain Run state.  A trusted route source supplies Run routing
metadata, a trusted scheduler resolver supplies durable schedulers, and the
ordinary projector/Store claim path remains authoritative.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol, Sequence, runtime_checkable

from .remote_fleet import (
    FleetQueueReconcileReport,
    FleetTaskBinding,
    RemoteFleetCoordinator,
)
from .remote_fleet_control import (
    DurableFleetProjector,
    FleetTerminalReconcileReport,
    RemoteFleetControlConfigurationError,
    RemoteFleetControlConflict,
    SecureRemoteFleetPoller,
)
from .remote_scheduling import RemoteTask
from .scheduler import DurableScheduler
from .store import FleetFairnessCursor, FleetShardOwnership

MAX_FLEET_RUN_ROUTES = 100_000


def _authority_key(
    value: FleetFairnessCursor | FleetShardOwnership,
) -> tuple[str, str, str, int, str]:
    return (
        value.shard_id,
        value.pool_id,
        value.owner_id,
        value.fencing_epoch,
        value.policy_digest,
    )


@dataclass(frozen=True, slots=True)
class FleetRunRoute:
    """Trusted tenant/pool routing for one durable Run."""

    run_id: str
    tenant_id: str
    pool_id: str

    def __post_init__(self) -> None:
        try:
            task = RemoteTask(
                task_id="fleet-route-validation",
                tenant_id=self.tenant_id,
                pool_id=self.pool_id,
                tool_name="fleet.route",
            )
            binding = FleetTaskBinding(
                run_id=self.run_id,
                task=task,
            )
        except (TypeError, ValueError) as exc:
            raise RemoteFleetControlConfigurationError(
                "Fleet Run route is invalid"
            ) from exc
        object.__setattr__(self, "run_id", binding.run_id)
        object.__setattr__(self, "tenant_id", task.tenant_id)
        object.__setattr__(self, "pool_id", task.pool_id)


@runtime_checkable
class FleetRunSource(Protocol):
    @property
    def production_security_ready(self) -> bool: ...

    def snapshot(self, limit: int) -> Sequence[FleetRunRoute]: ...


class StaticFleetRunSource:
    """Immutable deployment-owned Run routing catalog."""

    production_security_ready = True

    def __init__(
        self,
        routes: Sequence[FleetRunRoute],
    ) -> None:
        if not isinstance(routes, Sequence) or isinstance(
            routes,
            (str, bytes),
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet Run routes must be a bounded sequence"
            )
        if len(routes) > MAX_FLEET_RUN_ROUTES:
            raise RemoteFleetControlConfigurationError(
                "Fleet Run route registry is full"
            )
        normalized: list[FleetRunRoute] = []
        run_ids: set[str] = set()
        for route in routes:
            if not isinstance(route, FleetRunRoute):
                raise RemoteFleetControlConfigurationError(
                    "Fleet Run routes contain an invalid value"
                )
            if route.run_id in run_ids:
                raise RemoteFleetControlConfigurationError(
                    "Fleet Run route is duplicated"
                )
            run_ids.add(route.run_id)
            normalized.append(route)
        self._routes = tuple(normalized)

    def snapshot(self, limit: int) -> Sequence[FleetRunRoute]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_FLEET_RUN_ROUTES + 1
        ):
            raise ValueError("Fleet Run route limit is invalid")
        return self._routes[:limit]


@dataclass(frozen=True, slots=True)
class FleetProjectionReconcileReport:
    routes_examined: int
    schedulers_resolved: int
    desired_tasks: int
    restored_pools: int
    terminal: FleetTerminalReconcileReport
    queue: FleetQueueReconcileReport
    execution_truth: bool = False


SchedulerResolver = Callable[[str], DurableScheduler]


class DurableFleetReconciler:
    """Converge trusted Run routes into a non-authoritative Fleet queue."""

    def __init__(
        self,
        fleet: RemoteFleetCoordinator,
        poller: SecureRemoteFleetPoller,
        projector: DurableFleetProjector,
        run_source: FleetRunSource,
        scheduler_resolver: SchedulerResolver,
        *,
        max_routes: int = 4_096,
    ) -> None:
        if not isinstance(fleet, RemoteFleetCoordinator):
            raise TypeError("fleet must be a RemoteFleetCoordinator")
        if not isinstance(poller, SecureRemoteFleetPoller):
            raise TypeError("poller must be a SecureRemoteFleetPoller")
        if not isinstance(projector, DurableFleetProjector):
            raise TypeError("projector must be a DurableFleetProjector")
        if not isinstance(run_source, FleetRunSource):
            raise TypeError("run_source must implement FleetRunSource")
        if run_source.production_security_ready is not True:
            raise RemoteFleetControlConfigurationError(
                "Fleet Run source is not production ready"
            )
        if not callable(scheduler_resolver):
            raise TypeError("scheduler_resolver must be callable")
        if (
            isinstance(max_routes, bool)
            or not isinstance(max_routes, int)
            or not 1 <= max_routes <= MAX_FLEET_RUN_ROUTES
        ):
            raise RemoteFleetControlConfigurationError(
                "Fleet reconciler max_routes is invalid"
            )
        if poller.production_security_ready is not True:
            raise RemoteFleetControlConfigurationError(
                "Fleet poller is not production ready"
            )
        self._fleet = fleet
        self._poller = poller
        self._projector = projector
        self._run_source = run_source
        self._scheduler_resolver = scheduler_resolver
        self.max_routes = max_routes

    @property
    def production_security_ready(self) -> bool:
        try:
            return (
                self._run_source.production_security_ready is True
                and self._poller.production_security_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return False

    def run_once(self) -> FleetProjectionReconcileReport:
        try:
            return self._run_once()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            try:
                self._fleet.reconcile_queued(())
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as quarantine_error:
                exc.add_note(
                    "Fleet queue quarantine failed: "
                    f"{type(quarantine_error).__name__}"
                )
            raise

    def _run_once(self) -> FleetProjectionReconcileReport:
        try:
            source_ready = (
                self._run_source.production_security_ready is True
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            raise RemoteFleetControlConflict(
                "Fleet Run source readiness failed"
            ) from exc
        if not source_ready:
            raise RemoteFleetControlConflict(
                "Fleet Run source is not production ready"
            )
        try:
            raw_routes = self._run_source.snapshot(
                self.max_routes + 1
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            raise RemoteFleetControlConflict(
                "Fleet Run source snapshot failed"
            ) from exc
        if not isinstance(raw_routes, Sequence) or isinstance(
            raw_routes,
            (str, bytes),
        ):
            raise RemoteFleetControlConflict(
                "Fleet Run source returned an invalid snapshot"
            )
        if len(raw_routes) > self.max_routes:
            raise RemoteFleetControlConflict(
                "Fleet Run source exceeded its bound"
            )

        routes: list[FleetRunRoute] = []
        run_ids: set[str] = set()
        for route in raw_routes:
            if not isinstance(route, FleetRunRoute):
                raise RemoteFleetControlConflict(
                    "Fleet Run source returned an invalid route"
                )
            if route.run_id in run_ids:
                raise RemoteFleetControlConflict(
                    "Fleet Run source returned a duplicate Run"
                )
            run_ids.add(route.run_id)
            routes.append(route)
        routes.sort(
            key=lambda route: (
                route.pool_id,
                route.tenant_id,
                route.run_id,
            )
        )

        desired: list[FleetTaskBinding] = []
        task_ids: set[str] = set()
        cursors: dict[str, FleetFairnessCursor] = {}
        pool_stores: dict[str, Path] = {}
        resolved = 0
        for route in routes:
            try:
                scheduler = self._scheduler_resolver(route.run_id)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:
                raise RemoteFleetControlConflict(
                    "Fleet scheduler resolution failed"
                ) from exc
            if not isinstance(scheduler, DurableScheduler):
                raise RemoteFleetControlConflict(
                    "Fleet scheduler resolver returned an invalid scheduler"
                )
            run = scheduler.store.get_run(route.run_id)
            if run is None:
                raise RemoteFleetControlConflict(
                    "Fleet route has no durable Run"
                )
            resolved += 1

            ownership = None
            if self._fleet.require_durable_ownership:
                ownership = scheduler.store.get_fleet_pool_ownership(
                    route.pool_id
                )
                cursor = scheduler.store.get_fleet_fairness_cursor(
                    route.pool_id
                )
                if (
                    ownership is None
                    or cursor is None
                    or ownership.owner_id
                    != self._fleet.fleet_owner_id
                    or _authority_key(ownership)
                    != _authority_key(cursor)
                ):
                    raise RemoteFleetControlConflict(
                        "Fleet route lacks current pool authority"
                    )
                store_path = scheduler.store.path
                existing_store = pool_stores.get(route.pool_id)
                if (
                    existing_store is not None
                    and existing_store != store_path
                ):
                    raise RemoteFleetControlConflict(
                        "strict Fleet pool spans multiple Stores"
                    )
                pool_stores[route.pool_id] = store_path
                existing_cursor = cursors.get(route.pool_id)
                if (
                    existing_cursor is not None
                    and _authority_key(existing_cursor)
                    != _authority_key(cursor)
                ):
                    raise RemoteFleetControlConflict(
                        "Fleet pool cursor snapshot changed"
                    )
                cursors[route.pool_id] = cursor

            bindings = self._projector.project_ready(
                scheduler,
                route.run_id,
                tenant_id=route.tenant_id,
                pool_id=route.pool_id,
                shard_ownership=ownership,
            )
            for binding in bindings:
                if binding.task.task_id in task_ids:
                    raise RemoteFleetControlConflict(
                        "Fleet projection returned a duplicate task"
                    )
                if len(desired) >= self._fleet.max_task_bindings:
                    raise RemoteFleetControlConflict(
                        "Fleet projection exceeds coordinator capacity"
                    )
                task_ids.add(binding.task.task_id)
                desired.append(binding)

        terminal = self._poller.reconcile_terminals()
        restored = 0
        for pool_id in sorted(cursors):
            if self._fleet.ensure_fairness_cursor(cursors[pool_id]):
                restored += 1
        queue = self._fleet.reconcile_queued(desired)
        return FleetProjectionReconcileReport(
            routes_examined=len(routes),
            schedulers_resolved=resolved,
            desired_tasks=len(desired),
            restored_pools=restored,
            terminal=terminal,
            queue=queue,
        )


__all__ = [
    "DurableFleetReconciler",
    "FleetProjectionReconcileReport",
    "FleetRunRoute",
    "FleetRunSource",
    "StaticFleetRunSource",
]
