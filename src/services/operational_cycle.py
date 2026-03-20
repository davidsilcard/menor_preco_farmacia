from datetime import UTC, datetime, time, timedelta

from src.core.config import settings
from src.models.base import delete_old_prices
from src.services.scheduled_collection import build_scheduled_collection_plan, run_scheduled_collection


def parse_collection_slots(raw: str | None = None):
    raw = raw or settings.SCHEDULED_COLLECTION_SLOTS
    slots = []
    for index, chunk in enumerate((raw or "").split(","), start=1):
        value = chunk.strip()
        if not value:
            continue
        hour_str, minute_str = value.split(":", 1)
        slots.append(
            {
                "name": _slot_name(index),
                "label": value,
                "time": time(hour=int(hour_str), minute=int(minute_str)),
            }
        )
    return slots


def _slot_name(index: int):
    if index == 1:
        return "morning"
    if index == 2:
        return "afternoon"
    return f"slot_{index}"


def collection_schedule_status(now: datetime | None = None):
    now = now or datetime.now(UTC).astimezone()
    slot_window = settings.SCHEDULED_COLLECTION_SLOT_WINDOW_MINUTES
    slots = parse_collection_slots()
    scheduled = []
    current_slot = None
    next_slot = None

    for slot in slots:
        scheduled_at = now.replace(
            hour=slot["time"].hour,
            minute=slot["time"].minute,
            second=0,
            microsecond=0,
        )
        window_end = scheduled_at + timedelta(minutes=slot_window)
        if scheduled_at <= now <= window_end:
            current_slot = {
                "name": slot["name"],
                "label": slot["label"],
                "scheduled_at": scheduled_at.isoformat(),
                "window_ends_at": window_end.isoformat(),
            }
        if scheduled_at > now and next_slot is None:
            next_slot = {
                "name": slot["name"],
                "label": slot["label"],
                "scheduled_at": scheduled_at.isoformat(),
            }
        scheduled.append(
            {
                "name": slot["name"],
                "label": slot["label"],
                "scheduled_at": scheduled_at.isoformat(),
                "window_ends_at": window_end.isoformat(),
            }
        )

    if next_slot is None and slots:
        tomorrow = now + timedelta(days=1)
        first_slot = slots[0]
        next_dt = tomorrow.replace(
            hour=first_slot["time"].hour,
            minute=first_slot["time"].minute,
            second=0,
            microsecond=0,
        )
        next_slot = {
            "name": first_slot["name"],
            "label": first_slot["label"],
            "scheduled_at": next_dt.isoformat(),
        }

    return {
        "now": now.isoformat(),
        "slot_window_minutes": slot_window,
        "scheduled_slots": scheduled,
        "current_slot": current_slot,
        "next_slot": next_slot,
        "due_now": current_slot is not None,
    }


def run_operational_cycle(*, cep: str | None = None, force_collection: bool = False, now: datetime | None = None):
    schedule = collection_schedule_status(now)
    collection_due = force_collection or schedule["due_now"]
    plan = build_scheduled_collection_plan(cep)
    collection_result = None
    if collection_due:
        collection_result = run_scheduled_collection(cep)

    deleted_snapshots = delete_old_prices(retention_days=settings.PRICE_RETENTION_DAYS)
    return {
        "executed_at": (now or datetime.now(UTC).astimezone()).isoformat(),
        "schedule": schedule,
        "collection_due": collection_due,
        "collection_executed": collection_result is not None,
        "collection_plan": plan,
        "collection_result": collection_result,
        "retention": {
            "retention_days": settings.PRICE_RETENTION_DAYS,
            "deleted_snapshots": deleted_snapshots,
        },
    }
