"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlalchemy.sql import text
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _transform_lab_id(lab: str) -> str:
    """Transform lab identifier from 'lab-04' to 'Lab 04'."""
    # Transform 'lab-04' -> 'Lab 04'
    return lab.replace("-", " ").capitalize()


async def _find_lab_by_title(session: AsyncSession, lab: str):
    """Find lab item by matching the transformed lab ID prefix."""
    lab_prefix = _transform_lab_id(lab)  # e.g., "Lab 04"
    # Match titles that start with "Lab 04" (e.g., "Lab 04 — Testing")
    stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.like(f"{lab_prefix}%")
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    Returns a JSON array with four buckets:
    [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    """
    lab_item = await _find_lab_by_title(session, lab)

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Get all task item_ids that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    task_ids_result = await session.execute(task_ids_stmt)
    task_ids = task_ids_result.scalars().all()

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions for these tasks with scores
    score_bucket = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")

    stmt = (
        select(score_bucket, func.count().label("count"))
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(score_bucket)
    )

    result = await session.execute(stmt)
    rows = result.all()
    bucket_counts = {row.bucket: row.count for row in rows}

    # Always return all four buckets
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    Returns a JSON array:
    [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    """
    lab_item = await _find_lab_by_title(session, lab)

    if not lab_item:
        return []

    # Get all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.execute(tasks_stmt)
    tasks = tasks_result.scalars().all()

    result = []
    for task in sorted(tasks, key=lambda t: t.title):
        # Get avg_score and attempts for this task
        stmt = (
            select(
                func.avg(InteractionLog.score).label("avg_score"),
                func.count().label("attempts"),
            )
            .where(InteractionLog.item_id == task.id)
            .where(InteractionLog.score.isnot(None))
        )
        row_result = await session.execute(stmt)
        row = row_result.first()

        if row and row.attempts > 0:
            avg = row.avg_score
            result.append(
                {
                    "task": task.title,
                    "avg_score": round(avg, 1) if avg is not None else 0.0,
                    "attempts": row.attempts,
                }
            )

    return result


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    Returns a JSON array:
    [{"date": "2026-02-28", "submissions": 45}, ...]
    """
    lab_item = await _find_lab_by_title(session, lab)

    if not lab_item:
        return []

    # Get all task item_ids that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    task_ids_result = await session.execute(task_ids_stmt)
    task_ids = task_ids_result.scalars().all()

    if not task_ids:
        return []

    # Group interactions by date
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.execute(stmt)
    rows = result.all()
    return [{"date": row.date, "submissions": row.submissions} for row in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    Returns a JSON array:
    [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    """
    lab_item = await _find_lab_by_title(session, lab)

    if not lab_item:
        return []

    # Get all task item_ids that belong to this lab
    task_ids_stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    task_ids_result = await session.execute(task_ids_stmt)
    task_ids = task_ids_result.scalars().all()

    if not task_ids:
        return []

    # Join interactions with learners and group by student_group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.execute(stmt)
    rows = result.all()
    return [
        {
            "group": row.group,
            "avg_score": round(row.avg_score, 1) if row.avg_score is not None else 0.0,
            "students": row.students,
        }
        for row in rows
    ]