"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password)
        )
        response.raise_for_status()
        return response.json()
    


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    all_logs = []
    current_since = since
    has_more = True
    
    async with httpx.AsyncClient() as client:
        while has_more:
            params = {"limit": 500}
            if current_since:
                # Convert datetime to ISO format with Z suffix
                params["since"] = current_since.isoformat().replace("+00:00", "Z")
            
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            all_logs.extend(data["logs"])
            has_more = data["has_more"]
            
            if has_more and data["logs"]:
                # Use the submitted_at of the last log as next since
                last_log = data["logs"][-1]
                current_since = datetime.fromisoformat(last_log["submitted_at"].replace("Z", "+00:00"))
    
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    created_count = 0
    lab_map = {}  # short_id -> ItemRecord
    
    # Process labs first
    for item in items:
        if item["type"] == "lab":
            # Check if lab already exists - with parent_id IS NULL
            stmt = select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == item["title"],
                ItemRecord.parent_id.is_(None)  # FIXED: labs have no parent
            )
            result = await session.execute(stmt)
            lab = result.scalar_one_or_none()
            
            if not lab:
                lab = ItemRecord(
                    type="lab",
                    title=item["title"]
                )
                session.add(lab)
                await session.flush()  # Get the ID
                created_count += 1
            
            lab_map[item["lab"]] = lab
    
    # Process tasks
    for item in items:
        if item["type"] == "task":
            parent_lab = lab_map.get(item["lab"])
            if not parent_lab:
                # Skip if parent lab not found
                continue
            
            # Check if task already exists
            stmt = select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == item["title"],
                ItemRecord.parent_id == parent_lab.id
            )
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()
            
            if not task:
                task = ItemRecord(
                    type="task",
                    title=item["title"],
                    parent_id=parent_lab.id
                )
                session.add(task)
                created_count += 1
    
    await session.commit()
    return created_count



async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    # Build lookup: (lab, task) -> title
    title_lookup = {}
    for item in items_catalog:
        lab = item["lab"]
        task = item.get("task")  # may be None for labs
        title_lookup[(lab, task)] = item["title"]
    
    created_count = 0
    
    for log in logs:
        # 1. Find or create Learner
        stmt = select(Learner).where(Learner.external_id == log["student_id"])
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()
        
        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()
        
        # 2. Find the matching Item
        lookup_key = (log["lab"], log.get("task"))
        title = title_lookup.get(lookup_key)
        
        if not title:
            # Skip if no matching item found in catalog
            continue
        
        # Query for the actual ItemRecord in DB - with more precise conditions
        if log.get("task"):  # This is a task
            stmt = select(ItemRecord).where(
                ItemRecord.title == title,
                ItemRecord.type == "task"  # FIXED: specify type
            )
        else:  # This is a lab
            stmt = select(ItemRecord).where(
                ItemRecord.title == title,
                ItemRecord.type == "lab",
                ItemRecord.parent_id.is_(None)  # FIXED: labs have no parent
            )
        
        result = await session.execute(stmt)
        item = result.scalar_one_or_none()
        
        if not item:
            # Skip if item not in DB (shouldn't happen if load_items was called)
            continue
        
        # 3. Check if InteractionLog already exists (idempotent)
        stmt = select(InteractionLog).where(InteractionLog.external_id == log["id"])
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            continue
        
        # 4. Create InteractionLog
        submitted_at = datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00"))
        
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at
        )
        session.add(interaction)
        created_count += 1
    
    await session.commit()
    return created_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    # Step 1: Fetch and load items
    items = await fetch_items()
    new_items = await load_items(items, session)
    
    # Step 2: Determine last sync time
    stmt = select(func.max(InteractionLog.created_at))
    result = await session.execute(stmt)
    last_sync = result.scalar()
    
    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=last_sync)
    new_logs = await load_logs(logs, items, session)
    
    # Step 4: Get total count
    stmt = select(func.count(InteractionLog.id))
    result = await session.execute(stmt)
    total = result.scalar() or 0
    
    return {
        "new_records": new_logs,
        "total_records": total
    }