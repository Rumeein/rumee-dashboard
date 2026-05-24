"""
Rumee Dashboard — Supabase Connector
Handles all read/write operations for the rumee_insights and rumee_tasks tables.

Auth: SUPABASE_URL and SUPABASE_KEY environment variables.

Usage:
    from supabase_connector import write_insight, write_task, fetch_open_tasks
"""

import os
from datetime import date, datetime, timezone
from supabase import create_client, Client


def get_client() -> Client:
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY required")
    return create_client(url, key)


def write_insight(platform, sku_id, sku_name, category, text, severity='info'):
    """Write a new insight. Returns the inserted row."""
    try:
        client = get_client()
        result = client.table('rumee_insights').insert({
            'platform':     platform,
            'sku_id':       sku_id,
            'sku_name':     sku_name,
            'category':     category,
            'insight_text': text,
            'severity':     severity,
            'status':       'new',
            'email_count':  0
        }).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        print(f"Warning: Could not write insight: {e}")
        return None


def write_task(task_text, platform, sku_id=None, priority='medium',
               due_date=None, linked_insight_id=None, created_by='pipeline'):
    """Write a new task."""
    try:
        client = get_client()
        result = client.table('rumee_tasks').insert({
            'task_text':          task_text,
            'platform':           platform,
            'sku_id':             sku_id,
            'priority':           priority,
            'due_date':           str(due_date) if due_date else None,
            'linked_insight_id':  str(linked_insight_id) if linked_insight_id else None,
            'created_by':         created_by,
            'status':             'pending'
        }).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        print(f"Warning: Could not write task: {e}")
        return None


def update_task_status(task_id, status):
    """Update task status. Records completion time if marking done."""
    try:
        client = get_client()
        update_data = {'status': status}
        if status == 'done':
            update_data['completed_at'] = datetime.now(timezone.utc).isoformat()
        client.table('rumee_tasks').update(update_data).eq('id', task_id).execute()
        return True
    except Exception as e:
        print(f"Warning: Could not update task: {e}")
        return False


def mark_insight_resolved(insight_id):
    """Mark insight as resolved."""
    try:
        client = get_client()
        client.table('rumee_insights').update({'status': 'resolved'}).eq('id', insight_id).execute()
        return True
    except Exception as e:
        print(f"Warning: Could not resolve insight: {e}")
        return False


def fetch_open_insights(limit=50):
    """Fetch all non-resolved insights, newest first."""
    try:
        client = get_client()
        result = client.table('rumee_insights')\
            .select('*')\
            .neq('status', 'resolved')\
            .order('created_at', desc=True)\
            .limit(limit)\
            .execute()
        return result.data or []
    except Exception as e:
        print(f"Warning: Could not fetch insights: {e}")
        return []


def fetch_open_tasks(limit=100):
    """Fetch pending and in-progress tasks."""
    try:
        client = get_client()
        result = client.table('rumee_tasks')\
            .select('*, rumee_insights(insight_text, severity)')\
            .in_('status', ['pending', 'in_progress'])\
            .order('created_at', desc=True)\
            .limit(limit)\
            .execute()
        return result.data or []
    except Exception as e:
        print(f"Warning: Could not fetch tasks: {e}")
        return []


def fetch_all_tasks(limit=200):
    """Fetch all tasks including completed."""
    try:
        client = get_client()
        result = client.table('rumee_tasks')\
            .select('*')\
            .order('created_at', desc=True)\
            .limit(limit)\
            .execute()
        return result.data or []
    except Exception as e:
        print(f"Warning: Could not fetch tasks: {e}")
        return []


def insight_exists_today(sku_id, category):
    """
    Check if insight already written for this SKU+category today.
    Prevents duplicate insights on repeated pipeline runs.
    """
    try:
        client = get_client()
        today = date.today().isoformat()
        result = client.table('rumee_insights')\
            .select('id')\
            .eq('sku_id',    sku_id)\
            .eq('category',  category)\
            .neq('status',   'resolved')\
            .gte('created_at', f'{today}T00:00:00')\
            .execute()
        return len(result.data) > 0
    except Exception:
        return False


def get_insights_for_email():
    """
    Get insights that should be included in today's email.

    Rules:
    - Not resolved
    - email_count < 3  (3-day cap: stop emailing after 3 separate days)
    - last_emailed_date is not today (max 1 email per insight per day)

    Returns list of insights to email, then updates their email_count
    and last_emailed_date so next call today returns nothing.
    """
    try:
        client = get_client()
        today = date.today().isoformat()

        # Fetch insights eligible for email
        result = client.table('rumee_insights')\
            .select('*')\
            .neq('status', 'resolved')\
            .lt('email_count', 3)\
            .or_(f'last_emailed_date.is.null,last_emailed_date.lt.{today}')\
            .order('severity', desc=True)\
            .execute()
        insights = result.data or []

        # Stamp each insight with today's date and increment counter
        for insight in insights:
            client.table('rumee_insights').update({
                'email_count':       insight['email_count'] + 1,
                'last_emailed_date': today
            }).eq('id', insight['id']).execute()

        return insights
    except Exception as e:
        print(f"Warning: Could not fetch email insights: {e}")
        return []
