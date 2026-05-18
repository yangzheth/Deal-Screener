"""Event-monitoring pipelines that run alongside the daily market watch.

Each monitor lives in its own sub-package and reuses the shared HTTP and
Notion plumbing. The first monitor tracks the CLARITY Act (H.R. 3633).
"""
