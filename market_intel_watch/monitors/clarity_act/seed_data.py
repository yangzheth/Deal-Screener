"""Baseline facts for the CLARITY Act monitor.

This module is the single source of truth that initializes the Notion tracker
and gives the classifier a roster to match senator mentions against. Numbers
reflect the briefing baseline; the pipeline updates them as events arrive.
"""

from __future__ import annotations

from market_intel_watch.monitors.clarity_act.models import Milestone, SenatorPosition


BILL = {
    "congress": 119,
    "bill_type": "hr",
    "bill_number": 3633,
    "title": "Digital Asset Market Clarity (CLARITY) Act",
    "congress_url": "https://www.congress.gov/bill/119th-congress/house-bill/3633",
}

# Floor cloture math.
CLOTURE_VOTES_NEEDED = 60
REPUBLICAN_BASE = 53
DEMOCRATS_NEEDED_MIN = CLOTURE_VOTES_NEEDED - REPUBLICAN_BASE  # 7
DEMOCRATS_NEEDED_STIFEL = 8  # Stifel / Brian Gardner cushion estimate

KEY_DATES = {
    "house_passage": "2025-07-17",          # H.R. 3633 passed 294-134
    "senate_floor_window": "2026-06",       # expected cloture / floor window
    "white_house_target_signing": "2026-07-04",
    "hard_deadline": "before the August 2026 recess",
}

# Galaxy Research "crypto posture map" of Senate Banking Committee Democrats,
# plus broad-caucus floor-vote watch targets. Committee membership flags are a
# best-effort reading of the briefing and should be re-verified against the
# official roster before being used for whip math.
SENATORS: list[SenatorPosition] = [
    SenatorPosition(
        "Ruben Gallego", "D-AZ", True, "Lean Yes", "Yes",
        "Voted yes in committee; has not committed on a floor vote.", 0.70),
    SenatorPosition(
        "Angela Alsobrooks", "D-MD", True, "Lean Yes", "Yes",
        "Voted yes in committee; leads the Tillis-Alsobrooks stablecoin-yield compromise.", 0.70),
    SenatorPosition(
        "Mark Warner", "D-VA", True, "Swing", "",
        "Yes on the GENIUS Act, no on SAB 121; warns of 'crypto purgatory'; "
        "pushing a DeFi-definition provision.", 0.50),
    SenatorPosition(
        "Catherine Cortez Masto", "D-NV", True, "Swing", "",
        "Former state AG; yes on the GENIUS Act; focused on sanctions enforcement.", 0.50),
    SenatorPosition(
        "Andy Kim", "D-NJ", True, "Swing", "",
        "Final yes on the GENIUS Act; conditioning support on added safeguards.", 0.50),
    SenatorPosition(
        "Raphael Warnock", "D-GA", True, "Swing", "",
        "Yes on the GENIUS Act; emphasizes AML provisions.", 0.50),
    SenatorPosition(
        "Lisa Blunt Rochester", "D-DE", True, "Lean No", "",
        "Final no on the GENIUS Act.", 0.65),
    SenatorPosition(
        "Kirsten Gillibrand", "D-NY", False, "Unknown", "N/A",
        "Historically crypto-engaged; no public position on CLARITY yet.", 0.20),
    SenatorPosition(
        "Cory Booker", "D-NJ", False, "Unknown", "N/A",
        "Historically crypto-engaged; no public position on CLARITY yet.", 0.20),
    SenatorPosition(
        "Chris Coons", "D-DE", False, "Unknown", "N/A",
        "Historically crypto-engaged; no public position on CLARITY yet.", 0.20),
    SenatorPosition(
        "Elizabeth Warren", "D-MA", True, "No", "",
        "Calls the bill a 'corruption superhighway'; Banking Committee Ranking Member.", 0.95),
    SenatorPosition(
        "Jack Reed", "D-RI", True, "No", "No",
        "Voted no in committee; author of the sanctions amendment.", 0.95),
    SenatorPosition(
        "Chris Van Hollen", "D-MD", True, "No", "No",
        "Voted no in committee.", 0.95),
    SenatorPosition(
        "Tina Smith", "D-MN", True, "No", "No",
        "Voted no in committee.", 0.95),
]

# Legislative stage gates. One milestone per Stage option in the Notion tracker.
MILESTONES: list[Milestone] = [
    Milestone(
        "House passage of H.R. 3633 (CLARITY Act)", "House Passage", "Completed",
        "Simple majority",
        "House passed the Digital Asset Market Clarity Act with bipartisan support.",
        actual_date="2025-07-17", vote_tally="294-134",
        source_url=BILL["congress_url"]),
    Milestone(
        "Senate Banking Committee markup", "Senate Committee", "Completed",
        "Simple majority",
        "Committee-level Democratic votes are tracked per member in the Senator "
        "Position Tracker. The Senate version diverges from the House bill on "
        "stablecoin yield, DeFi treatment, and ethics provisions.",
        vote_tally="TBD", source_url="https://www.banking.senate.gov/"),
    Milestone(
        "Senate floor cloture vote", "Senate Floor Cloture", "Pending",
        "60 (cloture)",
        "Expected window: June 2026. Math: 53 Republicans + at least 7 Democrats; "
        "Stifel's Brian Gardner estimates 8 Democrats are needed to cover possible "
        "Republican defections.",
        target_date="2026-06-01", vote_tally="TBD"),
    Milestone(
        "Senate floor final passage", "Senate Floor Final", "Pending",
        "Simple majority", "Occurs after a successful cloture vote.",
        vote_tally="TBD"),
    Milestone(
        "Conference committee (if House rejects Senate amendments)", "Conference",
        "Pending", "N/A",
        "Triggered only if the House does not concur with the Senate-amended bill. "
        "A conference report would reconcile the stablecoin-yield, DeFi, and ethics "
        "differences."),
    Milestone(
        "House concurrence with Senate-amended bill", "House Concurrence", "Pending",
        "Simple majority",
        "Fork point: the House either accepts the Senate-amended bill directly or "
        "sends it to a conference committee."),
    Milestone(
        "Presidential signing", "Presidential Signing", "Pending", "N/A",
        "White House target signing date is July 4, 2026 (Independence Day). "
        "Hard deadline is before the August 2026 recess.",
        target_date="2026-07-04"),
]

# Events that should always trigger a structured deep-dive analysis. The
# pipeline flags these in the digest; the analysis itself stays human-driven.
DEEP_ANALYSIS_TRIGGERS = [
    "Senate floor vote schedule officially announced",
    "48 hours before a cloture vote (vote-count scenario analysis)",
    "Cloture vote result (pass or fail)",
    "Senate floor final passage",
    "House decides to concur vs go to conference",
    "Conference report released",
    "Any hard-no senator's position softens (leading indicator)",
]


def senator_names() -> list[str]:
    return [senator.name for senator in SENATORS]


def stage_keywords() -> dict[str, list[str]]:
    """Maps a milestone Stage to keywords used for rule-based linking."""
    return {
        "House Passage": ["house passage", "passed the house", "294-134"],
        "Senate Committee": ["committee markup", "markup", "banking committee", "committee vote"],
        "Senate Floor Cloture": ["cloture", "procedural vote", "60 votes", "filibuster"],
        "Senate Floor Final": ["final passage", "floor vote", "senate passage", "passed the senate"],
        "Conference": ["conference committee", "conference report", "conference"],
        "House Concurrence": ["house concurrence", "concur", "house vote on senate"],
        "Presidential Signing": ["signed into law", "presidential signing", "president signs", "signing ceremony"],
    }
