# monitor/diff_utils.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ----------------------------
# Defaults
# ----------------------------
PRIMARY_KEY_DEFAULT = ["profile_url"]          # stable identifier for a person/profile
IGNORE_FIELDS_DEFAULT = []                     # e.g., ["location"] if it's too volatile
SNAPSHOTS_DIR = "snapshots"                    # top-level snapshots folder


# ----------------------------
# Utilities
# ----------------------------
def _collapse_ws(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    return re.sub(r"\s+", " ", s)


def normalize_df(
    df: pd.DataFrame,
    key_cols: List[str],
    ignore_fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Normalize a dataframe for diffing:
      - cast all to string, fillna ""
      - strip + collapse whitespace
      - ensure key columns exist (else empty)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=key_cols).astype(str).fillna("")

    df = df.copy()
    # Make sure required key columns exist
    for c in key_cols:
        if c not in df.columns:
            df[c] = ""

    # Cast to string + fillna
    df = df.astype(str).fillna("")

    # Strip + collapse whitespace on all columns
    for c in df.columns:
        df[c] = df[c].map(_collapse_ws)

    # Drop duplicate rows on primary key just in case
    df = df.drop_duplicates(subset=key_cols, keep="first")

    # Optional: ensure ignore fields exist (so reindexing later won't fail)
    if ignore_fields:
        for c in ignore_fields:
            if c not in df.columns:
                df[c] = ""

    return df


def list_snapshots(award_id: str, base: str = SNAPSHOTS_DIR) -> List[Path]:
    """Return sorted list of CSV snapshot paths for a given award id."""
    d = Path(base) / award_id
    if not d.exists():
        return []
    snaps = sorted(p for p in d.glob("*.csv"))
    return snaps


def load_latest_two(
    award_id: str,
    base: str = SNAPSHOTS_DIR,
    primary_key: Optional[List[str]] = None,
    ignore_fields: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[Path], Optional[Path]]:
    """
    Load the last two CSV snapshots for an award and normalize them.
    Returns (prev_df, curr_df, prev_path, curr_path).
    If only one snapshot exists, prev_df will be empty and prev_path None.
    If none exist, both DFs will be empty.
    """
    pk = primary_key or PRIMARY_KEY_DEFAULT
    snaps = list_snapshots(award_id, base=base)

    if len(snaps) == 0:
        return pd.DataFrame(), pd.DataFrame(), None, None
    if len(snaps) == 1:
        curr = pd.read_csv(snaps[-1], dtype=str).fillna("")
        return pd.DataFrame(), normalize_df(curr, pk, ignore_fields), None, snaps[-1]

    prev_path, curr_path = snaps[-2], snaps[-1]
    prev = pd.read_csv(prev_path, dtype=str).fillna("")
    curr = pd.read_csv(curr_path, dtype=str).fillna("")
    return (
        normalize_df(prev, pk, ignore_fields),
        normalize_df(curr, pk, ignore_fields),
        prev_path,
        curr_path,
    )


def _align_columns_for_compare(
    prev: pd.DataFrame,
    curr: pd.DataFrame,
    key_cols: List[str],
    ignore_fields: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    Make sure both frames have the same set/order of columns for comparison.
    Drops ignored fields from comparison (but they remain in the raw DFs).
    Returns (prev_aligned, curr_aligned, compare_cols)
    """
    ignore_fields = ignore_fields or []
    all_cols = sorted(set(prev.columns).union(set(curr.columns)))

    # Columns to compare = all minus ignored
    compare_cols = [c for c in all_cols if c not in ignore_fields]

    # Ensure key columns are present & first in order for readability
    for k in key_cols:
        if k not in compare_cols:
            compare_cols.insert(0, k)
        else:
            compare_cols.remove(k)
            compare_cols.insert(0, k)

    # Reindex both frames to the same columns
    prev_aligned = prev.reindex(columns=compare_cols, fill_value="")
    curr_aligned = curr.reindex(columns=compare_cols, fill_value="")
    return prev_aligned, curr_aligned, compare_cols


def compute_diff(
    prev: pd.DataFrame,
    curr: pd.DataFrame,
    primary_key: Optional[List[str]] = None,
    ignore_fields: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Compute added / removed / modified records between prev and curr.

    - 'added': rows in curr whose PRIMARY KEY does not exist in prev.
    - 'removed': rows in prev whose PRIMARY KEY does not exist in curr.
    - 'modified': rows where PRIMARY KEY exists in both, but any non-ignored field changed.
                  Returned as a side-by-side merge with _before/_after suffixes.
    """
    pk = primary_key or PRIMARY_KEY_DEFAULT
    ignore_fields = ignore_fields or IGNORE_FIELDS_DEFAULT

    # Handle empty cases gracefully
    if prev.empty and curr.empty:
        return {"added": pd.DataFrame(), "removed": pd.DataFrame(), "modified": pd.DataFrame()}
    if prev.empty:
        # Everything is new
        return {"added": curr.copy(), "removed": pd.DataFrame(), "modified": pd.DataFrame()}
    if curr.empty:
        # Everything disappeared
        return {"added": pd.DataFrame(), "removed": prev.copy(), "modified": pd.DataFrame()}

    # Ensure both frames contain the PK columns
    for k in pk:
        if k not in prev.columns:
            prev[k] = ""
        if k not in curr.columns:
            curr[k] = ""

    # Added / Removed
    added_keys = curr.merge(prev[pk].drop_duplicates(), on=pk, how="left", indicator=True)
    added = added_keys.loc[added_keys["_merge"] == "left_only", curr.columns].copy()

    removed_keys = prev.merge(curr[pk].drop_duplicates(), on=pk, how="left", indicator=True)
    removed = removed_keys.loc[removed_keys["_merge"] == "left_only", prev.columns].copy()

    # Modified:
    # Align columns (excluding ignored fields) so we can compare apples-to-apples
    prev_cmp, curr_cmp, compare_cols = _align_columns_for_compare(prev, curr, pk, ignore_fields)

    # Index by PK for row-wise equality checks
    prev_cmp_idx = prev_cmp.set_index(pk, drop=False)
    curr_cmp_idx = curr_cmp.set_index(pk, drop=False)

    # Only compare rows with keys present in both
    common_idx = prev_cmp_idx.index.intersection(curr_cmp_idx.index)
    if len(common_idx) == 0:
        modified = pd.DataFrame()
    else:
        prev_common = prev_cmp_idx.loc[common_idx]
        curr_common = curr_cmp_idx.loc[common_idx]

        # Compare field-by-field for any change (excluding ignored fields)
        changed_mask = (prev_common != curr_common).any(axis=1)
        changed_keys = list(common_idx[changed_mask])

        if not changed_keys:
            modified = pd.DataFrame()
        else:
            # Create side-by-side before/after view using full original columns,
            # not just compare_cols (useful to include ignored fields in the output context).
            prev_full = prev.set_index(pk, drop=False).loc[changed_keys].reset_index(drop=True)
            curr_full = curr.set_index(pk, drop=False).loc[changed_keys].reset_index(drop=True)
            modified = prev_full.merge(curr_full, on=pk, suffixes=("_before", "_after"))

    return {"added": added.reset_index(drop=True),
            "removed": removed.reset_index(drop=True),
            "modified": modified.reset_index(drop=True)}


def diff_summary_str(diff: Dict[str, pd.DataFrame]) -> str:
    """Return a compact human-readable summary like '+2 / -1 / ~3'."""
    a = len(diff.get("added", pd.DataFrame()))
    r = len(diff.get("removed", pd.DataFrame()))
    m = len(diff.get("modified", pd.DataFrame()))
    return f"+{a} / -{r} / ~{m}"


def write_diff_csvs(
    diff: Dict[str, pd.DataFrame],
    out_prefix: Path,
) -> List[Path]:
    """
    Write CSVs for non-empty diffs next to the current snapshot.
    out_prefix should be the snapshot path WITHOUT extension (e.g., snapshots/3008/20250112_090000)

    Returns a list of paths written.
    """
    written: List[Path] = []
    if diff.get("added") is not None and not diff["added"].empty:
        p = out_prefix.with_name(out_prefix.name + "__added.csv")
        diff["added"].to_csv(p, index=False)
        written.append(p)

    if diff.get("removed") is not None and not diff["removed"].empty:
        p = out_prefix.with_name(out_prefix.name + "__removed.csv")
        diff["removed"].to_csv(p, index=False)
        written.append(p)

    if diff.get("modified") is not None and not diff["modified"].empty:
        p = out_prefix.with_name(out_prefix.name + "__modified.csv")
        diff["modified"].to_csv(p, index=False)
        written.append(p)

    return written
