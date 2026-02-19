#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
import pandas as pd


DEFAULT_INPUT_GLOB = "all_campaigns_*.json"
OUT_XLSX_NAME = "campaigns_stats_clean.xlsx"

STATS_KEYS = [
    "stopped",
    "finished",
    "in_queue",
    "connected",
    "initiated",
    "step_count",
    "maybe_people",
    "contacted_people",
    "latest_action_id",
    "interested_people",
    "people_in_campaign",
    "replied_first_action",
    "not_interested_people",
    "replied_other_actions",
]


def find_latest_json(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def get_first(d: dict, keys: list[str], default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            return d[k]
    return default


def normalize_campaign_name(campaign: dict) -> str:
    # běžné varianty názvu kampaně
    return (
        get_first(campaign, ["name", "title", "campaign_name"])
        or get_first(campaign.get("campaign") if isinstance(campaign.get("campaign"), dict) else {}, ["name", "title"])
        or ""
    )


def normalize_list(x) -> str:
    """
    tags / labels chceme do jedné buňky:
    - [] -> ""
    - ["a","b"] -> "a, b"
    - list objektů -> zkusíme vytáhnout name/label/title/id, jinak json string
    """
    if x is None:
        return ""
    if not isinstance(x, list):
        return str(x)

    if len(x) == 0:
        return ""

    out = []
    for item in x:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            out.append(
                str(
                    item.get("name")
                    or item.get("label")
                    or item.get("title")
                    or item.get("id")
                    or json.dumps(item, ensure_ascii=False)
                )
            )
        else:
            out.append(str(item))

    # unikáty, zachovat pořadí
    seen = set()
    dedup = []
    for v in out:
        if v not in seen and v != "":
            seen.add(v)
            dedup.append(v)

    return ", ".join(dedup)


def flatten_row(row: dict) -> dict:
    campaign = row.get("campaign") or {}
    if not isinstance(campaign, dict):
        campaign = {}

    stats = campaign.get("stats") or {}
    if not isinstance(stats, dict):
        stats = {}

    out = {
        "account_name": row.get("account_name"),
        "account_id": row.get("account_id"),
        "campaign_name": normalize_campaign_name(campaign),
        "campaign_instance_id": get_first(campaign, ["id", "_id", "pk", "uuid", "campaign_instance_id"]),
        "status": get_first(campaign, ["status", "state"]),
        # nové věci, co chceš:
        "activated": get_first(campaign, ["activated", "activated_at", "activatedAt"]),
        "deactivated": get_first(campaign, ["deactivated", "deactivated_at", "deactivatedAt"]),
        "tags": normalize_list(campaign.get("tags")),
        "labels": normalize_list(campaign.get("labels")),
    }

    for k in STATS_KEYS:
        out[f"stats_{k}"] = stats.get(k)

    return out


def main():
    here = Path(__file__).resolve().parent

    input_path = find_latest_json(here, DEFAULT_INPUT_GLOB) or find_latest_json(Path.cwd(), DEFAULT_INPUT_GLOB)
    if input_path is None:
        raise SystemExit(
            f"❌ Nenašel jsem žádný soubor podle patternu '{DEFAULT_INPUT_GLOB}'.\n"
            "Dej JSON do stejné složky jako skript nebo skript spusť ve složce s JSONem."
        )

    print(f"📥 Načítám: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise SystemExit("❌ JSON nemá očekávaný formát (čekal jsem list řádků).")

    df = pd.DataFrame([flatten_row(r) for r in data])

    # vyhoď řádky bez názvu kampaně
    df["campaign_name"] = df["campaign_name"].fillna("").astype(str)
    df = df[df["campaign_name"].str.strip() != ""].copy()

    ordered_cols = [
        "account_name",
        "account_id",
        "campaign_name",
        "campaign_instance_id",
        "status",
        "activated",
        "deactivated",
        "tags",
        "labels",
    ] + [f"stats_{k}" for k in STATS_KEYS]
    ordered_cols = [c for c in ordered_cols if c in df.columns]
    df = df[ordered_cols]

    out_path = here / OUT_XLSX_NAME
    print(f"💾 Zapisuju: {out_path}")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="CAMPAIGN_STATS")

        # summary podle účtu + kampaně (stejně jako předtím)
        sum_cols = [f"stats_{k}" for k in STATS_KEYS if k not in ("latest_action_id", "step_count")]
        agg_dict = {c: "sum" for c in sum_cols}
        if "stats_latest_action_id" in df.columns:
            agg_dict["stats_latest_action_id"] = "max"
        if "stats_step_count" in df.columns:
            agg_dict["stats_step_count"] = "max"

        # activated/deactivated: dáme min/max (kdy poprvé aktivováno, kdy naposledy deaktivováno)
        if "activated" in df.columns:
            agg_dict["activated"] = "min"
        if "deactivated" in df.columns:
            agg_dict["deactivated"] = "max"

        # tags/labels: spojíme unikátní hodnoty
        def join_unique(series: pd.Series) -> str:
            vals = []
            for x in series.fillna("").astype(str):
                for part in [p.strip() for p in x.split(",") if p.strip()]:
                    vals.append(part)
            seen = set()
            out = []
            for v in vals:
                if v not in seen:
                    seen.add(v)
                    out.append(v)
            return ", ".join(out)

        if "tags" in df.columns:
            agg_dict["tags"] = join_unique
        if "labels" in df.columns:
            agg_dict["labels"] = join_unique

        if len(df) > 0:
            summary = (
                df.groupby(["account_name", "campaign_name"], dropna=False)
                .agg(agg_dict)
                .reset_index()
            )
            summary.to_excel(writer, index=False, sheet_name="SUMMARY_BY_CAMPAIGN")

    print("✅ Hotovo.")
    print(f"   CAMPAIGN_STATS: {len(df)} řádků")
    print("   SUMMARY_BY_CAMPAIGN: agregace podle účtu + názvu kampaně")


if __name__ == "__main__":
    main()
