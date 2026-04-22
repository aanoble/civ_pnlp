"""Pipeline to sync organisation units between SNIS and Nmdr."""

import json
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2


class OrgUnitCounts(TypedDict):
    """Count of organisation units by import action type.

    Attributes
    ----------
    imported : int
        Number of organisation units imported.
    updated : int
        Number of organisation units updated.
    ignored : int
        Number of organisation units ignored.
    deleted : int
        Number of organisation units deleted.
    """

    imported: int
    updated: int
    ignored: int
    deleted: int


class SyncPlan(TypedDict):
    """Structured plan for synchronising organisation units between SNIS and Nmdr.

    Attributes
    ----------
    metadata : dict[str, Any]
        Metadata about the sync plan generation.
    comparison_summary : dict[str, Any]
        Summary of the comparison between source and target organisation units.
    payload : list[dict[str, Any]]
        List of organisation unit payloads to be imported.
    actions : dict[str, list[str]]
        Mapping of action types to lists of organisation unit IDs.
    """

    metadata: dict[str, Any]
    comparison_summary: dict[str, Any]
    payload: list[dict[str, Any]]
    actions: dict[str, list[str]]


@pipeline("snis_to_nmdr_sync_orgunits")
@parameter(
    "snis_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for SNIS",
    help="DHIS2 connection to fetch SNIS data from.",
    default="snis-dhis2",
    required=True,
)
@parameter(
    "nmdr_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for NMDR",
    help="DHIS2 connection to fetch NMDR data from.",
    default="dhis2-nmdr-temp",
    required=True,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help="Directory to save the output files",
    default="sync SNIS NMDR/data/output",
    required=True,
)
@parameter(
    "import_mode",
    type=str,  # type: ignore
    name="Import mode",
    help=(
        "The import strategy to use when pushing data to Nmdr. "
        "Can be 'CREATE', 'UPDATE' or 'CREATE_AND_UPDATE'."
    ),
    default="CREATE_AND_UPDATE",
    required=False,
    choices=["CREATE", "UPDATE", "CREATE_AND_UPDATE"],
)
@parameter(
    "sync_existing_geometries",
    type=bool,  # type: ignore
    name="Synchroniser les geometries existantes",
    help="Mettre a jour les geometries des org units deja presentes dans Nmdr",
    default=True,
    required=False,
)
@parameter(
    "dry_run",
    type=bool,  # type: ignore
    name="Dry run mode",
    help="If True, the pipeline will run in dry-run mode and will not write any data to Nmdr.",
    default=False,
    required=False,
)
def snis_to_nmdr_sync_orgunits(
    snis_connection: DHIS2Connection,
    nmdr_connection: DHIS2Connection,
    output_directory: str,
    sync_existing_geometries: bool,
    dry_run: bool = False,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
):
    """Pipeline to sync organisation units from SNIS to Nmdr."""
    snis = DHIS2(connection=snis_connection)
    nmdr = DHIS2(connection=nmdr_connection)

    check_server_health(snis)
    check_server_health(nmdr)

    ou_snis = fetch_org_units(snis)
    ou_nmdr = fetch_org_units(nmdr)

    sync_plan = build_sync_plan(ou_snis, ou_nmdr, sync_existing_geometries)
    orgunit_payload = build_orgunit_payload(sync_plan)
    import_summary = push_data_to_dhis2(
        nmdr, orgunit_payload, dry_run, import_mode, post_batch_size
    )
    report = build_final_report(sync_plan, import_summary)

    report_written = write_import_report(
        (Path(output_directory) / "orgUnits"),
        orgunit_payload,
        report,
    )
    cleanup_old_directory_files((Path(output_directory) / "orgUnits"), report_written)


@snis_to_nmdr_sync_orgunits.task
def fetch_org_units(dhis2: DHIS2) -> pl.DataFrame:
    """Fetch organisation units from DHIS2.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 connection instance.

    Returns
    -------
    pl.DataFrame
        DataFrame containing organisation units.
    """
    current_run.log_info(f"Fetching organisation units from DHIS2 {dhis2.api.url}...")
    return pl.DataFrame(
        dhis2.meta.organisation_units(
            fields=(
                "id,name,shortName,code,openingDate,closedDate,level,path,parent,"
                "geometry,featureType,coordinates"
            ),
        )
    )


@snis_to_nmdr_sync_orgunits.task
def build_sync_plan(
    ou_snis: pl.DataFrame, ou_nmdr: pl.DataFrame, sync_existing_geometries: bool
) -> SyncPlan:
    """Compare source and target org units and build a structured sync plan.

    Parameters
    ----------
    ou_snis : pl.DataFrame
        DataFrame containing SNIS organisation units.
    ou_nmdr : pl.DataFrame
        DataFrame containing Nmdr organisation units.
    sync_existing_geometries : bool
        Whether to sync existing geometries.

    Returns
    -------
    SyncPlan
        Structured plan for synchronising organisation units.
    """
    current_run.log_info(
        f"Comparing {len(ou_snis)} SNIS org units with {len(ou_nmdr)} Nmdr org units..."
    )
    snis_by_id = {row["id"]: row for row in ou_snis.iter_rows(named=True)}
    nmdr_by_id = {row["id"]: row for row in ou_nmdr.iter_rows(named=True)}

    snis_ids = set(snis_by_id)
    nmdr_ids = set(nmdr_by_id)

    actions: dict[str, list[str]] = {
        "create": [],
        "close": [],
        "update_core": [],
        "update_geometry": [],
        "unchanged": [],
        "already_closed": [],
    }
    payload_by_id: dict[str, dict[str, Any]] = {}

    for ou_id in sorted(nmdr_ids - snis_ids):
        if nmdr_by_id[ou_id].get("closedDate"):
            actions["already_closed"].append(ou_id)
            continue

        actions["close"].append(ou_id)
        payload_by_id[ou_id] = _build_orgunit_payload(nmdr_by_id[ou_id], closing=True)

    for ou_id in sorted(snis_ids - nmdr_ids):
        actions["create"].append(ou_id)
        payload_by_id[ou_id] = _build_orgunit_payload(
            snis_by_id[ou_id],
            include_geometry=True,
        )

    for ou_id in sorted(snis_ids & nmdr_ids):
        source_ou = snis_by_id[ou_id]
        target_ou = nmdr_by_id[ou_id]

        core_changed = _core_signature(source_ou) != _core_signature(target_ou)
        geometry_changed = sync_existing_geometries and _geometry_signature(
            source_ou
        ) != _geometry_signature(target_ou)

        if core_changed:
            actions["update_core"].append(ou_id)
        if geometry_changed:
            actions["update_geometry"].append(ou_id)

        if core_changed or geometry_changed:
            payload_by_id[ou_id] = _build_orgunit_payload(
                source_ou,
                include_geometry=geometry_changed,
            )
        else:
            actions["unchanged"].append(ou_id)

    comparison_summary = {
        "sync_existing_geometries": sync_existing_geometries,
        "source_total": len(ou_snis),
        "target_total": len(ou_nmdr),
        "common_ids": len(snis_ids & nmdr_ids),
        "missing_from_target": len(actions["create"]),
        "missing_from_source": len(nmdr_ids - snis_ids),
        "to_create": len(actions["create"]),
        "to_close": len(actions["close"]),
        "already_closed": len(actions["already_closed"]),
        "to_update_core": len(actions["update_core"]),
        "to_update_geometry": len(actions["update_geometry"]),
        "unchanged": len(actions["unchanged"]),
        "payload_total": len(payload_by_id),
    }

    _log_sync_plan_summary(comparison_summary)

    return {
        "metadata": {
            "generated_at": datetime.now(UTC).isoformat(),
            "sync_existing_geometries": sync_existing_geometries,
        },
        "comparison_summary": comparison_summary,
        "payload": [payload_by_id[ou_id] for ou_id in sorted(payload_by_id)],
        "actions": actions,
    }


@snis_to_nmdr_sync_orgunits.task
def build_orgunit_payload(sync_plan: SyncPlan) -> list[dict[str, Any]]:
    """Extract the DHIS2 payload from the sync plan.

    Parameters
    ----------
    sync_plan : SyncPlan
        Structured plan for synchronising organisation units.

    Returns
    -------
    list[dict[str, Any]]
        List of organisation unit payloads to be imported.
    """
    return sync_plan["payload"]


def _build_orgunit_payload(
    ou: Mapping[str, object], closing: bool = False, include_geometry: bool = False
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": ou["id"],
        "name": ou["name"],
        "shortName": ou["shortName"],
        "code": ou.get("code"),
        "openingDate": ou["openingDate"],
        "parent": {"id": parent_id} if (parent_id := _parent_id(ou)) else None,
    }

    if closing:
        payload["closedDate"] = ou["closedDate"] or datetime.now(UTC).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )
    elif ou.get("closedDate") is not None:
        payload["closedDate"] = ou["closedDate"]

    if include_geometry:
        if ou.get("geometry") is not None:
            payload["geometry"] = ou["geometry"]
        elif ou.get("featureType") and ou.get("coordinates"):
            payload["featureType"] = ou["featureType"]
            payload["coordinates"] = ou["coordinates"]

    return payload


def _geometry_signature(ou: Mapping[str, object]) -> str:
    geometry = ou.get("geometry")
    if geometry is not None:
        return json.dumps(geometry, sort_keys=True, separators=(",", ":"))

    return json.dumps(
        {"featureType": ou.get("featureType"), "coordinates": ou.get("coordinates")},
        sort_keys=True,
        separators=(",", ":"),
    )


def _core_signature(ou: Mapping[str, object]) -> str:
    return json.dumps(
        {
            "name": ou.get("name"),
            "shortName": ou.get("shortName"),
            "code": ou.get("code"),
            "openingDate": ou.get("openingDate"),
            "closedDate": ou.get("closedDate"),
            "parent": _parent_id(ou),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _parent_id(ou: Mapping[str, object]) -> str | None:
    parent = ou.get("parent")
    if isinstance(parent, Mapping):
        parent_id = parent.get("id")
        if isinstance(parent_id, str):
            return parent_id
    return None


def _extract_import_counts(
    chunk_summary: Mapping[str, Any],
) -> tuple[OrgUnitCounts, str | None]:
    raw_counts = chunk_summary.get("importCount")
    source_key = "importCount"
    if not isinstance(raw_counts, Mapping):
        raw_counts = chunk_summary.get("stats")
        source_key = "stats" if isinstance(raw_counts, Mapping) else None

    counts: OrgUnitCounts = {
        "ignored": int(raw_counts.get("ignored", 0)) if isinstance(raw_counts, Mapping) else 0,
        "imported": int(raw_counts.get("created", 0)) if isinstance(raw_counts, Mapping) else 0,
        "updated": int(raw_counts.get("updated", 0)) if isinstance(raw_counts, Mapping) else 0,
        "deleted": int(raw_counts.get("deleted", 0)) if isinstance(raw_counts, Mapping) else 0,
    }
    return counts, source_key


@snis_to_nmdr_sync_orgunits.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    payload: list[dict[str, Any]],
    dry_run: bool,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
) -> dict[str, Any]:
    """Send org unit payload to DHIS2 with chunking, retries and structured reporting.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 connection instance.
    payload : list[dict[str, Any]]
        List of organisation unit payloads to import.
    dry_run : bool
        If True, run in dry-run mode without writing data.
    import_mode : str
        Import strategy: 'CREATE', 'UPDATE', or 'CREATE_AND_UPDATE'.
    post_batch_size : int
        Number of organisation units per batch request.

    Returns
    -------
    dict[str, Any]
        Import summary containing status, chunk results, and aggregate counts.
    """
    total = len(payload)
    if total == 0:
        current_run.log_info("No organisation units to import into DHIS2.")
        return {
            "status": "skipped",
            "import_strategy": import_mode,
            "dry_run": dry_run,
            "total": 0,
            "successful_chunks": 0,
            "failed_chunks": 0,
            "totals": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            "conflicts_total": 0,
            "errors_total": 0,
            "chunks": [],
            "imported": 0,
        }

    def _chunks(seq: list[dict[str, Any]], size: int):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    aggregated: dict[str, Any] = {
        "status": "completed",
        "import_strategy": import_mode,
        "dry_run": dry_run,
        "total": total,
        "successful_chunks": 0,
        "failed_chunks": 0,
        "totals": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        "conflicts_total": 0,
        "errors_total": 0,
        "chunks": [],
    }

    request_params = {"dryRun": dry_run, "importStrategy": import_mode}
    max_retries = 2
    backoff_base = 1.0
    url = dhis2.api.url + "/metadata.json"

    for idx, chunk in enumerate(_chunks(payload, post_batch_size), start=1):
        import_counts: OrgUnitCounts = {
            "imported": 0,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
        }
        issues: list[dict[str, Any]] = []
        attempts = 0
        response = None

        for attempt in range(1, max_retries + 1):
            attempts = attempt
            response = dhis2.api.session.post(
                url=url, json={"organisationUnits": chunk}, params=request_params
            )
            status = response.status_code
            if status == 200:
                break
            if status == 429 or 500 <= status < 600:
                sleep_s = backoff_base * (2 ** (attempt - 1))
                current_run.log_warning(
                    f"Chunk {idx} attempt {attempt}/{max_retries} failed (status={status}). "
                    f"Retrying in {sleep_s:.1f}s..."
                )
                time.sleep(sleep_s)
                continue
            break

        if response is None:
            aggregated["failed_chunks"] += 1
            aggregated["errors_total"] += 1
            aggregated["chunks"].append(
                {
                    "index": idx,
                    "size": len(chunk),
                    "attempts": attempts,
                    "status": "failed",
                    "status_code": None,
                    "issues": [],
                    "error": "No response from DHIS2",
                }
            )
            current_run.log_error(
                f"Error importing chunk {idx}: no response from DHIS2 (strategy={import_mode})"
            )
            continue

        try:
            resp_data = response.json()
        except Exception:
            resp_data = {}

        if response.status_code != 200:
            aggregated["failed_chunks"] += 1
            aggregated["errors_total"] += 1
            aggregated["chunks"].append(
                {
                    "index": idx,
                    "size": len(chunk),
                    "attempts": attempts,
                    "status": "failed",
                    "status_code": response.status_code,
                    "summary": resp_data,
                    "issues": [],
                    "error": response.text,
                }
            )
            current_run.log_error(
                f"Error importing chunk {idx}: {response.text} (strategy={import_mode})"
            )
            continue

        chunk_summary = resp_data.get("response", resp_data)
        import_counts, counts_source = _extract_import_counts(chunk_summary)

        for conflict in chunk_summary.get("conflicts", []) or []:
            current_run.log_warning(
                "Conflict in chunk {i}: {obj} - {val}".format(
                    i=idx, obj=conflict.get("object", ""), val=conflict.get("value", "")
                )
            )
            issues.append(dict(conflict))

        if counts_source is None:
            current_run.log_warning(
                f"Chunk {idx} succeeded but no import counters were found in DHIS2 response."
            )

        aggregated["successful_chunks"] += 1
        aggregated["conflicts_total"] += len(issues)
        aggregated["chunks"].append(
            {
                "index": idx,
                "size": len(chunk),
                "attempts": attempts,
                "status": "success",
                "status_code": response.status_code,
                "importCount": import_counts,
                "import_count_source": counts_source,
                "issues": issues,
                "summary": chunk_summary,
            }
        )

        aggregated["totals"]["imported"] += import_counts["imported"]
        aggregated["totals"]["updated"] += import_counts["updated"]
        aggregated["totals"]["ignored"] += import_counts["ignored"]
        aggregated["totals"]["deleted"] += import_counts["deleted"]

    if aggregated["failed_chunks"] > 0:
        aggregated["status"] = "completed_with_errors"

    total_success = aggregated["totals"]["imported"] + aggregated["totals"]["updated"]
    aggregated["imported"] = total_success
    current_run.log_info(
        "Import summary: payload={payload}, chunks_success={success}, chunks_failed={failed}, "
        "created={created}, updated={updated}, ignored={ignored}, deleted={deleted}, "
        "conflicts={conflicts}".format(
            payload=total,
            success=aggregated["successful_chunks"],
            failed=aggregated["failed_chunks"],
            created=aggregated["totals"]["imported"],
            updated=aggregated["totals"]["updated"],
            ignored=aggregated["totals"]["ignored"],
            deleted=aggregated["totals"]["deleted"],
            conflicts=aggregated["conflicts_total"],
        )
    )
    return aggregated


@snis_to_nmdr_sync_orgunits.task
def build_final_report(sync_plan: SyncPlan, import_summary: dict[str, Any]) -> dict[str, Any]:
    """Combine comparison and import summaries into a single reviewable report."""  # noqa: DOC201
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "comparison_summary": sync_plan["comparison_summary"],
        "action_ids": sync_plan["actions"],
        "import_summary": import_summary,
    }


@snis_to_nmdr_sync_orgunits.task
def write_import_report(
    output_dir: Path, payload: list[dict[str, Any]], report: dict[str, Any]
) -> None:
    """Write payload and report files for the current execution."""
    base_output_dir = Path(workspace.files_path) / output_dir
    base_output_dir.mkdir(parents=True, exist_ok=True)

    output_dir = base_output_dir / datetime.now(tz=UTC).strftime("%Y-%m-%d_%H-%M-%S_%f")
    output_dir.mkdir(parents=True, exist_ok=True)

    payload_fp = output_dir / "payload.json"
    with payload_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    report_fp = output_dir / "report.json"
    with report_fp.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    current_run.log_info(f"Import report written to {output_dir.as_posix()}")
    current_run.add_file_output(payload_fp.as_posix())
    current_run.add_file_output(report_fp.as_posix())


@snis_to_nmdr_sync_orgunits.task
def cleanup_old_directory_files(output_dir: Path, _write: None, retention_days: int = 10) -> None:
    """Remove old report directories."""
    output_dir = Path(workspace.files_path) / output_dir
    if not output_dir.exists():
        return

    now = datetime.now()
    for item in output_dir.iterdir():
        if item.is_dir():
            try:
                try:
                    folder_time = datetime.strptime(item.name, "%Y-%m-%d_%H-%M-%S_%f")
                except ValueError:
                    folder_time = datetime.strptime(item.name, "%Y-%m-%d_%H-%M-%S")
                if (now - folder_time).days >= retention_days:
                    for sub_item in item.iterdir():
                        sub_item.unlink()
                    item.rmdir()
                    current_run.log_info(f"Deleted old report directory: {item.as_posix()}")
            except Exception:
                continue


def _log_sync_plan_summary(comparison_summary: Mapping[str, Any]) -> None:
    current_run.log_info(
        "Comparison summary: source={source}, target={target}, create={create}, "
        "close={close}, already_closed={already_closed}, update_core={update_core}, "
        "update_geometry={update_geometry}, unchanged={unchanged}, payload={payload}".format(
            source=comparison_summary["source_total"],
            target=comparison_summary["target_total"],
            create=comparison_summary["to_create"],
            close=comparison_summary["to_close"],
            already_closed=comparison_summary["already_closed"],
            update_core=comparison_summary["to_update_core"],
            update_geometry=comparison_summary["to_update_geometry"],
            unchanged=comparison_summary["unchanged"],
            payload=comparison_summary["payload_total"],
        )
    )


def check_server_health(dhis2: DHIS2) -> bool:
    """Check if the DHIS2 server is responding."""  # noqa: DOC201
    try:
        dhis2.ping()  # type: ignore
        current_run.log_info(f"✅ Serveur DHIS2 {dhis2.api.url} accessible")
        return True
    except ConnectionError as err:
        current_run.log_error(f"❌ Impossible d'atteindre l'instance DHIS2 à {dhis2.api.url}")
        raise ConnectionError(
            f"Impossible d'atteindre l'instance DHIS2 à l'URL {dhis2.api.url}"
        ) from err


if __name__ == "__main__":
    snis_to_nmdr_sync_orgunits()
