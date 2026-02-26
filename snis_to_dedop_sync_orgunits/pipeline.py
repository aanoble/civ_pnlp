"""Template for newly generated pipelines."""

import json
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("snis_to_dedop_sync_orgunits")
@parameter(
    "snis_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for SNIS",
    help="DHIS2 connection to fetch SNIS data from.",
    default="snis-dhis2",
    required=True,
)
@parameter(
    "dedop_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for Dedop",
    help="DHIS2 connection to fetch Dedop data from.",
    default="dhis2-nmdr-temp",
    required=True,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help="Directory to save the output files",
    default="sync SNIS DEDOP/data/output",
    required=True,
)
@parameter(
    "sync_existing_geometries",
    type=bool,  # type: ignore
    name="Synchroniser les geometries existantes",
    help="Mettre a jour les geometries des org units deja presentes dans Dedop",
    default=True,
    required=False,
)
@parameter(
    "dry_run",
    type=bool,  # type: ignore
    name="Dry run mode",
    help="If True, the pipeline will run in dry-run mode and will not write any data to Dedop.",
    default=False,
    required=False,
)
@parameter(
    "import_mode",
    type=str,  # type: ignore
    name="Import mode",
    help=(
        "The import strategy to use when pushing data to Dedop. "
        "Can be 'CREATE', 'UPDATE' or 'CREATE_AND_UPDATE'."
    ),
    default="CREATE_AND_UPDATE",
    required=False,
    choices=["CREATE", "UPDATE", "CREATE_AND_UPDATE"],
)
def snis_to_dedop_sync_orgunits(  # noqa: D417
    snis_connection: DHIS2Connection,
    dedop_connection: DHIS2Connection,
    output_directory: str,
    sync_existing_geometries: bool,
    dry_run: bool = False,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
):
    """Pipeline to sync organisation units from SNIS to Dedop.

    Parameters
    ----------
    snis_connection : DHIS2Connection
        The DHIS2 connection to fetch SNIS data from.
    dedop_connection : DHIS2Connection
        The DHIS2 connection to fetch Dedop data from.
    output_directory : str
        The directory to save the output files.
    dry_run : bool, optional
        If True, the pipeline will run in dry-run mode and will not write any data to Dedop, by default False.
    import_mode : str, optional
        The import strategy to use when pushing data to Dedop. Can be "CREATE", "UPDATE" or "CREATE_AND_UPDATE". By default "CREATE_AND_UPDATE".
    post_batch_size : int, optional
        The batch size to use when pushing data to Dedop. By default 5000.
    """  # noqa: E501
    snis = DHIS2(connection=snis_connection)
    dedop = DHIS2(connection=dedop_connection)

    check_server_health(snis)
    check_server_health(dedop)

    ou_snis = fetch_org_units(snis)
    ou_dedop = fetch_org_units(dedop)

    payload = prepare_organisation_units(ou_snis, ou_dedop, sync_existing_geometries)

    summary = push_data_to_dhis2(dedop, payload, dry_run, import_mode, post_batch_size)

    write = write_import_report((Path(output_directory) / "orgUnits"), payload, summary)

    cleanup_old_directory_files((Path(output_directory) / "orgUnits"), write)


@snis_to_dedop_sync_orgunits.task
def fetch_org_units(dhis2: DHIS2) -> pl.DataFrame:
    """Fetch organisation units from DHIS2.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to fetch data from.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the organisation units.
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


@snis_to_dedop_sync_orgunits.task
def prepare_organisation_units(
    ou_snis: pl.DataFrame, ou_dedop: pl.DataFrame, sync_existing_geometries: bool
) -> list[dict]:
    """Prepare the payload for the sync between SNIS and Dedop.

    Parameters
    ----------
    ou_snis : pl.DataFrame
        A DataFrame containing the organisation units from SNIS.
    ou_dedop : pl.DataFrame
        A DataFrame containing the organisation units from Dedop.
    sync_existing_geometries : bool
        Whether to update geometries of existing org units in Dedop when they differ from SNIS

    Returns
    -------
    list[dict]
        A list of dictionaries containing the organisation units to create or update in Dedop.
    """
    current_run.log_info(
        f"Comparing {len(ou_snis)} SNIS org units with {len(ou_dedop)} Dedop org units..."
    )
    snis_by_id = {row["id"]: row for row in ou_snis.iter_rows(named=True)}
    dedop_by_id = {row["id"]: row for row in ou_dedop.iter_rows(named=True)}

    snis_ids = set(snis_by_id)
    dedop_ids = set(dedop_by_id)

    payload: list[dict] = []

    # Orgunit to close in Dedop
    ids_to_close = sorted(dedop_ids - snis_ids)
    if ids_to_close:
        ids_to_close_pending = [
            ou_id for ou_id in ids_to_close if not dedop_by_id[ou_id].get("closedDate")
        ]
        if ids_to_close_pending:
            current_run.log_info(
                f"{len(ids_to_close_pending)} organisation units to close in Dedop "
                f"(out of {len(ids_to_close)} missing from source)"
            )
            for ou_id in ids_to_close_pending:
                payload.append(_build_orgunit_payload(dedop_by_id[ou_id], closing=True))

    # Orgunit to create in Dedop
    ids_to_create = sorted(snis_ids - dedop_ids)
    if ids_to_create:
        current_run.log_info(f"{len(ids_to_create)} organisation units to create in Dedop")
        for ou_id in ids_to_create:
            payload.append(_build_orgunit_payload(snis_by_id[ou_id], include_geometry=True))

    # Existing orgunits: update when core fields and/or geometry changed.
    ids_to_update_core = []
    ids_to_update_geometry = []
    for ou_id in sorted(snis_ids & dedop_ids):
        core_changed = _core_signature(snis_by_id[ou_id]) != _core_signature(dedop_by_id[ou_id])
        geometry_changed = sync_existing_geometries and _geometry_signature(
            snis_by_id[ou_id]
        ) != _geometry_signature(dedop_by_id[ou_id])

        if core_changed:
            ids_to_update_core.append(ou_id)
        if geometry_changed:
            ids_to_update_geometry.append(ou_id)

        if core_changed or geometry_changed:
            payload.append(
                _build_orgunit_payload(
                    snis_by_id[ou_id],
                    include_geometry=bool(geometry_changed),
                )
            )

    if ids_to_update_core:
        current_run.log_info(
            f"{len(ids_to_update_core)} organisation units to update with core field changes"
        )
    if ids_to_update_geometry:
        current_run.log_info(
            f"{len(ids_to_update_geometry)} organisation units to update with geometry changes"
        )

    return payload


def _build_orgunit_payload(
    ou: Mapping[str, object], closing: bool = False, include_geometry: bool = False
) -> dict:
    payload: dict[str, object] = {
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
            # Compatibility fallback for legacy geometry representation.
            payload["featureType"] = ou["featureType"]
            payload["coordinates"] = ou["coordinates"]

    return payload


def _geometry_signature(ou: Mapping[str, object]) -> str:
    geometry = ou.get("geometry")
    if geometry is not None:
        return json.dumps(geometry, sort_keys=True, separators=(",", ":"))

    # Legacy fallback to detect geometry differences when `geometry` is absent.
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


@snis_to_dedop_sync_orgunits.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    payload: list[dict],
    dry_run: bool,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
) -> dict:
    """Envoi des données à DHIS2 avec découpage en chunks et retry.

    Args:
        dhis2: Client DHIS2 configuré
        payload: Données à importer
        dry_run: Mode test sans écriture
        import_mode: Stratégie d'import DHIS2 (CREATE, UPDATE, CREATE_AND_UPDATE)
        post_batch_size: Taille des lots pour les requêtes POST DHIS2

    Returns:
        Dict de résumé d'import agrégé.
    """
    total = len(payload)
    if total == 0:
        return {"status": "skipped", "imported": 0}

    def _chunks(seq: list[dict], size: int):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    aggregated: dict = {
        "status": "completed",
        "import_strategy": import_mode,
        "dry_run": dry_run,
        "total": total,
        "chunks": [],
        "totals": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
    }

    request_params = {"dryRun": dry_run, "importStrategy": import_mode}
    max_retries = 2
    backoff_base = 1.0
    url = dhis2.api.url + "/metadata.json"

    for idx, chunk in enumerate(_chunks(payload, post_batch_size), start=1):
        import_counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
        issues: list = []

        response = None
        for attempt in range(1, max_retries + 1):
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
            # Non-retryable error
            break

        if response is None:
            aggregated["chunks"].append(
                {"index": idx, "size": len(chunk), "summary": {}, "status": "failed"}
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
            aggregated["chunks"].append(
                {"index": idx, "size": len(chunk), "summary": resp_data, "status": "failed"}
            )
            current_run.log_error(
                f"Error importing chunk {idx}: {response.text} (strategy={import_mode})"
            )
            continue

        chunk_summary = resp_data.get("response", resp_data)
        if "importCount" in chunk_summary:
            ic = chunk_summary.get("importCount", {})
            import_counts["ignored"] = ic.get("ignored", 0)
            import_counts["imported"] = ic.get("imported", 0)
            import_counts["updated"] = ic.get("updated", 0)
            import_counts["deleted"] = ic.get("deleted", 0)

        for conflict in chunk_summary.get("conflicts", []) or []:
            current_run.log_warning(
                "Conflict in chunk {i}: {obj} - {val}".format(
                    i=idx, obj=conflict.get("object", ""), val=conflict.get("value", "")
                )
            )
            issues.append(conflict)

        aggregated["chunks"].append(
            {
                "index": idx,
                "size": len(chunk),
                "importCount": import_counts,
                "issues": issues,
                "status": "success",
            }
        )

        aggregated["totals"]["imported"] += import_counts["imported"]
        aggregated["totals"]["updated"] += import_counts["updated"]
        aggregated["totals"]["ignored"] += import_counts["ignored"]
        aggregated["totals"]["deleted"] += import_counts["deleted"]

    total_success = aggregated["totals"]["imported"] + aggregated["totals"]["updated"]
    aggregated["imported"] = total_success
    current_run.log_info(
        f"Imported {total_success}/{total} data values to DHIS2 (strategy={import_mode})"
    )
    return aggregated  # type: ignore


@snis_to_dedop_sync_orgunits.task
def write_import_report(output_dir: Path, payload: list[dict], summary: dict) -> None:
    """Génère les rapports d'import.

    Args:
        output_dir: Répertoire de sortie
        payload: Données envoyées
        summary: Résumé DHIS2
    """
    if len(payload) == 0:
        current_run.log_info("Aucun enregistrement à écrire dans le rapport d'import.")
        return

    base_output_dir = Path(workspace.files_path) / output_dir
    base_output_dir.mkdir(parents=True, exist_ok=True)

    output_dir = base_output_dir / datetime.now(tz=UTC).strftime("%Y-%m-%d_%H-%M-%S_%f")
    output_dir.mkdir(parents=True, exist_ok=True)

    payload_fp = output_dir / "payload.json"
    with payload_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    report_fp = output_dir / "report.json"
    with report_fp.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    current_run.log_info(f"Import report written to {output_dir.as_posix()}")
    current_run.add_file_output(payload_fp.as_posix())
    current_run.add_file_output(report_fp.as_posix())

    return


@snis_to_dedop_sync_orgunits.task
def cleanup_old_directory_files(output_dir: Path, _write: None, retention_days: int = 10) -> None:
    """Supprime les anciens fichiers de rapport.

    Pour avoir une chronologie des exécutions des tâches les deux paramètres
    ont été rajoutés mais ils ne sont pas utilisés dans la tâche.

    Args:
        output_dir: Répertoire de sortie
        _write: Paramètre factice pour la chronologie
        retention_days: Nombre de jours à conserver
    """
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


def check_server_health(dhis2: DHIS2) -> bool:
    """Check if the DHIS2 server is responding.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check.

    Returns
    -------
        bool: True if the server is responding, raises ConnectionError otherwise.
    """
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
    snis_to_dedop_sync_orgunits()
