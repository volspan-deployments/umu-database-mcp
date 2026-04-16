from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse
import uvicorn
from fastmcp import FastMCP
import httpx
import os
import csv
import json
import io
from typing import Optional

mcp = FastMCP("UMU Database API")

BASE_URL = "https://umu.openwinecomponents.org"
GOG_GAMESDB_URL = "https://gamesdb.gog.com/games"
PLATFORM_WHITELIST = {"steam", "amazon", "battlenet", "origin", "epic", "humble", "itch", "gog", "uplay"}
SUPPORTED_STORES = {"amazon", "battlenet", "ea", "egs", "gog", "humble", "itchio", "steam", "ubisoft", "umu", "zoomplatform", "none"}
CSV_URL = "https://raw.githubusercontent.com/Open-Wine-components/umu-database/main/umu-database.csv"


@mcp.tool()
async def query_umu_database(
    store: Optional[str] = None,
    codename: Optional[str] = None,
    umu_id: Optional[str] = None,
    title: Optional[str] = None,
) -> dict:
    """Query the live UMU database API to look up games by store, codename, UMU ID, or title.
    Use this when you need to find a game's UMU ID, title, or store release information.
    Supports filtering by any combination of store, codename, umu_id, and title parameters.
    Omit all parameters to list all entries."""
    params = {}
    if store is not None:
        params["store"] = store
    if codename is not None:
        params["codename"] = codename
    if umu_id is not None:
        params["umu_id"] = umu_id
    if title is not None:
        params["title"] = title

    url = f"{BASE_URL}/umu_api.php"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        try:
            data = response.json()
        except Exception:
            data = response.text
        return {
            "status": "success",
            "query_params": params,
            "results": data,
        }


@mcp.tool()
async def validate_umu_csv(csv_path: str) -> dict:
    """Validate a UMU database CSV file for correctness before importing or submitting.
    Checks column count, required fields, supported store values, and duplicate entries.
    Use this before running an import or when reviewing a CSV for contribution.
    Returns validation errors in GitHub Actions annotation format."""
    errors = []
    warnings = []

    if not os.path.exists(csv_path):
        return {
            "status": "error",
            "message": f"File not found: {csv_path}",
            "errors": [],
            "warnings": [],
        }

    expected_columns = 6
    required_fields = {0: "TITLE", 1: "STORE", 2: "CODENAME", 3: "UMU_ID"}
    seen_entries = set()

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            header = None
            for line_num, row in enumerate(reader, start=1):
                if line_num == 1:
                    header = row
                    if len(row) != expected_columns:
                        errors.append(
                            f"::error file={csv_path},line={line_num}::Header has {len(row)} columns, expected {expected_columns}"
                        )
                    continue

                if len(row) != expected_columns:
                    errors.append(
                        f"::error file={csv_path},line={line_num}::Row has {len(row)} columns, expected {expected_columns}: {row}"
                    )
                    continue

                title_val = row[0].strip()
                store_val = row[1].strip().lower()
                codename_val = row[2].strip()
                umu_id_val = row[3].strip()

                for col_idx, col_name in required_fields.items():
                    if not row[col_idx].strip():
                        errors.append(
                            f"::error file={csv_path},line={line_num}::Missing required field '{col_name}' in row: {row}"
                        )

                if store_val and store_val not in SUPPORTED_STORES:
                    errors.append(
                        f"::error file={csv_path},line={line_num}::Unsupported store value '{store_val}'. Supported: {sorted(SUPPORTED_STORES)}"
                    )

                if umu_id_val and not umu_id_val.startswith("umu-"):
                    warnings.append(
                        f"::warning file={csv_path},line={line_num}::UMU_ID '{umu_id_val}' does not follow 'umu-XXXXX' format"
                    )

                entry_key = (store_val, codename_val)
                if entry_key in seen_entries:
                    errors.append(
                        f"::error file={csv_path},line={line_num}::Duplicate entry for store='{store_val}', codename='{codename_val}'"
                    )
                else:
                    seen_entries.add(entry_key)

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to read CSV: {str(e)}",
            "errors": [],
            "warnings": [],
        }

    is_valid = len(errors) == 0
    return {
        "status": "valid" if is_valid else "invalid",
        "csv_path": csv_path,
        "total_errors": len(errors),
        "total_warnings": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "message": "CSV is valid" if is_valid else f"CSV has {len(errors)} error(s)",
    }


@mcp.tool()
async def import_umu_database() -> dict:
    """Trigger a full rebuild of the MySQL UMU database from the upstream GitHub CSV source.
    Drops and recreates the game and gamerelease tables, then imports all records in a single transaction.
    Use this to refresh the local database with the latest upstream data, typically on a schedule or after upstream CSV changes.
    NOTE: This tool fetches the upstream CSV and reports what would be imported. Actual DB write requires local MySQL credentials."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(CSV_URL)
        response.raise_for_status()
        csv_content = response.text

    reader = csv.DictReader(io.StringIO(csv_content))
    rows = list(reader)

    games_seen = {}
    release_rows = []
    errors = []

    for i, row in enumerate(rows, start=2):
        umu_id = row.get("UMU_ID", "").strip()
        title = row.get("TITLE", "").strip()
        acronym = row.get("COMMON ACRONYM (Optional)", "").strip() or None
        codename = row.get("CODENAME", "").strip()
        store = row.get("STORE", "").strip()
        exe_string = row.get("EXE_STRING (Optional)", "").strip() or None
        notes = row.get("NOTE (Optional)", "").strip() or None

        if not umu_id or not title:
            errors.append(f"Row {i}: Missing UMU_ID or TITLE")
            continue

        if umu_id not in games_seen:
            games_seen[umu_id] = {"umu_id": umu_id, "title": title, "acronym": acronym}

        release_rows.append({
            "umu_id": umu_id,
            "codename": codename,
            "store": store,
            "exe_string": exe_string,
            "notes": notes,
        })

    return {
        "status": "success",
        "source": CSV_URL,
        "unique_games": len(games_seen),
        "total_releases": len(release_rows),
        "parse_errors": errors,
        "message": (
            f"Fetched upstream CSV successfully. Found {len(games_seen)} unique games and "
            f"{len(release_rows)} release entries. "
            "Actual database import requires local MySQL credentials (not available in this MCP context)."
        ),
        "sample_games": list(games_seen.values())[:10],
    }


@mcp.tool()
async def search_gog_galaxy_db(title: str) -> dict:
    """Search the GOG Galaxy gamesdb API for games matching a title.
    Returns matching games filtered to supported platforms along with their platform-specific release IDs.
    Use this to look up GOG release IDs, platform availability, or to cross-reference a game title against GOG's catalog."""
    import urllib.parse

    params = urllib.parse.urlencode({"title": title})
    url = f"{GOG_GAMESDB_URL}?{params}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, headers={"Accept": "application/json"})
        response.raise_for_status()
        json_data = response.json()

    results = []
    for item in json_data.get("items", []):
        platform_ids = {release["platform_id"] for release in item.get("releases", [])}
        if not PLATFORM_WHITELIST.intersection(platform_ids):
            continue

        first_release_date = item.get("first_release_date")
        year = None
        if first_release_date:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(first_release_date)
                year = dt.strftime("%Y")
            except Exception:
                year = first_release_date

        filtered_releases = [
            {"platform_id": r["platform_id"], "external_id": r.get("external_id")}
            for r in item.get("releases", [])
            if r["platform_id"] in PLATFORM_WHITELIST
        ]

        results.append({
            "title": item.get("title", {}).get("*", "Unknown"),
            "year": year,
            "releases": filtered_releases,
        })

    return {
        "status": "success",
        "query_title": title,
        "total_results": len(results),
        "results": results,
    }


@mcp.tool()
async def find_missing_amazon_games(library_path: str, database_path: str) -> dict:
    """Cross-reference an Amazon game library JSON file against a UMU database CSV file and identify
    Amazon games not yet present in the UMU database. Outputs CSV-formatted lines including Steam ID
    and UMU ID for each missing game. Use this when adding Amazon games to the UMU database or auditing
    Amazon library coverage."""
    if not os.path.exists(library_path):
        return {
            "status": "error",
            "message": f"Library file not found: {library_path}",
        }

    if not os.path.exists(database_path):
        return {
            "status": "error",
            "message": f"Database file not found: {database_path}",
        }

    # Load existing Amazon game IDs from the database CSV
    amazon_game_ids = set()
    try:
        with open(database_path, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            header = True
            for row in reader:
                if header:
                    header = False
                    continue
                if len(row) >= 3:
                    store = row[1].strip()
                    codename = row[2].strip()
                    if store == "amazon":
                        amazon_game_ids.add(codename)
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to read database CSV: {str(e)}",
        }

    # Load Amazon library JSON
    try:
        with open(library_path, "r", encoding="utf-8") as f:
            library_data = json.load(f)
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to read library JSON: {str(e)}",
        }

    missing_games = []
    csv_lines = []

    for game in library_data:
        try:
            steam_url = game["product"]["productDetail"]["details"]["websites"]["steam"]
        except (KeyError, TypeError):
            continue

        product_id = game["product"]["id"]
        if product_id in amazon_game_ids:
            continue

        parts = steam_url.split("/")
        if len(parts) < 5:
            continue
        steam_id = parts[4]
        title = game["product"]["title"]

        csv_title = f'"{title}"' if "," in title else title
        csv_line = f"{csv_title},amazon,{product_id},umu-{steam_id},,"
        csv_lines.append(csv_line)

        missing_games.append({
            "title": title,
            "store": "amazon",
            "codename": product_id,
            "umu_id": f"umu-{steam_id}",
            "steam_id": steam_id,
            "csv_line": csv_line,
        })

    return {
        "status": "success",
        "library_path": library_path,
        "database_path": database_path,
        "existing_amazon_entries": len(amazon_game_ids),
        "missing_count": len(missing_games),
        "missing_games": missing_games,
        "csv_output": "\n".join(csv_lines) if csv_lines else "",
        "message": f"Found {len(missing_games)} Amazon game(s) missing from the UMU database.",
    }




async def health(request):
    return JSONResponse({"status": "ok", "server": mcp.name})

async def tools(request):
    registered = await mcp.list_tools()
    tool_list = [{"name": t.name, "description": t.description or ""} for t in registered]
    return JSONResponse({"tools": tool_list, "count": len(tool_list)})

mcp_app = mcp.http_app(transport="streamable-http", stateless_http=True)

class _FixAcceptHeader:
    """Ensure Accept header includes both types FastMCP requires."""
    def __init__(self, app):
        self.app = app
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            accept = headers.get(b"accept", b"").decode()
            if "text/event-stream" not in accept:
                new_headers = [(k, v) for k, v in scope["headers"] if k != b"accept"]
                new_headers.append((b"accept", b"application/json, text/event-stream"))
                scope = dict(scope, headers=new_headers)
        await self.app(scope, receive, send)

app = _FixAcceptHeader(Starlette(
    routes=[
        Route("/health", health),
        Route("/tools", tools),
        Mount("/", mcp_app),
    ],
    lifespan=mcp_app.lifespan,
))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
