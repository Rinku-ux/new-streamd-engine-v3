import os
import asyncio
import time
import json
import traceback
from datetime import datetime
from core.redash import RedashClient
from core.engine import DataEngine
from core.config import Config

# Helper to print with timestamp
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

async def do_sync_headless():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')
    
    # Load config (Prefer env vars if available)
    config = Config(config_path)
    
    # Environment variables for CI/CD environments (GitHub Actions)
    redash_url = os.environ.get("REDASH_URL", config.get("url"))
    redash_key = os.environ.get("REDASH_KEY", config.get("key"))
    q993 = os.environ.get("REDASH_Q993", config.get("q993"))
    q994 = os.environ.get("REDASH_Q994", config.get("q994"))
    q1011 = os.environ.get("REDASH_Q1011", config.get("q1011"))
    
    sync_target = os.environ.get("SYNC_TARGET", "full") # full, drilldown, client
    if sync_target == "full":
        # Dynamic range: Last 3 years from current month
        from datetime import datetime, timedelta
        now = datetime.now()
        # Default to current month as end
        default_end = now.strftime("%Y-%m")
        # Default to 3 years ago (January of that year to be thorough)
        three_years_ago = now.year - 3
        default_start = f"{three_years_ago}-01"
        
        start_date = os.environ.get("SYNC_START_DATE", default_start)
        end_date = os.environ.get("SYNC_END_DATE", default_end)
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Target Range: {start_date} to {end_date} (3-Year Dynamic Window)")
    else:
        start_date = os.environ.get("SYNC_START_DATE", config.get("start_date", "2024-01"))
        end_date = os.environ.get("SYNC_END_DATE", config.get("end_date", "2024-12"))
        
    voucher_type = os.environ.get("SYNC_VOUCHER_TYPE", config.get("voucher_type", "all"))
    threads = int(os.environ.get("SYNC_THREADS", config.get("threads", 5)))
    sync_mode = os.environ.get("SYNC_MODE", "diff") # diff, clean
    limit = os.environ.get("SYNC_LIMIT", config.get("limit"))
    offset = os.environ.get("SYNC_OFFSET", config.get("offset"))
    
    sync_limit = int(limit) if limit and str(limit).isdigit() else None
    sync_offset = int(offset) if offset and str(offset).isdigit() else 0

    if not redash_url or not redash_key:
        log("ERROR: REDASH_URL or REDASH_KEY is missing.")
        return

    engine = DataEngine(base_dir)
    engine.initialize_db()
    
    if sync_mode == "clean":
        log("Full Sync (Clean) selected. Resetting local data...")
        engine.reset_data()
    else:
        # Load existing data to perform differential merge
        log("Differential Sync selected. Loading existing data...")
        engine.reload_master_data()
        engine.reload_drilldown_data()

    rc = RedashClient(redash_url, redash_key)
    
    # Fetch full client list (q993)
    log(f"Fetching client list from Redash (Query {q993})...")
    raw_clients = await rc.fetch_query(q993)
    
    # --- Deduplicate clients by client_id ---
    seen_ids = set()
    all_clients = []
    for c in raw_clients:
        cid = str(c.get("client_id") or "")
        if cid and cid not in seen_ids:
            seen_ids.add(cid)
            all_clients.append(c)
    
    total_unique = len(all_clients)
    log(f"Fetched {len(raw_clients)} rows. Found {total_unique} unique clients.")
    
    # --- Incremental Sync Logic: Skip clients who already have the latest month ---
    try:
        engine.initialize_db()
        # Ensure local data is loaded
        engine.reload_master_data()
        
        # Check existing clients who HAVE data for the end_date month
        up_to_date_clients = set()
        tables = [t[0] for t in engine.conn.execute("SHOW TABLES").fetchall()]
        if "master_data" in tables:
            # We skip only if the client has data for the TARGET end_date
            rows = engine.conn.execute(
                'SELECT DISTINCT "クライアントID" FROM master_data WHERE "処理月" = ?',
                (end_date,)
            ).fetchall()
            up_to_date_clients = {str(r[0]) for r in rows if r[0]}
            log(f"Found {len(up_to_date_clients)} clients already up-to-date for {end_date}.")
        
        # Filter clients
        clients_to_process = [c for c in all_clients if str(c.get("client_id")) not in up_to_date_clients]
        skip_count = total_unique - len(clients_to_process)
        if skip_count > 0:
            log(f"Skipping {skip_count} up-to-date clients. Remaining: {len(clients_to_process)}")
        else:
            log(f"No clients to skip. Proceeding with {len(clients_to_process)} clients.")
            
    except Exception as e:
        log(f"Warning: Failed to check incremental status, proceeding with full sync: {e}")
        clients_to_process = all_clients
    # ---------------------------------------------------------
    
    # --- Prioritize existing clients (to make sure they are updated first) ---
    in_db_ids = set()
    try:
        rows = engine.conn.execute('SELECT DISTINCT "クライアントID" FROM master_data').fetchall()
        in_db_ids = {str(r[0]) for r in rows if r[0]}
    except: pass
    
    # Sort: clients in DB first, then new ones
    clients_to_process.sort(key=lambda c: str(c.get("client_id")) not in in_db_ids)
    
    # Apply Offset and Limit
    clients = clients_to_process[sync_offset:]
    if sync_limit:
        clients = clients[:sync_limit]
        
    next_offset = sync_offset + len(clients)
    total = len(clients)
    log(f"Processing {total} clients (Offset={sync_offset}, Limit={sync_limit or 'None'}).")
    
    if len(clients) == 0:
        if sync_offset >= len(clients_to_process) and len(clients_to_process) > 0:
            log("SUCCESS: All clients are already processed for this range/mode.")
        else:
            log("No clients matching criteria were found to process.")
        
        # Save completion status even if 0 processed
        try:
            status_data = {
                "last_offset": sync_offset,
                "total_clients": len(clients_to_process),
                "timestamp": datetime.now().isoformat()
            }
            with open(os.path.join(base_dir, 'sync_status.json'), 'w', encoding='utf-8') as sf:
                json.dump(status_data, sf, indent=4, ensure_ascii=False)
        except: pass
        
        log("SYNC COMPLETE. (No new data to fetch)")
        return
    
    name_map = {str(c.get("client_id")): (c.get("enterprise_name") or c.get("client_name") or "") for c in all_clients if c.get("client_id")}

    chunk_size = 50
    sem = asyncio.Semaphore(threads)
    
    ranking_buffer = []
    dd_buffer = []
    buffer_threshold = 2500
    
    total_ranking = 0
    total_dd = 0

    async def fetch_chunk(chunk, chunk_num):
        nonlocal total_ranking, total_dd
        async with sem:
            params = {f"id{idx+1}": str(c.get("client_id")) for idx, c in enumerate(chunk)}
            for idx in range(len(chunk), 50):
                params[f"id{idx+1}"] = "0"
            
            try:
                # 1. Ranking data
                if sync_target in ("full", "client"):
                    data_994 = await rc.fetch_query(q994, parameters=params)
                    for row in data_994:
                        cid = str(row.get("client_id") or row.get("クライアントID") or "")
                        if cid in name_map:
                            if "enterprise_name" not in row: row["enterprise_name"] = name_map[cid]
                            if "企業名" not in row: row["企業名"] = name_map[cid]
                    ranking_buffer.extend(data_994)
                    total_ranking += len(data_994)
                
                # 2. Drilldown data
                if sync_target in ("full", "drilldown"):
                    dd_params = params.copy()
                    dd_params.update({
                        "start_date": start_date,
                        "end_date": end_date,
                        "voucher_type": voucher_type,
                        "item_filter": "overall"
                    })
                    data_1011 = await rc.fetch_query(q1011, parameters=dd_params)
                    dd_buffer.extend(data_1011)
                    total_dd += len(data_1011)
                
                log(f"Chunk {chunk_num} done. (Ranking: {len(ranking_buffer)}, DD: {len(dd_buffer)})")
                
                # Flush if buffer full
                if len(ranking_buffer) >= buffer_threshold or len(dd_buffer) >= buffer_threshold:
                    if len(ranking_buffer) >= buffer_threshold:
                        log(f"Flushing {len(ranking_buffer)} ranking rows to engine...")
                        engine.append_data(ranking_buffer, is_drilldown=False)
                        ranking_buffer.clear()
                    
                    if len(dd_buffer) >= buffer_threshold:
                        log(f"Flushing {len(dd_buffer)} drilldown rows to engine...")
                        engine.append_data(dd_buffer, is_drilldown=True)
                        dd_buffer.clear()
                    
                    # Periodic Save to disk
                    log("Performing periodic save to disk...")
                    engine.save_to_parquet()
                    engine.save_to_csv()

            except Exception as e:
                log(f"ERROR in Chunk {chunk_num}: {e}")
                traceback.print_exc()

    tasks = []
    for i in range(0, total, chunk_size):
        chunk = clients[i:i + chunk_size]
        tasks.append(fetch_chunk(chunk, i // chunk_size + 1))
    
    await asyncio.gather(*tasks)

    # Final flush
    if ranking_buffer:
        log(f"Final flush: {len(ranking_buffer)} ranking rows...")
        engine.append_data(ranking_buffer, is_drilldown=False)
    if dd_buffer:
        log(f"Final flush: {len(dd_buffer)} drilldown rows...")
        engine.append_data(dd_buffer, is_drilldown=True)

    log("Deduplicating data...")
    engine.deduplicate()
    
    log("Final saving to Parquet, CSV and ZIP...")
    engine.save_to_parquet()
    engine.save_to_csv()
    engine.save_to_zip()
    
    # Save sync status to JSON for UI to display progress
    try:
        status_data = {
            "last_offset": sync_offset + len(clients),
            "total_clients": len(clients_to_process),
            "timestamp": datetime.now().isoformat()
        }
        with open(os.path.join(base_dir, 'sync_status.json'), 'w', encoding='utf-8') as sf:
            json.dump(status_data, sf, indent=4, ensure_ascii=False)
        log(f"Sync status saved: {status_data['last_offset']} / {status_data['total_clients']}")
    except Exception as se:
        log(f"Warning: Failed to save sync status: {se}")
    
    log(f"SYNC COMPLETE. Total Ranking: {total_ranking}, Total Drilldown: {total_dd}")
    
    # Signal if more work is needed (for self-chaining workflows)
    has_more = next_offset < len(clients_to_process)
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"has_more={'true' if has_more else 'false'}\n")
            f.write(f"next_offset={next_offset}\n")
    if has_more:
        log(f"--- NOTICE: {len(clients_to_process) - len(clients)} clients still pending. More runs needed. ---")
        log(f"Next Run Offset: {next_offset}")

if __name__ == "__main__":
    asyncio.run(do_sync_headless())
