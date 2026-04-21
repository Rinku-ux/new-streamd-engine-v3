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
    
    start_date = os.environ.get("SYNC_START_DATE", config.get("start_date", "2024-01"))
    end_date = os.environ.get("SYNC_END_DATE", config.get("end_date", "2024-12"))
    voucher_type = os.environ.get("SYNC_VOUCHER_TYPE", config.get("voucher_type", "all"))
    threads = int(os.environ.get("SYNC_THREADS", config.get("threads", 5)))
    sync_target = os.environ.get("SYNC_TARGET", "full") # full, drilldown, client
    sync_mode = os.environ.get("SYNC_MODE", "diff") # diff, clean

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
    
    log(f"Fetching client list (Query {q993})...")
    clients = await rc.fetch_query(q993)
    total = len(clients)
    log(f"Found {total} clients.")
    
    name_map = {str(c.get("client_id")): (c.get("enterprise_name") or c.get("client_name") or "") for c in clients if c.get("client_id")}

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
                if len(ranking_buffer) >= buffer_threshold:
                    log(f"Flushing {len(ranking_buffer)} ranking rows to engine...")
                    engine.append_data(ranking_buffer, is_drilldown=False)
                    ranking_buffer.clear()
                
                if len(dd_buffer) >= buffer_threshold:
                    log(f"Flushing {len(dd_buffer)} drilldown rows to engine...")
                    engine.append_data(dd_buffer, is_drilldown=True)
                    dd_buffer.clear()

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
    
    log("Saving to Parquet and CSV...")
    engine.save_to_parquet()
    engine.save_to_csv()
    
    log(f"Sync complete. Total rows: Ranking={total_ranking}, Drilldown={total_dd}")

if __name__ == "__main__":
    asyncio.run(do_sync_headless())
