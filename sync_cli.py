import os
import asyncio
import time
import json
import traceback
from datetime import datetime
from core.redash import RedashClient
from core.engine import DataEngine
from core.config import Config

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# --- DB書き込み専用のコンシューマー（ワーカー） ---
async def db_writer(queue, engine, buffer_threshold):
    ranking_buffer = []
    dd_buffer = []
    total_ranking = 0
    total_dd = 0
    
    last_save_time = time.time()
    save_interval = 300  # 5分に1回セーブ（I/Oボトネック回避）

    while True:
        item = await queue.get()
        try:
            if item is None:
                # 終了シグナル: 残りのバッファを最終フラッシュ
                if ranking_buffer:
                    engine.append_data(ranking_buffer, is_drilldown=False)
                    total_ranking += len(ranking_buffer)
                if dd_buffer:
                    engine.append_data(dd_buffer, is_drilldown=True)
                    total_dd += len(dd_buffer)
                
                log("Finalizing DB writer... Performing final periodic save.")
                engine.save_to_parquet()
                engine.save_to_csv()
                break
                
            chunk_num, rank_data, dd_data = item
            
            if rank_data: ranking_buffer.extend(rank_data)
            if dd_data: dd_buffer.extend(dd_data)
            
            # バッファ閾値でフラッシュ
            if len(ranking_buffer) >= buffer_threshold:
                log(f"Flushing {len(ranking_buffer)} ranking rows to engine...")
                engine.append_data(ranking_buffer, is_drilldown=False)
                total_ranking += len(ranking_buffer)
                ranking_buffer.clear()
                
            if len(dd_buffer) >= buffer_threshold:
                log(f"Flushing {len(dd_buffer)} drilldown rows to engine...")
                engine.append_data(dd_buffer, is_drilldown=True)
                total_dd += len(dd_buffer)
                dd_buffer.clear()
            
            current_time = time.time()
            if current_time - last_save_time > save_interval:
                log("Performing periodic save to disk (Interval reached)...")
                engine.save_to_parquet()
                engine.save_to_csv()
                last_save_time = current_time

        except asyncio.CancelledError:
            log("DB Writer was cancelled.")
            raise
        except Exception as e:
            log(f"CRITICAL ERROR in DB Writer: {e}")
            log(traceback.format_exc())
            raise
        finally:
            # 何があっても必ずタスク完了をマーク (デッドロック防止)
            queue.task_done()
            
    return total_ranking, total_dd

async def do_sync_headless():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')
    config = Config(config_path)
    
    redash_url = os.environ.get("REDASH_URL", config.get("url"))
    redash_key = os.environ.get("REDASH_KEY", config.get("key"))
    q993 = os.environ.get("REDASH_Q993", config.get("q993"))
    q994 = os.environ.get("REDASH_Q994", config.get("q994"))
    q1011 = os.environ.get("REDASH_Q1011", config.get("q1011"))
    
    sync_target = os.environ.get("SYNC_TARGET", "full")
    if sync_target == "full":
        now = datetime.now()
        default_end = now.strftime("%Y-%m")
        three_years_ago = now.year - 3
        default_start = f"{three_years_ago}-01"
        start_date = os.environ.get("SYNC_START_DATE", default_start)
        end_date = os.environ.get("SYNC_END_DATE", default_end)
        log(f"Target Range: {start_date} to {end_date} (3-Year Dynamic Window)")
    else:
        start_date = os.environ.get("SYNC_START_DATE", config.get("start_date", "2024-01"))
        end_date = os.environ.get("SYNC_END_DATE", config.get("end_date", "2024-12"))
        
    voucher_type = os.environ.get("SYNC_VOUCHER_TYPE", config.get("voucher_type", "all"))
    threads = int(os.environ.get("SYNC_THREADS", config.get("threads", 5)))
    sync_mode = os.environ.get("SYNC_MODE", "diff")
    
    raw_limit = os.environ.get("SYNC_LIMIT", config.get("limit"))
    try:
        sync_limit = int(raw_limit) if raw_limit and str(raw_limit).lower() != 'none' else None
    except ValueError:
        sync_limit = None

    raw_offset = os.environ.get("SYNC_OFFSET", config.get("offset"))
    try:
        sync_offset = int(raw_offset)
    except ValueError:
        sync_offset = 0

    if not redash_url or not redash_key:
        log("ERROR: REDASH_URL or REDASH_KEY is missing.")
        return

    engine = DataEngine(base_dir)
    engine.initialize_db()
    
    if sync_mode == "clean":
        log("Full Sync (Clean) selected. Resetting local data...")
        engine.reset_data()
    else:
        log("Differential Sync selected. Loading existing data...")
        if not engine.has_data():
            if os.path.exists(engine.zip_path):
                log(f"Extracting existing state from {engine.zip_path}...")
                engine.load_from_zip(engine.zip_path, progress_callback=lambda m: log(f"[ZIP] {m}"))
                
        if not engine.has_data():
            engine.reload_master_data()
            engine.reload_drilldown_data()

    rc = RedashClient(redash_url, redash_key)
    
    log(f"Fetching client list from Redash (Query {q993})...")
    raw_clients = await rc.fetch_query(q993)
    
    seen_ids = set()
    all_clients = []
    for c in raw_clients:
        cid = str(c.get("client_id") or "")
        if cid and cid not in seen_ids:
            seen_ids.add(cid)
            all_clients.append(c)
    
    total_unique = len(all_clients)
    log(f"Fetched {len(raw_clients)} rows. Found {total_unique} unique clients.")
    
    try:
        if not engine.has_data():
            if os.path.exists(engine.zip_path):
                log(f"Restoring previous state from existing ZIP: {engine.zip_path}")
                engine.load_from_zip(engine.zip_path)
            else:
                engine.reload_master_data()
                engine.reload_drilldown_data()
        
        up_to_date_clients = set()
        
        # 抽象化されたテーブル存在チェック（無い場合は安全なフォールバック）
        has_table = False
        if hasattr(engine, "table_exists"):
            has_table = engine.table_exists("master_data")
        else:
            try:
                tables = [t[0] for t in engine.conn.execute("SELECT name FROM sqlite_master WHERE type='table' UNION SELECT name FROM sqlite_temp_master WHERE type='table' UNION SELECT name FROM main.sqlite_master WHERE type='table';").fetchall()]
                has_table = "master_data" in tables
            except Exception:
                tables = [t[0] for t in engine.conn.execute("SHOW TABLES").fetchall()]
                has_table = "master_data" in tables

        if has_table:
            rows = engine.conn.execute('SELECT DISTINCT "クライアントID" FROM master_data WHERE "処理月" = ?', (end_date,)).fetchall()
            up_to_date_clients = {str(r[0]) for r in rows if r[0]}
            log(f"Found {len(up_to_date_clients)} clients already up-to-date for {end_date}.")
        
        clients_to_process = [c for c in all_clients if str(c.get("client_id")) not in up_to_date_clients]
        skip_count = total_unique - len(clients_to_process)
        if skip_count > 0:
            log(f"Skipping {skip_count} up-to-date clients. Remaining: {len(clients_to_process)}")
        else:
            log(f"No clients to skip. Proceeding with {len(clients_to_process)} clients.")
            
    except Exception as e:
        log(f"Warning: Failed to check incremental status, proceeding with full sync: {e}")
        log(traceback.format_exc())
        clients_to_process = all_clients
    
    in_db_ids = set()
    try:
        rows = engine.conn.execute('SELECT DISTINCT "クライアントID" FROM master_data').fetchall()
        in_db_ids = {str(r[0]) for r in rows if r[0]}
    except Exception as e:
        log(f"Notice: Failed to fetch existing client IDs (may be initial run): {e}")
    
    clients_to_process.sort(key=lambda c: str(c.get("client_id")) not in in_db_ids)
    
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
        
        try:
            status_data = {
                "last_offset": sync_offset,
                "total_clients": len(clients_to_process),
                "timestamp": datetime.now().isoformat(),
                "failed_chunks": []
            }
            with open(os.path.join(base_dir, 'sync_status.json'), 'w', encoding='utf-8') as sf:
                json.dump(status_data, sf, indent=4, ensure_ascii=False)
        except Exception as e:
            log(f"Failed to save status json: {e}")
            
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write("has_more=false\n")
                f.write(f"next_offset={sync_offset}\n")
                
        log("SYNC COMPLETE. (No new data to fetch)")
        return
    
    name_map = {str(c.get("client_id")): (c.get("enterprise_name") or c.get("client_name") or "") for c in all_clients if c.get("client_id")}

    chunk_size = 50
    sem = asyncio.Semaphore(threads)
    queue = asyncio.Queue(maxsize=threads * 2)
    writer_task = asyncio.create_task(db_writer(queue, engine, buffer_threshold=2500))
    failed_chunks = []

    async def fetch_chunk(chunk, chunk_num):
        async with sem:
            params = {f"id{idx+1}": str(c.get("client_id")) for idx, c in enumerate(chunk)}
            for idx in range(len(chunk), 50):
                params[f"id{idx+1}"] = "0"
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Writerが死んでいたら即座にフェッチを中止（デッドロック防止）
                    if writer_task.done():
                        raise RuntimeError("DB Writer stopped unexpectedly. Aborting fetch to prevent deadlock.")

                    data_994, data_1011 = [], []
                    
                    if sync_target in ("full", "client"):
                        data_994 = await rc.fetch_query(q994, parameters=params)
                        for row in data_994:
                            cid = str(row.get("client_id") or row.get("クライアントID") or "")
                            if cid in name_map:
                                if "enterprise_name" not in row: row["enterprise_name"] = name_map[cid]
                                if "企業名" not in row: row["企業名"] = name_map[cid]
                                
                    if sync_target in ("full", "drilldown"):
                        dd_params = params.copy()
                        dd_params.update({
                            "start_date": start_date,
                            "end_date": end_date,
                            "voucher_type": voucher_type,
                            "item_filter": "overall"
                        })
                        data_1011 = await rc.fetch_query(q1011, parameters=dd_params)
                    
                    log(f"Chunk {chunk_num} fetched. Enqueuing... (Ranking: {len(data_994)}, DD: {len(data_1011)})")
                    await queue.put((chunk_num, data_994, data_1011))
                    return chunk_num, True
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # Exponential Backoff によるリトライ
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        log(f"Transient error in Chunk {chunk_num} (Attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        log(f"FATAL ERROR in Chunk {chunk_num} after {max_retries} attempts: {e}")
                        log(traceback.format_exc())
                        return chunk_num, False

    tasks = []
    for i in range(0, total, chunk_size):
        chunk = clients[i:i + chunk_size]
        tasks.append(fetch_chunk(chunk, i // chunk_size + 1))
    
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                log(f"Critical task exception: {res}")
            elif not res[1]:
                failed_chunks.append(res[0])
    finally:
        log("Sending termination signal to DB Writer...")
        await queue.put(None)
        log("Waiting for queue to drain...")
        
        # タイムアウトを設定し、永久ハングを防止
        try:
            await asyncio.wait_for(queue.join(), timeout=300.0)
        except asyncio.TimeoutError:
            log("CRITICAL: Queue drain timed out after 5 minutes. Writer may be deadlocked.")
            raise RuntimeError("Queue drain timeout")

    # WriterTaskのエラーハンドリング
    try:
        total_ranking, total_dd = await writer_task
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log(f"CRITICAL: DB Writer task crashed: {e}")
        raise RuntimeError("Pipeline failed due to DB Writer crash.") from e

    log("Deduplicating data...")
    engine.deduplicate()
    
    log("Final saving to Parquet, CSV and ZIP...")
    try:
        engine.save_to_parquet()
        engine.save_to_csv()
        engine.save_to_zip()
    except Exception as e:
        log(f"Error during final data serialization: {e}")
        log(traceback.format_exc())
        raise RuntimeError("Failed to finalize data files.") from e
    
    try:
        status_data = {
            "last_offset": next_offset,
            "total_clients": len(clients_to_process),
            "timestamp": datetime.now().isoformat(),
            "failed_chunks": failed_chunks
        }
        with open(os.path.join(base_dir, 'sync_status.json'), 'w', encoding='utf-8') as sf:
            json.dump(status_data, sf, indent=4, ensure_ascii=False)
        log(f"Sync status saved: {status_data['last_offset']} / {status_data['total_clients']}")
    except Exception as se:
        log(f"Warning: Failed to save sync status: {se}")
        log(traceback.format_exc())
    
    log(f"SYNC COMPLETE. Total Ranking: {total_ranking}, Total Drilldown: {total_dd}")
    
    has_more = next_offset < len(clients_to_process) and not failed_chunks
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"has_more={'true' if has_more else 'false'}\n")
            f.write(f"next_offset={next_offset}\n")

    if failed_chunks:
        raise RuntimeError(f"Sync failed for chunks: {failed_chunks}. Stopping pipeline to prevent silent data loss.")
        
    if has_more:
        log(f"--- NOTICE: {len(clients_to_process) - len(clients)} clients still pending. More runs needed. ---")
        log(f"Next Run Offset: {next_offset}")

if __name__ == "__main__":
    asyncio.run(do_sync_headless())
