import os
import csv
import duckdb
import zipfile
import traceback
import threading
import subprocess
import shutil
import tempfile
import pandas as pd
import requests
import urllib.parse
from datetime import datetime


class DataEngine:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        # Paths for both CSV (legacy/source) and Parquet (optimized storage)
        self.master_csv = os.path.join(base_dir, 'streamdbi_ranking_data_master.csv')
        self.drilldown_csv = os.path.join(base_dir, 'streamdbi_drilldown_data_master.csv')
        self.master_parquet = os.path.join(base_dir, 'streamdbi_ranking_data_master.parquet')
        self.drilldown_parquet = os.path.join(base_dir, 'streamdbi_drilldown_data_master.parquet')
        
        self.zip_path = os.path.join(base_dir, 'streamdbi_data.zip')
        self.conn = None
        self._columns = []
        self._row_count = 0
        self._drilldown_row_count = 0
        self._lock = threading.Lock()
        self._data_hash = None  # for chart cache invalidation

    def load_from_url(self, url, is_drilldown=False, progress_callback=None):
        """Download data from a URL and load it into the engine with schema validation."""
        if not url:
            return False

        is_zip = url.lower().split('?')[0].endswith('.zip')
        target_path = self.drilldown_csv if is_drilldown else self.master_csv
        table_name = "drilldown_data" if is_drilldown else "master_data"

        try:
            if progress_callback:
                progress_callback(f"Downloading from {url}...")
            
            # Use a temporary file for download
            fd, temp_path = tempfile.mkstemp(suffix=".zip" if is_zip else ".csv")
            os.close(fd)

            try:
                response = requests.get(url, timeout=30, stream=True)
                response.raise_for_status()
                
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                if is_zip:
                    # SPECIAL CASE: ZIP archive handling
                    if progress_callback: progress_callback("Extracting data from ZIP...")
                    success = self.load_from_zip(temp_path, progress_callback)
                    return success

                # SCHEMA VALIDATION (for CSV only)
                if progress_callback: progress_callback("Validating schema...")
                
                with open(temp_path, 'r', encoding='utf-8-sig') as f:
                    first_line = f.readline().strip().replace('"', '')
                    header = first_line.split(',')
                    
                    # Canonical check
                    required = ["クライアントID", "処理月"] if not is_drilldown else ["client_id", "target_month"]
                    # Clean up header names (remove BOM, whitespace)
                    clean_header = [c.replace('\ufeff', '').strip() for c in header]
                    
                    if not any(req in clean_header for req in required):
                        raise Exception(f"Schema mismatch. Missing required columns: {required}.\nFound: {clean_header[:5]}...")

                if progress_callback:
                    progress_callback(f"Download complete. Loading into database...")
                
                # Replace the master file
                if os.path.exists(target_path):
                    os.remove(target_path)
                shutil.move(temp_path, target_path)

                # Use _reload_csv directly to ensure the new CSV is prioritized over old Parquet
                success = self._reload_csv(target_path, table_name, progress_callback)
                if success:
                    self.save_to_parquet()
                return success

            finally:
                if os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except: pass

        except Exception as e:
            if progress_callback:
                progress_callback(f"Remote load error: {e}")
            print(f"[ENGINE] Remote load error: {e}")
            return False

    def initialize_db(self):
        """Create in-memory DuckDB connection and ensure tables exist."""
        self.conn = duckdb.connect(database=':memory:')
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS master_data (
                "取得日時" VARCHAR, "クライアントID" VARCHAR, "企業名" VARCHAR, "処理月" VARCHAR, 
                "証憑タイプ" VARCHAR, "対象仕訳数" VARCHAR, "全体正解件数" VARCHAR,
                "金額_対象" VARCHAR, "金額_正解" VARCHAR, "日付_対象" VARCHAR, "日付_正解" VARCHAR,
                "科目_対象" VARCHAR, "科目_正解" VARCHAR, "支払先_対象" VARCHAR, "支払先_正解" VARCHAR,
                "税区分_対象" VARCHAR, "税区分_正解" VARCHAR, "登録_対象" VARCHAR, "登録_正解" VARCHAR,
                "内容_対象" VARCHAR, "内容_正解" VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS drilldown_data (
                "client_id" VARCHAR, "target_month" VARCHAR, "voucher_type" VARCHAR, 
                "journal_id" VARCHAR, "error_field" VARCHAR, 
                "initial_value" VARCHAR, "latest_value" VARCHAR
            )
        """)

    def _ensure_conn(self):
        if not self.conn:
            self.initialize_db()

    def reload_master_data(self, progress_callback=None):
        """Prefer Parquet for speed, fallback to CSV with migration."""
        return self._reload_generic(self.master_parquet, self.master_csv, "master_data", progress_callback)

    def reload_drilldown_data(self, progress_callback=None):
        """Prefer Parquet for speed, fallback to CSV with migration."""
        return self._reload_generic(self.drilldown_parquet, self.drilldown_csv, "drilldown_data", progress_callback)

    def _reload_generic(self, parquet_path, csv_path, table_name, progress_callback=None):
        """Load from Parquet if available, otherwise CSV (and migrate)."""
        self._ensure_conn()

        # 1. Try Parquet first (10-100x faster than CSV)
        if os.path.exists(parquet_path):
            try:
                if progress_callback: progress_callback(f"Loading {os.path.basename(parquet_path)}...")
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path.replace(chr(92), '/')}')")
                
                count = self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
                self._update_metadata(table_name, count, parquet_path)
                if progress_callback: progress_callback(f"Table {table_name} ready: {count:,} rows (Parquet)")
                return True
            except Exception as e:
                print(f"[ENGINE] Parquet load failed for {table_name}: {e}")
                # Fall through to CSV

        # 2. Fallback to CSV and Migrate
        if os.path.exists(csv_path):
            if progress_callback: progress_callback(f"Migrating {os.path.basename(csv_path)} to Parquet...")
            success = self._reload_csv(csv_path, table_name, progress_callback)
            if success:
                # Background save to Parquet for next time
                self.save_to_parquet()
            return success

        if progress_callback:
            progress_callback(f"No data file found for {table_name}")
        return False

    def _update_metadata(self, table_name, count, file_path):
        if table_name == "master_data":
            self._row_count = count
            self._columns = [col[0] for col in self.conn.execute(f"DESCRIBE {table_name}").fetchall()]
            mtime = datetime.now().timestamp()
            try:
                if os.path.exists(file_path):
                    mtime = os.path.getmtime(file_path)
            except:
                pass
            self._data_hash = f"{count}_{mtime}"
        else:
            self._drilldown_row_count = count

    def _reload_csv(self, csv_path, table_name, progress_callback=None):
        """Load CSV directly via DuckDB read_csv_auto — no Pandas intermediate."""
        self._ensure_conn()

        if not os.path.exists(csv_path):
            if progress_callback:
                progress_callback(f"File not found: {os.path.basename(csv_path)}")
            return False

        try:
            # TRY R-BRIDGE FIRST for ultra-fast mapping if possible
            if self.load_from_csv_r(csv_path, table_name, progress_callback):
                return True
                
            if progress_callback:
                progress_callback(f"Reading {os.path.basename(csv_path)}...")

            # DuckDB native read — skips Pandas entirely for 2-3x speedup on large files
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv_auto('{csv_path.replace(chr(92), '/')}',
                    header=true, 
                    all_varchar=true,
                    encoding='utf-8',
                    quote='"',
                    escape='"',
                    ignore_errors=true
                )
            """)

            count = self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            self._update_metadata(table_name, count, csv_path)

            if progress_callback:
                progress_callback(f"Table {table_name} ready: {count:,} rows")

            return True
        except Exception as e:
            traceback.print_exc()
            # Fallback: try with pandas if DuckDB native fails (e.g. BOM encoding)
            try:
                return self._reload_csv_pandas_fallback(csv_path, table_name, progress_callback)
            except Exception as e2:
                if progress_callback:
                    progress_callback(f"Load Error ({table_name}): {e2}")
                return False

    def _reload_csv_pandas_fallback(self, csv_path, table_name, progress_callback=None):
        """Fallback loader using pandas for edge cases (BOM files, etc)."""
        import pandas as pd
        if progress_callback:
            progress_callback(f"Fallback: loading via pandas...")
        
        df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig', low_memory=False)
        df = df.astype(str).replace(['nan', 'None'], '')

        self.conn.register("__tmp_df", df)
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM __tmp_df")
        self.conn.unregister("__tmp_df")

        count = self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        self._update_metadata(table_name, count, csv_path)

        if progress_callback:
            progress_callback(f"Table {table_name} ready: {count:,} rows (fallback)")
        return True

    def load_from_zip(self, zip_file_path, progress_callback=None):
        """Extract CSV from ZIP and load into DuckDB."""
        self._ensure_conn()

        if not os.path.exists(zip_file_path):
            if progress_callback:
                progress_callback(f"ZIP not found: {zip_file_path}")
            return False

        try:
            # TRY R-BRIDGE for fast bulk extraction and mapping
            if self.load_from_zip_r(zip_file_path, progress_callback):
                return True
                
            with zipfile.ZipFile(zip_file_path, 'r') as zf:
                if progress_callback:
                    progress_callback(f"Checking ZIP contents...")
                
                namelist = zf.namelist()
                files_found = 0
                for name in namelist:
                    target_path = None
                    if 'ranking_data_master' in name and name.endswith('.csv'):
                        target_path = self.master_csv
                    elif 'drilldown_data_master' in name and name.endswith('.csv'):
                        target_path = self.drilldown_csv
                    
                    if target_path:
                        if progress_callback: progress_callback(f"Extracting {name}...")
                        zf.extract(name, self.base_dir)
                        extracted = os.path.join(self.base_dir, name)
                        if os.path.abspath(extracted) != os.path.abspath(target_path):
                            import shutil
                            shutil.copy2(extracted, target_path)
                        files_found += 1

                if files_found == 0:
                    if progress_callback: progress_callback("No relevant CSV files found in ZIP.")
                    return False

            self.reload_master_data(progress_callback)
            self.reload_drilldown_data(progress_callback)
            return True
        except Exception as e:
            traceback.print_exc()
            if progress_callback:
                progress_callback(f"ZIP Load Error: {e}")
            return False

    def has_data(self):
        return self._row_count > 0

    def get_columns(self):
        return self._columns

    def get_row_count(self):
        return self._row_count

    def get_drilldown_row_count(self):
        return self._drilldown_row_count

    def get_data_hash(self):
        """Return a hash string for cache invalidation."""
        return self._data_hash

    def query(self, sql_query):
        """Execute SQL query and return list of dicts — non-blocking to avoid UI freeze."""
        self._ensure_conn()
        if not self._lock.acquire(blocking=False):
            # Sync worker is busy — return empty rather than blocking the UI thread
            return []
        try:
            result = self.conn.execute(sql_query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            try:
                print(f"[ENGINE] Query Error: {e}")
            except:
                pass
            return []
        finally:
            self._lock.release()

    def query_df(self, sql_query):
        """Execute SQL and return DataFrame — non-blocking to avoid UI freeze."""
        self._ensure_conn()
        import pandas as pd
        if not self._lock.acquire(blocking=False):
            # Sync worker is busy — return empty DataFrame rather than blocking
            return pd.DataFrame()
        try:
            df = self.conn.execute(sql_query).fetchdf()
            # Cleanup column names (remove BOM, whitespace)
            df.columns = [c.replace('\ufeff', '').strip() for c in df.columns]
            return df
        except Exception as e:
            try:
                print(f"[ENGINE] Query Error: {e}")
            except:
                pass
            return pd.DataFrame()
        finally:
            self._lock.release()

    def get_canonical_columns(self, is_drilldown=False):
        if is_drilldown:
            return ["client_id", "target_month", "voucher_type", "journal_id", "error_field", "initial_value", "latest_value"]
        
        return [
            "取得日時", "クライアントID", "企業名", "処理月", "証憑タイプ", 
            "対象仕訳数", "全体正解件数", 
            "金額_対象", "金額_正解", 
            "日付_対象", "日付_正解", 
            "科目_対象", "科目_正解", 
            "支払先_対象", "支払先_正解", 
            "税区分_対象", "税区分_正解", 
            "登録_対象", "登録_正解", 
            "内容_対象", "内容_正解"
        ]

    def save_to_parquet(self):
        """Export DuckDB tables to highly-compressed Parquet files."""
        self._ensure_conn()
        try:
            if self._row_count > 0:
                self.conn.execute(f"COPY master_data TO '{self.master_parquet.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            if self._drilldown_row_count > 0:
                self.conn.execute(f"COPY drilldown_data TO '{self.drilldown_parquet.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            return True
        except Exception as e:
            print(f"[ENGINE] Parquet Save Error: {e}")
            return False

    def save_to_csv(self):
        """Export DuckDB tables back to legacy CSVs using R (data.table) bridge if available."""
        self._ensure_conn()
        r_path = self._find_r()
        try:
            if self._row_count > 0:
                self.export_query_to_csv("SELECT * FROM master_data", self.master_csv, r_path=r_path)
            if self._drilldown_row_count > 0:
                self.export_query_to_csv("SELECT * FROM drilldown_data", self.drilldown_csv, r_path=r_path)
            return True
        except Exception as e:
            print(f"[ENGINE] CSV Save Error: {e}")
            return False
    def save_to_zip(self):
        """Create a concentrated ZIP archive containing the master and drilldown CSVs."""
        try:
            with zipfile.ZipFile(self.zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                if os.path.exists(self.master_csv):
                    zf.write(self.master_csv, os.path.basename(self.master_csv))
                if os.path.exists(self.drilldown_csv):
                    zf.write(self.drilldown_csv, os.path.basename(self.drilldown_csv))
            return True
        except Exception as e:
            print(f"[ENGINE] ZIP Save Error: {e}")
            return False

    def _find_r(self):
        """Internal R-path helper."""
        import shutil
        import os
        r = shutil.which("Rscript")
        if r: return r
        paths = [r"C:\Program Files\R\*\bin\Rscript.exe", r"C:\Program Files (x86)\R\*\bin\Rscript.exe"]
        import glob
        for p in paths:
            found = glob.glob(p)
            if found:
                found.sort(reverse=True)
                # Prefer x64 if it exists
                x64_path = os.path.join(os.path.dirname(found[0]), 'x64', 'Rscript.exe')
                if os.path.exists(x64_path):
                    return x64_path
                return found[0]
        return None

    def _get_short_path(self, path):
        """Get 8.3 short path for Windows to avoid non-ASCII issues."""
        import ctypes
        if not path: return path
        try:
            buf = ctypes.create_unicode_buffer(1024)
            ctypes.windll.kernel32.GetShortPathNameW(path, buf, 1024)
            return buf.value or path
        except:
            return path

    def deduplicate(self):
        """Perform a highly optimized columnar deduplication. Prefers R-engine for massive speed."""
        r_path = self._find_r()
        if r_path and self.deduplicate_r():
            return True
            
        self._ensure_conn()
        try:
            with self._lock:
                # Deduplicate master_data (keep latest by 取得日時)
                self.conn.execute("""
                    CREATE OR REPLACE TABLE master_data AS
                    SELECT * EXCLUDE (row_num)
                    FROM (
                        SELECT *, 
                            ROW_NUMBER() OVER(PARTITION BY "クライアントID", "処理月", "証憑タイプ" ORDER BY "取得日時" DESC) as row_num
                        FROM master_data
                    )
                    WHERE row_num = 1
                """)
                
                # Deduplicate drilldown_data (keep latest entry if duplicates exist)
                self.conn.execute("""
                    CREATE OR REPLACE TABLE drilldown_data AS
                    SELECT * EXCLUDE (row_num)
                    FROM (
                        SELECT *, 
                            ROW_NUMBER() OVER(PARTITION BY "client_id", "target_month", "voucher_type", "journal_id", "error_field" ORDER BY "latest_value" DESC) as row_num 
                        FROM drilldown_data
                    )
                    WHERE row_num = 1
                """)

                # Update accurate counts at the VERY END
                self._row_count = self.conn.execute("SELECT COUNT(*) FROM master_data").fetchone()[0]
                self._drilldown_row_count = self.conn.execute("SELECT COUNT(*) FROM drilldown_data").fetchone()[0]
                self._data_hash = f"{self._row_count}_{datetime.now().timestamp()}"
                return True
        except Exception as e:
            print(f"[ENGINE] Deduplication Error: {e}")
            return False

    def deduplicate_r(self):
        """Ultra-fast deduplication using R data.table."""
        r_path = self._find_r()
        if not r_path: return False
        
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                # 1. Export tables to temp CSV/Parquet
                master_tmp = os.path.join(tmp_dir, "master_dirty.csv")
                dd_tmp = os.path.join(tmp_dir, "dd_dirty.csv")
                
                self.conn.execute(f"COPY master_data TO '{master_tmp}' (FORMAT CSV, HEADER)")
                self.conn.execute(f"COPY drilldown_data TO '{dd_tmp}' (FORMAT CSV, HEADER)")
                
                # 2. Run R script to deduplicate
                r_script = """
                library(data.table)
                # Master
                m <- fread('MASTER_TMP', encoding='UTF-8', colClasses='character')
                if(nrow(m)>0){
                    setorder(m, -`取得日時`)
                    m <- unique(m, by=c('クライアントID', '処理月', '証憑タイプ'))
                    fwrite(m, 'MASTER_CLEAN', bom=FALSE)
                }
                # DD
                dd <- fread('DD_TMP', encoding='UTF-8', colClasses='character')
                if(nrow(dd)>0){
                    setorder(dd, -latest_value)
                    dd <- unique(dd, by=c('client_id', 'target_month', 'voucher_type', 'journal_id', 'error_field'))
                    fwrite(dd, 'DD_CLEAN', bom=FALSE)
                }
                cat('DONE_SUCCESS\n')
                """.replace('MASTER_TMP', master_tmp.replace('\\', '/'))\
                   .replace('MASTER_CLEAN', master_tmp.replace('dirty', 'clean').replace('\\', '/'))\
                   .replace('DD_TMP', dd_tmp.replace('\\', '/'))\
                   .replace('DD_CLEAN', dd_tmp.replace('dirty', 'clean').replace('\\', '/'))
                
                r_script_path = os.path.join(tmp_dir, "dedup.R")
                with open(r_script_path, 'w', encoding='utf-8') as f: f.write(r_script)
                
                cmd = f'"{r_path}" --vanilla "{r_script_path}"'
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', shell=True)
                stdout, stderr = proc.communicate()
                
                if "DONE_SUCCESS" in stdout:
                    m_clean = master_tmp.replace('dirty', 'clean')
                    dd_clean = dd_tmp.replace('dirty', 'clean')
                    
                    if os.path.exists(m_clean):
                        # STAGING APPROACH: Delete and Insert to preserve Schema
                        self.conn.execute("DELETE FROM master_data")
                        self.conn.execute(f"INSERT INTO master_data SELECT * FROM read_csv_auto('{m_clean.replace(chr(92),'/')}', all_varchar=true)")
                    if os.path.exists(dd_clean):
                        self.conn.execute("DELETE FROM drilldown_data")
                        self.conn.execute(f"INSERT INTO drilldown_data SELECT * FROM read_csv_auto('{dd_clean.replace(chr(92),'/')}', all_varchar=true)")
                    
                    # UPDATE INTERNAL COUNTS
                    self._row_count = self.conn.execute("SELECT COUNT(*) FROM master_data").fetchone()[0]
                    self._drilldown_row_count = self.conn.execute("SELECT COUNT(*) FROM drilldown_data").fetchone()[0]
                    self._data_hash = f"{self._row_count}_{datetime.now().timestamp()}"
                    return True
                return False
        except Exception as e:
            print(f"[ENGINE-R-DEDUP] Error: {e}")
            return False

    def append_data(self, data_rows, is_drilldown=False, sync_type="diff", progress_callback=None):
        """Append sync data to DuckDB with Smart Merge (deduplication)."""
        if not data_rows: return
        
        with self._lock:
            # Map columns
            mapping = {
                "client_id": "クライアントID", "enterprise_name": "企業名",
                "target_month": "処理月", "processing_month": "処理月",
                "voucher_type_code": "証憑タイプ", "voucher_type": "証憑タイプ",
                "target_count": "対象仕訳数", "total_count": "対象仕訳数",
                "correct_count": "全体正解件数"
            }
            
            score_mapping = {
                "date_target": "日付_対象", "date_correct": "日付_正解",
                "content_target": "内容_対象", "content_correct": "内容_正解",
                "account_target": "科目_対象", "account_correct": "科目_正解",
                "amount_target": "金額_対象", "amount_correct": "金額_正解",
                "supplier_target": "支払先_対象", "supplier_correct": "支払先_正解",
                "tax_target": "税区分_対象", "tax_correct": "税区分_正解",
                "regnum_target": "登録_対象", "regnum_correct": "登録_正解",
                "登録番号_対象": "登録_対象", "登録番号_正解": "登録_正解"
            }
            mapping.update(score_mapping)
            
            now_str = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            
            for row in data_rows:
                if not is_drilldown and "取得日時" not in row:
                    row["取得日時"] = now_str
                
                for eng, jp in mapping.items():
                    if eng in row and jp not in row:
                        row[jp] = row[eng]
                
                if is_drilldown:
                    if "target_month" not in row and "processing_month" in row:
                        row["target_month"] = row["processing_month"]
                    if "voucher_type" not in row and "voucher_type_code" in row:
                        row["voucher_type"] = row["voucher_type_code"]
                else:
                    if "企業名" not in row: row["企業名"] = ""
                    if "クライアントID" not in row: row["クライアントID"] = ""

            # Direct Injection into DuckDB
            self._ensure_conn()
            table_name = "drilldown_data" if is_drilldown else "master_data"
            
            try:
                # HIGH-PERFORMANCE BRIDGE: Use R for large chunks (>1000 rows)
                if len(data_rows) >= 1000 and self._find_r():
                    if self.append_data_r(data_rows, table_name, progress_callback):
                        if is_drilldown:
                            self._drilldown_row_count += len(data_rows)
                        else:
                            self._row_count += len(data_rows)
                            self._data_hash = f"{self._row_count}_{datetime.now().timestamp()}"
                        return
                
                df_to_insert = pd.DataFrame(data_rows)
                
                db_cols = [c[0] for c in self.conn.execute(f"SELECT * FROM {table_name} LIMIT 0").description]
                for col in db_cols:
                    if col not in df_to_insert.columns:
                        df_to_insert[col] = ""
                
                insert_df = df_to_insert[db_cols].astype(str).replace('nan', '')
                
                self.conn.register("__tmp_sync", insert_df)
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM __tmp_sync")
                self.conn.unregister("__tmp_sync")
                
                # OPTIMIZED: Update internal counts WITHOUT expensive COUNT(*)
                if is_drilldown:
                    self._drilldown_row_count += len(data_rows)
                else:
                    self._row_count += len(data_rows)
                    self._data_hash = f"{self._row_count}_{datetime.now().timestamp()}"
                    
                return True
            except Exception as db_err:
                print(f"[ENGINE] Direct DB Insert failed: {db_err}")
                return False

    def reset_data(self):
        for f in [self.master_csv, self.drilldown_csv, self.master_parquet, self.drilldown_parquet]:
            if os.path.exists(f):
                os.remove(f)
        self.initialize_db()
        self._columns = []
        self._row_count = 0
        self._drilldown_row_count = 0
        self._data_hash = None

    def get_stats_summary(self):
        """Fast stats from cached values — non-blocking DB access."""
        result = {
            "total_rows": self._row_count,
            "drilldown_rows": self._drilldown_row_count,
            "total_clients": 0,
            "columns": len(self._columns),
            "csv_size_mb": 0,
            "drilldown_size_mb": 0,
            "parquet_size_mb": 0,
            "drilldown_parquet_size_mb": 0,
            "zip_exists": os.path.exists(self.zip_path),
        }

        # Only attempt DB query if lock is available (non-blocking)
        if self._row_count > 0 and self.conn and self._lock.acquire(blocking=False):
            try:
                candidates = ['クライアントID', 'client_id', 'ClientID']
                col_in_db = [c[0] for c in self.conn.execute("SELECT * FROM master_data LIMIT 0").description]
                
                for candidate in candidates:
                    if candidate in col_in_db:
                        try:
                            result["total_clients"] = self.conn.execute(
                                f'SELECT count(DISTINCT "{candidate}") FROM master_data'
                            ).fetchone()[0]
                        except:
                            pass
                        break
            except:
                pass
            finally:
                self._lock.release()

        try:
            csv_size = os.path.getsize(self.master_csv) if os.path.exists(self.master_csv) else 0
            dd_size = os.path.getsize(self.drilldown_csv) if os.path.exists(self.drilldown_csv) else 0
            pq_size = os.path.getsize(self.master_parquet) if os.path.exists(self.master_parquet) else 0
            dd_pq_size = os.path.getsize(self.drilldown_parquet) if os.path.exists(self.drilldown_parquet) else 0

            result["csv_size_mb"] = round(csv_size / 1048576, 1)
            result["drilldown_size_mb"] = round(dd_size / 1048576, 1)
            result["parquet_size_mb"] = round(pq_size / 1048576, 1)
            result["drilldown_parquet_size_mb"] = round(dd_pq_size / 1048576, 1)
        except:
            pass

        return result

    def export_query_to_csv(self, sql_query, output_path, r_path=None):
        """Export query results to CSV, prioritizing R (data.table) for speed if requested."""
        if r_path:
            return self.export_query_to_csv_r(sql_query, output_path, r_path)
            
        self._ensure_conn()
        try:
            df = self.query_df(sql_query)
            if df is not None and not df.empty:
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                return True
            return False
        except Exception as e:
            print(f"[ENGINE] Export Error: {e}")
            return False

    def load_from_zip_r(self, zip_path, progress_callback=None):
        """Ultra-fast ZIP import by batch-processing all CSVs with R."""
        r_path = self._find_r()
        if not r_path: return False
        
        try:
            if progress_callback: progress_callback("ZIP内のファイルを高速スキャン中...")
            with tempfile.TemporaryDirectory() as tmp_dir:
                # 1. Extract all CSVs to a temp workspace
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    csv_files = [n for n in zf.namelist() if n.endswith('.csv')]
                    if not csv_files: return False
                    zf.extractall(tmp_dir)
                
                # 2. Identify master and drilldown files
                master_files = []
                dd_files = []
                for f in csv_files:
                    full_p = os.path.join(tmp_dir, f)
                    if 'ranking_data_master' in f: master_files.append(full_p)
                    elif 'drilldown_data_master' in f: dd_files.append(full_p)
                
                # 3. Process each group with R if they exist
                success = False
                if master_files:
                    if progress_callback: progress_callback(f"マスターデータ({len(master_files)}件)を高度処理中...")
                    if self._run_fast_import_r(master_files, "master_data", progress_callback):
                        success = True
                
                if dd_files:
                    if progress_callback: progress_callback(f"ドリルダウンデータ({len(dd_files)}件)を高度処理中...")
                    if self._run_fast_import_r(dd_files, "drilldown_data", progress_callback):
                        success = True
                        
                return success
        except Exception as e:
            print(f"[ENGINE-R-ZIP] Error: {e}")
            return False

    def _run_fast_import_r(self, input_paths, table_name, progress_callback):
        """Helper to run fast_import.R on a list of files."""
        r_path = self._find_r()
        if not r_path: return False
        
        with tempfile.TemporaryDirectory() as bridge_tmp:
            r_script_src = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fast_import.R'))
            r_script_tmp = os.path.join(bridge_tmp, "import.R")
            shutil.copy2(r_script_src, r_script_tmp)
            
            config_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json'))
            config_tmp = os.path.join(bridge_tmp, "config.json")
            if os.path.exists(config_path): shutil.copy2(config_path, config_tmp)
            
            final_tmp = os.path.join(bridge_tmp, "cleaned_bulk.csv")
            input_paths_str = ",".join(input_paths)
            is_dd = "drilldown" in table_name
            
            cmd = f'"{r_path}" --vanilla "{r_script_tmp}" "{input_paths_str}" "{final_tmp}" "{config_tmp}" {"TRUE" if is_dd else "FALSE"}'
            
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', shell=True, cwd=bridge_tmp)
            stdout, stderr = proc.communicate()
            
            if "DONE_SUCCESS" in stdout or "DONE_SUCCESS" in stderr:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{final_tmp.replace(chr(92), '/')}', header=true, all_varchar=true)")
                count = self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
                self._update_metadata(table_name, count, "bulk_import")
                return True
            return False

    def append_data_r(self, data_rows, table_name, progress_callback=None):
        """Ultra-fast JSON buffer to DuckDB ingestion using R."""
        r_path = self._find_r()
        if not r_path: return False
        
        try:
            import json
            import time
            with tempfile.TemporaryDirectory() as tmp_dir:
                # 1. Get Target Schema
                target_cols = self.get_canonical_columns(is_drilldown="drilldown" in table_name)
                cols_json = json.dumps(target_cols)
                
                # 2. Dump JSON buffer to temp file
                json_p = os.path.join(tmp_dir, "buffer.json")
                with open(json_p, 'w', encoding='utf-8') as f:
                    json.dump(data_rows, f, ensure_ascii=False)
                
                # 3. R Script for high-speed transformation
                csv_p = os.path.join(tmp_dir, "buffer.csv")
                r_start = time.time()
                if progress_callback: progress_callback(f"[ENGINE-R] Starting R-Bridge transformation for {len(data_rows):,} rows...")
                
                r_script = """
                library(jsonlite)
                library(data.table)
                # Read JSON and Schema
                d <- fromJSON('JSON_P')
                target_cols <- fromJSON('COLS_JSON')
                dt <- as.data.table(d)
                
                # Ensure all target columns exist (fill missing with "")
                for(col in target_cols){
                    if(!(col %in% names(dt))){
                        dt[[col]] <- ""
                    }
                }
                
                # Filter and Reorder to match schema exactly
                dt_final <- dt[, ..target_cols]
                
                # Force all to character
                for(col in names(dt_final)) dt_final[[col]] <- as.character(dt_final[[col]])
                
                # Write CSV (NO BOM for internal DuckDB ingestion)
                fwrite(dt_final, 'CSV_P', bom=FALSE)
                cat('DONE_SUCCESS\\n')
                """.replace('JSON_P', json_p.replace('\\', '/'))\
                   .replace('CSV_P', csv_p.replace('\\', '/'))\
                   .replace('COLS_JSON', cols_json)
                
                r_script_p = os.path.join(tmp_dir, "convert.R")
                with open(r_script_p, 'w', encoding='utf-8') as f: f.write(r_script)
                
                cmd = f'"{r_path}" --vanilla "{r_script_p}"'
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', shell=True)
                stdout, stderr = proc.communicate()
                
                if "DONE_SUCCESS" in stdout:
                    r_elapsed = time.time() - r_start
                    if progress_callback: progress_callback(f"[ENGINE-R] Transformation complete! ({r_elapsed:.1f}s)")
                    
                    # 4. Load from CSV into DuckDB
                    db_start = time.time()
                    if progress_callback: progress_callback("[ENGINE-DB] Bulk inserting into DuckDB...")
                    self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv_auto('{csv_p.replace(chr(92),'/')}', all_varchar=true)")
                    db_elapsed = time.time() - db_start
                    if progress_callback: progress_callback(f"[ENGINE-DB] Insert complete! ({db_elapsed:.1f}s)")
                    return True
                return False
        except Exception as e:
            print(f"[ENGINE-R-APPEND] Error: {e}")
            return False

    def load_from_csv_r(self, csv_path, table_name, progress_callback=None):
        """Ultra-fast CSV load with R-powered pre-mapping."""
        r_path = self._find_r()
        if not r_path: return False
        
        try:
            if progress_callback: progress_callback("R-Engineで高速マッピング読み込み中...")
            
            # Isolation Strategy
            with tempfile.TemporaryDirectory() as tmp_dir:
                # 1. Prepare R script
                r_script_src = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fast_import.R'))
                r_script_tmp = os.path.join(tmp_dir, "import.R")
                if not os.path.exists(r_script_src): return False
                shutil.copy2(r_script_src, r_script_tmp)
                
                # 2. Config path (shared root)
                config_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json'))
                config_tmp = os.path.join(tmp_dir, "config.json")
                if os.path.exists(config_path): shutil.copy2(config_path, config_tmp)
                
                # 3. Destination
                final_tmp = os.path.join(tmp_dir, "cleaned.csv")
                
                # 4. Run R
                is_dd = "drilldown" in table_name
                cmd = f'"{r_path}" --vanilla "{r_script_tmp}" "{csv_path}" "{final_tmp}" "{config_tmp}" {"TRUE" if is_dd else "FALSE"}'
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', shell=True, cwd=tmp_dir)
                stdout, stderr = proc.communicate()
                
                if "DONE_SUCCESS" in stdout or "DONE_SUCCESS" in stderr:
                    # 5. Load the cleaned CSV into DuckDB (Native)
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    self.conn.execute(f"""
                        CREATE TABLE {table_name} AS 
                        SELECT * FROM read_csv_auto('{final_tmp.replace(chr(92), '/')}', 
                            header=true, all_varchar=true)
                    """)
                    count = self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
                    self._update_metadata(table_name, count, csv_path)
                    if progress_callback: progress_callback(f"{table_name}準備完了 (R高速読込): {count:,}行")
                    return True
                else:
                    print(f"[ENGINE-R-IMPORT] Failed: {stderr}")
                    return False
        except Exception as e:
            print(f"[ENGINE-R-IMPORT] Error: {e}")
            return False

    def export_query_to_csv_r(self, sql_query, output_path, r_path):
        """Ultra-fast export using R data.table bridge."""
        import tempfile
        import subprocess
        import os
        import shutil
        
        self._ensure_conn()
        try:
            # 1. Get data as DataFrame
            df = self.query_df(sql_query)
            if df is None or df.empty: return False
            
            # 2. Write to a temporary CSV (no BOM, basic UTF-8)
            # ISOALTION STRATEGY: Copy everything to a clean temp directory
            if r_path:
                import tempfile
                import shutil
                
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # 1. Copy R script to temp workspace
                    r_script_src = os.path.join(os.path.dirname(__file__), 'fast_export.R')
                    r_script_tmp = os.path.join(tmp_dir, "export.R")
                    shutil.copy2(r_script_src, r_script_tmp)
                    
                    # 2. Write data to temp workspace
                    data_tmp = os.path.join(tmp_dir, "data.csv")
                    df.to_csv(data_tmp, index=False, encoding='utf-8')
                    
                    # 3. Create output path and command
                    final_data_tmp = os.path.join(tmp_dir, "output.csv")
                    cmd = f'"{r_path}" --vanilla "{r_script_tmp}" "{data_tmp}" "{final_data_tmp}"'
                    
                    # DEBUG INFO
                    import sys
                    debug_info = f"R:{r_path} Exists:{os.path.exists(r_path)} CWD:{tmp_dir} Cmd:{cmd}"
                    print(f"[ENGINE-R-DEBUG] {debug_info}")
                    
                    try:
                        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                                 text=True, encoding='utf-8', cwd=tmp_dir, shell=True)
                        stdout, stderr = process.communicate()
                        
                        success_msg = "DONE_SUCCESS"
                        if success_msg in stdout or success_msg in stderr:
                            if os.path.exists(final_data_tmp):
                                # Ensure we overwrite the destination
                                if os.path.exists(output_path): os.remove(output_path)
                                shutil.copy2(final_data_tmp, output_path)
                                return True
                            else:
                                print(f"[ENGINE-R] R Success but output missing: {final_data_tmp}")
                                return False
                        else:
                            print(f"[ENGINE-R] R Export Failed (Code {process.returncode}):\nSTDOUT: {stdout}\nSTDERR: {stderr}\nDEBUG: {debug_info}")
                            return False
                    except Exception as e:
                        print(f"[ENGINE-R] Launch Error: {e}\nDEBUG: {debug_info}")
                        return False
            else:
                return False
        except Exception as e:
            print(f"[ENGINE-R] Error: {e}")
            return False
