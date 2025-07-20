# snapshot_portable.py
#
# Prerequisites: pip install xxhash
#
# A portable, high-performance filesystem snapshot tool for Windows, macOS, and Linux.
# Features a full command-line interface and a multi-process, streaming architecture.

import os
import sqlite3
import stat
import time
import sys
import platform
import argparse
from multiprocessing import Process, Pool, Manager, cpu_count

import xxhash

# --- Platform Abstraction Layer ---

class PlatformAdapter:
    """
    Provides a consistent interface for platform-specific configurations and functions.
    This isolates OS-specific code from the main application logic.
    """
    def __init__(self):
        self.system = platform.system()
        self.home_dir = os.path.expanduser("~")
        
        if self.system == "Windows":
            self._configure_windows()
        elif self.system == "Darwin": # macOS
            self._configure_darwin()
        else: # Assume Linux-like
            self._configure_linux()

    def _configure_windows(self):
        self.default_scan_dirs = [
            os.getenv('ProgramFiles'),
            os.getenv('ProgramW6432'),
            os.path.join(self.home_dir, "AppData"),
            os.getenv('ProgramData')
        ]
        # Filter out None values if an env var doesn't exist
        self.default_scan_dirs = [d for d in self.default_scan_dirs if d]
        
        self.default_exclude_dirs = [
            os.getenv('WinDir'),
            os.path.join(self.home_dir, "AppData\\Local\\Temp")
        ]

    def _configure_darwin(self):
        self.default_scan_dirs = [
            "/Applications",
            os.path.join(self.home_dir, "Library"),
            "/Library",
            "/System"
        ]
        self.default_exclude_dirs = [
            os.path.join(self.home_dir, "Library/Caches"),
            "/private/var/log",
            "/private/var/folders",
            "/tmp",
            "/dev",
            "/Volumes"
        ]

    def _configure_linux(self):
        self.default_scan_dirs = ["/etc", "/usr", "/opt", "/var", self.home_dir]
        self.default_exclude_dirs = ["/proc", "/sys", "/tmp", "/dev", "/run"]

    def get_metadata(self, stat_result):
        """Returns (owner, permissions) tuple in a platform-agnostic way."""
        if self.system == "Windows":
            # Windows permissions are complex (ACLs) and not easily represented
            # as a simple string. UIDs also don't exist.
            return "N/A", "N/A"
        else:
            # Standard Unix-like permissions and ownership
            owner = str(stat_result.st_uid)
            permissions = stat.filemode(stat_result.st_mode)
            return owner, permissions

# --- Core Application Logic (largely unchanged) ---

def get_file_hash_fast(filepath):
    try:
        h = xxhash.xxh3_64()
        with open(filepath, "rb") as f:
            while chunk := f.read(131072):
                h.update(chunk)
        return h.hexdigest()
    except (IOError, PermissionError):
        return "unreadable"

def producer_task(task_queue, directories, exclusion_set):
    print("[Producer] Starting filesystem scan...")
    for root_dir in directories:
        if not os.path.exists(root_dir):
            print(f"[Producer] Warning: Scan path '{root_dir}' does not exist. Skipping.", file=sys.stderr)
            continue
        if not os.access(root_dir, os.R_OK):
            print(f"[Producer] Warning: No read access to {root_dir}. Skipping.", file=sys.stderr)
            continue
        try:
            for entry in os.scandir(root_dir):
                _walk(entry, task_queue, exclusion_set)
        except (OSError, PermissionError) as e:
            print(f"[Producer] Error scanning {root_dir}: {e}", file=sys.stderr)
    print("[Producer] Filesystem scan complete. Signaling workers to terminate.")
    for _ in range(cpu_count()):
        task_queue.put(None)

def _walk(entry, task_queue, exclusion_set):
    try:
        if entry.is_dir(follow_symlinks=False):
            if entry.path in exclusion_set:
                return
            for sub_entry in os.scandir(entry.path):
                _walk(sub_entry, task_queue, exclusion_set)
        elif entry.is_file(follow_symlinks=False):
            task_queue.put(entry.path)
    except (OSError, PermissionError):
        pass

def consumer_worker(task_queue, results_queue, before_cache, platform_adapter):
    while True:
        filepath = task_queue.get()
        if filepath is None:
            break
        try:
            st = os.stat(filepath, follow_symlinks=False)
            if not stat.S_ISREG(st.st_mode):
                continue
        except (OSError, PermissionError):
            continue
        size = st.st_size
        mtime = int(st.st_mtime)
        file_hash = None
        if before_cache and filepath in before_cache:
            old_size, old_mtime, old_hash = before_cache[filepath]
            if size == old_size and mtime == old_mtime:
                file_hash = old_hash
        if file_hash is None:
            file_hash = get_file_hash_fast(filepath)
        
        owner, permissions = platform_adapter.get_metadata(st)
        results_queue.put((filepath, size, mtime, permissions, owner, file_hash))

def writer_task(results_queue, db_file, table_name, worker_count):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA cache_size = -2000000;")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            full_path TEXT PRIMARY KEY, size_bytes INTEGER, mod_time INTEGER,
            permissions TEXT, owner_id TEXT, file_hash TEXT
        )""")
    conn.commit()
    batch, processed_count, workers_finished = [], 0, 0
    print(f"[Writer] Ready to receive results for table '{table_name}'.")
    while workers_finished < worker_count:
        result = results_queue.get()
        if result is None:
            workers_finished += 1
            continue
        batch.append(result)
        if len(batch) >= 2000:
            cursor.executemany(f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)", batch)
            conn.commit()
            processed_count += len(batch)
            print(f"  ... committed {processed_count} records", end='\r')
            batch = []
    if batch:
        cursor.executemany(f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)", batch)
        conn.commit()
        processed_count += len(batch)
    print(f"\n[Writer] Task complete. Total records processed: {processed_count}")
    conn.close()

def load_before_cache(db_file):
    if not os.path.exists(db_file): return None
    print("[Main] Loading 'before' snapshot into memory for delta-checking...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT full_path, size_bytes, mod_time, file_hash FROM snapshot_before")
        cache = {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}
        print(f"[Main] Cache loaded with {len(cache)} entries.")
        return cache
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()

def run_snapshot(args):
    start_time = time.time()
    table_name = f"snapshot_{args.command}"
    before_cache = load_before_cache(args.db_file) if args.command == "after" else None
    if args.command == "after" and not before_cache:
        print("Error: 'before' snapshot not found. Please run the 'before' command first.", file=sys.stderr)
        sys.exit(1)
    
    exclusion_set = {os.path.abspath(os.path.expanduser(p)) for p in args.exclude}
    scan_dirs = [os.path.abspath(os.path.expanduser(p)) for p in args.scan]

    with Manager() as manager:
        task_queue, results_queue = manager.Queue(), manager.Queue()
        writer_proc = Process(target=writer_task, args=(results_queue, args.db_file, table_name, args.workers))
        writer_proc.start()
        
        pool_args = (task_queue, results_queue, before_cache, PlatformAdapter())
        with Pool(processes=args.workers, initializer=consumer_worker, initargs=pool_args) as pool:
            producer_task(task_queue, scan_dirs, exclusion_set)
            pool.close()
            pool.join()
        
        for _ in range(args.workers):
            results_queue.put(None)
        writer_proc.join()
    
    print(f"\nTotal execution time for '{args.command}' snapshot: {time.time() - start_time:.2f} seconds.")

def run_analysis(args):
    if not os.path.exists(args.db_file):
        print(f"Error: Database file '{args.db_file}' not found.", file=sys.stderr)
        return
    print("\n--- Analysis Report ---")
    conn = sqlite3.connect(args.db_file)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT full_path, size_bytes FROM snapshot_after WHERE full_path NOT IN (SELECT full_path FROM snapshot_before)")
        new_files = cursor.fetchall()
        print(f"\n[+] {len(new_files)} New Files Created:")
        for path, size in new_files: print(f"  - {path} ({size} bytes)")
        cursor.execute("SELECT full_path FROM snapshot_before WHERE full_path NOT IN (SELECT full_path FROM snapshot_after)")
        deleted_files = cursor.fetchall()
        print(f"\n[-] {len(deleted_files)} Files Deleted:")
        for path, in deleted_files: print(f"  - {path}")
        cursor.execute("SELECT a.full_path, a.size_bytes FROM snapshot_after a JOIN snapshot_before b ON a.full_path = b.full_path WHERE a.file_hash != b.file_hash")
        modified_files = cursor.fetchall()
        print(f"\n[*] {len(modified_files)} Files Modified:")
        for path, size in modified_files: print(f"  - {path} (new size: {size} bytes)")
    except sqlite3.OperationalError as e:
        print(f"\nError during analysis: {e}. Did you run both 'before' and 'after' snapshots?", file=sys.stderr)
    finally:
        conn.close()
        print("\n--- End of Report ---")

def main():
    # Instantiate the adapter once to get default paths
    platform_adapter = PlatformAdapter()

    parser = argparse.ArgumentParser(description="A high-performance filesystem snapshot and analysis tool.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- Parent parser for shared arguments ---
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("-f", "--db-file", default="filesystem_snapshot.db", help="Path to the SQLite database file (default: filesystem_snapshot.db)")
    parent_parser.add_argument("-s", "--scan", action="append", default=platform_adapter.default_scan_dirs, help="Directory to scan (can be specified multiple times). Defaults to OS-specific paths.")
    parent_parser.add_argument("-e", "--exclude", action="append", default=platform_adapter.default_exclude_dirs, help="Directory to exclude from scan (can be specified multiple times). Defaults to OS-specific paths.")
    parent_parser.add_argument("-w", "--workers", type=int, default=cpu_count(), help=f"Number of worker processes to use (default: {cpu_count()})")

    # --- 'before' command ---
    parser_before = subparsers.add_parser("before", help="Create the initial 'before' snapshot of the filesystem.", parents=[parent_parser])
    parser_before.set_defaults(func=run_snapshot)

    # --- 'after' command ---
    parser_after = subparsers.add_parser("after", help="Create the 'after' snapshot to compare against the 'before' state.", parents=[parent_parser])
    parser_after.set_defaults(func=run_snapshot)

    # --- 'analyze' command ---
    parser_analyze = subparsers.add_parser("analyze", help="Analyze the differences between the 'before' and 'after' snapshots.")
    parser_analyze.add_argument("-f", "--db-file", default="filesystem_snapshot.db", help="Path to the SQLite database file to analyze (default: filesystem_snapshot.db)")
    parser_analyze.set_defaults(func=run_analysis)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
