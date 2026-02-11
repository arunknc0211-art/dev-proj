import os
import re
import zipfile
from datetime import datetime
from urllib.parse import urlparse, unquote

import pandas as pd
import paramiko

from src.utils.config_utils import ConfigUtils
from src.utils.csv_excel_mssql_utils import upload_to_mssql
from src.utils.logging_utils import get_logger


def connect_sftp_from_conn_string(conn_string):
    parsed = urlparse(conn_string)
    if parsed.scheme != "sftp":
        raise ValueError("Connection string must start with 'sftp://'")

    transport = paramiko.Transport((parsed.hostname, parsed.port or 22))
    transport.default_timeout = 120  # 2-minute timeout
    transport.connect(username=unquote(parsed.username), password=unquote(parsed.password))
    sftp = paramiko.SFTPClient.from_transport(transport)
    get_logger().info("Connected to SFTP server.")
    return sftp, transport


def get_latest_zip_file(files, prefix):
    pattern = re.compile(r"(\d{4}-\d{2}) " + re.escape(prefix))
    dated_files = []
    for file in files:
        match = pattern.match(file)
        if match:
            date_str = match.group(1)
            try:
                file_date = datetime.strptime(date_str, "%Y-%m")
                dated_files.append((file_date, file))
            except Exception:
                continue
    if not dated_files:
        return None
    dated_files.sort(key=lambda x: x[0], reverse=True)
    return dated_files[0][1]


def migrate_data_from_sftp_csv_to_sql(module_name):
    config = ConfigUtils()

    # === Read configuration values ===
    conn_str = config.get(module_name, "source.connection.url")
    db_conn = config.get(module_name, "destination.connection.url").strip('$')
    db_name = config.get(module_name, "destination.connection.dbname")

    sftp_config = config.get(module_name, "source.files")[0]
    base_folder = sftp_config["base_folder"]
    pattern_prefix = sftp_config["pattern_prefix"]
    local_path = sftp_config["local_download_path"]
    table_name = sftp_config["target_table_name"]
    find_csv_name = sftp_config["find_csv_name"]
    mode = sftp_config.get("mode", "truncate")

    # === Additional config and optional backfill month ===
    delimiter = config.get(module_name, "source.delimiter", ",")
    encoding = config.get(module_name, "source.encoding", "utf-8")

    retries = int(config.get(module_name, "destination.retries", 3))
    delay = int(config.get(module_name, "destination.delay", 5))
    filter_column = config.get(module_name, "destination.filter_column", "SqlBulkCopy_TS")
    target_month = config.get(module_name, "options.target_month", None)  # <--- NEW

    os.makedirs(local_path, exist_ok=True)

    sftp, transport = connect_sftp_from_conn_string(conn_str)

    try:
        get_logger().info(f"Listing files in SFTP folder: {base_folder}")
        files = sftp.listdir(base_folder)

        # === NEW: Choose ZIP file (specific month or latest) ===
        if target_month:
            get_logger().info(f"Searching for ZIP file for specified month: {target_month}")
            month_pattern = re.compile(rf"^{re.escape(target_month)} .*{re.escape(pattern_prefix)}$")
            matched_files = [f for f in files if month_pattern.match(f)]
            if matched_files:
                latest_zip = matched_files[0]
                get_logger().info(f"Found ZIP for {target_month}: {latest_zip}")
            else:
                raise FileNotFoundError(f"No ZIP found for specified month: {target_month}")
        else:
            latest_zip = get_latest_zip_file(files, pattern_prefix)
            if not latest_zip:
                get_logger().warning(
                    f"No ZIP files found in '{base_folder}' matching pattern '{pattern_prefix}'."
                )
                return
            get_logger().info(f"Latest ZIP file found: {latest_zip}")

        # === Extract date from filename or fallback to file modified date ===
        date_match = re.match(r"(\d{4}-\d{2})", latest_zip)
        if date_match:
            file_date = datetime.strptime(date_match.group(1), "%Y-%m").date()
            get_logger().info(f"Extracted file date from filename: {file_date}")
        else:
            file_stat = sftp.stat(os.path.join(base_folder, latest_zip))
            file_date = datetime.fromtimestamp(file_stat.st_mtime).date()
            get_logger().info(f"Using SFTP file modification date as file date: {file_date}")

        # === Download ZIP to local path ===
        remote_zip_path = os.path.join(base_folder, latest_zip)
        local_zip_path = os.path.join(local_path, latest_zip)

        with sftp.open(remote_zip_path, "rb") as remote_file, open(local_zip_path, "wb") as local_file:
            local_file.write(remote_file.read())
        get_logger().info(f"Downloaded ZIP file to local path: {local_zip_path}")

        # === Unzip contents ===
        unzip_dir = os.path.join(local_path, "unzipped")
        if os.path.exists(unzip_dir):
            for root, dirs, files_in_dir in os.walk(unzip_dir, topdown=False):
                for file in files_in_dir:
                    os.remove(os.path.join(root, file))
                for d in dirs:
                    os.rmdir(os.path.join(root, d))
        else:
            os.makedirs(unzip_dir, exist_ok=True)

        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(unzip_dir)
        get_logger().info(f"Extracted ZIP contents to {unzip_dir}")

        # === Locate CSV ===
        found_csv_path = None
        for root, _, files_in_dir in os.walk(unzip_dir):
            for file in files_in_dir:
                if file.lower() == find_csv_name.lower() and file.lower().endswith(".csv"):
                    found_csv_path = os.path.join(root, file)
                    break
            if found_csv_path:
                break

        if not found_csv_path:
            raise FileNotFoundError(f"CSV file '{find_csv_name}' not found in extracted ZIP folder.")

        get_logger().info(f"Found CSV file: {found_csv_path}")

        # === Read and upload to SQL Server ===
        df = pd.read_csv(found_csv_path, dtype=str, delimiter=delimiter, encoding=encoding)
        if "sqlbulkcopyTS" in df.columns:
            df = df.drop(columns=["sqlbulkcopyTS"])
        df["File_Date"] = pd.to_datetime(file_date)

        oc, ic, fc, skip_msg = upload_to_mssql(
            df,
            db_conn,
            db_name,
            table_name,
            retries=retries,
            delay=delay,
            mode=mode,
            filter_column=filter_column,
        )

        get_logger().info(f"Data upload completed. Original rows: {oc}, Inserted rows: {ic}, Failed rows: {fc}")
        if skip_msg:
            get_logger().warning(f"Upload message: {skip_msg}")

    except Exception as e:
        get_logger().error(f"Error during SFTP ingestion: {str(e)}")
        raise
    finally:
        sftp.close()
        transport.close()


if __name__ == "__main__":
    migrate_data_from_sftp_csv_to_sql("INGEST_RJ_HEALTH_DATA_FROM_SFTP")
