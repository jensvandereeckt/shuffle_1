import time
import json
import io
import re
import logging
from datetime import datetime
from collections import defaultdict
from pyspark.sql import SparkSession
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1dvuwVEPnCEiN7uK1lkTl8ZZL_lAta5kB"  # generated_votes
OUTPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"  # reduced_votes
LOGS_FOLDER_ID = "1hXa-sxiy11T4NKLxWfDodhkliWcQr2Ba"             # logs folder op Drive
CHECK_INTERVAL = 15  # seconden
TOTAL_RUNTIME = 120  # seconden
COUNTRY_FILTER = ["be"]  # alleen deze landen verwerken

# === LOGGING CONFIGURATIE ===
LOG_FILENAME = f"vote_log_shuffle1{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def init_spark():
    return SparkSession.builder.appName("SongVoteCount").master("local[*]").getOrCreate()


def authenticate_drive():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=credentials)


def get_vote_files(drive_service, seen_files):
    query = f"'{INPUT_FOLDER_ID}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])
    pattern = re.compile(r"^generated_votes_([a-z]{2})\.txt$")
    filtered = []
    for item in items:
        match = pattern.match(item["name"])
        if match:
            country_code = match.group(1)
            if country_code in COUNTRY_FILTER and item["name"] not in seen_files:
                filtered.append((item["name"], item["id"]))
    return filtered


def download_file(drive_service, file_id, filename):
    request = drive_service.files().get_media(fileId=file_id)
    with open(filename, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def process_votes(spark, input_filename):
    rdd = spark.sparkContext.textFile(input_filename)
    mapped = rdd.map(lambda line: line.strip().split("\t")) \
                .filter(lambda fields: len(fields) >= 3 and fields[2].isdigit()) \
                .map(lambda fields: ((fields[0], fields[2]), 1))
    reduced = mapped.reduceByKey(lambda a, b: a + b)
    grouped = reduced.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                     .groupByKey() \
                     .mapValues(list)
    result = grouped.map(lambda x: {
        "country": x[0],
        "votes": [{"song_number": song, "count": count} for song, count in x[1]]
    }).collect()
    return result


def remove_old_drive_file(drive_service, filename):
    query = f"name='{filename}' and '{OUTPUT_FOLDER_ID}' in parents"
    old_files = drive_service.files().list(q=query, fields="files(id)").execute().get("files", [])
    for file in old_files:
        drive_service.files().delete(fileId=file["id"]).execute()


def upload_to_drive(drive_service, filename, folder_id):
    metadata = {"name": filename, "parents": [folder_id]}
    mimetype = "application/json" if filename.endswith(".json") else "text/plain"
    media = MediaFileUpload(filename, mimetype=mimetype)
    return drive_service.files().create(body=metadata, media_body=media, fields="id").execute()


def main():
    spark = init_spark()
    drive_service = authenticate_drive()
    seen_files = set()
    start_time = time.time()

    logging.info("Start stemverwerking voor 2 minuten.")
    print("Starten met controleren van stemmenbestanden voor 2 minuten (Ctrl+C om te stoppen)...")

    try:
        while time.time() - start_time < TOTAL_RUNTIME:
            new_files = get_vote_files(drive_service, seen_files)

            if new_files:
                for filename, file_id in new_files:
                    logging.info(f"Nieuw bestand gevonden: {filename}")
                    print(f"Nieuw bestand gevonden: {filename}")
                    download_file(drive_service, file_id, filename)
                    seen_files.add(filename)

                    logging.info("Verwerken met Spark...")
                    result = process_votes(spark, filename)

                    output_filename = filename.replace("generated_votes", "reduced_votes").replace(".txt", ".json")
                    with open(output_filename, "w") as f:
                        json.dump(result, f, indent=4)
                    logging.info(f"{output_filename} lokaal opgeslagen.")
                    print(f"'{output_filename}' lokaal opgeslagen.")

                    remove_old_drive_file(drive_service, output_filename)
                    response = upload_to_drive(drive_service, output_filename, OUTPUT_FOLDER_ID)
                    logging.info(f"Upload voltooid naar Drive (ID: {response.get('id')})")
                    print(f"Geüpload naar Drive (ID: {response.get('id')})")
            else:
                logging.info("Geen nieuwe bestanden gevonden.")
                print("Geen nieuwe bestanden gevonden, opnieuw proberen...")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        logging.warning("Handmatig gestopt door gebruiker.")
        print("🛑 Handmatig gestopt.")
    except Exception as e:
        logging.error(f"Fout opgetreden: {e}")
        print(f"⚠️  Fout: {e}")
    finally:
        spark.stop()
        logging.info("Spark sessie afgesloten.")
        print("🏁 Spark afgesloten.")

        try:
            response = upload_to_drive(drive_service, LOG_FILENAME, LOGS_FOLDER_ID)
            logging.info("Logbestand geüpload naar Drive.")
            print(f" Logbestand geüpload naar Drive (ID: {response.get('id')})")
        except Exception as e:
            print("⚠️  Upload logbestand naar Drive mislukt.")
            logging.error(f"Fout bij uploaden logbestand: {e}")


if __name__ == "__main__":
    main()
