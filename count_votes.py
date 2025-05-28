from pyspark.sql import SparkSession
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import json
import re

# ===== CONFIG =====
SERVICE_ACCOUNT_FILE = "service_account.json"
INPUT_FOLDER_ID = "1dvuwVEPnCEiN7uK1lkTl8ZZL_lAta5kB"
OUTPUT_FOLDER_ID = "1BqafbJaqYDzTe0en_IQnsPDZOGoHizql"

# ===== AUTHENTICATE TO GOOGLE DRIVE =====
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# ===== ZOEK STEMMINGSBESTANDEN =====
print("🔍 Zoeken naar gegenereerde stemmingsbestanden in Drive-folder...")
query = f"'{INPUT_FOLDER_ID}' in parents"
results = drive_service.files().list(q=query, fields="files(id, name)").execute()
items = results.get("files", [])

# Filter bestanden met patroon 'generated_votes_XX.txt'
pattern = re.compile(r"^generated_votes_([a-z]{2})\.txt$")
vote_files = [(item["name"], item["id"]) for item in items if pattern.match(item["name"])]

if not vote_files:
    raise Exception("❌ Geen gegenereerde stemmingsbestanden gevonden.")

# Start Spark
spark = SparkSession.builder \
    .appName("SongVoteCount") \
    .master("local[*]") \
    .getOrCreate()

# Verwerk elk stemmingsbestand
for input_filename, file_id in vote_files:
    print(f"⬇️ Downloaden van: {input_filename}")
    request = drive_service.files().get_media(fileId=file_id)
    with open(input_filename, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    print(f"✅ Bestand '{input_filename}' gedownload.")

    # ===== STEMMEN VERWERKEN MET SPARK =====
    print("⚙️ Spark-telling wordt uitgevoerd...")
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

    # ===== LOKAAL OPSLAAN =====
    output_filename = input_filename.replace("generated_votes", "reduced_votes").replace(".txt", ".json")
    with open(output_filename, "w") as f:
        json.dump(result, f, indent=4)
    print(f"💾 Resultaat opgeslagen in '{output_filename}'.")

    # ===== DRIVE: OUDE VERSIE VERWIJDEREN =====
    query = f"name='{output_filename}' and '{OUTPUT_FOLDER_ID}' in parents"
    old_files = drive_service.files().list(q=query, fields="files(id)").execute().get("files", [])
    for file in old_files:
        drive_service.files().delete(fileId=file["id"]).execute()
        print(f"🗑️ Oude versie '{output_filename}' verwijderd van Drive.")

    # ===== UPLOAD NIEUWE VERSIE =====
    file_metadata = {
        "name": output_filename,
        "parents": [OUTPUT_FOLDER_ID]
    }
    media = MediaFileUpload(output_filename, mimetype="application/json")
    upload_response = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    print(f"📤 '{output_filename}' geüpload naar Drive (ID: {upload_response.get('id')})")

print("🏁 Alle bestanden verwerkt.")
