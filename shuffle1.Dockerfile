from pyspark.sql import SparkSession
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import json

# === SETUP ===
SERVICE_ACCOUNT_FILE = "service_account.json"
DRIVE_FOLDER_ID = "1dvuwVEPnCEiN7uK1lkTl8ZZL_lAta5kB"
INPUT_FILE_NAME = "generated_votes_be.txt"
OUTPUT_FILE_NAME = "reduced_votes.json"

# === DRIVE AUTH ===
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)
service = build("drive", "v3", credentials=creds)

# === DOWNLOAD inputbestand van Google Drive ===
query = f"name='{INPUT_FILE_NAME}' and '{DRIVE_FOLDER_ID}' in parents"
results = service.files().list(q=query, fields="files(id, name)").execute()
items = results.get("files", [])

if not items:
    raise Exception(f"Bestand '{INPUT_FILE_NAME}' niet gevonden in de Drive-map.")

file_id = items[0]['id']
request = service.files().get_media(fileId=file_id)
fh = io.FileIO(INPUT_FILE_NAME, 'wb')
downloader = MediaIoBaseDownload(fh, request)

done = False
while not done:
    status, done = downloader.next_chunk()

print(f"✅ Bestand {INPUT_FILE_NAME} is gedownload.")

# === SPARK STEMMENTELLING ===
spark = SparkSession.builder.appName("VoteCounter").master("local[*]").getOrCreate()
rdd = spark.sparkContext.textFile(INPUT_FILE_NAME)

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

# Save locally
with open(OUTPUT_FILE_NAME, "w") as f:
    json.dump(result, f, indent=4)

print(f"✅ Resultaat opgeslagen in {OUTPUT_FILE_NAME}")

# === UPLOAD naar Google Drive ===
file_metadata = {
    "name": OUTPUT_FILE_NAME,
    "parents": [DRIVE_FOLDER_ID]
}
media = MediaFileUpload(OUTPUT_FILE_NAME, mimetype="application/json")

uploaded = service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id"
).execute()

print(f"✅ {OUTPUT_FILE_NAME} geüpload naar Google Drive (ID: {uploaded.get('id')})")
