from google.oauth2 import service_account


def upload_to_gcs(local_file: str, bucket_name: str, folder: str, target_file: str, auth_json: str) -> None:
    client = storage.Client.from_service_account_json(auth_json)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(folder + target_file)

    print(f'Uploading {local_file} to gs://{bucket_name}/{folder}{target_file}')

    blob.upload_from_filename(local_file)
