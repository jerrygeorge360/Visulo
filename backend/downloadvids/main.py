import os
import time

import pandas as pd
from yt_dlp import YoutubeDL
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

def extract_cookies(url,username,password):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Launch browser in headless mode
        page = browser.new_page()

        # Go to the desired URL
        page.goto(url)
        page.screenshot(path="page_screenshot.png")
        #
        # # Wait for the login button or other elements
        # page.wait_for_selector('input[type="email"]')
        # page.screenshot(path="page_screenshot.png")
        # page.fill('input[type="email"]',username )  # Fill in the email
        # page.click('button[type="button"]')  # Click the "Next" button after email
        #
        # page.wait_for_selector('input[type="password"]')  # Wait for the password field
        # page.fill('input[type="password"]', password)  # Fill in the password
        # page.click('button[type="button"]')  # Click the "Sign In" button
        #
        # # Wait for some element after login (e.g., your profile icon or a page element)
        # page.wait_for_selector('ytd-masthead #avatar-btn')  # Wait for the avatar button to appear
        #

        cookies = page.context.cookies()

        # Save cookies to a file
        with open("cookies.txt", "w") as f:
            for cookie in cookies:
                f.write(f"{cookie['name']}={cookie['value']}\n")

        browser.close()

    print("Cookies saved to cookies.txt")

class VideosMeta(ABC):
    @abstractmethod
    def get_videos(self, video_id, max_workers):
        ...

class Videos(VideosMeta):
    def __init__(self, output_folder: str, lang: list, storage_connection_string: str, container_name: str, cookies_path: str = 'cookies.txt'):
        self.output_folder = output_folder
        self.language = lang
        self.storage_connection_string = storage_connection_string
        self.container_name = container_name
        self.cookies_path = cookies_path  # Path to cookies file

        self.blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)

        # Ensure the output folder exists locally (for any temporary files if needed)
        os.makedirs(output_folder, exist_ok=True)

    def get_videos(self, csv_url: str, csv_column: str, max_workers: int = 5):
        df = pd.read_csv(csv_url)
        video_ids = df[csv_column].tolist()

        failed_ids = []
        total_videos = len(video_ids)
        completed = 0
        successful_download = 0
        lock = threading.Lock()
        stop_event = threading.Event()

        def download_videos(video_id):
            if stop_event.is_set():
                return

            nonlocal completed, successful_download
            unique_folder = os.path.join(self.output_folder, video_id)
            os.makedirs(unique_folder, exist_ok=True)
            ydl_opts = {
                "outtmpl": os.path.join(unique_folder, "%(id)s.%(ext)s"),
                "format": "best",
                "writesubtitles": True,
                "subtitleslangs": self.language,
                "subtitlesformat": "json3",
                "cookies": self.cookies_path  # Use the cookies file
            }

            with YoutubeDL(ydl_opts) as ydl:
                try:
                    info_dict = ydl.extract_info(f"youtube.com/watch?v={video_id}", download=False)
                    subs = list(info_dict["subtitles"].keys())

                    if "subtitles" in info_dict and any(lang in subs for lang in self.language):
                        print(f'Relevant subtitles found for {video_id}...')
                        ydl.download([video_id])
                        time.sleep(1)

                        # Upload files to Azure Blob Storage in a folder named after video_id
                        for file_name in os.listdir(unique_folder):
                            file_path = os.path.join(unique_folder, file_name)
                            if os.path.isfile(file_path):
                                # Here, we simulate a folder by including the video_id in the blob name
                                blob_name = f"{video_id}/{file_name}"  # video_id acts as the "folder"
                                blob_client = self.container_client.get_blob_client(blob_name)
                                with open(file_path, "rb") as data:
                                    blob_client.upload_blob(data, overwrite=True)
                                print(f"Uploaded {file_name} to Azure Blob Storage in folder {video_id}.")

                        # Delete local files after uploading to Azure Blob Storage
                        for file_name in os.listdir(unique_folder):
                            file_path = os.path.join(unique_folder, file_name)
                            if os.path.isfile(file_path):
                                os.remove(file_path)  # Remove the file
                                print(f"Deleted local file: {file_name}")

                        # Remove the folder if it's empty
                        if not os.listdir(unique_folder):
                            os.rmdir(unique_folder)
                            print(f"Deleted empty local folder: {unique_folder}")

                        with lock:
                            successful_download += 1
                            if successful_download >= 1000:
                                print("Stopping after first successful download.")
                                stop_event.set()
                                return

                    else:
                        print(f"Subtitles available for video {video_id}: {subs}")
                        failed_ids.append(video_id)
                        # Remove the folder if it's empty
                        if not os.listdir(unique_folder):
                            os.rmdir(unique_folder)
                            print(f"Deleted empty local folder: {unique_folder}")

                except Exception as e:
                    print(f"Failed to download captions for {video_id}: {e}")
                    failed_ids.append(video_id)
                finally:
                    with lock:
                        completed += 1
                        print(f"Progress: {completed}/{total_videos} videos completed.")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_videos, video_id) for video_id in video_ids]

            for future in as_completed(futures):
                if stop_event.is_set():
                    break

        if failed_ids:
            print(f"Failed to download captions for the following video IDs: {failed_ids}")


if __name__ == '__main__':
    load_dotenv()
    url = "https://www.youtube.com"
    username = os.getenv('YOUTUBE_USERNAME')
    password = os.getenv('YOUTUBE_PASSWORD')
    extract_cookies(url,username, password)
    storage_connection_string = os.getenv('AZURE_BLOB_STRING')
    container_name = os.getenv('AZURE_CONTAINER_NAME')
    captions = Videos(output_folder='icelandic', lang=['en-US', 'en-GB', 'en'],
                      storage_connection_string=storage_connection_string, container_name=container_name, cookies_path='cookies.txt')
    captions.get_videos('youtube-sl-25_youtube-sl-25-metadata.csv', csv_column='Bdj5MUf_3Hc', max_workers=1)