import requests
import os
import gzip


# Check if directory is present
def check_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")


# Download and save JSONL file
def download_file(url, file_path):

    response = requests.get(url, stream=True, verify=False)

    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded: {file_path}")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")


# Test function to read file contents (handle gzip if needed)
def get_file_content(file_path):

    try:
        # If file is .gz, open with gzip
        if file_path.endswith(".gz"):
            with gzip.open(file_path, "rt", encoding="utf-8") as file:
                for i in range(5):
                    line = file.readline().strip()
                    if not line:
                        break
                    print(line)

        # Otherwise, read normally
        else:
            with open(file_path, "r", encoding="utf-8") as file:
                for i in range(5):
                    line = file.readline().strip()
                    if not line:
                        break
                    print(line)

    except Exception as e:
        print(f"Error reading file {file_path}: {e}")


if __name__ == "__main__":

    # Define root directory and data directory
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, "data")
    check_directory_exists(data_dir)

    # Download and read the review file
    review_url = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Software.jsonl.gz"
    review_file = os.path.join(data_dir, "software_reviews.jsonl.gz")

    download_file(review_url, review_file)
    get_file_content(review_file)

    # Download and read the metadata file
    metadata_url = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Software.jsonl.gz"
    metadata_file = os.path.join(data_dir, "software_metadata.jsonl.gz")

    download_file(metadata_url, metadata_file)
    get_file_content(metadata_file)
