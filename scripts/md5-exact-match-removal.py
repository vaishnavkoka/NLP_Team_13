import hashlib
import os
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def calculate_md5(file_path):
    """Calculates the MD5 hash of a given file."""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        # Read the file in chunks
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest(), file_path

def process_files_in_folder(folder_path):
    total_size_before = 0
    total_files_before = 0
    file_md5_dict = {}

    # Step 1: Create a file to MD5 dictionary using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        futures = []
        
        # Collecting all text files
        for root, _, files in os.walk(folder_path):
            for file in tqdm(files):
                file_path = os.path.join(root, file)
                if file_path.endswith('.txt'):
                    futures.append(executor.submit(calculate_md5, file_path))
        
        # Collect results
        for future in tqdm(as_completed(futures), total=len(futures), desc="Calculating MD5"):
            md5_hash, file_path = future.result()
            file_md5_dict[file_path] = md5_hash
            total_size_before += os.path.getsize(file_path)
            total_files_before += 1

    # Step 2: Create MD5 to file mapping dictionary
    md5_to_file = {}
    for file_path, md5_hash in file_md5_dict.items():
        md5_to_file[md5_hash] = file_path  # Override if MD5 already exists

    # Step 3: Calculate dataset size after deduplication
    total_size_after = sum(os.path.getsize(path) for path in md5_to_file.values())
    total_files_after = len(md5_to_file)
    
    # Step 6: Write unique file paths to a text file
    with open("unique_file_md5-exact-match.txt", "w") as sys.stdout:
        # Step 5: Print dataset size and number of articles before and after deduplication
        print(f"Total size before deduplication: {total_size_before / (1024 * 1024):.2f} MB")
        print(f"Total files before deduplication: {total_files_before}")
        print(f"Total size after deduplication: {total_size_after / (1024 * 1024):.2f} MB")
        print(f"Total files after deduplication: {total_files_after}")
        for file_path in md5_to_file.values():
            print(file_path)

# Replace 'your_folder_path' with the actual folder path containing text files
folder_path = "/mnt/HDFS1/language_nlp/nlp_hindi_team_13/scraping8-29"
process_files_in_folder(folder_path)
