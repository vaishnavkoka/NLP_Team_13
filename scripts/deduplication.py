import os
from datasketch import MinHash, MinHashLSH
from tqdm import tqdm
import shutil
import concurrent.futures

unique_folder = "unique_folder"
total_size_before_deduplication = 0
total_size_after_deduplication = 0

def get_files_from_folders(folders):
    """Collects all file paths from the folders list."""
    file_paths = []
    for folder in tqdm(folders, total=len(folders), desc="getting all file paths"):
        for root, _, files in os.walk(folder):
            for file in files:
                if file.endswith('.txt'):
                    file_paths.append(os.path.join(root, file))
    return file_paths

def compute_minhash(file_path, num_perm=128):
    """Compute MinHash incrementally by reading file"""
    m = MinHash(num_perm=num_perm)
    try:
        text = ''
        with open(file_path, 'r', encoding='utf-8') as f:
            text += f.read()  # Read the entire file content
        m.update(text.encode('utf-8'))  # Update MinHash with the file content (encoded to bytes)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
    return m

def deduplicate_files(file_paths, minhashes, threshold=0.75, num_perm=128):
    """Performs deduplication based on MinHash similarity."""
    global total_size_after_deduplication
    global total_size_before_deduplication
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    unique_files = []
    os.makedirs(unique_folder, exist_ok=True)
    for file_path, minhash in tqdm(zip(file_paths, minhashes), total=len(file_paths), desc="Processing files"):
        data_size = os.path.getsize(file_path)
        total_size_before_deduplication+=data_size
        if lsh.query(minhash):  # If the file is similar to any in the LSH index, it's considered a duplicate
            continue
        else:
            lsh.insert(file_path, minhash)  # Insert the MinHash into the LSH for future queries
            unique_files.append(file_path)  # Add to the list of unique files
            total_size_after_deduplication+=data_size

    return unique_files

import concurrent.futures
from tqdm import tqdm

def minhash_computation(file_paths, num_perm=128, threshold=0.75):
    # MinHash computed in parallel, executer.map() preserves the order of input and output
    minhashes = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(compute_minhash, file_paths), total=len(file_paths), desc="MinHash computation")) 
    return deduplicate_files(file_paths, results, threshold, num_perm)


if __name__ == "__main__":
    # List of folders to process
    folders = [
        '/home/hindi_nlp/scrap/scraping8-29'
    ]

    # Get all file paths from the folders
    file_paths = get_files_from_folders(folders)

    # Perform MinHash computation and deduplication
    unique_files = minhash_computation(file_paths)

    # Output unique files
    with open('unique_files.txt', 'w') as file:
        file.write(f'total files before deduplication: {len(file_paths)}\n')
        file.write(f'size before deduplication: {total_size_before_deduplication/(1024*1024)} MB\n')
        file.write(f'total files after deduplication: {len(unique_files)}\n')
        file.write(f'size after deduplication: {total_size_after_deduplication/(1024*1024)} MB\n')
        for file_path in unique_files:
            file.write(f'{file_path}\n')
