import os
import hashlib
import shutil
from tqdm import tqdm
import concurrent.futures

# Global variables to track sizes
total_size_before_deduplication = 0
total_size_after_deduplication = 0

# Function to collect all file paths from the provided folder
def get_files_from_folder(folder_path):
    file_paths = []
    for root, _, files in os.walk(folder_path):
        for file in tqdm(files, total = len(files), desc = 'getting file_paths'):
            if file.endswith('.txt'):
                file_paths.append(os.path.join(root, file))
    return file_paths

# Function to calculate SHA-256 checksum for a document
def calculate_sha256(file_path):
    """Compute SHA-256 checksum for a file's content."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:  # Read as binary for checksum
            # Read file in chunks to handle large files
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return [file_path, sha256_hash.hexdigest()]
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# Function to convert hex hash to binary string
def hex_to_bin(hex_value):
    """Convert hexadecimal to binary string."""
    return bin(int(hex_value, 16))[2:].zfill(256)  # Convert hex to binary and pad to 256 bits

# Function to calculate Hamming distance between two binary strings
def hamming_distance(bin1, bin2):
    """Compute Hamming distance between two binary strings."""
    return sum(c1 != c2 for c1, c2 in zip(bin1, bin2))/256

# Function to deduplicate files based on SHA-256 and Hamming distance
def deduplicate_files(file_sha_map, threshold):
    global total_size_after_deduplication, total_size_before_deduplication
    unique_files = []
    
    for i in tqdm(range(len(file_sha_map)), desc="Processing files", unit="file"):
        data_size = os.path.getsize(file_sha_map[i][0])
        total_size_before_deduplication += data_size
        
        # Calculate checksum
        checksum = file_sha_map[i][1]
        
        if checksum is not None:
            binary_hash = hex_to_bin(checksum)  # Convert hex to binary
            
            # Compare with already seen unique files
            is_duplicate = False
            for j in unique_files:
                if hamming_distance(file_sha_map[i][1], file_sha_map[j][1]) <= threshold:
                    is_duplicate = True
                    break
            
            if is_duplicate:
                continue
            else:
                # Store the unique file's binary hash and path
                unique_files.append(i)
                total_size_after_deduplication += data_size

    return [file_sha_map[j] for j in unique_files]

# Function to compute SHA-256 checksums, Hamming distance, and deduplicate files
def sha256_hamming_computation(file_paths, threshold=.75):
    # Perform deduplication
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Calculate unique words in parallel
        file_sha_map = list(tqdm(executor.map(calculate_sha256, file_paths), total=len(file_paths), desc="sha256 calculation"))
    unique_files = deduplicate_files(file_sha_map, threshold)
    return unique_files

if __name__ == "__main__":
    folder_path = "/mnt/HDFS1/language_nlp/nlp_hindi_team_13/scraping8-29"  # Folder containing the text files
    
    # Get all file paths from the folder
    file_paths = get_files_from_folder(folder_path)
    # Perform SHA-256 computation and deduplication based on Hamming distance
    unique_files = sha256_hamming_computation(file_paths, threshold=.2)
    
    # Output the results
    with open('unique_files-md256.txt', 'w') as f:
        f.write(f"Total files before deduplication: {len(file_paths)}\n")
        f.write(f"Total size before deduplication: {total_size_before_deduplication / (1024 * 1024)} MB\n")
        f.write(f"Total files after deduplication: {len(unique_files)}\n")
        f.write(f"Total size after deduplication: {total_size_after_deduplication / (1024 * 1024)} MB\n")
        for file_path in unique_files:
            f.write(f"{file_path}\n")
