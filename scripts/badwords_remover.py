import os

# create new file from source after removing badwords, this is done to not affect raw dataset
def remove_words_from_file(source_file, words_to_remove, output_file):

    with open(source_file, 'r', encoding='utf-8') as f:
        source_content = f.read()


    result_content = ' '.join([word for word in source_content.split() if word not in words_to_remove])


    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(result_content)

def get_folder_size(folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def batch_process_files(input_folder, words_file, output_folder):
    with open(words_file, 'r', encoding='utf-8') as f:
        words_to_remove = set(f.read().split())

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(input_folder):
        if filename.endswith(".txt"):
            input_file = os.path.join(input_folder, filename)
            output_file = os.path.join(output_folder, filename)
            remove_words_from_file(input_file, words_to_remove, output_file)
            print(f"Processed {filename}")

    original_size = get_folder_size(input_folder)
    cleaned_size = get_folder_size(output_folder)
    print(f"Total size of original folder '{input_folder}': {original_size / (1024**3):.2f} GB")
    print(f"Total size of cleaned folder '{output_folder}': {cleaned_size / (1024**3):.2f} GB")

input_folder = '/mnt/HDFS1/language_nlp/nlp_hindi_team_13/scraping8-29' # raw dataset folder
words_file = '/mnt/HDFS1/language_nlp/nlp_hindi_team_13/hindibadwords.txt' #file containing bad words
output_folder = '/mnt/HDFS1/language_nlp/nlp_hindi_team_13/cleaned_files' # folder to keep cleaned file

batch_process_files(input_folder, words_file, output_folder)