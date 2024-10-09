import pandas as pd
import os
from urllib.parse import urlparse
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


folder = "/mnt/HDFS1/language_nlp/nlp_hindi_team_13/scraping8-29/urls"
csv_file_path = folder+'/updated_urls.csv'
df = pd.read_csv(csv_file_path, error_bad_lines=False)
df.columns = ['file_name', 'domain', 'url']

result = df.groupby('domain')['file_name'].agg(['count']).reset_index()
result['total_size_MB'] = 0

for index, row in result.iterrows():
    domain = row['domain']
    total_size = 0
    for file_name in df[df['domain'] == domain]['file_name']:
        file_path = os.path.join('scraping8-29', file_name + '.txt')
        if os.path.isfile(file_path):
            total_size += os.path.getsize(file_path)
    result.at[index, 'total_size_MB'] = total_size

result['total_size_MB'] = result['total_size_MB'] // (1024 * 1024)
result_sorted = result.sort_values(by='domain', ascending=True)

total_domains = result_sorted.shape[0]
total_files = result_sorted['count'].sum()
total_size_MB = result_sorted['total_size_MB'].sum()
summary_row = pd.DataFrame({'domain': ['Total: '+ str(total_domains)], 'count': [total_files], 'total_size_MB': [total_size_MB]})
result_final = pd.concat([result_sorted, summary_row], ignore_index=True)

print(result_final)

output_csv_path = folder+'/result.csv'
result_final.to_csv(output_csv_path, index=False)
print(f"Results saved to {output_csv_path}")

# Create PDF
pdf_path = folder+'/result.pdf'  # Replace with your desired output path
with PdfPages(pdf_path) as pdf:
    fig, ax = plt.subplots(figsize=(8, len(result_final) * 0.4)) 
    ax.axis('tight')
    ax.axis('off')
    table_data = result_final.values.tolist()
    column_labels = result_final.columns.tolist()
    ax.table(cellText=table_data, colLabels=column_labels, cellLoc = 'left', loc='center')
    pdf.savefig(fig, bbox_inches='tight')
    plt.close()

print(f"Results saved to {pdf_path}")
