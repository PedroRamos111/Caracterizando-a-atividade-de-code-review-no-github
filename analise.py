import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

def process_file(file_path):

    data = pd.read_csv(file_path)

    data['createdAt'] = pd.to_datetime(data['createdAt'])
    data['mergedAt'] = pd.to_datetime(data['mergedAt'])
    data['closedAt'] = pd.to_datetime(data['closedAt'])

    rq01 = data.groupby('reviewDecision')['filesChanged'].median()

    rq02 = data.groupby('reviewDecision')['reviewTimeHours'].median()

    rq03 = data.groupby('reviewDecision')['description'].median()

    rq04 = data.groupby('reviewDecision')['reviewsCount'].median()

    rq05 = data.groupby('reviewsCount')['filesChanged'].median()

    rq06 = data.groupby('reviewsCount')['reviewTimeHours'].median()

    rq07 = data.groupby('reviewsCount')['description'].median()
    
    rq08 = data.groupby('reviewsCount')['reviewsCount'].median()

    return rq01, rq02, rq03, rq04, rq05, rq06, rq07, rq08


def process_all_files(directory):
    all_data = pd.DataFrame()

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            data = pd.read_csv(file_path)
            all_data = pd.concat([all_data, data], ignore_index=True)

    all_data['createdAt'] = pd.to_datetime(all_data['createdAt'])
    all_data['mergedAt'] = pd.to_datetime(all_data['mergedAt'])
    all_data['closedAt'] = pd.to_datetime(all_data['closedAt'])

    return all_data

directory_path = 'D:\Faculdade\Sexto Periodo\laboratorio 6\Lab3\Caracterizando-a-atividade-de-code-review-no-github\Dados'

all_data = process_all_files(directory_path)

approved_prs = all_data[all_data['reviewDecision'] == 'APPROVED']

median_files_changed = approved_prs.groupby('reviewDecision')['filesChanged'].median()
print("RQ 01 - Mediana de arquivos alterados por feedback final:")
print(median_files_changed)

median_review_time = approved_prs.groupby('reviewDecision')['reviewTimeHours'].median()
print("\nRQ 02 - Mediana do tempo de revisão por feedback final:")
print(median_review_time)

median_description = approved_prs.groupby('reviewDecision')['description'].median()
print("\nRQ 03 - Mediana da descrição por feedback final:")
print(median_description)

median_reviews_count = approved_prs.groupby('reviewDecision')['reviewsCount'].median()
print("\nRQ 04 - Mediana do número de revisões por feedback final:")
print(median_reviews_count)

median_reviews = approved_prs['reviewsCount'].median()
print("\nMediana do número de revisões:", median_reviews)

filtered_prs = approved_prs[approved_prs['reviewsCount'] == median_reviews]

median_files_changed_reviews = filtered_prs.groupby('reviewsCount')['filesChanged'].median()
print("\nRQ 05 - Mediana de arquivos alterados para a mediana do número de revisões:")
print(median_files_changed_reviews)

median_review_time_reviews = filtered_prs.groupby('reviewsCount')['reviewTimeHours'].median()
print("\nRQ 06 - Mediana do tempo de revisão para a mediana do número de revisões:")
print(median_review_time_reviews)

median_description_reviews = filtered_prs.groupby('reviewsCount')['description'].median()
print("\nRQ 07 - Mediana da descrição para a mediana do número de revisões:")
print(median_description_reviews)

median_reviews_count_reviews = filtered_prs.groupby('reviewsCount')['reviewsCount'].median()
print("\nRQ 08 - Mediana do número de revisões para a mediana do número de revisões:")
print(median_reviews_count_reviews)
directory_path = 'D:\Faculdade\Sexto Periodo\laboratorio 6\Lab3\Caracterizando-a-atividade-de-code-review-no-github\Dados'
results = process_all_files(directory_path)

for i, result in enumerate(results, 1):
    print(f"RQ {i:02d} - Resultado:\n{result}\n")

def process_all_files(directory):
    all_data = pd.DataFrame()

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            data = pd.read_csv(file_path)
            all_data = pd.concat([all_data, data], ignore_index=True)

    all_data['createdAt'] = pd.to_datetime(all_data['createdAt'])
    all_data['mergedAt'] = pd.to_datetime(all_data['mergedAt'])
    all_data['closedAt'] = pd.to_datetime(all_data['closedAt'])

    return all_data

def generate_boxplot(data, x_col, y_col, title):
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=data, x=x_col, y=y_col)
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def generate_scatter_plot(data, x_col, y_col, title):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=data, x=x_col, y=y_col)
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.tight_layout()
    plt.show()

directory_path = 'D:\Faculdade\Sexto Periodo\laboratorio 6\Lab3\Caracterizando-a-atividade-de-code-review-no-github\Dados'

all_data = process_all_files(directory_path)

approved_prs = all_data[all_data['reviewDecision'] == 'APPROVED']

generate_boxplot(approved_prs, 'reviewDecision', 'filesChanged', 'Arquivos Alterados para Pull Requests Aprovados')

generate_boxplot(approved_prs, 'reviewDecision', 'reviewTimeHours', 'Tempo de Revisão para Pull Requests Aprovados')

generate_boxplot(approved_prs, 'reviewDecision', 'description', 'Descrição e Pull Requests Aprovados')

generate_boxplot(approved_prs, 'reviewDecision', 'reviewsCount', 'Número de Revisões para Pull Requests Aprovados')

generate_scatter_plot(approved_prs, 'filesChanged', 'reviewsCount', 'Arquivos Alterados vs Número de Revisões para Pull Requests Aprovados')

generate_scatter_plot(approved_prs, 'reviewTimeHours', 'reviewsCount', 'Tempo de Revisão vs Número de Revisões para Pull Requests Aprovados')

generate_scatter_plot(approved_prs, 'description', 'reviewsCount', 'Descrição vs Número de Revisões')

generate_scatter_plot(approved_prs, 'reviewsCount', 'reviewsCount', 'Número de Revisões por Interações nos PRs')