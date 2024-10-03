import requests 
import pandas as pd
from datetime import datetime, timedelta

# Token de autenticação do GitHub
GITHUB_TOKEN = ""
GITHUB_API_URL = "https://api.github.com/graphql"

# Cabeçalhos para a requisição GraphQL
headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

# Função para verificar o limite de requisições
def check_rate_limit():
    response = requests.get("https://api.github.com/rate_limit", headers=headers)
    if response.status_code == 200:
        rate_limit_data = response.json()
        remaining = rate_limit_data['rate']['remaining']
        reset_time = datetime.fromtimestamp(rate_limit_data['rate']['reset'])
        print(f"Requisições restantes: {remaining}")
        print(f"Reinicialização do limite em: {reset_time}")
    else:
        print(f"Falha ao verificar o limite da API. Status: {response.status_code}")

# Consulta GraphQL para obter repositórios populares (ordenados por estrelas)
query_repos = """
{
  search(query: "stars:>50000", type: REPOSITORY, first: 10) {
    edges {
      node {
        ... on Repository {
          name
          owner {
            login
          }
          pullRequests(states: [MERGED, CLOSED]) {
            totalCount
          }
          stargazerCount
        }
      }
    }
  }
}
"""

# Função para realizar a requisição GraphQL
def run_query(query):
    try:
        request = requests.post(GITHUB_API_URL, json={'query': query}, headers=headers, timeout=10)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception(f"Query falhou com status code {request.status_code}: {request.text}")
    except requests.Timeout:
        raise Exception("A requisição ao GitHub expirou.")

# Consulta GraphQL para obter PRs de um repositório
def get_pull_requests(owner, name, cursor=None):
    after_cursor = f', after: "{cursor}"' if cursor else ""
    query_prs = f"""
    {{
      repository(owner: "{owner}", name: "{name}") {{
        pullRequests(states: [MERGED, CLOSED], first: 10{after_cursor}) {{  # Limitando a 10 PRs por página
          edges {{
            node {{
              title
              createdAt
              mergedAt
              closedAt
              reviews {{
                totalCount
              }}
              reviewDecision
              bodyText  # Corpo do PR em formato Markdown
              changedFiles  # Quantidade de arquivos alterados
            }}
          }}
          pageInfo {{
            hasNextPage
            endCursor
          }}
        }}
      }}
    }}
    """
    return run_query(query_prs)

# Função para calcular a diferença de tempo entre dois timestamps
def calculate_review_time(pr):
    created_at = datetime.strptime(pr['createdAt'], '%Y-%m-%dT%H:%M:%SZ')
    closed_at = pr['mergedAt'] or pr['closedAt']
    closed_at = datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ')
    return (closed_at - created_at).total_seconds() / 3600  # Retorna a diferença em horas

# Função para salvar PRs em arquivo CSV
def save_to_csv(data, owner, name):
    df = pd.DataFrame(data)
    df.to_csv(f"{owner}_{name}_pull_requests.csv", index=False)

# Checa o limite da API antes de começar
check_rate_limit()

# Coleta os 100 primeiros repositórios populares
repos_data = run_query(query_repos)

# Verifica se o campo 'data' existe na resposta
if 'data' not in repos_data:
    print(f"Erro: A resposta não contém a chave 'data'. Resposta recebida: {repos_data}")
else:
    # Filtra repositórios que possuem mais de 100 PRs (MERGED + CLOSED)
    filtered_repos = [repo['node'] for repo in repos_data['data']['search']['edges'] if repo['node']['pullRequests']['totalCount'] > 100]

    # Itera sobre os repositórios e coleta PRs
    for repo in filtered_repos:
        owner = repo['owner']['login']
        name = repo['name']
        print(f"Coletando PRs do repositório {owner}/{name}...")

        all_prs = []
        has_next_page = True
        cursor = None

        while has_next_page:
            print(f"Buscando PRs a partir do cursor: {cursor}")
            pr_data = get_pull_requests(owner, name, cursor)
            prs = pr_data['data']['repository']['pullRequests']['edges']
            page_info = pr_data['data']['repository']['pullRequests']['pageInfo']
            has_next_page = page_info['hasNextPage']
            cursor = page_info['endCursor']
            print(f"Próxima página: {has_next_page}, Cursor atual: {cursor}")

            # Modificação para capturar o resultado da PR
            for pr in prs:
                pr_node = pr['node']
                if pr_node['reviews']['totalCount'] > 0:
                    review_time = calculate_review_time(pr_node)
                    if review_time > 1:
                        pr_result = "Merged" if pr_node['mergedAt'] else "Closed"
                        all_prs.append({
                            'title': pr_node['title'],
                            'createdAt': pr_node['createdAt'],
                            'mergedAt': pr_node['mergedAt'],
                            'closedAt': pr_node['closedAt'],
                            'reviewsCount': pr_node['reviews']['totalCount'],
                            'reviewDecision': pr_node['reviewDecision'],
                            'reviewTimeHours': review_time,
                            'descriptionLength': len(pr_node['bodyText']),  # Número de caracteres da descrição
                            'changedFiles': pr_node['changedFiles'],  # Quantidade de arquivos alterados
                            'prResult': pr_result  # Resultado da PR: "Merged" ou "Closed"
                        })

        # Salva os PRs filtrados em um arquivo CSV
        if all_prs:
            save_to_csv(all_prs, owner, name)
        print(f"Dados salvos em {owner}_{name}_pull_requests.csv")
        check_rate_limit()  # Verifica o limite da API após cada repositório
