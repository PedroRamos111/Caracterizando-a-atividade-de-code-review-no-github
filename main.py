import requests
import pandas as pd
from datetime import datetime
import time
import os  # Import necessário para verificar a existência do arquivo

# Token de autenticação do GitHub
GITHUB_TOKEN = ""
GITHUB_API_URL = "https://api.github.com/graphql"

# Cabeçalhos para a requisição GraphQL
headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

# Função para realizar a requisição GraphQL com maior espera entre tentativas
def run_query(query, retries=5, wait_time=10):
    for attempt in range(retries):
        try:
            request = requests.post(GITHUB_API_URL, json={'query': query}, headers=headers, timeout=30)
            
            if request.status_code == 200:
                return request.json()
            
            # Trata erros de rate limit (403)
            elif request.status_code == 403 and "X-RateLimit-Remaining" in request.headers:
                rate_limit_remaining = int(request.headers["X-RateLimit-Remaining"])
                rate_limit_reset = int(request.headers["X-RateLimit-Reset"])
                if rate_limit_remaining == 0:
                    wait_time = rate_limit_reset - int(time.time())
                    print(f"Limite de requisições excedido. Aguardando {wait_time} segundos...")
                    time.sleep(wait_time + 1)
                else:
                    print(f"Rate limit excedido, mas ainda há {rate_limit_remaining} requisições restantes.")
            
            else:
                print(f"Tentativa {attempt + 1} falhou com status code {request.status_code}: {request.text}")
                if attempt < retries - 1:
                    time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição (tentativa {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(wait_time)

    raise Exception(f"Falha ao executar a consulta após {retries} tentativas.")

# Consulta GraphQL para obter repositórios populares (ordenados por estrelas), com paginação
def get_repos_with_pagination(after_cursor=None):
    after = f', after: "{after_cursor}"' if after_cursor else ""
    query_repos = f"""
    {{
      search(query: "stars:>50000", type: REPOSITORY, first: 10{after}) {{
        edges {{
          node {{
            ... on Repository {{
              name
              owner {{
                login
              }}
              pullRequests(states: [MERGED, CLOSED]) {{
                totalCount
              }}
              stargazerCount
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    return run_query(query_repos)

# Função para calcular a diferença de tempo entre dois timestamps
def calculate_review_time(pr):
    try:
        created_at = datetime.strptime(pr['createdAt'], '%Y-%m-%dT%H:%M:%SZ')
        closed_at = pr['mergedAt'] or pr['closedAt']
        closed_at = datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ')
        return (closed_at - created_at).total_seconds() / 3600  # Retorna a diferença em horas
    except Exception as e:
        print(f"Erro ao calcular tempo de revisão: {e}")
        return 0

# Consulta GraphQL para obter PRs de um repositório
def get_pull_requests(owner, name, cursor=None):
    after_cursor = f', after: "{cursor}"' if cursor else ""
    query_prs = f"""
    {{
      repository(owner: "{owner}", name: "{name}") {{
        pullRequests(states: [MERGED, CLOSED], first: 50{after_cursor}) {{
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
              files(first: 1) {{
                totalCount
              }}
              body
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

# Função para salvar PRs em arquivo CSV
def save_to_csv(data, owner, name):
    df = pd.DataFrame(data)
    df.to_csv(f"{name}_pull_requests.csv", index=False)

# Função para coletar PRs de um repositório
def collect_pull_requests(repo):
    owner = repo['owner']['login']
    name = repo['name']
    
    # Verifica se o arquivo já existe
    file_name = f"{name}_pull_requests.csv"
    if os.path.exists(file_name):
        print(f"Arquivo {file_name} já existe. Pulando coleta para {owner}/{name}.")
        return  # Pula a coleta se o arquivo já existir

    print(f"Coletando PRs do repositório {owner}/{name}...")

    all_prs = []
    has_next_page = True
    cursor = None

    while has_next_page:
        print(f"Consultando PRs para {owner}/{name} com cursor: {cursor}")  # Log antes da requisição
        try:
            pr_data = get_pull_requests(owner, name, cursor)
            print(f"Dados recebidos para {owner}/{name}: {pr_data}")  # Log da resposta da API

            if 'data' not in pr_data:
                print(f"Erro ao obter PRs para o repositório {owner}/{name}: {pr_data}")
                break

            prs = pr_data['data']['repository']['pullRequests']['edges']
            page_info = pr_data['data']['repository']['pullRequests']['pageInfo']
            has_next_page = page_info['hasNextPage']
            cursor = page_info['endCursor']

            print(f"Processando {len(prs)} PRs para {owner}/{name}...")  # Log da quantidade de PRs

            for pr in prs:
                pr_node = pr['node']
                if pr_node['reviews']['totalCount'] > 0:
                    review_time = calculate_review_time(pr_node)
                    if review_time > 1:
                        all_prs.append({
                            'title': pr_node['title'],
                            'createdAt': pr_node['createdAt'],
                            'mergedAt': pr_node['mergedAt'],
                            'closedAt': pr_node['closedAt'],
                            'reviewsCount': pr_node['reviews']['totalCount'],
                            'reviewDecision': pr_node['reviewDecision'],
                            'reviewTimeHours': review_time,
                            'filesChanged': pr_node['files']['totalCount'],
                            'description': len(pr_node['body']) if pr_node['body'] else 0,
                            'result': 'Merged' if pr_node['mergedAt'] else 'Closed'
                        })
        except Exception as e:
            print(f"Erro ao coletar PRs para {owner}/{name}: {e}")
            break

    if all_prs:
        save_to_csv(all_prs, owner, name)
    print(f"Dados salvos em {file_name}")

# Função para coletar 200 repositórios em múltiplas chamadas e processar os PRs de cada lote
def collect_repos_and_prs():
    all_repos = []
    has_next_page = True
    cursor = None
    total_repos = 0
    batch_size = 10
    total_target = 200

    while total_repos < total_target and has_next_page:
        print(f"Coletando lote de repositórios, total até agora: {total_repos}")
        repos_data = get_repos_with_pagination(cursor)
        
        if 'data' not in repos_data:
            print(f"Erro: A resposta não contém a chave 'data'. Resposta recebida: {repos_data}")
            break

        repos = repos_data['data']['search']['edges']
        filtered_repos = [repo['node'] for repo in repos if repo['node']['pullRequests']['totalCount'] > 100]

        # Processa PRs dos repositórios filtrados
        for repo in filtered_repos:
            collect_pull_requests(repo)

        all_repos.extend(filtered_repos)
        total_repos += len(filtered_repos)

        page_info = repos_data['data']['search']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']

        time.sleep(5)  # Espera 5 segundos entre as consultas

    print(f"Total de repositórios processados: {total_repos}")
    return all_repos

# Executa a coleta de repositórios e PRs

collect_repos_and_prs()

