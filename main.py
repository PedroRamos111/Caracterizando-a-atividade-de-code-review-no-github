import requests
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Token de autenticação do GitHub
GITHUB_TOKEN = ""
GITHUB_API_URL = "https://api.github.com/graphql"

# Cabeçalhos para a requisição GraphQL
headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

# Função para imprimir status a cada 60 segundos
def print_status():
    print("O script está em execução...")
    threading.Timer(60, print_status).start()  # Verifica a cada 60 segundos

# Chama a função de status
print_status()

# Função para realizar a requisição GraphQL
def run_query(query, retries=3, wait_time=5):
    for attempt in range(retries):
        try:
            request = requests.post(GITHUB_API_URL, json={'query': query}, headers=headers)
            
            # Verifica se a requisição foi bem-sucedida
            if request.status_code == 200:
                return request.json()
            
            # Trata erros de rate limit (403)
            elif request.status_code == 403 and "X-RateLimit-Remaining" in request.headers:
                rate_limit_remaining = int(request.headers["X-RateLimit-Remaining"])
                rate_limit_reset = int(request.headers["X-RateLimit-Reset"])
                if rate_limit_remaining == 0:
                    wait_time = rate_limit_reset - int(time.time())
                    print(f"Limite de requisições excedido. Aguardando {wait_time} segundos...")
                    time.sleep(wait_time + 1)  # Espera até que o limite seja resetado
                else:
                    print(f"Rate limit excedido, mas ainda há {rate_limit_remaining} requisições restantes.")
            
            else:
                print(f"Tentativa {attempt + 1} falhou com status code {request.status_code}: {request.text}")
                if request.status_code == 502:
                    print("Erro 502: Timeout ou erro no servidor. Tentando novamente...")
                if attempt < retries - 1:
                    time.sleep(wait_time)  # Aguarda antes de tentar novamente

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição (tentativa {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(wait_time)  # Tenta novamente após uma breve pausa

        except Exception as e:
            print(f"Erro inesperado na tentativa {attempt + 1}: {e}")
            break

    raise Exception(f"Falha ao executar a consulta após {retries} tentativas.")

# Função para obter os repositórios (limitando para 10 por requisição)
def get_pull_requests(cursor=None):
    after_cursor = f', after: "{cursor}"' if cursor else ""
    query_repos = f"""
    {{
      search(query: "stars:>50000", type: REPOSITORY, first: 10{after_cursor}) {{
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

# Função para coletar repositórios com paginação (10 por vez)
def collect_repositories():
    all_repos = []
    has_next_page = True
    cursor = None

    while has_next_page and len(all_repos) < 200:
        repos_data = get_pull_requests(cursor)
        
        # Verifica se o campo 'data' existe na resposta
        if 'data' not in repos_data:
            print(f"Erro: A resposta não contém a chave 'data'. Resposta recebida: {repos_data}")
            break

        repos = repos_data['data']['search']['edges']
        page_info = repos_data['data']['search']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']

        # Adiciona os repositórios ao conjunto geral
        all_repos.extend([repo['node'] for repo in repos if repo['node']['pullRequests']['totalCount'] > 100])

        # Pausa para evitar sobrecarga
        time.sleep(2)

    return all_repos[:200]  # Garante que não pegue mais de 200

# Coleta os 200 primeiros repositórios populares
filtered_repos = collect_repositories()

# Função para calcular a diferença de tempo entre dois timestamps
def calculate_review_time(pr):
    try:
        created_at = datetime.strptime(pr['createdAt'], '%Y-%m-%dT%H:%M:%SZ')
        closed_at = pr['mergedAt'] or pr['closedAt']
        closed_at = datetime.strptime(closed_at, '%Y-%m-%dT%H:%M:%SZ')
        return (closed_at - created_at).total_seconds() / 3600  # Retorna a diferença em horas
    except Exception as e:
        print(f"Erro ao calcular tempo de revisão: {e}")
        return 0  # Retorna 0 horas em caso de erro

# Função para salvar PRs em arquivo CSV
def save_to_csv(data, owner, name):
    df = pd.DataFrame(data)
    df.to_csv(f"{owner}_{name}_pull_requests.csv", index=False)

# Função para coletar PRs de um repositório
def collect_pull_requests(repo):
    owner = repo['owner']['login']
    name = repo['name']
    print(f"Coletando PRs do repositório {owner}/{name}...")

    all_prs = []
    has_next_page = True
    cursor = None

    while has_next_page:
        try:
            pr_data = get_pull_requests(owner, name, cursor)

            if 'data' not in pr_data:
                print(f"Erro ao obter PRs para o repositório {owner}/{name}: {pr_data}")
                break

            prs = pr_data['data']['repository']['pullRequests']['edges']
            page_info = pr_data['data']['repository']['pullRequests']['pageInfo']
            has_next_page = page_info['hasNextPage']
            cursor = page_info['endCursor']

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
    print(f"Dados salvos em {owner}_{name}_pull_requests.csv")

# Executa a coleta de PRs para múltiplos repositórios em paralelo
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_repo = {executor.submit(collect_pull_requests, repo): repo for repo in filtered_repos}
    
    for future in as_completed(future_to_repo):
        repo = future_to_repo[future]
        try:
            future.result()
        except Exception as e:
            owner = repo['owner']['login']
            name = repo['name']
            print(f"Erro no processamento do repositório {owner}/{name}: {e}")
