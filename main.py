import requests
import datetime
import hashlib
import hmac
import base64
import csv
import time

# Informações do Log Analytics
workspace_id = 'KEY'  # ID do workspace
shared_key = 'KEY'  # Workspace PK
log_type = 'IbrDataTable'  # Tabela do Sentinel

# Função para gerar assinatura
def build_signature(workspace_id, shared_key, date, content_length, method, content_type, resource):
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode('utf-8')
    authorization = "SharedKey {}:{}".format(workspace_id, encoded_hash)
    return authorization

# Função para enviar dados
def send_data(batch_data):
    body = str(batch_data)
    date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(body)
    signature = build_signature(workspace_id, shared_key, date, content_length, 'POST', 'application/json', '/api/logs')
    uri = f'https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01'

    headers = {
        "Content-Type": "application/json",
        "Authorization": signature,
        "Log-Type": log_type,
        "x-ms-date": date
    }

    try:
        response = requests.post(uri, data=body, headers=headers, timeout=30, verify=False)
        if response.status_code == 200:
            print("Pacote de dados enviado com sucesso!")
        else:
            print(f"Erro ao enviar pacote: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão: {e}")

# Leitura do arquivo CSV e envio em pacotes de 50 linhas
csv_file = 'C:\\Users\\Natan\\OneDrive\\Documentos\\tcc natan\\merged_ibr\\dataset.csv'  # Dataset
batch_size = 1000  # Tamanho do pacote
batch_data = []  # Lista para armazenar os dados em pacotes

with open(csv_file, mode='r') as file:
    csv_reader = csv.DictReader(file)
    for i, row in enumerate(csv_reader, 1):
        # Supondo que os cabeçalhos do CSV correspondam aos campos JSON
        json_data = {
            "Time": row['Time'],
            "Source": row['Source'],
            "Destination": row['Destination'],
            "Protocol": row['Protocol'],
            "Length": int(row['Length']),
            "Info": row['Info']
        }
        batch_data.append(json_data)

        # Quando o pacote atingir 50 linhas, ele é enviado
        if i % batch_size == 0:
            send_data(batch_data)
            batch_data = []  # Limpa a lista para o próximo lote
            time.sleep(1)  # Pequeno atraso para evitar sobrecarga na API

    # Envia o restante dos dados (se houver menos de 50 no último lote)
    if batch_data:
        send_data(batch_data)

print("Envio de dados concluído.")
