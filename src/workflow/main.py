import os
import dotenv
import requests
import json

dotenv.load_dotenv(".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") #variavel de ambiente
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN") #variavel de ambiente



def list_job_names():
    return [i.replace(".json", "") for i in os.listdir(".") if i.endswith(".json")] #pegar nome do arquivo


def load_settings(job_name):
    with open(f"{job_name}.json", "r") as open_file:
        settings = json.load(open_file) #ler o arquivo json
    return settings

def reset_job(settings):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/reset" #API databricks para modificar os Jobs de forma local.
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"} #Processo de autenticação
    resp = requests.post(url=url, headers=header, json=settings)
    return resp


def main():
    for i in list_job_names():
        settings = load_settings(job_name=i)
        resp = reset_job(settings=settings)
        if resp.status_code == 200:
            print(f"Workflow '{i}' atualizado")
        else:
            print(f"Error: {resp.text}")

if __name__ == "__main__":
    main()