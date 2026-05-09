import pandas as pd
import glob

def extrair_times_unicos():
    print("🔍 Iniciando a varredura nos arquivos Bronze...")
    
    # Busca todos os CSVs na pasta bronze
    arquivos = glob.glob("./data/bronze/*.csv")
    
    # Usamos a estrutura 'set' (conjunto) do Python porque ela 
    # automaticamente ignora valores duplicados
    times_unicos = set()
    
    if not arquivos:
        print("❌ Nenhum arquivo encontrado. Verifique se o caminho '../data/bronze/' está correto.")
        return

    for arquivo in arquivos:
        try:
            df = pd.read_csv(arquivo)
            
            # Pega os valores únicos das colunas Mandante e Visitante e adiciona ao conjunto
            if 'Mandante' in df.columns and 'Visitante' in df.columns:
                times_unicos.update(df['Mandante'].dropna().unique())
                times_unicos.update(df['Visitante'].dropna().unique())
            else:
                print(f"⚠️ Aviso: O arquivo {arquivo} não possui as colunas 'Mandante' ou 'Visitante'.")
                
        except Exception as e:
            print(f"❌ Erro ao ler o arquivo {arquivo}: {e}")

    # Converte o conjunto de volta para uma lista e ordena alfabeticamente
    times_ordenados = sorted(list(times_unicos))
    
    # Gera o arquivo de texto formatado como um dicionário Python
    caminho_saida = "esqueleto_de_para_times.txt"
    with open(caminho_saida, "w", encoding="utf-8") as f:
        f.write("DE_PARA_TIMES = {\n")
        for time in times_ordenados:
            # Limpa possíveis espaços em branco extras nas pontas antes de escrever
            time_limpo = str(time).strip()
            f.write(f'    "{time_limpo}": "{time_limpo}",\n')
        f.write("}\n")

    print("-" * 40)
    print(f"✅ Sucesso! Foram encontrados {len(times_ordenados)} times únicos em toda a história.")
    print(f"📂 Um arquivo chamado '{caminho_saida}' foi gerado na pasta atual.")
    print("👉 Dica: Abra o arquivo, altere apenas o nome do lado DIREITO dos dois pontos para o nome oficial, e depois cole no seu config.py!")

if __name__ == "__main__":
    extrair_times_unicos()