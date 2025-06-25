import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io # Para lidar com arquivos em memória para download
import urllib.parse # Para codificar o link mailto

# --- Motivos padrão ---
motivos_padrao = [
    "PREÇO CARO CUSTO BENEFÍCIO", "SEM CONDIÇÕES FINANCEIRAS", "DESEJA DESCONTO", "QUEBRA CONSTANTE",
    "PROBLEMA NÃO RESOLVIDO", "NÃO QUER INFORMAR O MOTIVO", "INSATISFAÇÃO SERVIÇO CAMPO", 
    "QUEBRA CONSTANTE/FERRUGEM/ BARULHO", "INSATISFAÇÃO COM O TÉCNICO",
    "DISPONIBILIDADE DE AGENDA - DATA DISTANTE", "AGENDA NÃO CUMPRIDA", "ATRASO DE MANUTENÇÃO PREVENTIVA",
    "POSTURA DO ATENDENTE", "JÁ COMPROU OU GANHOU OUTRO PURIFICADOR", "DEMORA PARA SER ATENDIDO", 
    "PURIFICADOR SEM UTILIZAÇÃO", "TAMANHO/DESIGN/COR", "FECHAMENTO EMPRESA / FILIAL OU DEPARTAMENTO",
    "FALTA DE PRODUTO"
]

# --- Função principal de processamento ---
def processar_dados_churn_com_motivos(df, categorias_permitidas, pagadores_a_remover):
    """
    Processa o DataFrame de churn aplicando filtros e prioridades.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        categorias_permitidas (list): Lista de categorias permitidas para filtro.
        pagadores_a_remover (list): Lista de pagadores a serem removidos.

    Returns:
        pd.DataFrame or None: DataFrame processado ou None em caso de erro.
    """
    # st.info(f"Iniciando processamento dos dados. Linhas iniciais: {len(df)}") # Mensagem de depuração removida

    # --- Lógica para remover pagadores específicos (reintroduzido) ---
    if pagadores_a_remover:
        coluna_pagador = None
        if 'PAGADOR' in df.columns:
            coluna_pagador = 'PAGADOR'
        elif 'Pagador' in df.columns: # Caso de letra minúscula
            coluna_pagador = 'Pagador'
            st.warning("A coluna 'Pagador' (com 'P' minúsculo) foi encontrada. Recomenda-se ajustar para 'PAGADOR' na planilha.")

        if coluna_pagador:
            # st.info(f"Removendo pagadores da coluna: {coluna_pagador}") # Mensagem de depuração removida
            try:
                # Converte os pagadores a remover para o mesmo tipo de dado da coluna
                pagadores_a_remover_convertidos = [int(p) for p in pagadores_a_remover]

                # Garante que a coluna 'PAGADOR' também seja do tipo numérico para comparação
                # Erros na conversão resultam em NaN
                df[coluna_pagador] = pd.to_numeric(df[coluna_pagador], errors='coerce')
                
                # Filtra o DataFrame, removendo as linhas onde 'PAGADOR' está na lista
                df = df[~df[coluna_pagador].isin(pagadores_a_remover_convertidos)]
                # st.success(f"Pagadores removidos. Linhas restantes após filtro de pagadores: {len(df)}") # Mensagem de depuração removida
            except ValueError:
                st.warning("Os pagadores para remover devem ser números inteiros. Ignorando filtro de pagadores.")
            except Exception as e:
                st.error(f"Ocorreu um erro ao filtrar pagadores: {e}. Ignorando filtro de pagadores.")
        # else: # Mensagem de depuração removida
            # st.warning("A coluna 'PAGADOR' (ou 'Pagador') não foi encontrada na planilha. Verifique a existência e a grafia correta. Ignorando filtro de pagadores.")


    # Filtros padrão
    df = df[df['Tipo de Churn'] != 'Involuntário']
    # st.info(f"Linhas restantes após filtro 'Tipo de Churn' (Involuntário): {len(df)}") # Mensagem de depuração removida

    df = df[df['FORMAJURIDICA'] != 'C1']
    # st.info(f"Linhas restantes após filtro 'FORMAJURIDICA' (C1): {len(df)}") # Mensagem de depuração removida

    data_atual = datetime.now()
    limite_data = data_atual - timedelta(days=60)
    
    if 'DATACRIACAOOS' in df.columns:
        df['DATACRIACAOOS'] = pd.to_datetime(df['DATACRIACAOOS'], errors='coerce')
        df = df[df['DATACRIACAOOS'].notna()] # Remove linhas onde a conversão falhou
        # st.info(f"Linhas restantes após remover DATACRIACAOOS inválidas: {len(df)}") # Mensagem de depuração removida

        df = df[df['DATACRIACAOOS'] >= limite_data]
        # st.info(f"Linhas restantes após filtro de data (últimos 60 dias) em 'DATACRIACAOOS': {len(df)}") # Mensagem de depuração removida
    else:
        st.warning("A coluna 'DATACRIACAOOS' não foi encontrada. O filtro por data não será aplicado.")

    if 'STATUSINADIMPLENTE' in df.columns:
        df = df[df['STATUSINADIMPLENTE'] != 'I']
        # st.info(f"Linhas restantes após filtro de inadimplência em 'STATUSINADIMPLENTE': {len(df)}") # Mensagem de depuração removida
    else:
        st.warning("A coluna 'STATUSINADIMPLENTE' não foi encontrada. O filtro de inadimplência não será aplicado.")

    if 'CATEGORIA4' in df.columns:
        df = df[df['CATEGORIA4'].isin(categorias_permitidas)]
        prioridade_map = {categoria: i + 1 for i, categoria in enumerate(categorias_permitidas)}
        df['Prioridade Motivo'] = df['CATEGORIA4'].map(prioridade_map)
        df = df.sort_values(by=['DATACRIACAOOS', 'Prioridade Motivo'], ascending=[False, True])
        df = df.drop(columns=['Prioridade Motivo'])
        # st.info(f"Linhas restantes após filtro e ordenação por 'CATEGORIA4': {len(df)}") # Mensagem de depuração removida
    else:
        st.warning("A coluna 'CATEGORIA4' não foi encontrada. O filtro por categorias não será aplicado.")

    return df

# --- Interface Streamlit ---
st.set_page_config(layout="centered", page_title="Criação Mailing RAF") # Título do dash alterado
st.title("📊 Criação Mailing RAF") # Título do dash alterado
st.markdown("Para criar o mailing é necessário baixar a base do Tableau, no painel Backlog Churn, converter a mesma para formatos aceitos (.xlsx) e adiciona-la na aplicação") # Texto instrutivo adicionado
st.markdown("---")

# Inicializa o estado para os motivos se ainda não estiver definido
if 'motivos_input' not in st.session_state:
    st.session_state.motivos_input = "\n".join(motivos_padrao)

# Upload do arquivo de entrada
st.subheader("1. Selecione o Arquivo Excel de Entrada")
uploaded_file = st.file_uploader("Escolha um arquivo Excel", type=["xlsx", "xls"])

st.markdown("---")

# Motivos permitidos
st.subheader("2. Motivos Permitidos (um por linha)")
col1, col2 = st.columns([3, 1])
with col1:
    user_motivos = st.text_area(
        "Edite a lista de motivos:",
        value=st.session_state.motivos_input,
        height=250,
        key="motivos_text_area"
    )
with col2:
    st.write("") # Espaçador
    st.write("") # Espaçador
    if st.button("Restaurar Motivos Padrão 🔄"):
        st.session_state.motivos_input = "\n".join(motivos_padrao)
        st.success("Motivos restaurados para o padrão.")
        st.rerun()

st.markdown("---")

# --- 3. Pagadores a Remover (reintroduzido) ---
st.subheader("3. Pagadores a Remover")
st.info("Insira os pagadores a serem removidos, separados por vírgula (ex: 123, 456, 789).")
pagadores_str = st.text_input("Pagadores (números inteiros separados por vírgula):")

st.markdown("---")

# Botão para criar o mailing (agora Passo 4)
st.subheader("4. Gerar Mailing")
if st.button("🚀 Criar Mailing", help="Clique para processar os dados e gerar a planilha de churn"):
    if uploaded_file is not None:
        try:
            df_input = pd.read_excel(uploaded_file)
            
            categorias_usuario = [linha.strip() for linha in user_motivos.splitlines() if linha.strip()]
            
            if not categorias_usuario:
                st.warning("A lista de motivos não pode estar vazia.")
            else:
                # Processa a string de pagadores para uma lista de strings
                pagadores_a_remover = [p.strip() for p in pagadores_str.split(',') if p.strip()]

                df_processado = processar_dados_churn_com_motivos(df_input.copy(), categorias_usuario, pagadores_a_remover)

                if df_processado is not None:
                    num_casos = len(df_processado)
                    st.success(f"✅ Processamento concluído! Foram encontrados **{num_casos}** casos após o processamento.")
                    
                    if num_casos > 0:
                        st.subheader("5. Baixar e Enviar por E-mail") 
                        
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            df_processado.to_excel(writer, index=False, sheet_name='Churn Processado')
                        processed_data = output.getvalue()

                        # Formata a data atual para o nome do arquivo
                        data_atual_formatada = datetime.now().strftime("%d.%m.%Y")
                        nome_arquivo_saida = f"Mailing RAF {data_atual_formatada}.xlsx"

                        st.download_button(
                            label="📥 Baixar Planilha Processada",
                            data=processed_data,
                            file_name=nome_arquivo_saida, # Nome do arquivo alterado aqui
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="Clique para baixar o arquivo Excel processado."
                        )

                        st.markdown("---")

                        st.markdown("Para enviar o arquivo por e-mail, clique no botão abaixo. Você precisará anexar a planilha baixada manualmente.")
                        
                        subject = "Mailing de Churn Processado"
                        body = "Prezados,\n\nSegue em anexo o arquivo de mailing de churn processado. Por favor, adicione o arquivo 'planilha_churn_processada.xlsx' que você baixou.\n\nAtenciosamente,"
                        
                        mailto_link = f"mailto:?subject={urllib.parse.quote(subject)}&body={urllib.parse.quote(body)}"
                        
                        st.markdown(f'<a href="{mailto_link}" target="_blank" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-align: center; text-decoration: none; border-radius: 5px; font-weight: bold;">📧 Gerar Link de E-mail</a>', unsafe_allow_html=True)
                        st.info("Este link abrirá seu cliente de e-mail padrão. Lembre-se de anexar o arquivo 'planilha_churn_processada.xlsx' que você baixou.")

                    else:
                        st.warning("Nenhum caso encontrado após aplicar os filtros. Não há planilha para exportar ou enviar.")

        except Exception as e:
            st.error(f"Ops! Ocorreu um erro ao ler ou processar a planilha: {e}")
            st.error("Por favor, verifique se o arquivo é um Excel válido e se as colunas necessárias existem.")
    else:
        st.warning("Por favor, faça o upload de um arquivo Excel primeiro.")

st.markdown("---")
st.info("Desenvolvido com Streamlit e Pandas. Se tiver dúvidas ou precisar de ajustes, é só chamar!")
