"""
fetch_all.py — Morais Engenharia · Site de Controle Interno
Busca dados de 3 fontes e gera data.json unificado:
  1. Notion — Banco de Dados Documentos
  2. Notion — Banco de Dados Vendas
  3. ERP    — API Mais Controle (Pagamentos + Propostas)
Chave de JOIN entre os 3 bancos: ENDEREÇO (normalizado)
"""

import os
import json
import requests
import base64
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CREDENCIAIS (via GitHub Secrets)
# ─────────────────────────────────────────────
NOTION_TOKEN_DOCS   = os.environ["NOTION_TOKEN_DOCS"]    # token do banco de documentos
NOTION_DB_DOCS      = os.environ["NOTION_DB_DOCS"]       # 32fc5ab532d380a0900dd7f4bfc619bd
NOTION_TOKEN_VENDAS = os.environ["NOTION_TOKEN_VENDAS"]  # token do banco de vendas
NOTION_DB_VENDAS    = os.environ["NOTION_DB_VENDAS"]     # 33cc5ab532d38047ae3aee8b87ac1f4d
ERP_USERNAME        = os.environ["ERP_USERNAME"]         # joaovitorcabral94@gmail.com
ERP_PASSWORD        = os.environ["ERP_PASSWORD"]         # senha do ERP

# ─────────────────────────────────────────────
# HELPERS GERAIS
# ─────────────────────────────────────────────

def normalizar_endereco(s):
    """Normaliza endereço para JOIN: maiúsculas, sem espaços duplos, strip."""
    if not s:
        return ""
    return " ".join(str(s).upper().split())


def prop(page, nome, tipo="texto"):
    """
    Extrai valor de propriedade do Notion de forma segura.
    tipo: texto | numero | data | checkbox | select | multi_select | formula
    """
    props = page.get("properties", {})
    p = props.get(nome)
    if not p:
        return None

    t = p.get("type", "")

    if tipo == "texto":
        # rich_text ou title
        if t == "title":
            arr = p.get("title", [])
        else:
            arr = p.get("rich_text", [])
        return "".join(c.get("plain_text", "") for c in arr) or None

    if tipo == "numero":
        return p.get("number")

    if tipo == "data":
        d = p.get("date")
        return d.get("start") if d else None

    if tipo == "checkbox":
        return p.get("checkbox")  # True/False

    if tipo == "select":
        s = p.get("select")
        return s.get("name") if s else None

    if tipo == "multi_select":
        return [o.get("name") for o in p.get("multi_select", [])]

    if tipo == "formula":
        f = p.get("formula", {})
        ft = f.get("type", "")
        if ft == "string":
            return f.get("string")
        if ft == "number":
            return f.get("number")
        if ft == "boolean":
            return f.get("boolean")
        if ft == "date":
            d = f.get("date")
            return d.get("start") if d else None

    return None


# ─────────────────────────────────────────────
# NOÇÃO — busca paginada genérica
# ─────────────────────────────────────────────

def notion_query_all(db_id, token, filtro=None):
    """Retorna todas as páginas de um banco Notion (paginação automática)."""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    results = []
    cursor = None

    while True:
        body = {}
        if cursor:
            body["start_cursor"] = cursor
        if filtro:
            body["filter"] = filtro

        r = requests.post(url, headers=headers, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))

        if data.get("has_more"):
            cursor = data.get("next_cursor")
        else:
            break

    return results


# ─────────────────────────────────────────────
# ERP — busca CSV paginado
# ─────────────────────────────────────────────

def erp_fetch(endpoint):
    """
    Busca dados da API do ERP com autenticação Basic.
    Retorna lista de dicts (cabeçalho = chaves).
    """
    url = f"https://api-clientes.maiscontroleerp.com.br/data-exports/{endpoint}"
    cred = base64.b64encode(f"{ERP_USERNAME}:{ERP_PASSWORD}".encode()).decode()
    headers = {"Authorization": f"Basic {cred}"}

    all_rows = []
    page = 1
    page_size = 100

    while True:
        params = {"page": page, "pageSize": page_size}
        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code != 200:
            print(f"[ERP] Erro {r.status_code} em /{endpoint} página {page}")
            break

        text = r.text.strip()
        if not text:
            break

        # Parse CSV manual (sem pandas — compatível com qualquer ambiente)
        linhas = text.splitlines()
        if len(linhas) < 2:
            break

        cabecalho = linhas[0].split(",")
        cabecalho = [c.strip().strip('"') for c in cabecalho]

        linhas_dados = linhas[1:] if page == 1 else linhas[1:]  # pula header nas páginas > 1
        if page > 1:
            linhas_dados = linhas  # nas páginas seguintes não tem header repetido

        for linha in linhas_dados:
            if not linha.strip():
                continue
            valores = linha.split(",")
            row = {}
            for i, col in enumerate(cabecalho):
                row[col] = valores[i].strip().strip('"') if i < len(valores) else ""
            all_rows.append(row)

        if len(linhas_dados) < page_size:
            break  # última página

        page += 1

    return all_rows


# ─────────────────────────────────────────────
# PROCESSAMENTO — Notion Documentos
# ─────────────────────────────────────────────

def processar_documentos():
    """Retorna dict { endereco_normalizado: {...dados...} }"""
    print("[Documentos] Buscando Notion...")
    pages = notion_query_all(NOTION_DB_DOCS, NOTION_TOKEN_DOCS)
    print(f"[Documentos] {len(pages)} registros")

    result = {}
    for p in pages:
        end = prop(p, "ENDEREÇO", "texto")
        ref = prop(p, "REF.", "texto")
        if not end:
            continue

        key = normalizar_endereco(end)
        result[key] = {
            "endereco":                end,
            "ref":                     ref,
            "setor":                   prop(p, "SETOR", "select") or prop(p, "SETOR", "texto"),
            "cidade":                  prop(p, "CIDADE", "select") or prop(p, "CIDADE", "texto"),
            "data_aquisicao_lote":     prop(p, "DATA DE AQUISIÇÃO DO LOTE", "data"),
            "cota_empresa":            prop(p, "COTA DA EMPRESA (%)", "numero"),
            "proprietario_documento":  prop(p, "PROPRIETARIO DOCUMENTO", "texto"),
            "proprietario_real":       prop(p, "PROPRIETARIO REAL", "texto"),
            "cpf_cnpj":                prop(p, "CPF/CNPJ", "texto"),
            "mestre":                  prop(p, "MESTRE", "texto"),
            "despachante":             prop(p, "DESPACHANTE", "texto"),
            "eng_execucao":            prop(p, "ENG. EXECUÇÃO", "texto"),
            "engenheiro_rt":           prop(p, "ENGENHEIRO RT", "texto"),
            "previsao_inicio_obra":    prop(p, "PREVISÃO DE INÍCIO DE OBRA", "data"),
            "obra_iniciada":           prop(p, "OBRA INCIADA", "checkbox"),
            "data_inicio_obra":        prop(p, "DATA DE INÍCIO DA OBRA", "data"),
            "uso_solo_solicitado":     prop(p, "USO DO SOLO SOLICITADO", "checkbox"),
            "data_sol_uso_solo":       prop(p, "DATA DE SOLICITAÇÃO USO DO SOLO", "data"),
            "uso_solo_emitido":        prop(p, "USO DO SOLO EMITIDO E ARMAZENADO", "checkbox"),
            "data_emissao_uso_solo":   prop(p, "DATA DE EMISSÃO DO USO DO SOLO", "data"),
            "escritura_assinada":      prop(p, "ESCRITURA ASSINADA POR TODOS?", "checkbox"),
            "itbi_pago":               prop(p, "ITBI PAGO ?", "checkbox"),
            "registro_pago":           prop(p, "REGISTRO PAGO?", "checkbox"),
            "projeto_feito":           prop(p, "PROJETO FEITO?", "checkbox"),
            "art_feita_paga":          prop(p, "ART FEITA E PAGA?", "checkbox"),
            "escritura_registrada":    prop(p, "ESCRITURA REGISTRADA E DIGITALIZADA?", "checkbox"),
            "certidao_lote":           prop(p, "CERTIDÃO DO LOTE ANEXADA?", "checkbox"),
            "contrato_mestre":         prop(p, "CONTRATO MESTRE ASSINADO E ARMAZENADO?", "checkbox"),
            "contrato_investidor":     prop(p, "CONTRATO INVESTIDOR ASSINADO E ARMAZENADO?", "checkbox"),
            "taxas_alvara_pagas":      prop(p, "TAXAS ENTRADA ALVARÁ EMITIDAS E PAGAS?", "checkbox"),
            "data_entrada_alvara":     prop(p, "DATA DE ENTRADA DE ALVARA", "data"),
            "mao_obra_despachante":    prop(p, "MAO OBRA INICIAL DESPACHANTE PAGA?", "checkbox"),
            "projeto_aprovado":        prop(p, "PROJETO APROVADO E ALVARA EMITIDO E ARMAZENADO?", "checkbox"),
            "data_aprovacao_projeto":  prop(p, "DATA DE APROVAÇÃO DO PROJETO", "data"),
            "entrada_incorporacao":    prop(p, "FOI DADO ENTRADA NA INCORPORAÇÃO? (OBRAS CNPJ)", "checkbox"),
            "taxas_habite_se":         prop(p, "FORAM EMITIDAS E PAGAS AS TAXAS DE NUM OFICIAL, HABITE-SE E VISTORIA?", "checkbox"),
            "incorporacao_finalizada": prop(p, "INCORPORAÇÃO FINALIZOU (OBRAS CNPJ)?", "checkbox"),
            "data_finalizacao_incorp": prop(p, "DATA DE FINALIZAÇÃO DA INCORPORAÇÃO", "data"),
            "entrada_ret":             prop(p, "FOI DATA A ENTRADA NO RET? (OBRAS CNPJ)", "checkbox"),
            "data_entrada_ret":        prop(p, "DATA DE ENTRADA DO RET", "data"),
            "ret_armazenado":          prop(p, "RET ARMAZENADO", "checkbox"),
            "agendou_habite_se":       prop(p, "AGENDOU HABITE-SE?", "checkbox"),
            "data_finalizacao_ret":    prop(p, "DATA DE FINALIZAÇÃO DO RET", "data"),
            "turno_habite_se":         prop(p, "TURNO HABITE-SE", "texto"),
            "data_habite_se":          prop(p, "DATA HABITE-SE", "data"),
            "aprovou_habite_se":       prop(p, "APROVOU HABITE-SE?", "checkbox"),
            "data_aprovacao_habite":   prop(p, "DATA DE APROVAÇÃO DO HABITE-SE", "data"),
            "armazenou_habite":        prop(p, "ARMAZENOU HABITE-SE?", "checkbox"),
            "issqn":                   prop(p, "GEROU E ARMAZENOU ISSQN?", "checkbox"),
            "cno_cnd":                 prop(p, "EMITIU CNO E CND DE OBRA?", "checkbox"),
            "art_acrescimo":           prop(p, "EMITIU ART DE ACRESCIMO?", "checkbox"),
            "certidoes_matricula":     prop(p, "SAIRAM AS CERTIDOES DE MATRICULA?", "checkbox"),
            "data_certidoes":          prop(p, "DATA DE EMISSÃO DAS CERTIDÕES", "data"),
            "docs_vistoria_scpo":      prop(p, "EMITIU DOCUMENTOS DE VISTORIA E SCPO?", "checkbox"),
            "boletos_vistoria":        prop(p, "PAGOU BOLETOS DE VISTORIA CAIXA?", "checkbox"),
            "data_termino_obra":       prop(p, "DATA DE TÉRMINO DE OBRA", "data"),
            # ERP — preenchido depois do JOIN
            "erp_orcado":              None,
            "erp_valor_pago":          None,
        }
    return result


# ─────────────────────────────────────────────
# PROCESSAMENTO — Notion Vendas
# ─────────────────────────────────────────────

def processar_vendas():
    """Retorna dict { endereco_normalizado: {...dados...} }"""
    print("[Vendas] Buscando Notion...")
    pages = notion_query_all(NOTION_DB_VENDAS, NOTION_TOKEN_VENDAS)
    print(f"[Vendas] {len(pages)} registros")

    result = {}
    for p in pages:
        end = prop(p, "ENDEREÇO", "texto")
        ref = prop(p, "REF", "texto")
        if not end:
            continue

        key = normalizar_endereco(end)
        # pode ter múltiplas casas por endereço — usar end+casa como chave
        casa = prop(p, "CASA", "numero") or prop(p, "CASA", "texto")
        chave = f"{key}__CASA{casa}" if casa else key

        result[chave] = {
            "endereco":              end,
            "casa":                  casa,
            "ref":                   ref,
            "cidade":                prop(p, "CIDADE", "select") or prop(p, "CIDADE", "texto"),
            "setor":                 prop(p, "SETOR", "select") or prop(p, "SETOR", "texto"),
            "clientes":              prop(p, "CLIENTES", "texto"),
            "data_venda":            prop(p, "DATA DA VENDA", "data"),
            "cpf":                   prop(p, "CPF", "texto"),
            "correspondente":        prop(p, "CORRESPONDENTE", "texto"),
            "corretor":              prop(p, "CORRETOR", "texto"),
            "avaliacao":             prop(p, "AVALIAÇÃO", "numero"),
            "validade":              prop(p, "VALIDADE", "data"),
            "valor_na_mao":          prop(p, "VALOR NA MÃO", "numero"),
            "comissao":              prop(p, "COMISSÃO", "numero"),
            "valor_venda_contrato":  prop(p, "VALOR DE COMPRA E VENDA NO CONTRATO (VENDIDA)", "numero"),
            "banco":                 prop(p, "BANCO", "texto"),
            "agencia":               prop(p, "AGÊNCIA", "texto"),
            "armazenou_contrato":    prop(p, "ARMAZENOU CONTRATO COMPRA E VENDA?", "checkbox"),
            "emitiu_rcpm":           prop(p, "EMITIU RCPM?", "checkbox"),
            "mandou_docs_corresp":   prop(p, "MANDOU TODOS OS DOCS. P/ CORESSPONDENTE?", "checkbox"),
            "data_envio_docs":       prop(p, "DATA DE ENVIO DOS DOCUMENTOS", "data"),
            "mandou_conformidade":   prop(p, "MANDOU P/ CONFORMIDADE?", "checkbox"),
            "processo_conforme":     prop(p, "PROCESSO CONFORME?", "checkbox"),
            "assinou_contrato_banco":prop(p, "ASSINOU E ARMAZENOU CONTRATO DO BANCO?", "checkbox"),
            "data_assinatura":       prop(p, "DATA DE ASSINATURA DO CONTRATO", "data"),
            "recebeu_taxa_vistoria": prop(p, "RECEBEU TAXA VISTORIA?", "checkbox"),
            "tem_cadastro":          prop(p, "TEM NUMERO DE CADASTRO?", "checkbox"),
            "num_cadastro":          prop(p, "Nº DE CADASTRO PREFEITURA", "texto"),
            "entrada_cartorio":      prop(p, "DEU ENTRADA NO CARTORIO?", "checkbox"),
            "agendou_pre_vistoria":  prop(p, "AGENDOU PRE VISTORIA?", "checkbox"),
            "data_pre_vistoria":     prop(p, "DATA DA PRÉ-VISTORIA", "data"),
            "tem_manual":            prop(p, "TEM MANUAL DE OBRA?", "checkbox"),
            "registro_pronto":       prop(p, "FICOU PRONTO O REGISTRO?", "checkbox"),
            "devolveu_banco":        prop(p, "DEVOLVEU NO BANCO?", "checkbox"),
            "recebeu":               prop(p, "RECEBEU?", "checkbox"),
            "entregou_casa":         prop(p, "ENTEGOU A CASA E PEGOU TERMO DE ENTREGA?", "checkbox"),
            "data_entrega":          prop(p, "DATA DA ENTREGA", "data"),
            "gcap_gerado":           prop(p, "GEROU E ARMAZENOU GCAP?", "checkbox"),
            "gcap_pago":             prop(p, "PAGOU GCAP?", "checkbox"),
            "pesquisa":              prop(p, "PREENCHEU A PESQUISA?", "checkbox"),
        }
    return result


# ─────────────────────────────────────────────
# PROCESSAMENTO — ERP
# ─────────────────────────────────────────────

def processar_erp():
    """
    Retorna:
      orcamentos: dict { endereco_normalizado: preco_total_com_desconto }
      pagamentos: dict { endereco_normalizado: soma_valor_pago }
    """
    print("[ERP] Buscando Propostas...")
    propostas = erp_fetch("propostas")
    print(f"[ERP] {len(propostas)} propostas")

    print("[ERP] Buscando Pagamentos...")
    pagamentos = erp_fetch("pagamentos")
    print(f"[ERP] {len(pagamentos)} pagamentos")

    # Orcamentos: pega o maior preco_total_com_desconto por obra
    # (pode ter múltiplas propostas por obra — usa a última/maior)
    orcamentos = {}
    for row in propostas:
        obra = normalizar_endereco(row.get("obra", ""))
        if not obra:
            continue
        try:
            valor = float(str(row.get("preco_total_com_desconto", "") or "0").replace(",", "."))
        except ValueError:
            valor = 0
        # mantém o maior valor encontrado para aquela obra
        if obra not in orcamentos or valor > orcamentos[obra]:
            orcamentos[obra] = valor

    # Pagamentos: soma valor_pago por centro_de_custo
    soma_pago = {}
    for row in pagamentos:
        cdc = normalizar_endereco(row.get("centro_de_custo", ""))
        if not cdc:
            continue
        try:
            valor = float(str(row.get("valor_pago", "") or "0").replace(",", "."))
        except ValueError:
            valor = 0
        soma_pago[cdc] = soma_pago.get(cdc, 0) + valor

    return orcamentos, soma_pago


# ─────────────────────────────────────────────
# MAIN — monta data.json
# ─────────────────────────────────────────────

def main():
    docs   = processar_documentos()
    vendas = processar_vendas()
    orcamentos, soma_pago = processar_erp()

    # ── JOIN ERP → Documentos ──
    sem_dados_erp = []
    for key, d in docs.items():
        end_norm = normalizar_endereco(d["endereco"])

        if end_norm in orcamentos:
            d["erp_orcado"] = orcamentos[end_norm]
        else:
            d["erp_orcado"] = "SEM DADOS"
            sem_dados_erp.append(d["endereco"])

        d["erp_valor_pago"] = soma_pago.get(end_norm, "SEM DADOS")
        if d["erp_valor_pago"] == 0:
            d["erp_valor_pago"] = "SEM DADOS"

    if sem_dados_erp:
        print(f"[JOIN] {len(sem_dados_erp)} endereços sem match no ERP:")
        for e in sem_dados_erp[:10]:
            print(f"  • {e}")
        if len(sem_dados_erp) > 10:
            print(f"  ... e mais {len(sem_dados_erp) - 10}")

    # ── Monta JSON final ──
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "documentos": list(docs.values()),
        "vendas":     list(vendas.values()),
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, default=str)

    print(f"\n✅ data.json gerado:")
    print(f"   {len(output['documentos'])} registros em documentos")
    print(f"   {len(output['vendas'])} registros em vendas")
    print(f"   updated_at: {output['updated_at']}")


if __name__ == "__main__":
    main()
