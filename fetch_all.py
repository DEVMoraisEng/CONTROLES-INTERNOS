"""
fetch_all.py — Morais Engenharia · Controles Internos
Busca dados dos dois bancos Notion + ERP e gera data.json
"""
import requests, json, os
from datetime import datetime, timezone

# ─── CREDENCIAIS (via GitHub Secrets) ────────────────────────
TOKEN_DOCS   = os.environ.get("NOTION_TOKEN_DOCS",   "")
DB_DOCS      = os.environ.get("NOTION_DB_DOCS",      "32fc5ab532d380a0900dd7f4bfc619bd")
TOKEN_VENDAS = os.environ.get("NOTION_TOKEN_VENDAS", "")
DB_VENDAS    = os.environ.get("NOTION_DB_VENDAS",    "33cc5ab532d38047ae3aee8b87ac1f4d")
ERP_USER     = os.environ.get("ERP_USERNAME",        "")
ERP_PASS     = os.environ.get("ERP_PASSWORD",        "")
ERP_BASE     = "https://app.contaazul.com/api/v1"  # ajustar se necessário

# ─── HELPERS NOTION ──────────────────────────────────────────
def prop_title(p):
    return "".join(c.get("plain_text","") for c in p.get("title",[])) or None

def prop_text(p):
    return "".join(c.get("plain_text","") for c in p.get("rich_text",[])) or None

def prop_select(p):
    s = p.get("select")
    return s.get("name") if s else None

def prop_number(p):
    return p.get("number")

def prop_date(p):
    d = p.get("date")
    return d.get("start") if d else None

def prop_checkbox(p):
    return p.get("checkbox")

def prop_formula_str(p):
    f = p.get("formula", {})
    t = f.get("type")
    if t == "string": return f.get("string") or None
    if t == "number": return f.get("number")
    if t == "boolean": return f.get("boolean")
    return None

def notion_pages(token, db_id, filtro=None):
    """Busca todas as páginas de um banco Notion (com paginação)."""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    pages, cursor = [], None
    while True:
        body = {}
        if cursor: body["start_cursor"] = cursor
        if filtro: body["filter"] = filtro
        r = requests.post(url, headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            print(f"  ERRO Notion {db_id}: {r.status_code} {r.text[:200]}")
            break
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"): break
        cursor = data.get("next_cursor")
    return pages

# ─── PARSE DOCUMENTOS ────────────────────────────────────────
def parse_doc(page):
    p = page.get("properties", {})
    def s(nome): return prop_select(p.get(nome, {}))
    def d(nome): return prop_date(p.get(nome, {}))
    def n(nome): return prop_number(p.get(nome, {}))
    return {
        "id":                       page.get("id"),
        "endereco":                 prop_title(p.get("ENDEREÇO", {})),
        "ref":                      prop_text(p.get("REF.", {})),
        "setor":                    s("SETOR"),
        "cidade":                   s("CIDADE"),
        "data_aquisicao_lote":      d("DATA DE AQUISIÇÃO DO LOTE"),
        "cota_empresa":             n("COTA DA EMPRESA (%)"),
        "proprietario_documento":   s("PROPRIETARIO DOCUMENTO"),
        "proprietario_real":        s("PROPRIETARIO REAL"),
        "cpf_cnpj":                 s("CPF/CNPJ"),
        "mestre":                   s("MESTRE"),
        "despachante":              s("DESPACHANTE"),
        "eng_execucao":             s("ENG. EXECUÇÃO"),
        "engenheiro_rt":            s("ENGENHEIRO RT"),
        "previsao_inicio_obra":     d("PREVISÃO DE INÍCIO DE OBRA"),
        "obra_iniciada":            s("OBRA INCIADA"),
        "data_inicio_obra":         d("DATA DE INÍCIO DA OBRA"),
        "uso_solo_solicitado":      s("USO DO SOLO SOLICITADO"),
        "data_sol_uso_solo":        d("DATA DE SOLICITAÇÃO USO DO SOLO"),
        "uso_solo_emitido":         s("USO DO SOLO EMITIDO E ARMAZENADO"),
        "data_emissao_uso_solo":    d("DATA DE EMISSÃO DO USO DO SOLO"),
        "escritura_assinada":       s("ESCRITURA ASSINADA POR TODOS?"),
        "itbi_pago":                s("ITBI PAGO ?"),
        "registro_pago":            s("REGISTRO PAGO?"),
        "projeto_feito":            s("PROJETO FEITO?"),
        "art_feita_paga":           s("ART FEITA E PAGA?"),
        "escritura_registrada":     s("ESCRITURA REGISTRADA E DIGITALIZADA?"),
        "certidao_lote":            s("CERTIDÃO DO LOTE ANEXADA?"),
        "contrato_mestre":          s("CONTRATO MESTRE ASSINADO E ARMAZENADO?"),
        "contrato_investidor":      s("CONTRATO INVESTIDOR ASSINADO E ARMAZENADO?"),
        "taxas_alvara_pagas":       s("TAXAS ENTRADA ALVARÁ EMITIDAS E PAGAS?"),
        "data_entrada_alvara":      d("DATA DE ENTRADA DE ALVARA"),
        "mao_obra_despachante":     s("MAO OBRA INICIAL DESPACHANTE PAGA?"),
        "projeto_aprovado":         s("PROJETO APROVADO E ALVARA EMITIDO E ARMAZENADO?"),
        "data_aprovacao_projeto":   d("DATA DE APROVAÇÃO DO PROJETO"),
        "entrada_incorporacao":     s("FOI DADO ENTRADA NA INCORPORAÇÃO? (OBRAS CNPJ)"),
        "taxas_habite_se":          s("FORAM EMITIDAS E PAGAS AS TAXAS DE NUM OFICIAL, HABITE-SE E VISTORIA?"),
        "incorporacao_finalizada":  s("INCORPORAÇÃO FINALIZOU (OBRAS CNPJ)?"),
        "data_finalizacao_incorp":  d("DATA DE FINALIZAÇÃO DA INCORPORAÇÃO"),
        "entrada_ret":              s("FOI DATA A ENTRADA NO RET? (OBRAS CNPJ)"),
        "data_entrada_ret":         d("DATA DE ENTRADA DO RET"),
        "ret_armazenado":           s("RET ARMAZENADO"),
        "agendou_habite_se":        s("AGENDOU HABITE-SE?"),
        "data_finalizacao_ret":     d("DATA DE FINALIZAÇÃO DO RET"),
        "turno_habite_se":          s("TURNO HABITE-SE"),
        "data_habite_se":           d("DATA HABITE-SE"),
        "aprovou_habite_se":        s("APROVOU HABITE-SE?"),
        "data_aprovacao_habite":    d("DATA DE APROVAÇÃO DO HABITE-SE"),
        "armazenou_habite":         s("ARMAZENOU HABITE-SE?"),
        "issqn":                    s("GEROU E ARMAZENOU ISSQN?"),
        "cno_cnd":                  s("EMITIU CNO E CND DE OBRA?"),
        "art_acrescimo":            s("EMITIU ART DE ACRESCIMO?"),
        "certidoes_matricula":      s("SAIRAM AS CERTIDOES DE MATRICULA?"),
        "data_certidoes":           d("DATA DE EMISSÃO DAS CERTIDÕES"),
        "docs_vistoria_scpo":       s("EMITIU DOCUMENTOS DE VISTORIA E SCPO?"),
        "boletos_vistoria":         s("PAGOU BOLETOS DE VISTORIA CAIXA?"),
        "data_termino_obra":        d("DATA DE TÉRMINO DE OBRA"),
        "entrada_incorporacao_data":d("DATA DE ENTRADA NA INCORPORAÇÃO"),
        # ERP — preenchido depois
        "erp_orcado":    None,
        "erp_valor_pago": None,
    }

# ─── PARSE VENDAS ─────────────────────────────────────────────
def parse_venda(page):
    p = page.get("properties", {})
    def s(nome): return prop_select(p.get(nome, {}))
    def d(nome): return prop_date(p.get(nome, {}))
    def n(nome): return prop_number(p.get(nome, {}))
    def t(nome): return prop_text(p.get(nome, {}))
    return {
        "id":                       page.get("id"),
        "endereco":                 prop_title(p.get("ENDEREÇO", {})),
        "casa":                     n("CASA"),
        "ref":                      t("REF"),
        "cidade":                   s("CIDADE VI"),
        "setor":                    s("SETOR"),
        "clientes":                 t("CLIENTES"),
        "data_venda":               d("DATA DA VENDA"),
        "cpf":                      t("CPF"),
        "correspondente":           t("CORRESPONDENTE"),
        "corretor":                 t("CORRETOR"),
        "avaliacao":                n("AVALIAÇÃO"),
        "validade":                 d("VALIDADE"),
        "valor_na_mao":             n(" VALOR NA MÃO"),
        "comissao":                 n(" COMISSÃO"),
        "valor_venda_contrato":     n("VALOR DE COMPRA E VENDA NO CONTRATO (VENDIDA)"),
        "banco":                    s("BANCO"),
        "agencia":                  n("AGÊNCIA"),
        "armazenou_contrato":       s("ARMAZENOU CONTRATO COMPRA E VENDA?"),
        "emitiu_rcpm":              s("EMITIU RCPM?"),
        "mandou_docs_corresp":      s("MANDOU TODOS OS DOCS. P/ CORESSPONDENTE?"),
        "data_envio_docs":          d("DATA DE ENVIO DOS DOCUMENTOS"),
        "mandou_conformidade":      s("MANDOU P/ CONFORMIDADE?"),
        "processo_conforme":        s("PROCESSO CONFORME?"),
        "assinou_contrato_banco":   s("ASSINOU E ARMAZENOU CONTRATO DO BANCO?"),
        "data_assinatura":          d("DATA DE ASSINATURA DO CONTRATO"),
        "recebeu_taxa_vistoria":    s("RECEBEU TAXA VISTORIA?"),
        "tem_cadastro":             s("TEM NUMERO DE CADASTRO?"),
        "num_cadastro":             n("Nº DE CADASTRO PREFEITURA"),
        "entrada_cartorio":         s("DEU ENTRADA NO CARTORIO?"),
        "agendou_pre_vistoria":     s("AGENDOU PRE VISTORIA?"),
        "data_pre_vistoria":        d("DATA DA PRÉ-VISTORIA"),
        "tem_manual":               s("TEM MANUAL DE OBRA?"),
        "registro_pronto":          s("FICOU PRONTO O REGISTRO?"),
        "devolveu_banco":           s("DEVOLVEU NO BANCO?"),
        "recebeu":                  s("RECEBEU?"),
        "entregou_casa":            s("ENTEGOU A CASA E PEGOU TERMO DE ENTREGA?"),
        "data_entrega":             d("DATA DA ENTREGA"),
        "gcap_gerado":              s("GEROU E ARMAZENOU GCAP?"),
        "gcap_pago":                s("PAGOU GCAP?"),
        "pesquisa":                 s("PREENCHEU A PESQUISA?"),
    }

# ─── ERP (Google Planilhas via API) ──────────────────────────
def buscar_erp():
    """Busca propostas e pagamentos do ERP."""
    orcados = {}   # endereco -> valor
    pagos   = {}   # endereco -> soma
    if not ERP_USER or not ERP_PASS:
        print("  ERP: credenciais não configuradas")
        return orcados, pagos
    try:
        session = requests.Session()
        # Login
        r = session.post(f"{ERP_BASE}/login",
            json={"username": ERP_USER, "password": ERP_PASS}, timeout=30)
        if r.status_code not in (200, 201):
            print(f"  ERP login falhou: {r.status_code}")
            return orcados, pagos

        # Propostas → orçado
        r = session.get(f"{ERP_BASE}/propostas", timeout=30)
        if r.status_code == 200:
            for row in r.json():
                obra = (row.get("obra") or "").strip().upper()
                val  = row.get("preco_total_com_desconto")
                if obra and val is not None:
                    orcados[obra] = val

        # Pagamentos → valor pago (soma)
        r = session.get(f"{ERP_BASE}/pagamentos", timeout=30)
        if r.status_code == 200:
            for row in r.json():
                cc  = (row.get("centro_de_custo") or "").strip().upper()
                val = row.get("valor_pago")
                if cc and val is not None:
                    pagos[cc] = pagos.get(cc, 0) + float(val)
    except Exception as e:
        print(f"  ERP erro: {e}")
    return orcados, pagos

# ─── MAIN ─────────────────────────────────────────────────────
def main():
    print("Buscando DOCUMENTOS...")
    pages_docs = notion_pages(TOKEN_DOCS, DB_DOCS)
    print(f"  {len(pages_docs)} registros")
    documentos = [parse_doc(p) for p in pages_docs]

    print("Buscando VENDAS...")
    pages_vendas = notion_pages(TOKEN_VENDAS, DB_VENDAS)
    print(f"  {len(pages_vendas)} registros")
    vendas = [parse_venda(p) for p in pages_vendas]

    print("Buscando ERP...")
    orcados, pagos = buscar_erp()
    print(f"  {len(orcados)} propostas, {len(pagos)} centros de custo")

    # Cruzar ERP com documentos pelo endereço
    for doc in documentos:
        end = (doc.get("endereco") or "").strip().upper()
        doc["erp_orcado"]    = orcados.get(end, "SEM DADOS")
        doc["erp_valor_pago"] = pagos.get(end, "SEM DADOS")

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "documentos": documentos,
        "vendas":     vendas,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\ndata.json gerado: {len(documentos)} docs, {len(vendas)} vendas")

if __name__ == "__main__":
    main()
