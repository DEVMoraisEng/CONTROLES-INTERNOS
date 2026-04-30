"""
Rode esse script na sua maquina para ver os tipos reais dos campos no Notion.
pip install requests
python diagnostico_notion.py
"""
import requests, json

# DOCS
TOKEN_DOCS = "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"
DB_DOCS    = "32fc5ab532d380a0900dd7f4bfc619bd"

# VENDAS
TOKEN_VENDAS = "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"
DB_VENDAS    = "33cc5ab532d38047ae3aee8b87ac1f4d"

def ver_tipos(token, db_id, nome):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json={}, timeout=30)
    print(f"\n{'='*60}")
    print(f"=== {nome} — Status: {r.status_code} ===")
    if r.status_code != 200:
        print(r.text[:300])
        return

    results = r.json().get("results", [])
    print(f"Registros: {len(results)}")
    if not results:
        return

    props = results[0].get("properties", {})
    print(f"\n{'CAMPO':<55} {'TIPO':<15} VALOR")
    print("-" * 90)
    for nome_campo, val in sorted(props.items()):
        tipo = val.get("type", "?")
        # Extrair valor
        if tipo == "title":
            v = "".join(c.get("plain_text","") for c in val.get("title",[]))
        elif tipo == "rich_text":
            v = "".join(c.get("plain_text","") for c in val.get("rich_text",[]))
        elif tipo == "checkbox":
            v = str(val.get("checkbox"))
        elif tipo == "select":
            s = val.get("select")
            v = s.get("name") if s else "None"
        elif tipo == "multi_select":
            v = str([o.get("name") for o in val.get("multi_select",[])])
        elif tipo == "date":
            d = val.get("date")
            v = d.get("start") if d else "None"
        elif tipo == "number":
            v = str(val.get("number"))
        elif tipo == "formula":
            f = val.get("formula", {})
            v = f"formula({f.get('type')}) = {f.get(f.get('type',''))}"
        elif tipo == "rollup":
            v = "rollup"
        elif tipo == "people":
            v = str([p.get("name") for p in val.get("people",[])])
        elif tipo == "relation":
            v = f"relation({len(val.get('relation',[]))} itens)"
        else:
            v = f"({tipo})"
        print(f"  {nome_campo:<53} {tipo:<15} {str(v)[:40]}")

ver_tipos(TOKEN_DOCS,   DB_DOCS,   "BASE DE DADOS DOCUMENTOS")
ver_tipos(TOKEN_VENDAS, DB_VENDAS, "BASE DE DADOS VENDAS")

print("\n\nPronto! Cole o resultado aqui.")
