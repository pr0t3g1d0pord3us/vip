#!/usr/bin/env python3
"""
vip.py — Scraper VIP Leilões → auctions.veiculos

Site: https://www.vipleiloes.com.br/pesquisa

Filtros aplicados:
  - Procedência: Recuperado Financiamento (4)
  - KM até: 150.000  (via painel Filtros Avançados)
  - Ano: 2014–2025   (filtrado no parse)

Uso local:
    python vip.py --no-upload
    python vip.py --no-upload --limit 10
    python vip.py --no-upload --show-browser

GitHub Actions (produção):
    python vip.py --output /tmp/vip_coleta.json

Dependências:
    pip install playwright httpx
    playwright install chromium
"""

import asyncio
import json
import re
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright, Page

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fipe_client import buscar_valor_mercado, fmt_brl as _fmt_fipe, _detectar_categorias, _parse_titulo
from supabase_client import SupabaseClient


# ─── Cores terminal ────────────────────────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"


# ─── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.vipleiloes.com.br"
PESQUISA = BASE_URL + "/pesquisa"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

ANO_MIN        = 2014
ANO_MAX        = 2030
CUSTO_REPARO   = 5_000   # carro
CUSTO_REPARO_MOTO = 1_500
MARGEM_MINIMA  = 10_000


# ─── Parsers ───────────────────────────────────────────────────────────────────

def parse_brl(v) -> float | None:
    if v is None:
        return None
    s = str(v).replace("R$", "").replace("\xa0", "").replace(" ", "").strip()
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        val = float(s)
        return val if 500 <= val <= 10_000_000 else None
    except Exception:
        return None


def fmt_brl(v) -> str:
    val = parse_brl(v)
    if val is None:
        return "—"
    s = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def parse_km(km_str: str | None) -> int | None:
    if not km_str:
        return None
    s = re.sub(r"[^\d]", "", str(km_str))
    try:
        val = int(s)
        return val if 0 <= val <= 2_000_000 else None
    except Exception:
        return None


def parse_ano(texto: str) -> tuple[int | None, int | None]:
    m = re.search(r"\b(19[5-9]\d|20[0-3]\d)\s*/\s*(19[5-9]\d|20[0-3]\d)\b", texto)
    if m:
        return int(m.group(1)), int(m.group(2))
    nums = re.findall(r"\b(19[5-9]\d|20[0-3]\d)\b", texto)
    if len(nums) >= 2:
        return int(nums[-2]), int(nums[-1])
    if len(nums) == 1:
        return int(nums[0]), int(nums[0])
    return None, None


def parse_combustivel(texto: str) -> str | None:
    t = (texto or "").lower()
    if "gasolina/alcool" in t or "gasolina/álcool" in t or "flex" in t:
        return "Flex"
    if "gaso/alco/gnv" in t:
        return "Flex/GNV"
    if "gasolina/gnv" in t:
        return "Gasolina/GNV"
    if "gasolina/eletrico" in t or "gasolina/elétrico" in t:
        return "Híbrido"
    if "diesel" in t:
        return "Diesel"
    if "elétrico" in t or "eletrico" in t:
        return "Elétrico"
    if "alcool" in t or "álcool" in t or "etanol" in t:
        return "Etanol"
    if "gasolina" in t:
        return "Gasolina"
    return None


def parse_data_iso(texto: str) -> str | None:
    m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})", texto)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d/%m/%Y %H:%M").isoformat()
        except Exception:
            pass
    m = re.search(r"(\d{2}/\d{2}/\d{4})", texto)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y").isoformat()
        except Exception:
            pass
    return None


def _campo(texto: str, padrao: str, max_len: int = 60) -> str | None:
    m = re.search(padrao, texto, re.IGNORECASE)
    if not m:
        return None
    val = m.group(1).strip()
    return val[:max_len] if len(val) <= max_len else None


# ─── Playwright: filtros ───────────────────────────────────────────────────────

async def aplicar_filtros(page: Page) -> None:
    """
    Aplica filtros na página de pesquisa:
      1. Procedência = Recuperado Financiamento (4)
      2. Abre painel "Filtros Avançados"
      3. Preenche KM até 150.000
      4. Clica em #btnFiltrar
    """
    await page.goto(PESQUISA, wait_until="networkidle", timeout=60_000)
    await asyncio.sleep(2.0)

    # 1. Procedência
    try:
        await page.select_option("#Filtro_Procedencia", "4")
        print(f"  {DIM}✓ Procedência = Recuperado Financiamento{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ #Filtro_Procedencia: {e}{RESET}")

    # 2. Abre Filtros Avançados
    try:
        el = await page.query_selector(
            'button:has-text("Filtros Avançados"), '
            'a:has-text("Filtros Avançados"), '
            'span:has-text("Filtros Avançados")'
        )
        if el:
            await el.click()
        else:
            await page.evaluate("""
                () => {
                    const el = [...document.querySelectorAll('button,a,span,div')]
                        .find(e => e.innerText.trim() === 'Filtros Avançados');
                    if (el) el.click();
                }
            """)
        await asyncio.sleep(1.5)
        print(f"  {DIM}✓ Filtros Avançados abertos{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ Filtros Avançados: {e}{RESET}")

    # 3. KM até 150.000
    try:
        await page.wait_for_selector("#Filtro_QuilometragemAte", state="visible", timeout=5_000)
        await page.fill("#Filtro_QuilometragemAte", "150000")
        print(f"  {DIM}✓ KM até 150.000{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ #Filtro_QuilometragemAte: {e}{RESET}")

    # 4. Filtrar
    print(f"  {DIM}Clicando em Filtrar...{RESET}")
    await page.click("#btnFiltrar")
    await page.wait_for_load_state("networkidle", timeout=30_000)
    await asyncio.sleep(2.5)
    print(f"  {GREEN}✓ Filtros aplicados{RESET}")


# ─── Playwright: coleta de cards ──────────────────────────────────────────────

async def coletar_cards_pagina(page: Page) -> list[dict]:
    """
    Extrai cards da listagem.
    VIP usa /evento/anuncio/<slug> como padrão de link.
    """
    return await page.evaluate("""
        () => {
            const BASE = 'https://www.vipleiloes.com.br';
            const results = [];

            // Pega links únicos de anuncio
            const hrefMap = {};
            for (const a of document.querySelectorAll('a[href*="/evento/anuncio/"]')) {
                const href = a.href.split('?')[0];
                if (!hrefMap[href]) hrefMap[href] = a;
            }

            for (const [href, a] of Object.entries(hrefMap)) {
                // Sobe na DOM até o container do card (.crd-link ou .card)
                let card = a;
                for (let i = 0; i < 10; i++) {
                    const p = card.parentElement;
                    if (!p) break;
                    if (p.classList.contains('crd-link') || p.classList.contains('card') || p.tagName === 'LI') {
                        card = p; break;
                    }
                    card = p;
                }

                // ── Título: só text nodes diretos do h1.mb-0 ──────────────
                let titulo = '';
                const h1 = card.querySelector('h1.mb-0');
                if (h1) {
                    titulo = [...h1.childNodes]
                        .filter(n => n.nodeType === 3)
                        .map(n => n.textContent.trim())
                        .filter(Boolean)
                        .join(' ');
                    if (!titulo) titulo = h1.innerText.split('\\n')[0].trim();
                } else {
                    const h = card.querySelector('h1, h2');
                    titulo = h ? h.innerText.split('\\n')[0].trim() : '';
                }

                // ── Marca / KM / placa do .anc-info ───────────────────────
                const infoSpans = [...card.querySelectorAll('.anc-info span')];
                const marca      = infoSpans[0] ? infoSpans[0].innerText.trim() : '';
                const placaFinal = infoSpans[2] ? infoSpans[2].innerText.trim() : '';
                let km_card = '';
                for (const sp of infoSpans) {
                    if (/\\d.*[Kk][Mm]/.test(sp.innerText)) { km_card = sp.innerText.trim(); break; }
                }

                // ── Valores ───────────────────────────────────────────────
                const valAtualEl = card.querySelector('.valor-atual');
                const valor_atual = valAtualEl ? valAtualEl.innerText.trim() : '';

                let valor_inicial = '';
                for (const el of card.querySelectorAll('.anc-lel')) {
                    const m = el.innerText.match(/Valor\\s+inicial[:\\s]+(R\\$[\\s\\d.,]+)/i);
                    if (m) { valor_inicial = m[1].trim(); break; }
                }

                // ── Status ────────────────────────────────────────────────
                const statusEl = card.querySelector('.situacao, .crd-status span');
                const status   = statusEl ? statusEl.innerText.trim() : '';

                // ── Lote / local / lances ─────────────────────────────────
                const texto    = card.innerText || '';
                const loteM    = texto.match(/Lote[:\\s]+(\\d+)/i);
                const localM   = texto.match(/Local[:\\s]+([A-Z]{2})\\b/i);
                const lancesM  = texto.match(/(\\d+)\\s+Lance/i);

                // ── Data / hora ───────────────────────────────────────────
                const dataEl  = card.querySelector('.anc-start');
                const horaEl  = card.querySelector('.anc-hour');
                const dataStr = dataEl ? dataEl.innerText.replace('Início:', '').trim() : '';
                const horaStr = horaEl ? horaEl.innerText.trim() : '';

                // ── Imagem thumbnail ──────────────────────────────────────
                const img = card.querySelector('.crd-image img, img[src*="armazup"], img[src*="blob"]');
                const imagem = img ? (img.src || img.getAttribute('data-src') || '') : '';

                results.push({
                    link:          href.startsWith('http') ? href : BASE + href,
                    titulo,
                    marca,
                    placa_final:   placaFinal,
                    km_card,
                    valor_atual,
                    valor_inicial,
                    status,
                    lote_num:      loteM  ? loteM[1]  : '',
                    local_uf:      localM ? localM[1] : '',
                    num_lances:    lancesM ? parseInt(lancesM[1]) : 0,
                    data_inicio:   dataStr + (horaStr ? ' ' + horaStr : ''),
                    imagem,
                });
            }

            // Dedup final por link
            const dedup = {};
            for (const r of results) if (r.link && !dedup[r.link]) dedup[r.link] = r;
            return Object.values(dedup);
        }
    """)


# ─── Playwright: detalhe do lote ──────────────────────────────────────────────

async def coletar_detalhe(page: Page, url: str) -> dict:
    """Acessa a página do lote e extrai texto completo + imagens."""
    try:
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(1.2)

        return await page.evaluate("""
            (url) => {
                const imgs = [...document.querySelectorAll('img')]
                    .map(i => i.src || i.getAttribute('data-src') || '')
                    .filter(src =>
                        src && src.length > 20 &&
                        !src.includes('logo') && !src.includes('banner') &&
                        !src.includes('icon') && !src.includes('.svg') &&
                        (src.includes('blob') || src.includes('armazup') ||
                         src.includes('vipleiloes') || src.match(/\\.(jpg|jpeg|png|webp)/i))
                    );

                const seen = new Set();
                const imagens = [];
                for (const s of imgs) {
                    const k = s.split('?')[0];
                    if (!seen.has(k)) { seen.add(k); imagens.push(k); }
                }

                const main = document.querySelector(
                    'main, [class*="detalhe"], [class*="lote-detail"], [class*="content"], article'
                );
                const texto = (main || document.body).innerText.trim();

                return { url, imagens: imagens.slice(0, 12), texto_pagina: texto };
            }
        """, url)

    except Exception as e:
        return {"url": url, "imagens": [], "texto_pagina": "", "erro": str(e)}


# ─── Paginação ─────────────────────────────────────────────────────────────────

async def tem_proxima_pagina(page: Page) -> bool:
    return await page.evaluate("""
        () => {
            const sels = [
                'a[aria-label*="próxima"]', 'a[aria-label*="Próxima"]',
                'a[aria-label*="next"]',
                '.pagination .next:not(.disabled)',
                '.pagination li:last-child a:not(.disabled)',
            ];
            for (const sel of sels) {
                const el = document.querySelector(sel);
                if (el && !el.classList.contains('disabled')) return true;
            }
            return [...document.querySelectorAll('a, button')].some(el => {
                const t = (el.innerText || '').trim().toLowerCase();
                return (t === '>' || t === '>>' || t === 'próximo' || t === 'next') &&
                       !el.disabled && !el.classList.contains('disabled');
            });
        }
    """)


async def ir_proxima_pagina(page: Page) -> bool:
    try:
        clicked = await page.evaluate("""
            () => {
                const sels = [
                    'a[aria-label*="próxima"]', 'a[aria-label*="Próxima"]',
                    'a[aria-label*="next"]',
                    '.pagination .next:not(.disabled) a',
                    '.pagination .next:not(.disabled)',
                ];
                for (const sel of sels) {
                    const el = document.querySelector(sel);
                    if (el && !el.classList.contains('disabled')) { el.click(); return true; }
                }
                for (const el of [...document.querySelectorAll('a, button')]) {
                    const t = (el.innerText || '').trim().toLowerCase();
                    if ((t === '>' || t === '>>' || t === 'próximo' || t === 'next') &&
                        !el.disabled && !el.classList.contains('disabled')) {
                        el.click(); return true;
                    }
                }
                return false;
            }
        """)
        if clicked:
            await page.wait_for_load_state("networkidle", timeout=20_000)
            await asyncio.sleep(1.5)
        return clicked
    except Exception:
        return False


# ─── Extração estruturada ──────────────────────────────────────────────────────

_UFS = {
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
    "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
}


def extract(card: dict, detalhe: dict) -> dict | None:
    # Título limpo: só a primeira linha (JS pode trazer "MODELO\nMarca\nKm")
    titulo = (card.get("titulo") or "").strip().split("\n")[0].strip()
    link   = card.get("link", "")

    if not titulo or not link:
        return None

    texto = detalhe.get("texto_pagina", "")

    # ── Marca: vem limpa do .anc-info ────────────────────────────────────────
    marca_card = (card.get("marca") or "").strip().upper()

    # ── Modelo: título sem marca no início e sem " - ANO/ANO" no fim ─────────
    titulo_sem_ano = re.sub(
        r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*/\s*(19[5-9]\d|20[0-3]\d).*$",
        "", titulo
    ).strip()
    titulo_sem_ano = re.sub(
        r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*$",
        "", titulo_sem_ano
    ).strip()

    if marca_card:
        marca = marca_card
        if titulo_sem_ano.upper().startswith(marca):
            modelo = titulo_sem_ano[len(marca):].lstrip(" -").strip(" -").strip()
        else:
            modelo = titulo_sem_ano.strip(" -").strip()
    else:
        # Fallback: primeira palavra como marca
        parts  = titulo_sem_ano.split(" ", 1)
        marca  = parts[0].upper() if parts else ""
        modelo = parts[1].strip() if len(parts) > 1 else ""

    # ── Ano ───────────────────────────────────────────────────────────────────
    ano_txt = _campo(texto, r"Ano[:\s]+([\d/\s]+)")
    ano_fab, ano_mod = parse_ano(ano_txt or titulo)

    # ── Filtro de ano ─────────────────────────────────────────────────────────
    if ano_fab and (ano_fab < ANO_MIN or ano_fab > ANO_MAX):
        return None

    # ── Valores ───────────────────────────────────────────────────────────────
    # Prioridade: card.valor_inicial → texto "Valor inicial:" → texto genérico
    lance_raw = parse_brl(card.get("valor_inicial"))
    if not lance_raw:
        m = re.search(r"Valor\s+inicial[:\s]+R\$\s*([\d.,]+)", texto, re.I)
        if m:
            lance_raw = parse_brl(m.group(1))
    if not lance_raw:
        for pat in [r"Lance\s+inicial[:\s]+R\$\s*([\d.,]+)",
                    r"Valor\s+m[ií]nimo[:\s]+R\$\s*([\d.,]+)"]:
            m = re.search(pat, texto, re.I)
            if m:
                lance_raw = parse_brl(m.group(1))
                break

    # ── Data ──────────────────────────────────────────────────────────────────
    data_leilao = parse_data_iso((card.get("data_inicio") or "") + " " + texto)

    # ── Localização ───────────────────────────────────────────────────────────
    estado = (card.get("local_uf") or "").strip() or None
    if not estado:
        m = re.search(r"\b([A-Z]{2})\b", texto)
        if m and m.group(1) in _UFS:
            estado = m.group(1)
    cidade = _campo(texto, r"Cidade[:\s]+([^\n/]+)")

    # ── Características ───────────────────────────────────────────────────────
    km = parse_km(
        _campo(texto, r"Quilometragem[:\s]+([\d.,]+\s*km?)", max_len=30)
        or _campo(texto, r"\bKM[:\s]+([\d.,]+)", max_len=20)
        or card.get("km_card")
    )
    combustivel = parse_combustivel(
        _campo(texto, r"Combust[íi]vel[:\s]+([^\n]+)") or titulo
    )
    cambio      = _campo(texto, r"C[âa]mbio[:\s]+([^\n]+)")
    ar_cond     = _campo(texto, r"Ar\s+[Cc]ondicionado[:\s]+([^\n]+)")
    chaves      = _campo(texto, r"Chave[:\s]+([^\n]+)")
    func        = _campo(texto, r"Ve[íi]culo\s+[Ff]uncionando[:\s]+([^\n]+)")
    cor         = _campo(texto, r"Cor[:\s]+([^\n]+)")
    placa       = _campo(texto, r"Placa[:\s]+([A-Z0-9\-]{5,8})", max_len=10)
    lote_num    = card.get("lote_num") or _campo(texto, r"(?:N[°º]\s*Lote|Lote\s*N[°º]?)[:\s]+([^\n]+)", max_len=20)

    imagens = detalhe.get("imagens") or ([card["imagem"]] if card.get("imagem") else [])

    return {
        "titulo":       titulo,
        "marca":        marca,
        "modelo":       modelo,
        "link":         link,
        "lote_num":     lote_num,
        "placa_final":  card.get("placa_final"),
        "status":       card.get("status"),
        "num_lances":   card.get("num_lances"),

        "ano_fab":      ano_fab,
        "ano_mod":      ano_mod,

        "lance_raw":    lance_raw,
        "lance":        fmt_brl(lance_raw),

        # FIPE — preenchido em enriquecer_fipe()
        "fipe_raw":           None,
        "fipe":               None,
        "fipe_min":           None,
        "fipe_max":           None,
        "fipe_fonte":         None,
        "desconto_pct":       None,
        "margem_bruta":       None,
        "margem_bruta_fmt":   None,
        "margem_liquida":     None,
        "margem_liquida_fmt": None,

        "km":           km,
        "combustivel":  combustivel,
        "cambio":       cambio,
        "ar_cond":      ar_cond,
        "chaves":       chaves,
        "funcionando":  func,
        "cor":          cor,
        "placa":        placa,

        "estado":       estado,
        "cidade":       cidade,
        "data_leilao":  data_leilao,
        "origem":       "Recuperado Financiamento",

        "imagens":      imagens,
    }


# ─── Enriquecimento FIPE ───────────────────────────────────────────────────────

def _titulo_fipe(lote: dict) -> str:
    """
    Monta query no formato esperado pelo fipe_client._parse_titulo():
      "Fiat - Cronos Drive 1.3 - 2022 / 2022"
    """
    marca  = (lote.get("marca") or "").strip().title()
    modelo = (lote.get("modelo") or "").strip()

    # Limpa sobras de ano/traço que possam ter ficado no modelo
    modelo = re.sub(
        r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*/\s*(19[5-9]\d|20[0-3]\d).*$",
        "", modelo
    ).strip(" -").strip()

    fab = lote.get("ano_fab")
    mod = lote.get("ano_mod")
    ano = f"{fab} / {mod}" if (fab and mod) else (str(fab) if fab else "")

    return " - ".join(p for p in [marca, modelo, ano] if p)


async def enriquecer_fipe(lotes: list[dict]) -> list[dict]:
    total  = len(lotes)
    ok     = 0
    falhou = 0

    for i, lote in enumerate(lotes, 1):
        query = _titulo_fipe(lote)
        print(f"  {DIM}[{i}/{total}]{RESET} {query[:60]}", end=" ", flush=True)

        r = await buscar_valor_mercado(query)

        if r["valor"] and r.get("confiavel", True):
            lote["fipe_raw"]   = r["valor"]
            lote["fipe"]       = _fmt_fipe(r["valor"])
            lote["fipe_min"]   = r["valor_min"]
            lote["fipe_max"]   = r["valor_max"]
            lote["fipe_fonte"] = r["fonte"]

            lance = lote.get("lance_raw")
            fipe  = r["valor_min"]

            if lance and fipe and fipe > 0:
                desc_pct     = round((1 - lance / fipe) * 100, 1)
                margem_bruta = round(fipe - lance, 2)

                # Detecta moto pela primeira palavra do modelo
                _m0 = (lote.get("modelo") or "").lower().split()
                _palavras_moto = {"cg","xre","biz","pop","pcx","cb","cbr","ninja",
                                  "fazer","ybr","nxr","bros","nmax","mt","fz"}
                custo = CUSTO_REPARO_MOTO if (_m0 and _m0[0] in _palavras_moto) else CUSTO_REPARO
                margem_liq = round(margem_bruta - custo, 2)

                if desc_pct > 0 and margem_liq >= MARGEM_MINIMA:
                    lote["desconto_pct"]       = desc_pct
                    lote["margem_bruta"]       = margem_bruta
                    lote["margem_bruta_fmt"]   = _fmt_fipe(margem_bruta)
                    lote["margem_liquida"]     = margem_liq
                    lote["margem_liquida_fmt"] = _fmt_fipe(margem_liq)

            label = (
                f"{GREEN}({lote['desconto_pct']}% desc · liq {lote['margem_liquida_fmt']}){RESET}"
                if lote.get("desconto_pct")
                else f"{YELLOW}sem margem{RESET}"
            )
            print(f"→ {lote['fipe']}  {label}")
            ok += 1
        else:
            motivo = "ano divergente" if (r["valor"] and not r.get("confiavel", True)) else "não encontrado"
            print(f"→ {RED}{motivo}{RESET}")
            falhou += 1

        await asyncio.sleep(1.2)

    print(f"\n  {GREEN}FIPE OK: {ok}{RESET}  ·  {RED}não encontrado: {falhou}{RESET}")
    return lotes


# ─── Coleta Playwright completa ────────────────────────────────────────────────

async def coletar_playwright(headless: bool = True, limit: int = 0) -> tuple[list[dict], dict]:
    cards_todos   = []
    detalhes_dict = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent=BROWSER_UA, locale="pt-BR")
        page    = await context.new_page()

        print(f"  {DIM}Aplicando filtros:{RESET}")
        print(f"  {DIM}  • Procedência: Recuperado Financiamento{RESET}")
        print(f"  {DIM}  • KM ≤ 150.000{RESET}")
        print(f"  {DIM}  • Ano: {ANO_MIN}–{ANO_MAX} (filtrado no parse){RESET}")
        await aplicar_filtros(page)

        pagina = 1
        while True:
            print(f"\n  {DIM}Página {pagina}:{RESET} coletando cards...")
            cards = await coletar_cards_pagina(page)

            if not cards:
                print(f"  {YELLOW}Sem cards — fim da listagem.{RESET}")
                break

            print(f"  {GREEN}✓  {len(cards)} cards{RESET}")
            cards_todos.extend(cards)

            if limit > 0 and len(cards_todos) >= limit:
                cards_todos = cards_todos[:limit]
                print(f"  {YELLOW}--limit {limit} atingido.{RESET}")
                break

            if not await tem_proxima_pagina(page):
                print(f"  {DIM}Última página detectada.{RESET}")
                break

            if not await ir_proxima_pagina(page):
                print(f"  {YELLOW}Não conseguiu avançar página.{RESET}")
                break

            pagina += 1
            if pagina > 50:
                break

        total = len(cards_todos)
        print(f"\n  {BOLD}Total para detalhar: {total}{RESET}\n")

        for i, card in enumerate(cards_todos, 1):
            link   = card.get("link", "")
            titulo = (card.get("titulo") or "?")[:55]
            print(f"  [{i:03d}/{total}] {titulo}", end=" ", flush=True)

            if not link:
                print(f"{YELLOW}sem link{RESET}")
                detalhes_dict[i] = {"url": "", "imagens": [], "texto_pagina": ""}
                continue

            det = await coletar_detalhe(page, link)
            detalhes_dict[i] = det
            imgs = len(det.get("imagens", []))
            print(
                f"{GREEN}✓ {imgs} imgs{RESET}"
                if not det.get("erro")
                else f"{RED}erro: {det['erro'][:60]}{RESET}"
            )
            await asyncio.sleep(0.8)

        await browser.close()

    return cards_todos, detalhes_dict


# ─── Normalização → schema auctions.veiculos ──────────────────────────────────

def normalize_to_db(lote: dict) -> dict | None:
    if not lote.get("link") or not lote.get("titulo"):
        return None
    if not lote.get("ano_fab"):
        return None
    if not lote.get("lance_raw"):
        return None
    if not lote.get("fipe_raw"):
        return None
    if not lote.get("margem_liquida"):
        return None

    imagens = lote.get("imagens") or []

    _marca_n, _modelo_n, *_ = _parse_titulo(lote.get("titulo", ""))
    _cats = _detectar_categorias(_marca_n or "", _modelo_n or "")
    _tipo = {"motorcycles": "moto", "trucks": "truck"}.get(_cats[0], "carro")

    return {
        "titulo":                 lote["titulo"],
        "descricao":              lote.get("cidade"),
        "tipo":                   _tipo,
        "marca":                  lote.get("marca"),
        "modelo":                 lote.get("modelo"),
        "estado":                 lote.get("estado"),
        "cidade":                 lote.get("cidade"),
        "ano_fabricacao":         lote.get("ano_fab"),
        "ano_modelo":             lote.get("ano_mod"),
        "modalidade":             "leilao",
        "valor_inicial":          lote["lance_raw"],
        "valor_atual":            lote["lance_raw"],
        "data_encerramento":      lote.get("data_leilao"),
        "link":                   lote["link"],
        "imagem_1":               imagens[0] if len(imagens) > 0 else None,
        "imagem_2":               imagens[1] if len(imagens) > 1 else None,
        "imagem_3":               imagens[2] if len(imagens) > 2 else None,
        "percentual_abaixo_fipe": lote.get("desconto_pct"),
        "margem_revenda":         lote.get("margem_liquida"),
        "km":                     lote.get("km"),
        "origem":                 "Recuperado Financiamento",
        "ativo":                  True,
    }


# ─── Upload para Supabase ─────────────────────────────────────────────────────

def upload_to_supabase(lotes: list[dict]) -> dict:
    try:
        db = SupabaseClient()
    except Exception as e:
        print(f"\n  {RED}❌  Falha ao inicializar SupabaseClient: {e}{RESET}")
        return {"inserted": 0, "updated": 0, "errors": len(lotes), "duplicates_removed": 0}

    registros, skipped_sem_margem, skipped_outros = [], 0, 0
    for lote in lotes:
        tem_fipe   = bool(lote.get("fipe_raw"))
        tem_margem = bool(lote.get("margem_liquida"))
        rec = normalize_to_db(lote)
        if rec:
            registros.append(rec)
        elif tem_fipe and not tem_margem:
            skipped_sem_margem += 1
        else:
            skipped_outros += 1

    if skipped_sem_margem:
        print(f"  {YELLOW}⚠️   {skipped_sem_margem} lote(s) ignorado(s) — sem margem líquida{RESET}")
    if skipped_outros:
        print(f"  {YELLOW}⚠️   {skipped_outros} lote(s) ignorado(s) — sem link/ano/lance/FIPE{RESET}")
    if not registros:
        print(f"  {RED}Nenhum registro válido para upload.{RESET}")
        return {}

    print(f"\n{BOLD}{'='*68}{RESET}")
    print(f"{BOLD}  ☁️   UPLOAD → auctions.veiculos  ({len(registros)} registros){RESET}")
    print(f"{BOLD}{'='*68}{RESET}\n")

    try:
        stats   = db.upsert_veiculos(registros)
        total_s = stats.get("inserted", 0) + stats.get("updated", 0)
        print(f"\n  ✅  Enviados:        {total_s}  "
              f"({stats.get('inserted',0)} novos + {stats.get('updated',0)} atualizados)")
        print(f"  🔄  Dupes removidas: {stats.get('duplicates_removed', 0)}")
        print(f"  ❌  Erros:           {stats.get('errors', 0)}\n")
        return stats
    except Exception as e:
        print(f"\n  {RED}❌  Erro no upsert: {e}{RESET}\n")
        return {"inserted": 0, "updated": 0, "errors": len(registros)}


# ─── Print ────────────────────────────────────────────────────────────────────

def print_lote(lote: dict, i: int, total: int):
    titulo  = lote["titulo"][:65]
    km_str  = f"{lote['km']:,} km".replace(",", ".") if lote.get("km") else "km ?"
    ano_fab = lote.get("ano_fab")
    ano_mod = lote.get("ano_mod")
    ano_str = f"{ano_fab}/{ano_mod}" if ano_fab != ano_mod else str(ano_fab or "?")
    desc_str = (
        f"  {GREEN}{lote['desconto_pct']}% abaixo FIPE{RESET}"
        if lote.get("desconto_pct") else ""
    )

    print(f"\n{'─'*68}")
    print(f"{BOLD}{YELLOW}[{i}/{total}] {titulo}{RESET}")
    print(f"{'─'*68}")
    print(f"  {DIM}marca:{RESET}          {lote.get('marca')}  ·  {lote.get('modelo')}")
    print(f"  {DIM}ano:{RESET}            {ano_str}  ·  {km_str}  ·  {lote.get('combustivel') or '?'}")
    print(f"  {DIM}local:{RESET}          {lote.get('cidade') or '?'} / {lote.get('estado') or '?'}")
    print(f"  {DIM}câmbio:{RESET}         {lote.get('cambio') or '?'}  ·  ar={lote.get('ar_cond') or '?'}")
    print(f"  {DIM}chave:{RESET}          {lote.get('chaves') or '?'}  ·  func={lote.get('funcionando') or '?'}")
    print(f"  {DIM}lance:{RESET}          {lote['lance']}{desc_str}")
    print(f"  {DIM}fipe:{RESET}           {lote.get('fipe') or '—'}  [{lote.get('fipe_fonte') or '—'}]")
    print(f"  {DIM}margem líquida:{RESET}  {lote.get('margem_liquida_fmt') or '—'}  "
          f"(bruta {lote.get('margem_bruta_fmt') or '—'} - R$5.000 reparo)")
    print(f"  {DIM}data:{RESET}           {lote.get('data_leilao') or '?'}")
    print(f"  {DIM}imagens:{RESET}        {len(lote.get('imagens', []))}x")
    print(f"  {DIM}link:{RESET}           {lote['link']}")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="VIP Leilões → auctions.veiculos")
    parser.add_argument("--no-upload",    action="store_true", help="Não sobe pro Supabase")
    parser.add_argument("--no-fipe",      action="store_true", help="Pula busca FIPE")
    parser.add_argument("--show-browser", action="store_true", help="Abre browser visível")
    parser.add_argument("--limit",        type=int, default=0, help="Limita a N lotes (debug)")
    parser.add_argument("--output",       default="vip_coleta.json")
    args = parser.parse_args()

    print(f"\n{BOLD}{'='*68}{RESET}")
    print(f"{BOLD}  🏎️   VIP LEILÕES — COLETA COMPLETA{RESET}")
    print(f"{BOLD}{'='*68}{RESET}")
    print(f"  {DIM}URL:    {PESQUISA}{RESET}")
    print(f"  {DIM}upload: {'não (debug)' if args.no_upload else 'sim → auctions.veiculos'}{RESET}\n")

    # 1. Coleta
    print(f"{BOLD}  🌐  Coletando com Playwright...{RESET}\n")
    cards_brutos, detalhes_dict = await coletar_playwright(
        headless=not args.show_browser,
        limit=args.limit,
    )

    # 2. Extração
    print(f"\n{BOLD}  🔧  Extraindo campos...{RESET}\n")
    lotes, falhos = [], []
    for i, card in enumerate(cards_brutos, 1):
        detalhe = detalhes_dict.get(i, {})
        lote = extract(card, detalhe)
        if lote:
            lotes.append(lote)
        else:
            falhos.append(card.get("titulo", "?"))

    print(f"  {GREEN}✓  {len(lotes)} extraídos{RESET}  ·  {RED}{len(falhos)} falhos/fora de ano{RESET}")

    # 3. FIPE
    if not args.no_fipe:
        print(f"\n{BOLD}  🔍  Buscando FIPE ({len(lotes)} lotes)...{RESET}\n")
        lotes = await enriquecer_fipe(lotes)

    # 4. Ordena por margem
    lotes.sort(key=lambda x: x.get("margem_liquida") or 0, reverse=True)

    # 5. Print
    for i, lote in enumerate(lotes, 1):
        print_lote(lote, i, len(lotes))

    com_fipe   = sum(1 for l in lotes if l.get("fipe_raw"))
    com_margem = sum(1 for l in lotes if l.get("margem_liquida"))

    print(f"\n\n{'='*68}")
    print(f"{BOLD}  📊  RESUMO{RESET}")
    print(f"{'='*68}")
    print(f"  Total coletados:  {len(lotes)}")
    print(f"  Com lance:        {sum(1 for l in lotes if l.get('lance_raw'))}")
    print(f"  Com FIPE:         {com_fipe}")
    print(f"  Com margem:       {com_margem}")
    if com_margem:
        top = [l for l in lotes if l.get("margem_liquida")]
        print(f"  Melhor margem:    {top[0]['margem_liquida_fmt']}  ({top[0]['titulo'][:45]})")
        print(f"  Maior desconto:   {max(l['desconto_pct'] for l in top if l.get('desconto_pct'))}%")

    # 6. Salva JSON
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "total_lotes": len(lotes),
            "com_fipe":    com_fipe,
            "com_margem":  com_margem,
            "lotes":       lotes,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n  JSON salvo em: {args.output}")

    # 7. Upload Supabase
    if not args.no_upload:
        upload_to_supabase(lotes)


if __name__ == "__main__":
    asyncio.run(main())