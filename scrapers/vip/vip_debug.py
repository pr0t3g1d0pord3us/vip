#!/usr/bin/env python3
"""
vip_debug.py — Scraper VIP Leilões (DEBUG, sem Supabase)

Filtro aplicado:
  - Procedência: Recuperado Financiamento (value=4)
  - Clica em #btnFiltrar e pagina tudo

Coleta todos os lotes, entra em cada detalhe,
calcula margem FIPE e imprime tudo no terminal.

Uso:
    python vip_debug.py                    # todos os lotes
    python vip_debug.py --limit 10         # só 10 (rápido)
    python vip_debug.py --show-browser     # abre browser visível
    python vip_debug.py --no-fipe          # pula cálculo FIPE
    python vip_debug.py --output saida.json

Dependências:
    pip install playwright httpx
    playwright install chromium

Coloca fipe_client.py no mesmo diretório (ou no PYTHONPATH).
"""

import asyncio
import json
import re
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright, Page

# Importa o fipe_client do mesmo diretório ou do pai
_HERE = Path(__file__).resolve().parent
for _p in [_HERE, _HERE.parent]:
    if (_p / "fipe_client.py").exists():
        sys.path.insert(0, str(_p))
        break

from fipe_client import buscar_valor_mercado, fmt_brl as _fmt_fipe, _parse_titulo

# ─── Cores terminal ────────────────────────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

# ─── Config ────────────────────────────────────────────────────────────────────
BASE_URL    = "https://www.vipleiloes.com.br"
PESQUISA    = BASE_URL + "/pesquisa"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Custo estimado de reparo (abate da margem bruta)
CUSTO_REPARO_CARRO = 5_000
CUSTO_REPARO_MOTO  = 1_500
MARGEM_MINIMA      = 10_000   # só exibe/conta quem tiver ≥ R$ 10 k líquido


# ─── Helpers de parse ──────────────────────────────────────────────────────────

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


def _campo(texto: str, padrao: str, max_len: int = 80) -> str | None:
    m = re.search(padrao, texto, re.IGNORECASE)
    if not m:
        return None
    val = m.group(1).strip()
    return val[:max_len] if len(val) <= max_len else None


# ─── Playwright: aplicar filtros ───────────────────────────────────────────────

async def aplicar_filtros(page: Page) -> None:
    """
    Navega para /pesquisa e aplica:
      1. Procedência = Recuperado Financiamento (4)
      2. Clica em "Filtros Avançados" para expandir o painel
      3. Preenche KM até 150.000
      4. Clica em #btnFiltrar
    """
    print(f"  {DIM}→ Navegando para {PESQUISA}{RESET}")
    await page.goto(PESQUISA, wait_until="networkidle", timeout=60_000)
    await asyncio.sleep(2.0)

    # ── 1. Procedência: Recuperado Financiamento (value=4) ──────────────────
    try:
        await page.select_option("#Filtro_Procedencia", "4")
        print(f"  {GREEN}✓ Procedência = Recuperado Financiamento{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ #Filtro_Procedencia: {e}{RESET}")

    # ── 2. Abre painel de Filtros Avançados ──────────────────────────────────
    # Procura qualquer elemento clicável com texto "Filtros Avançados"
    print(f"  {DIM}→ Abrindo Filtros Avançados...{RESET}")
    try:
        # Tenta o seletor mais provável primeiro (botão ou span com o texto)
        avancados = await page.query_selector(
            'button:has-text("Filtros Avançados"), '
            'a:has-text("Filtros Avançados"), '
            '[class*="filtro"]:has-text("Filtros Avançados"), '
            'span:has-text("Filtros Avançados")'
        )
        if avancados:
            await avancados.click()
            await asyncio.sleep(1.5)
            print(f"  {GREEN}✓ Filtros Avançados abertos{RESET}")
        else:
            # Fallback via JS — clica no elemento que contém o texto
            clicou = await page.evaluate("""
                () => {
                    const els = [...document.querySelectorAll('button, a, span, div')];
                    const el = els.find(e => e.innerText.trim() === 'Filtros Avançados');
                    if (el) { el.click(); return true; }
                    return false;
                }
            """)
            await asyncio.sleep(1.5)
            if clicou:
                print(f"  {GREEN}✓ Filtros Avançados abertos (fallback JS){RESET}")
            else:
                print(f"  {YELLOW}⚠ Não encontrou botão 'Filtros Avançados' — continuando mesmo assim{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ Erro ao abrir Filtros Avançados: {e}{RESET}")

    # ── 3. KM até 150.000 ───────────────────────────────────────────────────
    print(f"  {DIM}→ Preenchendo KM até 150000...{RESET}")
    try:
        # Aguarda o campo ficar visível (pode ter animação de expansão)
        await page.wait_for_selector("#Filtro_QuilometragemAte", state="visible", timeout=5_000)
        await page.fill("#Filtro_QuilometragemAte", "150000")
        print(f"  {GREEN}✓ KM até 150.000{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ #Filtro_QuilometragemAte: {e}{RESET}")

    # ── 4. Clica em Filtrar ──────────────────────────────────────────────────
    print(f"  {DIM}→ Clicando em #btnFiltrar...{RESET}")
    try:
        await page.click("#btnFiltrar")
        await page.wait_for_load_state("networkidle", timeout=30_000)
        await asyncio.sleep(2.5)
        print(f"  {GREEN}✓ Filtros aplicados e resultados carregados{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠ Erro ao clicar Filtrar: {e}{RESET}")


# ─── Playwright: coleta de cards (seletor corrigido) ───────────────────────────

async def coletar_cards_pagina(page: Page) -> list[dict]:
    """
    Extrai cards da listagem. O VIP usa /evento/anuncio/<slug> como padrão de link.
    Coleta: link, titulo, marca, placa_final, valor_atual, valor_inicial,
            lote_num, local_uf, num_lances, data_inicio, hora_inicio, status, imagem
    """
    return await page.evaluate("""
        () => {
            const BASE = 'https://www.vipleiloes.com.br';
            const results = [];
            const seen = new Set();

            // ── Seletor principal: /evento/anuncio/ ──────────────────────
            // Sobe até o container .crd-link (ou qualquer card pai)
            const links = [...document.querySelectorAll('a[href*="/evento/anuncio/"]')];

            // Pega somente links únicos que levam ao anuncio (evita dup de href)
            const hrefUnicos = {};
            for (const a of links) {
                const href = a.href.split('?')[0];
                if (!hrefUnicos[href]) hrefUnicos[href] = a;
            }

            for (const [href, a] of Object.entries(hrefUnicos)) {
                // Sobe na DOM até encontrar o card raiz
                let card = a;
                for (let i = 0; i < 10; i++) {
                    const p = card.parentElement;
                    if (!p) break;
                    // Para quando achar um container que pareça card
                    if (p.classList.contains('crd-link') ||
                        p.classList.contains('card') ||
                        p.tagName === 'LI') {
                        card = p;
                        break;
                    }
                    card = p;
                }

                // ── Título: só text nodes diretos do h1.mb-0 ────────
                let titulo = '';
                const h1mb0 = card.querySelector('h1.mb-0');
                if (h1mb0) {
                    // Pega só os text nodes diretos para não incluir .anc-info
                    titulo = [...h1mb0.childNodes]
                        .filter(n => n.nodeType === 3)
                        .map(n => n.textContent.trim())
                        .filter(Boolean)
                        .join(' ');
                    if (!titulo) titulo = h1mb0.innerText.split('\\n')[0].trim();
                } else {
                    const h = card.querySelector('h1, h2');
                    titulo = h ? h.innerText.split('\\n')[0].trim() : '';
                }

                // ── Marca / placa / km (do .anc-info, separado do h1) ───
                const infoSpans = [...card.querySelectorAll('.anc-info span, [class*="anc-info"] span')];
                const marca      = infoSpans[0] ? infoSpans[0].innerText.trim() : '';
                const placaFinal = infoSpans[2] ? infoSpans[2].innerText.trim() : '';
                // KM vem como 2º span ou qualquer span com "Km"
                let km_card = '';
                for (const sp of infoSpans) {
                    const t = sp.innerText.trim();
                    if (/\\d.*[Kk][Mm]/.test(t)) { km_card = t; break; }
                }

                // ── Valores ──────────────────────────────────────────────
                const valAtualEl = card.querySelector('.valor-atual, [class*="valor-atual"]');
                const valor_atual = valAtualEl ? valAtualEl.innerText.trim() : '';

                // "Valor inicial: R$ 12.000,00" — pega do texto do anc-lel
                let valor_inicial = '';
                const ancLels = [...card.querySelectorAll('.anc-lel, [class*="anc-lel"]')];
                for (const el of ancLels) {
                    const t = el.innerText;
                    const m = t.match(/Valor\\s+inicial[:\\s]+(R\\$[\\s\\d.,]+)/i);
                    if (m) { valor_inicial = m[1].trim(); break; }
                }

                // ── Status ───────────────────────────────────────────────
                const statusEl = card.querySelector('.situacao, [class*="situacao"], .crd-status span');
                const status   = statusEl ? statusEl.innerText.trim() : '';

                // ── Lote / local / lances ────────────────────────────────
                const texto = card.innerText || '';
                const loteM  = texto.match(/Lote[:\\s]+(\\d+)/i);
                const localM = texto.match(/Local[:\\s]+([A-Z]{2})\\b/i);
                const lancesM = texto.match(/(\\d+)\\s+Lance/i);

                // ── Data / hora ──────────────────────────────────────────
                const dataEl  = card.querySelector('.anc-start, [class*="anc-start"]');
                const horaEl  = card.querySelector('.anc-hour, [class*="anc-hour"]');
                const dataStr = dataEl ? dataEl.innerText.replace('Início:', '').trim() : '';
                const horaStr = horaEl ? horaEl.innerText.trim() : '';

                // ── Imagem ───────────────────────────────────────────────
                const img = card.querySelector('img[src*="blob"], img[src*="vipleiloes"], img[src*="armazup"], .crd-image img');
                const imagem = img ? (img.src || img.getAttribute('data-src') || '') : '';

                results.push({
                    link:          href.startsWith('http') ? href : BASE + href,
                    titulo:        titulo,
                    marca:         marca,
                    placa_final:   placaFinal,
                    valor_atual:   valor_atual,
                    valor_inicial: valor_inicial,
                    status:        status,
                    lote_num:      loteM  ? loteM[1]  : '',
                    local_uf:      localM ? localM[1] : '',
                    num_lances:    lancesM ? parseInt(lancesM[1]) : 0,
                    data_inicio:   dataStr + (horaStr ? ' ' + horaStr : ''),
                    imagem:        imagem,
                });
            }

            // Remove duplicatas por link (segurança extra)
            const dedup = {};
            for (const r of results) {
                if (r.link && !dedup[r.link]) dedup[r.link] = r;
            }
            return Object.values(dedup);
        }
    """)


# ─── Playwright: detalhe de cada lote ─────────────────────────────────────────

async def coletar_detalhe(page: Page, url: str) -> dict:
    """Acessa a página do lote e extrai texto completo + imagens."""
    try:
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(1.5)

        return await page.evaluate("""
            (url) => {
                // Imagens únicas do CDN VIP
                const imgs = [...document.querySelectorAll('img')]
                    .map(i => i.src || i.getAttribute('data-src') || '')
                    .filter(src =>
                        src && src.length > 20 &&
                        !src.includes('logo') && !src.includes('banner') &&
                        !src.includes('icon') && !src.includes('.svg') &&
                        (src.includes('blob') || src.includes('armazup') ||
                         src.includes('vipleiloes') || src.match(/\\.(jpg|jpeg|png|webp)/i))
                    );

                const seenImgs = new Set();
                const imagens = [];
                for (const s of imgs) {
                    const k = s.split('?')[0];
                    if (!seenImgs.has(k)) { seenImgs.add(k); imagens.push(k); }
                }

                // Texto principal
                const main = document.querySelector(
                    'main, [class*="detalhe"], [class*="lote"], [class*="content"], article'
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
            // Botões de paginação comuns
            const sels = [
                'a[aria-label*="próxima"]', 'a[aria-label*="Próxima"]',
                'a[aria-label*="next"]', 'a[aria-label*="Next"]',
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
            await asyncio.sleep(2.0)
        return clicked
    except Exception:
        return False


# ─── Extração estruturada ──────────────────────────────────────────────────────

_UFS = {
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
    "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
}


def _parse_marca_modelo(titulo: str) -> tuple[str | None, str | None]:
    """'AMAROK CD 4X4 S - 2012/2013' → ('AMAROK', 'CD 4X4 S')"""
    t = re.sub(r"\s+(19[5-9]\d|20[0-3]\d)\s*/\s*(19[5-9]\d|20[0-3]\d).*$", "", titulo).strip()
    t = re.sub(r"\s+(19[5-9]\d|20[0-3]\d).*$", "", t).strip()
    # Remove " - " do início se vier da marca
    t = t.lstrip("-").strip()
    parts = t.split(" ", 1)
    return (parts[0].upper() if parts else None,
            parts[1].strip() if len(parts) > 1 else None)


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


def extract(card: dict, detalhe: dict) -> dict:
    """
    Combina dados do card (listagem) com dados do detalhe (página interna).
    Retorna dict enriquecido pronto para o FIPE.
    """
    # Limpa título: JS pode trazer "CRONOS DRIVE 1.3 - 2022/2022\nFiat\n62 Km"
    # Só queremos a primeira linha
    titulo_raw = (card.get("titulo") or "").strip()
    titulo = titulo_raw.split("\n")[0].strip()

    link  = card.get("link", "")
    texto = detalhe.get("texto_pagina", "")

    # ── Marca: vem limpa do .anc-info (ex: "Fiat", "Volkswagen") ─────────
    marca_card = (card.get("marca") or "").strip().upper()

    # ── Modelo: título sem marca no início e sem ano no fim ───────────────
    # Remove "- 2022/2022" ou " 2022/2022" do final (inclui traço opcional antes do ano)
    titulo_sem_ano = re.sub(r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*/\s*(19[5-9]\d|20[0-3]\d).*$", "", titulo).strip()
    titulo_sem_ano = re.sub(r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*$", "", titulo_sem_ano).strip()

    if marca_card:
        marca = marca_card
        titulo_up = titulo_sem_ano.upper()
        if titulo_up.startswith(marca):
            modelo = titulo_sem_ano[len(marca):].lstrip(" -").strip()
        else:
            modelo = titulo_sem_ano  # título já começa pelo modelo
        modelo = modelo.strip(" -").strip()  # limpa traços residuais
    else:
        marca, modelo = _parse_marca_modelo(titulo)

    # ── Ano ───────────────────────────────────────────────────────────────
    ano_txt = _campo(texto, r"Ano[:\s]+([\d/\s]+)")
    ano_fab, ano_mod = parse_ano(ano_txt or titulo)

    # ── Valor inicial (lance de largada) ──────────────────────────────────
    # Prioridade: card.valor_inicial → texto "Valor inicial:" → texto genérico
    lance_raw = parse_brl(card.get("valor_inicial"))
    if not lance_raw:
        m = re.search(r"Valor\s+inicial[:\s]+R\$\s*([\d.,]+)", texto, re.I)
        if m:
            lance_raw = parse_brl(m.group(1))
    if not lance_raw:
        # Fallback: qualquer valor R$ no texto da página
        for pat in [r"Lance\s+inicial[:\s]+R\$\s*([\d.,]+)",
                    r"Valor\s+m[ií]nimo[:\s]+R\$\s*([\d.,]+)"]:
            m = re.search(pat, texto, re.I)
            if m:
                lance_raw = parse_brl(m.group(1))
                break

    valor_atual_raw = parse_brl(card.get("valor_atual"))

    # ── Data ──────────────────────────────────────────────────────────────
    data_leilao = parse_data_iso(card.get("data_inicio", "") + " " + texto)

    # ── Localização ───────────────────────────────────────────────────────
    estado = card.get("local_uf", "").strip() or None
    if not estado:
        m = re.search(r"\b([A-Z]{2})\b", texto)
        if m and m.group(1) in _UFS:
            estado = m.group(1)
    cidade = _campo(texto, r"Cidade[:\s]+([^\n/]+)")

    # ── Características (texto página) ────────────────────────────────────
    km         = parse_km(_campo(texto, r"Quilometragem[:\s]+([\d.,]+\s*km?)", max_len=30) or
                          _campo(texto, r"\bKM[:\s]+([\d.,]+)", max_len=20))
    combustivel = _campo(texto, r"Combust[íi]vel[:\s]+([^\n]+)")
    cambio      = _campo(texto, r"C[âa]mbio[:\s]+([^\n]+)")
    ar_cond     = _campo(texto, r"Ar\s+[Cc]ondicionado[:\s]+([^\n]+)")
    chaves      = _campo(texto, r"Chave[:\s]+([^\n]+)")
    funcionando = _campo(texto, r"Ve[íi]culo\s+[Ff]uncionando[:\s]+([^\n]+)")
    cor         = _campo(texto, r"Cor[:\s]+([^\n]+)")
    placa       = _campo(texto, r"Placa[:\s]+([A-Z0-9\-]{5,8})", max_len=10)
    lote_num    = card.get("lote_num") or _campo(texto, r"Lote[:\s]+(\d+)", max_len=10)
    num_lances  = card.get("num_lances")

    imagens = detalhe.get("imagens") or (
        [card["imagem"]] if card.get("imagem") else []
    )

    return {
        "titulo":        titulo,
        "marca":         marca,
        "modelo":        modelo,
        "link":          link,
        "lote_num":      lote_num,
        "placa_final":   card.get("placa_final"),
        "status":        card.get("status"),
        "num_lances":    num_lances,

        "ano_fab":       ano_fab,
        "ano_mod":       ano_mod,

        "lance_raw":     lance_raw,
        "lance":         fmt_brl(lance_raw),
        "valor_atual_raw": valor_atual_raw,
        "valor_atual":   fmt_brl(valor_atual_raw),

        # FIPE — preenchido em enriquecer_fipe()
        "fipe_raw":          None,
        "fipe":              None,
        "fipe_fonte":        None,
        "fipe_snippet":      None,
        "desconto_pct":      None,
        "margem_bruta":      None,
        "margem_bruta_fmt":  None,
        "margem_liquida":    None,
        "margem_liquida_fmt": None,

        "km":            km,
        "combustivel":   combustivel,
        "cambio":        cambio,
        "ar_cond":       ar_cond,
        "chaves":        chaves,
        "funcionando":   funcionando,
        "cor":           cor,
        "placa":         placa,

        "estado":        estado,
        "cidade":        cidade,
        "data_leilao":   data_leilao,
        "origem":        "Recuperado Financiamento",
        "imagens":       imagens,
    }


# ─── Enriquecimento FIPE ───────────────────────────────────────────────────────

def _titulo_fipe(lote: dict) -> str:
    """
    Monta query FIPE no formato esperado pelo fipe_client._parse_titulo():
      "Fiat - Cronos Drive 1.3 - 2022 / 2022"

    O fipe_client divide em marca / modelo / ano por hífen ou espaço.
    """
    marca  = (lote.get("marca") or "").strip().title()
    modelo = (lote.get("modelo") or "").strip()

    # Limpa qualquer sobra de ano/traço no modelo
    modelo = re.sub(r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*/\s*(19[5-9]\d|20[0-3]\d).*$", "", modelo).strip()
    modelo = re.sub(r"\s*[-–]?\s*(19[5-9]\d|20[0-3]\d)\s*$", "", modelo).strip()
    modelo = modelo.strip(" -").strip()

    fab = lote.get("ano_fab")
    mod = lote.get("ano_mod")
    ano = f"{fab} / {mod}" if (fab and mod) else (str(fab) if fab else "")

    partes = [p for p in [marca, modelo, ano] if p]
    return " - ".join(partes)


async def enriquecer_fipe(lotes: list[dict]) -> list[dict]:
    total  = len(lotes)
    ok     = 0
    falhou = 0

    for i, lote in enumerate(lotes, 1):
        query = _titulo_fipe(lote)
        print(f"  {DIM}[{i:03d}/{total}]{RESET} {query[:55]}", end=" ", flush=True)

        r = await buscar_valor_mercado(query)

        if r["valor"] and r.get("confiavel", True):
            lote["fipe_raw"]    = r["valor"]
            lote["fipe"]        = _fmt_fipe(r["valor"])
            lote["fipe_fonte"]  = r["fonte"]
            lote["fipe_snippet"] = r.get("snippet")

            lance = lote.get("lance_raw")
            fipe  = r["valor_min"]   # usa o menor (mais conservador)

            if lance and fipe and fipe > 0:
                desc_pct     = round((1 - lance / fipe) * 100, 1)
                margem_bruta = round(fipe - lance, 2)

                # Detecta se é moto pela primeira palavra do modelo
                _modelo_n = (lote.get("modelo") or "").lower().split()[0] if lote.get("modelo") else ""
                PALAVRAS_MOTO = {"cg","xre","biz","pop","pcx","cb","cbr","ninja","fazer","ybr","nxr"}
                custo = CUSTO_REPARO_MOTO if _modelo_n in PALAVRAS_MOTO else CUSTO_REPARO_CARRO
                margem_liq = round(margem_bruta - custo, 2)

                if desc_pct > 0:
                    lote["desconto_pct"]       = desc_pct
                    lote["margem_bruta"]       = margem_bruta
                    lote["margem_bruta_fmt"]   = _fmt_fipe(margem_bruta)
                    lote["margem_liquida"]     = margem_liq
                    lote["margem_liquida_fmt"] = _fmt_fipe(margem_liq)

            label = (
                f"{GREEN}({lote.get('desconto_pct')}% desc · liq {lote.get('margem_liquida_fmt')}){RESET}"
                if lote.get("desconto_pct") is not None
                else f"{YELLOW}sem desconto{RESET}"
            )
            print(f"→ FIPE {lote['fipe']}  {label}")
            ok += 1
        else:
            motivo = "ano divergente" if (r["valor"] and not r.get("confiavel", True)) else "não encontrado"
            print(f"→ {RED}{motivo}{RESET}")
            if r.get("snippet"):
                print(f"    {DIM}{r['snippet']}{RESET}")
            falhou += 1

        await asyncio.sleep(1.2)  # respeita rate limit da API

    print(f"\n  {GREEN}FIPE OK: {ok}{RESET}  ·  {RED}não encontrado: {falhou}{RESET}")
    return lotes


# ─── Print de cada lote ────────────────────────────────────────────────────────

def print_lote(lote: dict, i: int, total: int):
    titulo    = (lote.get("titulo") or "?")[:65]
    km_str    = f"{lote['km']:,} km".replace(",", ".") if lote.get("km") else "km ?"
    ano_fab   = lote.get("ano_fab")
    ano_mod   = lote.get("ano_mod")
    ano_str   = f"{ano_fab}/{ano_mod}" if ano_fab and ano_fab != ano_mod else str(ano_fab or "?")
    margem_ok = lote.get("margem_liquida") and lote["margem_liquida"] >= MARGEM_MINIMA

    cor_titulo = GREEN if margem_ok else YELLOW

    print(f"\n{'─'*72}")
    print(f"{BOLD}{cor_titulo}[{i}/{total}] {titulo}{RESET}")
    print(f"{'─'*72}")
    print(f"  {DIM}marca/modelo:{RESET}   {lote.get('marca') or '?'}  ·  {lote.get('modelo') or '?'}")
    print(f"  {DIM}ano:{RESET}            {ano_str}  ·  {km_str}  ·  {lote.get('combustivel') or '?'}")
    print(f"  {DIM}câmbio:{RESET}         {lote.get('cambio') or '?'}  ·  ar={lote.get('ar_cond') or '?'}")
    print(f"  {DIM}cor:{RESET}            {lote.get('cor') or '?'}  ·  {lote.get('placa_final') or lote.get('placa') or '?'}")
    print(f"  {DIM}chave:{RESET}          {lote.get('chaves') or '?'}  ·  func={lote.get('funcionando') or '?'}")
    print(f"  {DIM}local:{RESET}          {lote.get('cidade') or '?'} / {lote.get('estado') or '?'}")
    print(f"  {DIM}status:{RESET}         {lote.get('status') or '?'}  ·  lances={lote.get('num_lances') or '?'}")
    print(f"  {DIM}lance inicial:{RESET}  {lote['lance']}")
    print(f"  {DIM}valor atual:{RESET}    {lote.get('valor_atual') or '—'}")
    print(f"  {DIM}fipe (min):{RESET}     {lote.get('fipe') or '—'}  [{lote.get('fipe_fonte') or '—'}]")

    if lote.get("desconto_pct") is not None:
        desc = lote["desconto_pct"]
        mb   = lote.get("margem_bruta_fmt") or "—"
        ml   = lote.get("margem_liquida_fmt") or "—"
        cor  = GREEN if margem_ok else YELLOW
        print(f"  {DIM}desconto FIPE:{RESET}  {cor}{desc}%{RESET}")
        print(f"  {DIM}margem bruta:{RESET}   {mb}  (- R$5.000 reparo → líquida {cor}{ml}{RESET})")
    else:
        print(f"  {DIM}desconto FIPE:{RESET}  —")

    if lote.get("fipe_snippet"):
        print(f"  {DIM}fipe_snippet:{RESET}   {DIM}{lote['fipe_snippet'][:100]}{RESET}")

    print(f"  {DIM}data leilão:{RESET}    {lote.get('data_leilao') or '?'}")
    print(f"  {DIM}lote #{RESET}          {lote.get('lote_num') or '?'}")
    print(f"  {DIM}imagens:{RESET}        {len(lote.get('imagens', []))}x")
    print(f"  {DIM}link:{RESET}           {lote['link']}")


# ─── Coleta Playwright completa ────────────────────────────────────────────────

async def coletar_playwright(headless: bool = True, limit: int = 0) -> tuple[list[dict], dict]:
    cards_todos   = []
    detalhes_dict = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent=BROWSER_UA, locale="pt-BR")
        page    = await context.new_page()

        # ── Aplica filtro e carrega resultados ────────────────────────────
        await aplicar_filtros(page)

        # ── Paginação ─────────────────────────────────────────────────────
        pagina = 1
        while True:
            print(f"\n  {DIM}Página {pagina}:{RESET} coletando cards...")
            cards = await coletar_cards_pagina(page)

            if not cards:
                # Tenta dump da URL atual para diagnóstico
                url_atual = page.url
                print(f"  {YELLOW}⚠ Sem cards detectados na página {pagina}.")
                print(f"    URL atual: {url_atual}{RESET}")
                # Dump do HTML para debug
                try:
                    html_sample = await page.evaluate("() => document.body.innerHTML.slice(0, 3000)")
                    print(f"  {DIM}HTML (primeiros 3000 chars):\n{html_sample}{RESET}")
                except Exception:
                    pass
                break

            print(f"  {GREEN}✓  {len(cards)} cards na página {pagina}{RESET}")
            cards_todos.extend(cards)

            if limit > 0 and len(cards_todos) >= limit:
                cards_todos = cards_todos[:limit]
                print(f"  {YELLOW}--limit {limit} atingido.{RESET}")
                break

            if not await tem_proxima_pagina(page):
                print(f"  {DIM}Última página detectada.{RESET}")
                break

            avancou = await ir_proxima_pagina(page)
            if not avancou:
                print(f"  {YELLOW}Não conseguiu avançar página.{RESET}")
                break

            pagina += 1
            if pagina > 50:
                break

        total = len(cards_todos)
        print(f"\n  {BOLD}Total de cards coletados: {total}{RESET}\n")

        # ── Entra em cada detalhe ─────────────────────────────────────────
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
            erro = det.get("erro")
            if not erro:
                print(f"{GREEN}✓  {imgs} imgs{RESET}")
            else:
                print(f"{RED}erro: {erro[:60]}{RESET}")

            await asyncio.sleep(0.6)

        await browser.close()

    return cards_todos, detalhes_dict


# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="VIP Leilões Debug (sem Supabase)")
    parser.add_argument("--show-browser", action="store_true",  help="Abre browser visível")
    parser.add_argument("--fipe",         action="store_true",  help="Força busca FIPE mesmo no modo debug (limit ≤ 3)")
    parser.add_argument("--no-fipe",      action="store_true",  help="Pula busca FIPE")
    parser.add_argument("--all",          action="store_true",  help="Coleta todos os lotes (sem limit)")
    parser.add_argument("--limit",        type=int, default=1,  help="Limita a N lotes (default: 1 para debug)")
    parser.add_argument("--output",       default="vip_debug_output.json")
    args = parser.parse_args()

    limit_real = 0 if args.all else args.limit

    print(f"\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  🏎️  VIP LEILÕES — DEBUG (sem Supabase){RESET}")
    print(f"{BOLD}{'='*72}{RESET}")
    print(f"  {DIM}Site:    {PESQUISA}{RESET}")
    print(f"  {DIM}Filtros: Procedência=Recuperado Financiamento · KM ≤ 150.000{RESET}")
    print(f"  {DIM}FIPE:    {'não' if args.no_fipe else 'sim (parallelum API)'}{RESET}")
    print(f"  {DIM}Limit:   {limit_real or 'todos'}{RESET}\n")

    # 1. Coleta ─────────────────────────────────────────────────────────────
    print(f"{BOLD}  🌐 COLETANDO...{RESET}\n")
    cards_brutos, detalhes_dict = await coletar_playwright(
        headless=not args.show_browser,
        limit=limit_real,
    )

    # 2. Extração ────────────────────────────────────────────────────────────
    print(f"\n{BOLD}  🔧 EXTRAINDO CAMPOS...{RESET}\n")
    lotes = []
    for i, card in enumerate(cards_brutos, 1):
        detalhe = detalhes_dict.get(i, {})
        lote = extract(card, detalhe)
        lotes.append(lote)

    print(f"  {GREEN}✓  {len(lotes)} lotes estruturados{RESET}")

    # ── DUMP RAW (só no modo debug com limit pequeno) ──────────────────────
    if limit_real and limit_real <= 3:
        print(f"\n{BOLD}{'='*72}{RESET}")
        print(f"{BOLD}  🔬 DUMP RAW — card + detalhe + extract{RESET}")
        print(f"{BOLD}{'='*72}{RESET}")
        for i, (card, lote) in enumerate(zip(cards_brutos, lotes), 1):
            detalhe = detalhes_dict.get(i, {})
            print(f"\n{CYAN}── CARD BRUTO [{i}] ──────────────────────────────{RESET}")
            for k, v in card.items():
                if k != "imagem":  # pula URL longa de imagem
                    print(f"  {k:20s}: {str(v)[:120]}")

            print(f"\n{CYAN}── TEXTO DA PÁGINA (primeiros 1500 chars) ───────{RESET}")
            txt = detalhe.get("texto_pagina", "")
            print(f"{DIM}{txt[:1500]}{RESET}")

            print(f"\n{CYAN}── IMAGENS ({len(detalhe.get('imagens', []))}) ──────────────────────────────{RESET}")
            for img in detalhe.get("imagens", [])[:5]:
                print(f"  {img}")

            print(f"\n{CYAN}── EXTRACT (campos estruturados) ────────────────{RESET}")
            for k, v in lote.items():
                if k not in ("imagens", "fipe_raw", "margem_bruta", "margem_liquida"):
                    print(f"  {k:25s}: {str(v)[:100]}")
            print(f"  {'imagens':25s}: {len(lote.get('imagens', []))}x")

    # 3. FIPE ────────────────────────────────────────────────────────────────
    # No modo debug (limit ≤ 3) o FIPE só roda se passar --fipe explicitamente
    # Para rodar FIPE: python vip_debug.py --all  ou  python vip_debug.py --limit 5 --fipe
    fipe_habilitado = not args.no_fipe and (args.all or args.fipe or (limit_real and limit_real > 3))
    if fipe_habilitado and lotes:
        print(f"\n{BOLD}  🔍 BUSCANDO FIPE ({len(lotes)} lotes)...{RESET}\n")
        lotes = await enriquecer_fipe(lotes)

    # 4. Ordena por margem líquida ────────────────────────────────────────────
    lotes.sort(key=lambda x: x.get("margem_liquida") or float("-inf"), reverse=True)

    # 5. Print individual ─────────────────────────────────────────────────────
    print(f"\n\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  📋  LOTES COLETADOS  ({len(lotes)} total){RESET}")
    print(f"{BOLD}{'='*72}{RESET}")
    for i, lote in enumerate(lotes, 1):
        print_lote(lote, i, len(lotes))

    # 6. Resumo ───────────────────────────────────────────────────────────────
    com_lance  = sum(1 for l in lotes if l.get("lance_raw"))
    com_fipe   = sum(1 for l in lotes if l.get("fipe_raw"))
    com_margem = sum(1 for l in lotes if l.get("margem_liquida") and l["margem_liquida"] >= MARGEM_MINIMA)

    print(f"\n\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  📊  RESUMO{RESET}")
    print(f"{BOLD}{'='*72}{RESET}")
    print(f"  Total coletados:    {len(lotes)}")
    print(f"  Com lance:          {com_lance}")
    print(f"  Com FIPE:           {com_fipe}")
    print(f"  Com margem ≥ R$10k: {com_margem}")

    if com_margem:
        top = [l for l in lotes if l.get("margem_liquida") and l["margem_liquida"] >= MARGEM_MINIMA]
        print(f"\n  {BOLD}TOP 5 por margem líquida:{RESET}")
        for j, l in enumerate(top[:5], 1):
            print(f"    {j}. {l.get('titulo','?')[:50]}")
            print(f"       Lance {l['lance']}  →  FIPE {l.get('fipe','?')}  →  Liq {GREEN}{l.get('margem_liquida_fmt','?')}{RESET}  ({l.get('desconto_pct','?')}% desc)")

    if com_fipe and not com_margem:
        print(f"\n  {YELLOW}Nenhum lote atingiu R$10.000 de margem líquida.{RESET}")
        print(f"  {DIM}(pode ser que os lances estejam altos ou FIPE baixa para esses modelos){RESET}")

    # 7. Salva JSON ───────────────────────────────────────────────────────────
    output_data = {
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "filtro":       "Recuperado Financiamento",
        "total_lotes":  len(lotes),
        "com_lance":    com_lance,
        "com_fipe":     com_fipe,
        "com_margem":   com_margem,
        "lotes":        lotes,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"\n  {GREEN}✓ JSON salvo em: {args.output}{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())