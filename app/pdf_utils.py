# ============================================================
# pdf_utils.py – Funções de geração do PDF
# ============================================================

import os
from datetime import datetime
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, landscape

# BASE_DIR será definido no main.py e enviado para cá.
BASE_DIR = None


def configurar_base_dir(path):
    """Define o diretório base (passado pelo main.py)."""
    global BASE_DIR
    BASE_DIR = path


# ------------------------------------------------------------
# Funções utilitárias de texto e cor
# ------------------------------------------------------------

def texto_por_valor(v: int) -> str:
    """
    Converte o valor recebido em texto para exibição na célula:
    - 85–89 → -5 a -1 (faixa negativa)
    - 92–99 → códigos SR, NE, ...
    - demais → o próprio número.
    """

    # 👉 Converte o valor 0 em "."
    if v == 0:
        return "."
    
    if 85 <= v <= 89:
        return str(v - 90)  # 85→-5, 89→-1

    mapa_erros = {
        92: "SR",
        93: "NE",
        94: "TN",
        95: "SE",
        96: "SI",
        97: "TA",
        98: "SC",
        99: ".",
    }
    return mapa_erros.get(v, str(v))


def cor_por_valor(v):
    v = int(v)

    # 👉 Valor 0 fica cinza (C0C0C0)
    if v == 0:
        return colors.Color(192/255, 192/255, 192/255)

    if 1 <= v <= 7:
        return colors.Color(128/255, 255/255, 255/255)   # #80FFFF

    if 8 <= v <= 14:
        return colors.Color(0/255, 128/255, 255/255)     # #0080FF

    if 15 <= v <= 25:
        return colors.Color(0/255, 255/255, 128/255)     # #00FF80

    if 26 <= v <= 30:
        return colors.Color(255/255, 255/255, 128/255)   # #FFFF80

    if 31 <= v <= 32:
        return colors.Color(255/255, 255/255, 0/255)     # #FFFF00

    if 33 <= v <= 35:
        return colors.Color(255/255, 128/255, 128/255)   # #FF8080

    if 36 <= v <= 39:
        return colors.Color(255/255, 0/255, 0/255)       # #FF0000

    if 40 <= v <= 50:
        return colors.Color(128/255, 64/255, 64/255)     # #804040

    if 51 <= v <= 60:
        return colors.Color(128/255, 0/255, 0/255)       # #800000

    # 61–99 → cinza
    return colors.Color(192/255, 192/255, 192/255)       # #C0C0C0

# ------------------------------------------------------------
# Função principal para gerar páginas do silo
# ------------------------------------------------------------

def gerar_relatorio_silo(c, descricao, config, temperaturas, arcos=None, datahora=None):
    """
    Gera o relatório térmico de um silo, em modo paisagem,
    com até 2 linhas de cabos por página, e célula dimensionada de forma dinâmica.
    - Não quebra arco entre páginas
    - Não quebra arco entre linha 1 e linha 2
    """
    # === Construção da matriz de colunas (cabos) ===
    colunas = []
    idx = 0
    for nsens in config:
        col = []
        for _ in range(nsens):
            if idx >= len(temperaturas):
                break
            col.append(int(temperaturas[idx]))
            idx += 1
        colunas.append(col)

    total_cabos = len(colunas)
    if total_cabos == 0:
        return
    max_sensores = max((len(cabo) for cabo in colunas), default=0)
    if max_sensores == 0:
        return

    largura, altura = landscape(A4)

    # === Regras de layout ===
    MAX_CABOS_POR_LINHA = 36
    MAX_LINHAS_POR_PAGINA = 2
    CABOS_POR_PAGINA = MAX_CABOS_POR_LINHA * MAX_LINHAS_POR_PAGINA

    MARGEM_X = 80
    MARGEM_TOPO = 110
    MARGEM_RODAPE = 70
    GAP_ENTRE_LINHAS = 40

    # -------------------------------------------------
    # 1) Monta grupos de cabos por arco (índices globais)
    # -------------------------------------------------
    cabos_por_arco = []
    if arcos:
        inicio = 0
        for qtd in arcos:
            fim = min(inicio + qtd, total_cabos)
            if inicio >= fim:
                break
            cabos_por_arco.append(list(range(inicio, fim)))
            inicio = fim
        # Se sobraram cabos além dos arcos declarados, coloca tudo em um arco extra
        if inicio < total_cabos:
            cabos_por_arco.append(list(range(inicio, total_cabos)))
    else:
        # Sem arcos → cada cabo é tratado como um "arco" de 1
        cabos_por_arco = [ [i] for i in range(total_cabos) ]

    # -------------------------------------------------
    # 2) Paginação: distribui arcos entre páginas
    #    (não quebra arco entre páginas)
    # -------------------------------------------------
    paginas_indices = []   # lista de listas de cabos por página
    paginas_arcos = []     # lista de listas de "arcos" (cada arco é uma lista de cabos)

    cabos_restantes_pagina = CABOS_POR_PAGINA
    pagina_cabos = []
    pagina_arcos = []

    for arco_indices in cabos_por_arco:
        qtd = len(arco_indices)

        # Se o arco sozinho é maior que a página, aí não tem jeito, terá de ser quebrado.
        if qtd > CABOS_POR_PAGINA:
            # primeiro, fecha página atual se tiver algo
            if pagina_cabos:
                paginas_indices.append(pagina_cabos)
                paginas_arcos.append(pagina_arcos)
                pagina_cabos = []
                pagina_arcos = []
                cabos_restantes_pagina = CABOS_POR_PAGINA

            # quebra o arco gigante em pedaços do tamanho da página
            for i in range(0, qtd, CABOS_POR_PAGINA):
                chunk = arco_indices[i:i + CABOS_POR_PAGINA]
                paginas_indices.append(chunk)
                paginas_arcos.append([chunk])
            cabos_restantes_pagina = CABOS_POR_PAGINA
            continue

        # Se não cabe na página atual, pula para a próxima
        if qtd > cabos_restantes_pagina and pagina_cabos:
            paginas_indices.append(pagina_cabos)
            paginas_arcos.append(pagina_arcos)
            pagina_cabos = []
            pagina_arcos = []
            cabos_restantes_pagina = CABOS_POR_PAGINA

        pagina_cabos.extend(arco_indices)
        pagina_arcos.append(arco_indices)
        cabos_restantes_pagina -= qtd

    if pagina_cabos:
        paginas_indices.append(pagina_cabos)
        paginas_arcos.append(pagina_arcos)

    # -------------------------------------------------
    # 3) Desenho de cada página
    # -------------------------------------------------
    primeira = True
    for indices_pag, arcos_da_pagina in zip(paginas_indices, paginas_arcos):
        if not primeira:
            c.showPage()
        primeira = False

        c.setPageSize(landscape(A4))
        c.setFillColor(colors.white)
        c.rect(0, 0, largura, altura, fill=1, stroke=0)

        # === Cabeçalho moderno ===
        CABECALHO_ALTURA = 60
        MARGEM_INFERIOR_CABECALHO = 20
        cor_fundo_cab = colors.HexColor("#F0F4F8")
        c.setFillColor(cor_fundo_cab)
        c.rect(0, altura - CABECALHO_ALTURA, largura, CABECALHO_ALTURA, fill=1, stroke=0)

        # Logo
        logo_path = os.path.join(BASE_DIR, "assets", "logo eletromaass.png")
        if os.path.exists(logo_path):
            logo_alt, logo_larg = 40, 80
            y_logo = altura - (CABECALHO_ALTURA / 2) - (logo_alt / 2)
            c.drawImage(
                logo_path, 40, y_logo,
                width=logo_larg, height=logo_alt,
                preserveAspectRatio=True, mask='auto'
            )

        # Texto central
        centro_x = largura / 2
        centro_y_cab = altura - (CABECALHO_ALTURA / 2)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(centro_x, centro_y_cab + 6, f"Relatório Térmico - {descricao}")
        if datahora is None:
            datahora = datetime.now()
        c.setFont("Helvetica", 11)
        c.drawCentredString(centro_x, centro_y_cab - 12, f"Data: {datahora.strftime('%d/%m/%Y %H:%M')}")

        # -------------------------------------------------
        # 3.1) Quebra em linha 1 e linha 2 SEM quebrar arco
        # -------------------------------------------------
        indices_linha1: list[int] = []
        indices_linha2: list[int] = []
        cabos_restantes_linha1 = MAX_CABOS_POR_LINHA

        for arco_indices in arcos_da_pagina:
            # Interseção arco x página (normalmente já é igual, mas por segurança)
            grupo = [idx for idx in arco_indices if idx in indices_pag]
            if not grupo:
                continue

            qtd = len(grupo)

            # caso extremo: arco maior que a linha → divide (não tem jeito)
            if qtd > MAX_CABOS_POR_LINHA:
                # completa linha 1
                falta_l1 = MAX_CABOS_POR_LINHA - len(indices_linha1)
                indices_linha1.extend(grupo[:falta_l1])
                indices_linha2.extend(grupo[falta_l1:])
                cabos_restantes_linha1 = 0
                continue

            # Se ainda cabe inteiro na linha 1, coloca lá
            if qtd <= cabos_restantes_linha1:
                indices_linha1.extend(grupo)
                cabos_restantes_linha1 -= qtd
            else:
                # Senão, vai inteiro pra linha 2
                indices_linha2.extend(grupo)

        # Garante que a ordem dos cabos nas linhas siga a ordem da página
        indices_linha1 = [idx for idx in indices_pag if idx in indices_linha1]
        indices_linha2 = [idx for idx in indices_pag if idx in indices_linha2]

        linhas_indices = []
        if indices_linha1:
            linhas_indices.append(indices_linha1)
        if indices_linha2:
            linhas_indices.append(indices_linha2)

        num_linhas = len(linhas_indices)
        if num_linhas == 0:
            continue

        max_cabos_linha = max(len(l) for l in linhas_indices)

        # -------------------------------------------------
        # 3.2) Cálculo de tamanho das células
        # -------------------------------------------------
        altura_disp = altura - MARGEM_TOPO - MARGEM_RODAPE
        largura_disp = largura - 2 * MARGEM_X

        tam_h = (altura_disp - (num_linhas - 1) * GAP_ENTRE_LINHAS) / (max_sensores * num_linhas)
        tam_w = largura_disp / max_cabos_linha
        tam = min(tam_h, tam_w, 25)
        tam = max(tam, 8)

        altura_total = max_sensores * tam * num_linhas + (num_linhas - 1) * GAP_ENTRE_LINHAS
        inicio_y_global = ((altura - altura_total) / 2) - MARGEM_INFERIOR_CABECALHO

        # -------------------------------------------------
        # 3.3) Desenho das linhas (matriz térmica)
        # -------------------------------------------------
        c.setLineWidth(0.5)
        for idx_linha, indices_cabos in enumerate(reversed(linhas_indices)):
            y_base = inicio_y_global + idx_linha * (max_sensores * tam + GAP_ENTRE_LINHAS)
            num_cabos_linha = len(indices_cabos)
            largura_mat = num_cabos_linha * tam
            x_inicio = (largura - largura_mat) / 2

            # Células
            c.setFont("Helvetica-Bold", 6)
            for pos_cabo, idx_cabo in enumerate(indices_cabos):
                col = colunas[idx_cabo]
                x_cabo = x_inicio + pos_cabo * tam
                for linha_sensor, v in enumerate(col):
                    y_cel = y_base + linha_sensor * tam
                    cor = cor_por_valor(v)
                    texto = texto_por_valor(v)
                    c.setFillColor(cor)
                    c.rect(x_cabo, y_cel, tam, tam, fill=1, stroke=0)
                    c.setFillColor(colors.black)
                    c.drawCentredString(x_cabo + tam / 2, y_cel + tam / 2 - 2, texto)

            # Sensores à esquerda
            c.setFont("Helvetica-Bold", 7)
            c.setFillColor(colors.black)
            for i in range(max_sensores):
                y_label = y_base + i * tam + tam / 2 - 3
                c.drawRightString(x_inicio - 8, y_label, f"S{i+1:02}")

            # Cabos abaixo
            c.setFont("Helvetica-Bold", max(4, min(8, tam * 0.35)))
            for pos_cabo, idx_cabo in enumerate(indices_cabos):
                x_label = x_inicio + pos_cabo * tam + tam / 2
                y_label = y_base + max_sensores * tam + (tam * 0.3)
                c.drawCentredString(x_label, y_label, f"C{idx_cabo + 1:02}")

            # Etiquetas de arcos
            if arcos and arcos_da_pagina:
                c.setFont("Helvetica-Bold", 8)
                c.setFillColor(colors.black)
                for num_arco, arco_indices in enumerate(arcos_da_pagina, start=1):
                    cabos_linha = [idx for idx in indices_cabos if idx in arco_indices]
                    if not cabos_linha:
                        continue
                    primeiro_idx = cabos_linha[0]
                    ultimo_idx = cabos_linha[-1]
                    pos_p = indices_cabos.index(primeiro_idx)
                    pos_u = indices_cabos.index(ultimo_idx)
                    x_p = x_inicio + pos_p * tam
                    x_u = x_inicio + (pos_u + 1) * tam
                    x_centro = (x_p + x_u) / 2
                    y_arco = y_base + max_sensores * tam + (tam * 1.3)
                    c.drawCentredString(x_centro, y_arco, f"A{num_arco:02}")

        # # Linha divisória se houver duas fileiras
        # if num_linhas == 2:
        #     y_meio = inicio_y_global + max_sensores * tam + GAP_ENTRE_LINHAS / 2
        #     c.setStrokeColor(colors.lightgrey)
        #     c.line(MARGEM_X / 2, y_meio, largura - MARGEM_X / 2, y_meio)

        # === Legenda de gradiente ===
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(colors.black)
        c.drawCentredString(largura / 2, 50, "Escala de Temperatura")

        barra_larg, barra_alt = 400, 14
        x_ini_barra = (largura - barra_larg) / 2
        y_barra = 30
        num_passos = 100
        for i in range(num_passos):
            frac = i / (num_passos - 1)
            v = -5 + frac * 65  # -5 a 60
            cor = cor_por_valor(v)
            x_bar = x_ini_barra + i * (barra_larg / num_passos)
            c.setFillColor(cor)
            c.rect(x_bar, y_barra, barra_larg / num_passos, barra_alt, fill=1, stroke=0)

        c.setFillColor(colors.black)
        c.setFont("Helvetica", 7)
        c.drawString(x_ini_barra - 25, y_barra + 3, "Frio")
        c.drawCentredString(largura / 2, y_barra + 3, "Normal")
        c.drawRightString(x_ini_barra + barra_larg + 25, y_barra + 3, "Crítico")

    # Ao final deste silo, mantém o comportamento antigo:
    c.showPage()