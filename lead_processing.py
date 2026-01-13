"""
Hunter OS - Funcoes compartilhadas de normalizacao e scoring
"""

import re
from typing import Optional

# CNAE prefixos (4 digitos) com alta dor operacional, alinhados ao README
SERVICOS_CNAE_PREFIXOS = {
    "8211", "8219", "8220", "8291",
    "6910", "6920",
    "4930", "5211", "5250",
    "8610", "8630", "8650",
    "4110", "4120"
}

DOMINIOS_EMAIL_GRATUITOS = {
    "gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
    "bol.com.br", "uol.com.br", "icloud.com", "live.com"
}

def limpar_digitos(valor: str) -> str:
    return re.sub(r"\D", "", str(valor or ""))

def cnae_prefixo(cnae: str, tamanho: int = 4) -> str:
    cnae_limpo = limpar_digitos(cnae)
    return cnae_limpo[:tamanho] if len(cnae_limpo) >= tamanho else cnae_limpo

def normalizar_nome(nome: str) -> str:
    """Normaliza nome para Title Case"""
    if not nome:
        return ""
    palavras_minusculas = {"de", "da", "do", "das", "dos", "e", "em", "para"}
    palavras = str(nome).lower().split()
    resultado = []
    for i, palavra in enumerate(palavras):
        if i == 0 or palavra not in palavras_minusculas:
            resultado.append(palavra.capitalize())
        else:
            resultado.append(palavra)
    return " ".join(resultado)

def formatar_telefone(telefone: str) -> str:
    """Formata telefone para exibição"""
    if not telefone:
        return "-"
    tel = limpar_digitos(telefone)
    if len(tel) == 11:
        return f"({tel[:2]}) {tel[2:7]}-{tel[7:]}"
    if len(tel) == 10:
        return f"({tel[:2]}) {tel[2:6]}-{tel[6:]}"
    return telefone

def limpar_cnpj(cnpj: str) -> str:
    """Remove formatação do CNPJ"""
    return limpar_digitos(cnpj)

def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ para XX.XXX.XXX/XXXX-XX"""
    numeros = limpar_digitos(cnpj)
    if len(numeros) == 14:
        return f"{numeros[:2]}.{numeros[2:5]}.{numeros[5:8]}/{numeros[8:12]}-{numeros[12:]}"
    return cnpj or "-"

def email_dominio_proprio(email: str) -> bool:
    if not email or "@" not in email:
        return False
    dominio = email.split("@")[-1].lower().strip()
    return dominio not in DOMINIOS_EMAIL_GRATUITOS

def telefone_celular(telefone: str) -> bool:
    """Heuristica simples para identificar celular no Brasil"""
    digits = limpar_digitos(telefone)
    if digits.startswith("55") and len(digits) >= 12:
        digits = digits[2:]
    if len(digits) == 11 and digits[2] == "9":
        return True
    return False

def calcular_score_icp(empresa: dict) -> int:
    """
    Calcula score ICP (0-100) alinhado ao README:
    Base 50 + site/instagram + celular + CNAE servicos + dominio proprio
    """
    score = 50

    if empresa.get("site") or empresa.get("instagram"):
        score += 20

    telefone = empresa.get("ddd_telefone_1") or empresa.get("telefone") or ""
    if telefone_celular(telefone):
        score += 15

    cnae = empresa.get("cnae_fiscal") or empresa.get("cnae_principal") or ""
    if cnae_prefixo(cnae) in SERVICOS_CNAE_PREFIXOS:
        score += 15

    email = empresa.get("email", "")
    if email_dominio_proprio(email):
        score += 10

    return min(score, 100)

def classificar_score_icp(score: int) -> str:
    if score >= 85:
        return "Hot Lead"
    if score >= 70:
        return "Qualificado"
    if score >= 55:
        return "Potencial"
    return "Frio"
