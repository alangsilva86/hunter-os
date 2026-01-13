"""
Hunter OS - B2B Prospecting
Módulo de Utilitários para ETL de Leads B2B
"""

import re
import time
import json
import hashlib
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta

import requests
import backoff
import phonenumbers
from phonenumbers import PhoneNumberType
from bs4 import BeautifulSoup
from data_sources import DataCache, DEFAULT_CACHE_TTL_HOURS
from lead_processing import (
    normalizar_nome as normalizar_nome_padrao,
    formatar_telefone as formatar_telefone_padrao,
    limpar_cnpj as limpar_cnpj_padrao,
    formatar_cnpj as formatar_cnpj_padrao,
    calcular_score_icp as calcular_score_icp_padrao,
    classificar_score_icp as classificar_score_icp_padrao,
    email_dominio_proprio
)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTES E CONFIGURAÇÕES
# ============================================================================

# Cidades alvo (Maringá e região - raio 50km)
CIDADES_ALVO = {
    "MARINGA": "4115200",
    "SARANDI": "4126256",
    "MARIALVA": "4114807",
    "PAICANDU": "4117305",
    "MANDAGUARI": "4114203"
}

# CNAEs prioritários por categoria
CNAES_PRIORITARIOS = {
    "Serviços Administrativos": ["8211", "8219", "8220", "8291"],
    "Atividades Jurídicas e Contábeis": ["6910", "6920"],
    "Logística e Transporte": ["4930", "5211", "5250"],
    "Saúde e Clínicas": ["8610", "8630", "8650"],
    "Construção e Incorporação": ["4110", "4120"]
}

# Naturezas jurídicas a excluir (MEI)
NATUREZAS_EXCLUIR = ["213-5", "2135"]

# Naturezas jurídicas válidas
NATUREZAS_VALIDAS = ["LTDA", "S.A.", "S/A", "EIRELI", "SOCIEDADE", "LIMITADA"]

# ============================================================================
# CACHE LOCAL (SQLite)
# ============================================================================

class CacheManager:
    """Gerenciador de cache unificado (wrapper do DataCache)"""
    
    def __init__(self, db_path: str = "hunter_cache.db", ttl_hours: int = DEFAULT_CACHE_TTL_HOURS):
        self.cache = DataCache(db_path=db_path, ttl_hours=ttl_hours)
    
    def get(self, key: str) -> Optional[Dict]:
        """Recupera dados do cache"""
        return self.cache.get_cache(key)
    
    def set(self, key: str, data: Dict, ttl_hours: int = DEFAULT_CACHE_TTL_HOURS):
        """Armazena dados no cache"""
        self.cache.set_cache(key, data, ttl_hours=ttl_hours)
    
    def save_empresa(self, cnpj: str, data: Dict, fonte: str = "api"):
        """Salva dados de uma empresa"""
        self.cache.save_empresa(cnpj, data, fonte=fonte)
    
    def get_empresa(self, cnpj: str) -> Optional[Dict]:
        """Recupera dados de uma empresa"""
        return self.cache.get_empresa(cnpj)
    
    def get_all_empresas(self, limit: int = None) -> List[Dict]:
        """Recupera todas as empresas do cache"""
        return self.cache.get_all_empresas(limit=limit)

# ============================================================================
# EXTRAÇÃO DE DADOS (APIs)
# ============================================================================

class DataExtractor:
    """Classe para extração de dados de empresas via APIs públicas"""
    
    def __init__(self, cache: CacheManager):
        self.cache = cache
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'HunterOS-B2B-Prospecting/1.0'
        })
    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.Timeout),
        max_tries=3,
        max_time=30
    )
    def _make_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Faz requisição HTTP com retry e backoff"""
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit
                logger.warning("Rate limit atingido, aguardando...")
                time.sleep(60)
                return self._make_request(url, params)
            elif e.response.status_code == 404:
                return None
            raise
        except json.JSONDecodeError:
            logger.error(f"Erro ao decodificar JSON de {url}")
            return None
    
    def buscar_cnpj(self, cnpj: str) -> Optional[Dict]:
        """Busca dados de um CNPJ específico via BrasilAPI"""
        cnpj_limpo = re.sub(r'\D', '', cnpj)
        
        # Verifica cache primeiro
        cached = self.cache.get_empresa(cnpj_limpo)
        if cached:
            logger.info(f"CNPJ {cnpj_limpo} encontrado no cache")
            return cached
        
        # Tenta BrasilAPI
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"
        data = self._make_request(url)
        
        if data:
            self.cache.save_empresa(cnpj_limpo, data)
            time.sleep(0.5)  # Rate limiting
            return data
        
        return None
    
    def buscar_empresas_por_cnae_cidade(self, cnae: str, cidade: str, uf: str = "PR") -> List[Dict]:
        """
        Busca empresas por CNAE e cidade.
        Nota: A BrasilAPI não suporta busca por CNAE diretamente.
        Usamos uma abordagem alternativa com dados simulados/cache.
        """
        cache_key = f"empresas_{cnae}_{cidade}_{uf}"
        cached = self.cache.get(cache_key)
        
        if cached:
            logger.info(f"Dados de {cidade}/{cnae} encontrados no cache")
            return cached.get('empresas', [])
        
        # Como a BrasilAPI não suporta busca por CNAE,
        # retornamos lista vazia para ser preenchida manualmente
        # ou via outra fonte de dados
        logger.warning(f"Busca por CNAE não suportada diretamente pela API")
        return []

# ============================================================================
# TRANSFORMAÇÃO DE DADOS
# ============================================================================

class DataTransformer:
    """Classe para transformação e limpeza de dados"""
    
    @staticmethod
    def normalizar_nome(nome: str) -> str:
        """Normaliza nome para Title Case"""
        return normalizar_nome_padrao(nome)
    
    @staticmethod
    def formatar_telefone(telefone: str, ddd_padrao: str = "44") -> str:
        """Formata telefone para padrão (XX) XXXXX-XXXX"""
        if not telefone:
            return ""
        
        # Remove caracteres não numéricos
        numeros = re.sub(r'\D', '', str(telefone))
        
        if not numeros:
            return ""
        
        # Adiciona DDD se não tiver
        if len(numeros) == 8:
            numeros = ddd_padrao + numeros
        elif len(numeros) == 9:
            numeros = ddd_padrao + numeros
        
        # Formata
        return formatar_telefone_padrao(numeros)
    
    @staticmethod
    def validar_telefone_tipo(telefone: str) -> Dict[str, Any]:
        """Valida e identifica tipo de telefone (fixo/móvel)"""
        resultado = {
            'valido': False,
            'tipo': 'desconhecido',
            'celular': False,
            'whatsapp_provavel': False
        }
        
        numeros = re.sub(r'\D', '', str(telefone))
        
        if not numeros or len(numeros) < 10:
            return resultado
        
        try:
            # Parse com código do Brasil
            if not numeros.startswith('55'):
                numeros = '55' + numeros
            
            parsed = phonenumbers.parse(f"+{numeros}", "BR")
            
            if phonenumbers.is_valid_number(parsed):
                resultado['valido'] = True
                
                # Verifica tipo
                number_type = phonenumbers.number_type(parsed)
                
                if number_type == PhoneNumberType.MOBILE:
                    resultado['tipo'] = 'celular'
                    resultado['celular'] = True
                    resultado['whatsapp_provavel'] = True
                elif number_type == PhoneNumberType.FIXED_LINE:
                    resultado['tipo'] = 'fixo'
                else:
                    resultado['tipo'] = 'fixo_ou_celular'
                    # Heurística: números com 9 dígitos após DDD são celulares
                    if len(numeros) == 13 and numeros[4] == '9':
                        resultado['celular'] = True
                        resultado['whatsapp_provavel'] = True
        except Exception as e:
            logger.debug(f"Erro ao validar telefone: {e}")
        
        return resultado
    
    @staticmethod
    def limpar_cnpj(cnpj: str) -> str:
        """Remove formatação do CNPJ"""
        return limpar_cnpj_padrao(cnpj)
    
    @staticmethod
    def formatar_cnpj(cnpj: str) -> str:
        """Formata CNPJ para XX.XXX.XXX/XXXX-XX"""
        return formatar_cnpj_padrao(cnpj)
    
    @staticmethod
    def filtrar_por_porte(empresa: Dict) -> bool:
        """Verifica se empresa deve ser incluída baseado no porte"""
        # Exclui MEI
        natureza = str(empresa.get('natureza_juridica', '')).upper()
        codigo_natureza = str(empresa.get('codigo_natureza_juridica', ''))
        
        # Verifica código de natureza jurídica MEI
        if codigo_natureza in NATUREZAS_EXCLUIR:
            return False
        
        if 'MEI' in natureza or 'MICROEMPREENDEDOR INDIVIDUAL' in natureza:
            return False
        
        # Verifica porte
        porte = str(empresa.get('porte', '')).upper()
        if porte in ['MEI', 'MICROEMPREENDEDOR']:
            return False
        
        return True
    
    @staticmethod
    def filtrar_por_situacao(empresa: Dict) -> bool:
        """Verifica se empresa está ativa"""
        situacao = str(empresa.get('situacao_cadastral', '')).upper()
        descricao = str(empresa.get('descricao_situacao_cadastral', '')).upper()
        
        situacoes_invalidas = ['BAIXADA', 'INAPTA', 'SUSPENSA', 'NULA']
        
        for inv in situacoes_invalidas:
            if inv in situacao or inv in descricao:
                return False
        
        return True
    
    def transformar_empresa(self, empresa: Dict) -> Optional[Dict]:
        """Transforma e limpa dados de uma empresa"""
        if not self.filtrar_por_porte(empresa):
            return None
        
        if not self.filtrar_por_situacao(empresa):
            return None
        
        # Extrai e normaliza dados
        telefone1 = empresa.get('ddd_telefone_1', '') or ''
        telefone2 = empresa.get('ddd_telefone_2', '') or ''
        
        # Combina DDD com telefone se separados
        if empresa.get('ddd_telefone_1'):
            telefone1 = str(empresa.get('ddd_telefone_1', ''))
        
        telefone_info = self.validar_telefone_tipo(telefone1)
        
        # Extrai email
        email = empresa.get('email', '') or ''
        
        # Verifica domínio próprio
        dominio_proprio = email_dominio_proprio(email)
        
        # Monta endereço completo
        endereco_parts = [
            empresa.get('logradouro', ''),
            empresa.get('numero', ''),
            empresa.get('complemento', ''),
            empresa.get('bairro', ''),
            empresa.get('municipio', ''),
            empresa.get('uf', '')
        ]
        endereco = ', '.join([p for p in endereco_parts if p])
        
        # CNAE principal
        cnae_principal = empresa.get('cnae_fiscal', '') or empresa.get('cnae_fiscal_principal', '')
        cnae_descricao = empresa.get('cnae_fiscal_descricao', '') or ''
        
        empresa_final = {
            'cnpj': self.formatar_cnpj(empresa.get('cnpj', '')),
            'cnpj_limpo': self.limpar_cnpj(empresa.get('cnpj', '')),
            'razao_social': self.normalizar_nome(empresa.get('razao_social', '')),
            'nome_fantasia': self.normalizar_nome(empresa.get('nome_fantasia', '') or empresa.get('razao_social', '')),
            'cnae_principal': str(cnae_principal),
            'cnae_descricao': cnae_descricao,
            'telefone': self.formatar_telefone(telefone1),
            'telefone_2': self.formatar_telefone(telefone2),
            'telefone_celular': telefone_info['celular'],
            'whatsapp_provavel': telefone_info['whatsapp_provavel'],
            'email': email.lower() if email else '',
            'dominio_proprio': dominio_proprio,
            'endereco': endereco,
            'municipio': empresa.get('municipio', ''),
            'uf': empresa.get('uf', 'PR'),
            'cep': empresa.get('cep', ''),
            'porte': empresa.get('porte', ''),
            'natureza_juridica': empresa.get('descricao_natureza_juridica', ''),
            'capital_social': empresa.get('capital_social', 0),
            'data_abertura': empresa.get('data_inicio_atividade', ''),
            'situacao': empresa.get('descricao_situacao_cadastral', 'ATIVA'),
            'site': '',
            'instagram': '',
            'linkedin': '',
            'tem_formulario_contato': False,
            'icp_score': 0,
            'classificacao': ''
        }
        
        empresa_final['icp_score'] = calcular_score_icp_padrao(empresa_final)
        empresa_final['classificacao'] = classificar_score_icp_padrao(empresa_final['icp_score'])
        
        return empresa_final

# ============================================================================
# ENRIQUECIMENTO DE DADOS
# ============================================================================

class DataEnricher:
    """Classe para enriquecimento de dados de empresas"""
    
    def __init__(self, cache: CacheManager):
        self.cache = cache
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def buscar_site_google(self, nome_empresa: str, cidade: str = "Maringá") -> Optional[str]:
        """Busca site oficial da empresa via Google"""
        try:
            from googlesearch import search
            
            query = f"{nome_empresa} {cidade} site oficial"
            cache_key = f"google_{hashlib.md5(query.encode()).hexdigest()}"
            
            cached = self.cache.get(cache_key)
            if cached:
                return cached.get('site')
            
            # Busca no Google
            results = list(search(query, num_results=3, lang='pt-br'))
            
            if results:
                # Filtra resultados relevantes
                for url in results:
                    # Ignora redes sociais e diretórios
                    ignorar = ['facebook.com', 'instagram.com', 'linkedin.com', 
                              'twitter.com', 'youtube.com', 'reclameaqui.com',
                              'econodata.com', 'cnpj.info', 'consultasocio.com']
                    
                    if not any(ig in url.lower() for ig in ignorar):
                        self.cache.set(cache_key, {'site': url})
                        return url
            
            time.sleep(2)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Erro ao buscar site: {e}")
        
        return None
    
    def buscar_redes_sociais(self, nome_empresa: str, cidade: str = "Maringá") -> Dict[str, str]:
        """Busca perfis de redes sociais da empresa"""
        redes = {'instagram': '', 'linkedin': ''}
        
        try:
            from googlesearch import search
            
            # Busca Instagram
            query_ig = f"{nome_empresa} {cidade} instagram"
            results_ig = list(search(query_ig, num_results=2, lang='pt-br'))
            
            for url in results_ig:
                if 'instagram.com' in url.lower():
                    redes['instagram'] = url
                    break
            
            time.sleep(1)
            
            # Busca LinkedIn
            query_li = f"{nome_empresa} {cidade} linkedin"
            results_li = list(search(query_li, num_results=2, lang='pt-br'))
            
            for url in results_li:
                if 'linkedin.com' in url.lower():
                    redes['linkedin'] = url
                    break
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Erro ao buscar redes sociais: {e}")
        
        return redes
    
    def verificar_formulario_contato(self, url: str) -> bool:
        """Verifica se o site tem formulário de contato"""
        if not url:
            return False
        
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Keywords que indicam formulário de contato
            keywords = ['fale conosco', 'contato', 'orçamento', 'agendar', 
                       'solicitar', 'whatsapp', 'formulário', 'enviar mensagem']
            
            text = soup.get_text().lower()
            
            for keyword in keywords:
                if keyword in text:
                    return True
            
            # Verifica se tem formulário HTML
            forms = soup.find_all('form')
            if forms:
                return True
            
        except Exception as e:
            logger.debug(f"Erro ao verificar formulário: {e}")
        
        return False
    
    def enriquecer_empresa(self, empresa: Dict, buscar_web: bool = True) -> Dict:
        """Enriquece dados de uma empresa"""
        if not buscar_web:
            return empresa
        
        nome = empresa.get('nome_fantasia') or empresa.get('razao_social', '')
        cidade = empresa.get('municipio', 'Maringá')
        
        # Busca site
        if not empresa.get('site'):
            site = self.buscar_site_google(nome, cidade)
            if site:
                empresa['site'] = site
                empresa['tem_formulario_contato'] = self.verificar_formulario_contato(site)
        
        # Busca redes sociais
        if not empresa.get('instagram') or not empresa.get('linkedin'):
            redes = self.buscar_redes_sociais(nome, cidade)
            if redes['instagram']:
                empresa['instagram'] = redes['instagram']
            if redes['linkedin']:
                empresa['linkedin'] = redes['linkedin']
        
        return empresa

# ============================================================================
# SCORING DE ICP
# ============================================================================

class ICPScorer:
    """Classe para cálculo de score ICP"""
    
    @classmethod
    def calcular_score(cls, empresa: Dict) -> int:
        """Calcula score ICP (0-100) alinhado ao README"""
        return calcular_score_icp_padrao(empresa)
    
    @classmethod
    def classificar_lead(cls, score: int) -> str:
        """Classifica o lead baseado no score"""
        return classificar_score_icp_padrao(score)

# ============================================================================
# GERADOR DE DADOS DE EXEMPLO
# ============================================================================

def gerar_dados_exemplo() -> List[Dict]:
    """
    Gera dados de exemplo para demonstração da ferramenta.
    Em produção, esses dados viriam das APIs.
    """
    empresas_exemplo = [
        {
            "cnpj": "12345678000190",
            "razao_social": "CLINICA ODONTOLOGICA SORRISO LTDA",
            "nome_fantasia": "CLINICA SORRISO",
            "cnae_fiscal": "8630",
            "cnae_fiscal_descricao": "Atividade médica ambulatorial",
            "ddd_telefone_1": "44999887766",
            "email": "contato@clinicasorriso.com.br",
            "logradouro": "Av Brasil",
            "numero": "1500",
            "bairro": "Centro",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87013000",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 150000,
            "data_inicio_atividade": "2015-03-15",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "98765432000110",
            "razao_social": "ESCRITORIO CONTABIL PRECISAO LTDA",
            "nome_fantasia": "CONTABILIDADE PRECISAO",
            "cnae_fiscal": "6920",
            "cnae_fiscal_descricao": "Atividades de contabilidade",
            "ddd_telefone_1": "44998765432",
            "email": "contato@precisaocontabil.com.br",
            "logradouro": "Rua Santos Dumont",
            "numero": "850",
            "bairro": "Zona 7",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87020000",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 80000,
            "data_inicio_atividade": "2010-08-20",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "11223344000155",
            "razao_social": "TRANSPORTADORA RAPIDO NORTE LTDA",
            "nome_fantasia": "RAPIDO NORTE TRANSPORTES",
            "cnae_fiscal": "4930",
            "cnae_fiscal_descricao": "Transporte rodoviário de carga",
            "ddd_telefone_1": "4433221100",
            "email": "logistica@rapidonorte.com.br",
            "logradouro": "Rod PR 317",
            "numero": "KM 5",
            "bairro": "Parque Industrial",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87065000",
            "porte": "MEDIA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 500000,
            "data_inicio_atividade": "2008-01-10",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "55667788000122",
            "razao_social": "ADVOCACIA SILVA E ASSOCIADOS",
            "nome_fantasia": "SILVA ADVOGADOS",
            "cnae_fiscal": "6910",
            "cnae_fiscal_descricao": "Atividades jurídicas",
            "ddd_telefone_1": "44999112233",
            "email": "contato@silvaadvogados.adv.br",
            "logradouro": "Av Tiradentes",
            "numero": "2000",
            "complemento": "Sala 501",
            "bairro": "Centro",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87013260",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Simples Pura",
            "capital_social": 100000,
            "data_inicio_atividade": "2012-05-22",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "99887766000133",
            "razao_social": "CONSTRUTORA HORIZONTE LTDA",
            "nome_fantasia": "HORIZONTE CONSTRUCOES",
            "cnae_fiscal": "4120",
            "cnae_fiscal_descricao": "Construção de edifícios",
            "ddd_telefone_1": "44988776655",
            "email": "orcamento@horizonteconstrucoes.com.br",
            "logradouro": "Rua Neo Alves Martins",
            "numero": "3500",
            "bairro": "Zona 1",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87013060",
            "porte": "MEDIA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 1000000,
            "data_inicio_atividade": "2005-11-30",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "44332211000177",
            "razao_social": "CENTRO MEDICO VIDA PLENA LTDA",
            "nome_fantasia": "CLINICA VIDA PLENA",
            "cnae_fiscal": "8610",
            "cnae_fiscal_descricao": "Atividades de atendimento hospitalar",
            "ddd_telefone_1": "44997654321",
            "email": "agendamento@vidaplena.med.br",
            "logradouro": "Av Colombo",
            "numero": "5500",
            "bairro": "Zona 7",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87020900",
            "porte": "MEDIA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 800000,
            "data_inicio_atividade": "2000-06-15",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "77889900000144",
            "razao_social": "SERVICOS ADMINISTRATIVOS EFICIENCIA LTDA",
            "nome_fantasia": "EFICIENCIA SERVICOS",
            "cnae_fiscal": "8211",
            "cnae_fiscal_descricao": "Serviços combinados de escritório",
            "ddd_telefone_1": "44996543210",
            "email": "comercial@eficienciaservicos.com.br",
            "logradouro": "Rua Joubert de Carvalho",
            "numero": "700",
            "bairro": "Centro",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87013200",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 50000,
            "data_inicio_atividade": "2018-09-01",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "33221100000188",
            "razao_social": "ARMAZEM LOGISTICO CENTRAL LTDA",
            "nome_fantasia": "CENTRAL LOGISTICA",
            "cnae_fiscal": "5211",
            "cnae_fiscal_descricao": "Armazenamento",
            "ddd_telefone_1": "4432109876",
            "email": "operacoes@centrallogistica.com.br",
            "logradouro": "Rod BR 376",
            "numero": "KM 180",
            "bairro": "Distrito Industrial",
            "municipio": "SARANDI",
            "uf": "PR",
            "cep": "87113000",
            "porte": "MEDIA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 350000,
            "data_inicio_atividade": "2014-02-28",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "66554433000199",
            "razao_social": "CLINICA FISIOTERAPIA MOVIMENTO LTDA",
            "nome_fantasia": "FISIO MOVIMENTO",
            "cnae_fiscal": "8650",
            "cnae_fiscal_descricao": "Atividades de profissionais da área de saúde",
            "ddd_telefone_1": "44995432109",
            "email": "atendimento@gmail.com",
            "logradouro": "Av Parana",
            "numero": "1200",
            "bairro": "Zona 1",
            "municipio": "MARIALVA",
            "uf": "PR",
            "cep": "86990000",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 60000,
            "data_inicio_atividade": "2019-07-10",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "22110099000166",
            "razao_social": "ASSESSORIA EMPRESARIAL NORTE PR LTDA",
            "nome_fantasia": "NORTE PR ASSESSORIA",
            "cnae_fiscal": "8219",
            "cnae_fiscal_descricao": "Preparação de documentos e serviços especializados de apoio administrativo",
            "ddd_telefone_1": "44994321098",
            "email": "contato@nortepr.com.br",
            "logradouro": "Rua Pioneiro Joao Domingos",
            "numero": "450",
            "bairro": "Centro",
            "municipio": "PAICANDU",
            "uf": "PR",
            "cep": "87140000",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 40000,
            "data_inicio_atividade": "2020-03-05",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "88990011000177",
            "razao_social": "INCORPORADORA MANDAGUARI LTDA",
            "nome_fantasia": "MANDAGUARI IMOVEIS",
            "cnae_fiscal": "4110",
            "cnae_fiscal_descricao": "Incorporação de empreendimentos imobiliários",
            "ddd_telefone_1": "44993210987",
            "email": "vendas@mandaguariimoveis.com.br",
            "logradouro": "Av Brasil",
            "numero": "800",
            "bairro": "Centro",
            "municipio": "MANDAGUARI",
            "uf": "PR",
            "cep": "86975000",
            "porte": "MEDIA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 2000000,
            "data_inicio_atividade": "2003-12-01",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "11009988000155",
            "razao_social": "CALL CENTER ATENDIMENTO TOTAL LTDA",
            "nome_fantasia": "ATENDIMENTO TOTAL",
            "cnae_fiscal": "8220",
            "cnae_fiscal_descricao": "Atividades de teleatendimento",
            "ddd_telefone_1": "44992109876",
            "email": "rh@atendimentototal.com.br",
            "logradouro": "Av Horacio Raccanello Filho",
            "numero": "5400",
            "bairro": "Novo Centro",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87020035",
            "porte": "MEDIA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 200000,
            "data_inicio_atividade": "2016-04-18",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "55443322000133",
            "razao_social": "DESPACHANTE ADUANEIRO FRONTEIRA LTDA",
            "nome_fantasia": "FRONTEIRA DESPACHANTE",
            "cnae_fiscal": "5250",
            "cnae_fiscal_descricao": "Atividades relacionadas à organização do transporte de carga",
            "ddd_telefone_1": "4430987654",
            "email": "despacho@fronteira.com.br",
            "logradouro": "Rua Pioneira Maria Apparecida",
            "numero": "320",
            "bairro": "Zona Industrial",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87065100",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 120000,
            "data_inicio_atividade": "2011-10-25",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "99001122000144",
            "razao_social": "COBRANCA E RECUPERACAO CREDITO NORTE LTDA",
            "nome_fantasia": "NORTE COBRANCAS",
            "cnae_fiscal": "8291",
            "cnae_fiscal_descricao": "Atividades de cobrança e informações cadastrais",
            "ddd_telefone_1": "44991098765",
            "email": "cobranca@nortecobrancas.com.br",
            "logradouro": "Rua Sao Josafat",
            "numero": "150",
            "bairro": "Zona 7",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87020270",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 75000,
            "data_inicio_atividade": "2017-08-12",
            "descricao_situacao_cadastral": "ATIVA"
        },
        {
            "cnpj": "77665544000122",
            "razao_social": "LABORATORIO ANALISES CLINICAS SAUDE LTDA",
            "nome_fantasia": "LAB SAUDE",
            "cnae_fiscal": "8630",
            "cnae_fiscal_descricao": "Atividade médica ambulatorial",
            "ddd_telefone_1": "44990987654",
            "email": "exames@labsaude.com.br",
            "logradouro": "Av Mandacaru",
            "numero": "1800",
            "bairro": "Parque das Grevileas",
            "municipio": "MARINGA",
            "uf": "PR",
            "cep": "87083000",
            "porte": "PEQUENA",
            "descricao_natureza_juridica": "Sociedade Empresária Limitada",
            "capital_social": 180000,
            "data_inicio_atividade": "2013-05-08",
            "descricao_situacao_cadastral": "ATIVA"
        }
    ]
    
    return empresas_exemplo

# ============================================================================
# FUNÇÕES DE EXPORTAÇÃO
# ============================================================================

def exportar_csv_crm(df, filename: str = "leads_crm.csv") -> str:
    """Exporta dados para CSV formatado para CRM"""
    colunas_crm = [
        'razao_social', 'nome_fantasia', 'cnpj', 'cnae_principal',
        'cnae_descricao', 'telefone', 'email', 'endereco', 'municipio',
        'uf', 'site', 'instagram', 'linkedin', 'icp_score', 'classificacao'
    ]
    
    # Filtra apenas colunas existentes
    colunas_existentes = [c for c in colunas_crm if c in df.columns]
    
    df_export = df[colunas_existentes].copy()
    df_export.to_csv(filename, index=False, encoding='utf-8-sig')
    
    return filename

def gerar_relatorio_inteligencia(df) -> str:
    """Gera relatório estatístico de inteligência"""
    total = len(df)
    
    if total == 0:
        return "Nenhum dado disponível para análise."
    
    relatorio = []
    relatorio.append("# 📊 Relatório de Inteligência - Hunter OS\n")
    relatorio.append(f"**Data:** {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
    relatorio.append(f"**Total de Leads:** {total}\n")
    
    # Distribuição por cidade
    relatorio.append("\n## 🏙️ Distribuição por Cidade\n")
    if 'municipio' in df.columns:
        dist_cidade = df['municipio'].value_counts()
        for cidade, qtd in dist_cidade.items():
            pct = (qtd / total) * 100
            relatorio.append(f"- {cidade}: {qtd} ({pct:.1f}%)")
    
    # Distribuição por CNAE
    relatorio.append("\n\n## 🏢 Distribuição por Setor (CNAE)\n")
    if 'cnae_descricao' in df.columns:
        dist_cnae = df['cnae_descricao'].value_counts().head(10)
        for cnae, qtd in dist_cnae.items():
            pct = (qtd / total) * 100
            relatorio.append(f"- {cnae}: {qtd} ({pct:.1f}%)")
    
    # Score ICP
    relatorio.append("\n\n## 🎯 Análise de Score ICP\n")
    if 'icp_score' in df.columns:
        media_score = df['icp_score'].mean()
        hot_leads = len(df[df['icp_score'] >= 85])
        qualificados = len(df[(df['icp_score'] >= 70) & (df['icp_score'] < 85)])
        
        relatorio.append(f"- **Score Médio:** {media_score:.1f}")
        relatorio.append(f"- **Hot Leads (85+):** {hot_leads} ({(hot_leads/total)*100:.1f}%)")
        relatorio.append(f"- **Qualificados (70-84):** {qualificados} ({(qualificados/total)*100:.1f}%)")
    
    # Canais de contato
    relatorio.append("\n\n## 📞 Canais de Contato\n")
    if 'whatsapp_provavel' in df.columns:
        com_whatsapp = df['whatsapp_provavel'].sum()
        relatorio.append(f"- Com WhatsApp provável: {com_whatsapp} ({(com_whatsapp/total)*100:.1f}%)")
    
    if 'dominio_proprio' in df.columns:
        com_dominio = df['dominio_proprio'].sum()
        relatorio.append(f"- Com domínio próprio: {com_dominio} ({(com_dominio/total)*100:.1f}%)")
    
    if 'site' in df.columns:
        com_site = len(df[df['site'].notna() & (df['site'] != '')])
        relatorio.append(f"- Com site identificado: {com_site} ({(com_site/total)*100:.1f}%)")
    
    return '\n'.join(relatorio)
