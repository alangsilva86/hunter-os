"""
Hunter OS - Módulo de Fontes de Dados Reais
Integração com API Casa dos Dados + APIs públicas para enriquecimento
"""

import os
import time
import json
import logging
import sqlite3
import requests
import backoff
from typing import Optional, Dict, List, Any, Tuple, Callable
from datetime import datetime, timedelta
from lead_processing import limpar_digitos

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURAÇÕES DAS APIs
# ============================================================================

# API Key da Casa dos Dados (configurada via env ou Streamlit secrets)
DEFAULT_CACHE_TTL_HOURS = 24

API_CONFIG = {
    'casa_dos_dados': {
        'base_url': 'https://api.casadosdados.com.br/v5/cnpj/pesquisa',
        'rate_limit': 10,
        'timeout': 30
    },
    'cnpj_ws': {
        'base_url': 'https://publica.cnpj.ws/cnpj',
        'rate_limit': 3,
        'timeout': 15
    },
    'brasil_api': {
        'base_url': 'https://brasilapi.com.br/api/cnpj/v1',
        'rate_limit': 10,
        'timeout': 15
    }
}

# Mapeamento de setores para códigos CNAE
SETORES_CNAE = {
    "Serviços Administrativos": ["8211300", "8219999", "8220200", "8291100"],
    "Atividades Jurídicas e Contábeis": ["6910701", "6910702", "6920601", "6920602"],
    "Logística e Transporte": ["4930202", "4930201", "5211701", "5212500"],
    "Saúde e Clínicas": ["8630501", "8630502", "8630503", "8650001", "8650002"],
    "Construção e Incorporação": ["4110700", "4120400", "4121400", "4399103"],
    "Tecnologia e Software": ["6201501", "6201502", "6202300", "6203100"],
    "Comércio Varejista": ["4711301", "4711302", "4712100", "4713001"],
    "Alimentação e Restaurantes": ["5611201", "5611202", "5611203", "5612100"],
    "Educação": ["8511200", "8512100", "8513900", "8520100"],
    "Indústria": ["1011201", "1012101", "2211100", "2511000"]
}

# Lista de cidades disponíveis
CIDADES_DISPONIVEIS = [
    "MARINGA", "SARANDI", "MARIALVA", "PAICANDU", "MANDAGUARI",
    "LONDRINA", "CURITIBA", "CASCAVEL", "FOZ DO IGUACU", "PONTA GROSSA",
    "SAO PAULO", "RIO DE JANEIRO", "BELO HORIZONTE", "BRASILIA", "SALVADOR"
]

# ============================================================================
# CACHE DE DADOS
# ============================================================================

def _get_streamlit_secret(key: str) -> Optional[str]:
    try:
        import streamlit as st
        return st.secrets.get(key)
    except Exception:
        return None

def resolve_casa_dos_dados_api_key() -> Optional[str]:
    """Resolve API key via env ou Streamlit secrets"""
    return (
        os.getenv("CASA_DOS_DADOS_API_KEY")
        or os.getenv("HUNTER_OS_CASA_DOS_DADOS_API_KEY")
        or _get_streamlit_secret("CASA_DOS_DADOS_API_KEY")
        or _get_streamlit_secret("casa_dos_dados_api_key")
    )

class DataCache:
    """Gerenciador de cache para dados de empresas"""
    
    def __init__(self, db_path: str = "hunter_cache.db", ttl_hours: int = DEFAULT_CACHE_TTL_HOURS):
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self._init_db()
        self.purge_expired()
    
    def _init_db(self):
        """Inicializa o banco de dados"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS empresas (
                cnpj TEXT PRIMARY KEY,
                dados TEXT,
                fonte TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_cache (
                key TEXT PRIMARY KEY,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS buscas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parametros TEXT,
                total_encontrados INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_empresas_cnpj ON empresas(cnpj)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_api_cache_key ON api_cache(key)')
        
        conn.commit()
        conn.close()

    def purge_expired(self):
        """Remove itens expirados do cache"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM api_cache WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (datetime.now(),)
        )
        if self.ttl_hours:
            cursor.execute(
                "DELETE FROM empresas WHERE updated_at < datetime('now', ?)",
                (f"-{self.ttl_hours} hours",)
            )
        conn.commit()
        conn.close()
    
    def get_empresa(self, cnpj: str, max_age_hours: Optional[int] = None) -> Optional[Dict]:
        """Recupera empresa do cache respeitando TTL"""
        cnpj_limpo = limpar_digitos(cnpj)
        ttl = self.ttl_hours if max_age_hours is None else max_age_hours
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if ttl:
            cursor.execute(
                "SELECT dados FROM empresas WHERE cnpj = ? AND updated_at >= datetime('now', ?)",
                (cnpj_limpo, f"-{ttl} hours")
            )
        else:
            cursor.execute('SELECT dados FROM empresas WHERE cnpj = ?', (cnpj_limpo,))
        result = cursor.fetchone()
        conn.close()
        return json.loads(result[0]) if result else None
    
    def save_empresa(self, cnpj: str, dados: Dict, fonte: str = "api"):
        """Salva empresa no cache"""
        cnpj_limpo = limpar_digitos(cnpj)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO empresas (cnpj, dados, fonte, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (cnpj_limpo, json.dumps(dados, ensure_ascii=False), fonte, datetime.now()))
        conn.commit()
        conn.close()
    
    def save_empresas_batch(self, empresas: List[Dict], fonte: str = "api"):
        """Salva múltiplas empresas no cache"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for emp in empresas:
            cnpj = limpar_digitos(emp.get('cnpj', ''))
            if cnpj:
                cursor.execute('''
                    INSERT OR REPLACE INTO empresas (cnpj, dados, fonte, updated_at)
                    VALUES (?, ?, ?, ?)
                ''', (cnpj, json.dumps(emp, ensure_ascii=False), fonte, datetime.now()))
        conn.commit()
        conn.close()
    
    def get_all_empresas(self, limit: int = None, max_age_hours: Optional[int] = None) -> List[Dict]:
        """Recupera todas as empresas do cache respeitando TTL"""
        ttl = self.ttl_hours if max_age_hours is None else max_age_hours
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if ttl:
            if limit:
                cursor.execute(
                    "SELECT dados FROM empresas WHERE updated_at >= datetime('now', ?) ORDER BY updated_at DESC LIMIT ?",
                    (f"-{ttl} hours", limit)
                )
            else:
                cursor.execute(
                    "SELECT dados FROM empresas WHERE updated_at >= datetime('now', ?) ORDER BY updated_at DESC",
                    (f"-{ttl} hours",)
                )
        else:
            if limit:
                cursor.execute('SELECT dados FROM empresas ORDER BY updated_at DESC LIMIT ?', (limit,))
            else:
                cursor.execute('SELECT dados FROM empresas ORDER BY updated_at DESC')
        
        results = cursor.fetchall()
        conn.close()
        return [json.loads(r[0]) for r in results]
    
    def count_empresas(self) -> int:
        """Conta total de empresas no cache"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if self.ttl_hours:
            cursor.execute(
                "SELECT COUNT(*) FROM empresas WHERE updated_at >= datetime('now', ?)",
                (f"-{self.ttl_hours} hours",)
            )
        else:
            cursor.execute('SELECT COUNT(*) FROM empresas')
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def save_busca(self, parametros: Dict, total: int):
        """Salva registro de busca"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO buscas (parametros, total_encontrados)
            VALUES (?, ?)
        ''', (json.dumps(parametros, ensure_ascii=False), total))
        conn.commit()
        conn.close()

    def get_cache(self, key: str) -> Optional[Dict]:
        """Recupera dados do cache genérico"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT data FROM api_cache WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            (key, datetime.now())
        )
        result = cursor.fetchone()
        conn.close()
        return json.loads(result[0]) if result else None

    def set_cache(self, key: str, data: Dict, ttl_hours: int = DEFAULT_CACHE_TTL_HOURS):
        """Salva dados no cache genérico"""
        expires_at = datetime.now() + timedelta(hours=ttl_hours) if ttl_hours else None
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO api_cache (key, data, expires_at) VALUES (?, ?, ?)",
            (key, json.dumps(data, ensure_ascii=False), expires_at)
        )
        conn.commit()
        conn.close()

# ============================================================================
# CLIENTE DA API CASA DOS DADOS
# ============================================================================

class CasaDosDadosClient:
    """Cliente para API da Casa dos Dados"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or resolve_casa_dos_dados_api_key()
        self.base_url = API_CONFIG['casa_dos_dados']['base_url']
        self.session = requests.Session()
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'HunterOS/1.0'
        }
        if self.api_key:
            headers['api-key'] = self.api_key
        self.session.headers.update(headers)
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Aplica rate limiting"""
        min_interval = 60 / API_CONFIG['casa_dos_dados']['rate_limit']
        elapsed = time.time() - self.last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()
    
    class RetryableAPIError(Exception):
        """Erro que aciona retry/backoff"""
        pass

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, RetryableAPIError),
        max_tries=3,
        jitter=backoff.full_jitter
    )
    def _post_pesquisa(self, payload: Dict, tipo_resultado: str):
        self._rate_limit()
        response = self.session.post(
            f"{self.base_url}?tipo_resultado={tipo_resultado}",
            json=payload,
            timeout=API_CONFIG['casa_dos_dados']['timeout']
        )
        if response.status_code in {429, 500, 502, 503, 504}:
            raise self.RetryableAPIError(f"HTTP {response.status_code}")
        return response

    def pesquisar_empresas(
        self,
        cnpjs: List[str] = None,
        municipios: List[str] = None,
        uf: str = "PR",
        cnaes: List[str] = None,
        excluir_mei: bool = True,
        com_telefone: bool = False,
        com_email: bool = False,
        situacao: str = "ATIVA",
        limite: int = 100,
        pagina: int = 1,
        tipo_resultado: str = "completo"
    ) -> Tuple[List[Dict], int, str]:
        """
        Pesquisa empresas na API Casa dos Dados
        
        Returns:
            Tuple[List[Dict], int, str]: (empresas, total, mensagem_erro)
        """
        if not self.api_key:
            return [], 0, "API key não configurada. Defina CASA_DOS_DADOS_API_KEY em env ou st.secrets."
        
        # Monta o payload
        payload = {
            "situacao_cadastral": [situacao],
            "limite": min(limite, 1000),
            "pagina": pagina
        }

        # Filtro por CNPJs específicos
        if cnpjs:
            payload["cnpj"] = [limpar_digitos(c) for c in cnpjs if limpar_digitos(c)]
        
        # Adiciona UF
        if uf:
            payload["uf"] = [uf.lower()]
        
        # Adiciona municípios
        if municipios:
            payload["municipio"] = [m.lower().replace("_", " ") for m in municipios]
        
        # Adiciona CNAEs
        if cnaes:
            payload["codigo_atividade_principal"] = cnaes
            payload["incluir_atividade_secundaria"] = True
        
        # Filtro MEI
        if excluir_mei:
            payload["mei"] = {"excluir_optante": True}
        
        # Filtros adicionais
        if com_telefone or com_email:
            payload["mais_filtros"] = {}
            if com_telefone:
                payload["mais_filtros"]["com_telefone"] = True
            if com_email:
                payload["mais_filtros"]["com_email"] = True
        
        try:
            logger.info(f"Buscando empresas: {json.dumps(payload, ensure_ascii=False)[:200]}...")
            
            response = self._post_pesquisa(payload, tipo_resultado)
            
            if response.status_code == 200:
                data = response.json()
                total = data.get('total', 0)
                empresas = data.get('cnpjs', [])
                
                # Normaliza os dados
                empresas_normalizadas = [self._normalizar_empresa(e) for e in empresas]
                
                logger.info(f"Encontradas {len(empresas_normalizadas)} empresas (total: {total})")
                return empresas_normalizadas, total, None
            
            if response.status_code == 401:
                return [], 0, "API Key inválida ou expirada"
            if response.status_code == 403:
                return [], 0, "Acesso negado - verifique sua API Key"
            if response.status_code == 429:
                return [], 0, "Limite de requisições excedido - aguarde alguns minutos"
            return [], 0, f"Erro na API: {response.status_code} - {response.text[:200]}"
                
        except self.RetryableAPIError as e:
            return [], 0, f"Erro temporário na API: {str(e)}"
        except requests.exceptions.Timeout:
            return [], 0, "Timeout na requisição - tente novamente"
        except requests.exceptions.RequestException as e:
            return [], 0, f"Erro de conexão: {str(e)}"
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            return [], 0, f"Erro inesperado: {str(e)}"
    
    def _normalizar_empresa(self, data: Dict) -> Dict:
        """Normaliza dados da empresa para formato padrão"""
        endereco = data.get('endereco', {})
        situacao = data.get('situacao_cadastral', {})
        porte = data.get('porte_empresa', {})
        atividade = data.get('atividade_principal', {})
        
        # Monta telefone (campo correto: contato_telefonico)
        telefone = ""
        telefones = data.get('contato_telefonico', [])
        if telefones and len(telefones) > 0:
            tel = telefones[0]
            ddd = tel.get('ddd', '')
            numero = tel.get('numero', '')
            telefone = f"{ddd}{numero}"
        
        # Monta email (campo correto: contato_email)
        email = ""
        emails = data.get('contato_email', [])
        if emails and len(emails) > 0:
            email = emails[0].get('email', '')
        
        return {
            'cnpj': data.get('cnpj', ''),
            'cnpj_raiz': data.get('cnpj_raiz', ''),
            'razao_social': data.get('razao_social', ''),
            'nome_fantasia': data.get('nome_fantasia', '') or data.get('razao_social', ''),
            'cnae_fiscal': atividade.get('codigo', '') if isinstance(atividade, dict) else '',
            'cnae_fiscal_descricao': atividade.get('descricao', '') if isinstance(atividade, dict) else '',
            'ddd_telefone_1': telefone,
            'telefones': telefones,
            'email': email,
            'emails': emails,
            'logradouro': endereco.get('logradouro', ''),
            'numero': endereco.get('numero', ''),
            'complemento': endereco.get('complemento', ''),
            'bairro': endereco.get('bairro', ''),
            'municipio': endereco.get('municipio', ''),
            'uf': endereco.get('uf', ''),
            'cep': endereco.get('cep', ''),
            'porte': porte.get('descricao', '') if isinstance(porte, dict) else data.get('porte', ''),
            'natureza_juridica': data.get('descricao_natureza_juridica', ''),
            'capital_social': data.get('capital_social', 0),
            'data_inicio_atividade': data.get('data_abertura', ''),
            'situacao_cadastral': situacao.get('descricao', '') if isinstance(situacao, dict) else 'ATIVA',
            'matriz_filial': data.get('matriz_filial', ''),
            'quadro_societario': data.get('quadro_societario', []),
            'fonte': 'casa_dos_dados'
        }
    
    def consultar_cnpj(self, cnpj: str) -> Optional[Dict]:
        """Consulta um CNPJ específico"""
        cnpj_limpo = limpar_digitos(cnpj)
        if not cnpj_limpo:
            return None
        empresas, total, erro = self.pesquisar_empresas(
            cnpjs=[cnpj_limpo],
            limite=1,
            tipo_resultado="completo"
        )
        if erro or not empresas:
            return None
        return empresas[0]

# ============================================================================
# BUSCADOR PRINCIPAL
# ============================================================================

class HunterSearcher:
    """Buscador principal de empresas"""
    
    def __init__(self):
        self.cache = DataCache()
        self.casa_dos_dados = CasaDosDadosClient()
    
    def buscar_empresas(
        self,
        cidades: List[str] = None,
        setores: List[str] = None,
        uf: str = "PR",
        excluir_mei: bool = True,
        com_telefone: bool = False,
        com_email: bool = False,
        limite: int = 100,
        callback_progresso: Callable = None
    ) -> Tuple[List[Dict], Dict]:
        """
        Busca empresas com os filtros especificados
        
        Args:
            cidades: Lista de cidades
            setores: Lista de setores (serão convertidos para CNAEs)
            uf: Estado (default: PR)
            excluir_mei: Excluir MEIs
            com_telefone: Apenas com telefone
            com_email: Apenas com email
            limite: Quantidade máxima de resultados
            callback_progresso: Função de callback para progresso
            
        Returns:
            Tuple[List[Dict], Dict]: (empresas, estatísticas)
        """
        stats = {
            'total_na_base': 0,
            'total_filtrados': 0,
            'total_retornados': 0,
            'paginas_processadas': 0,
            'erros': [],
            'fonte': 'Casa dos Dados API',
            'cidades': ', '.join(cidades) if cidades else 'Todas',
            'setores': ', '.join(setores) if setores else 'Todos',
            'uf': uf,
            'excluir_mei': excluir_mei,
            'com_telefone': com_telefone,
            'com_email': com_email
        }
        
        # Converte setores para CNAEs
        cnaes = []
        if setores:
            for setor in setores:
                if setor in SETORES_CNAE:
                    cnaes.extend([limpar_digitos(c) for c in SETORES_CNAE[setor]])
        
        todas_empresas = []
        pagina = 1
        total_encontrado = 0
        empresas_por_pagina = min(limite, 100)  # API permite até 1000, mas usamos 100 por página
        
        while len(todas_empresas) < limite:
            if callback_progresso:
                callback_progresso(
                    fase="buscando",
                    pagina=pagina,
                    encontrados=len(todas_empresas),
                    meta=limite,
                    mensagem=f"Buscando página {pagina}..."
                )
            
            empresas, total, erro = self.casa_dos_dados.pesquisar_empresas(
                municipios=cidades,
                uf=uf,
                cnaes=cnaes if cnaes else None,
                excluir_mei=excluir_mei,
                com_telefone=com_telefone,
                com_email=com_email,
                limite=empresas_por_pagina,
                pagina=pagina
            )
            
            if erro:
                stats['erros'].append(erro)
                logger.error(f"Erro na busca: {erro}")
                break
            
            if pagina == 1:
                total_encontrado = total
                stats['total_na_base'] = total
            
            if not empresas:
                break
            
            todas_empresas.extend(empresas)
            stats['paginas_processadas'] = pagina
            
            if callback_progresso:
                callback_progresso(
                    fase="processando",
                    pagina=pagina,
                    encontrados=len(todas_empresas),
                    meta=limite,
                    total_base=total_encontrado,
                    mensagem=f"Processados {len(todas_empresas)} de {total_encontrado} empresas"
                )
            
            # Verifica se há mais páginas
            if len(empresas) < empresas_por_pagina or len(todas_empresas) >= total_encontrado:
                break
            
            pagina += 1
            
            # Pequena pausa entre páginas
            time.sleep(0.5)
        
        # Limita ao número solicitado
        empresas_final = todas_empresas[:limite]
        stats['total_filtrados'] = len(todas_empresas)
        stats['total_retornados'] = len(empresas_final)
        
        # Salva no cache
        if empresas_final:
            self.cache.save_empresas_batch(empresas_final, 'casa_dos_dados')
            self.cache.save_busca({
                'cidades': cidades,
                'setores': setores,
                'limite': limite
            }, len(empresas_final))
        
        if callback_progresso:
            callback_progresso(
                fase="concluido",
                pagina=pagina,
                encontrados=len(empresas_final),
                meta=limite,
                total_base=total_encontrado,
                mensagem=f"Busca concluída! {len(empresas_final)} empresas encontradas"
            )
        
        return empresas_final, stats
    
    def get_cache_count(self) -> int:
        """Retorna quantidade de empresas no cache"""
        return self.cache.count_empresas()
    
    def get_empresas_cache(self, limite: int = None) -> List[Dict]:
        """Retorna empresas do cache"""
        return self.cache.get_all_empresas(limite)

# ============================================================================
# FUNÇÕES DE CONVENIÊNCIA
# ============================================================================

def criar_searcher() -> HunterSearcher:
    """Cria instância do buscador"""
    return HunterSearcher()

def buscar_empresas(
    cidades: List[str] = None,
    setores: List[str] = None,
    limite: int = 100,
    callback_progresso: Callable = None
) -> Tuple[List[Dict], Dict]:
    """Função de conveniência para buscar empresas"""
    searcher = criar_searcher()
    return searcher.buscar_empresas(
        cidades=cidades,
        setores=setores,
        limite=limite,
        callback_progresso=callback_progresso
    )

def get_setores_disponiveis() -> List[str]:
    """Retorna lista de setores disponíveis"""
    return list(SETORES_CNAE.keys())

def get_cidades_disponiveis() -> List[str]:
    """Retorna lista de cidades disponíveis"""
    return CIDADES_DISPONIVEIS

# ============================================================================
# TESTE
# ============================================================================

if __name__ == "__main__":
    print("🔍 Testando integração com Casa dos Dados...")
    
    searcher = criar_searcher()
    
    def callback(fase, pagina, encontrados, meta, mensagem, **kwargs):
        print(f"  [{fase}] Página {pagina}: {encontrados}/{meta} - {mensagem}")
    
    empresas, stats = searcher.buscar_empresas(
        cidades=["MARINGA"],
        setores=["Serviços Administrativos"],
        limite=5,
        callback_progresso=callback
    )
    
    print(f"\n📊 Estatísticas:")
    print(f"   Total na base: {stats['total_na_base']}")
    print(f"   Total retornados: {stats['total_retornados']}")
    print(f"   Erros: {stats['erros']}")
    
    if empresas:
        print(f"\n📋 Empresas encontradas:")
        for emp in empresas[:3]:
            print(f"   - {emp.get('razao_social', 'N/A')[:50]}")
            print(f"     CNPJ: {emp.get('cnpj')}")
            print(f"     Cidade: {emp.get('municipio')}")
            print(f"     Telefone: {emp.get('ddd_telefone_1')}")
    
    print("\n✅ Teste concluído!")
