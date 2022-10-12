from decimal import Decimal
import requests
import xmltodict
import json
import pandas
import psycopg2

def busca_lista_deputados():
    response = requests.post("http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDeputados")
    return response.text

def busca_deputado_individual(ideCandidato, numLegislatura=56):
    response = requests.get("http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro={ide}&numLegislatura={numLegislatura}".format(ide=str(ideCandidato), numLegislatura=str(numLegislatura)))
    return response.text

def conecta_db():
  con = psycopg2.connect(host='localhost', 
                         database='postgres',
                         user='postgres', 
                         password='admin')
  return con

def salva_dados_no_bd(ideDeputado):
    dadosDeputado = xmltodict.parse(busca_deputado_individual(ideDeputado))
    df = pandas.DataFrame(dadosDeputado)
    conexao_db = conecta_db()
    cursor = conexao_db.cursor()
    sql = "INSERT INTO public.tabela_teste (texto_teste) VALUES();"
    try:
        cursor.execute(sql)
        conexao_db.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conexao_db.rollback()
        cursor.close()
        return 1
    cursor.close()
    
def salva_em_arquivo_texto(ideDeputado):
    dadosDeputado = xmltodict.parse(busca_deputado_individual(ideDeputado))
    with open('deputado_exemplo_BD_api_antiga.json', 'wb') as arquivo_saida:
        arquivo_saida.write(json.dumps(dadosDeputado).encode("utf-8"))
    return

def formata_dados_deputado(elemento):
    return elemento['id'], elemento['uri'], elemento['nome'].replace("'",""), elemento['siglaPartido'], elemento['uriPartido'], elemento['siglaUf'], elemento['idLegislatura'], elemento['urlFoto'], elemento['email']

def formata_dados_despesa(elemento):
    if('idDeputado' and 'urlDocumento' not in elemento):
        return elemento['nomeParlamentar'].replace("'",""), elemento['cpf'], elemento['numeroCarteiraParlamentar'], elemento['legislatura'], elemento['siglaUF'], elemento['siglaPartido'], elemento['codigoLegislatura'], elemento['numeroSubCota'], elemento['descricao'], elemento['numeroEspecificacaoSubCota'], elemento['descricaoEspecificacao'], elemento['fornecedor'], elemento['cnpjCPF'], elemento['numero'], elemento['tipoDocumento'], elemento['dataEmissao'], elemento['valorDocumento'], elemento['valorGlosa'], elemento['valorLiquido'], elemento['mes'], elemento['ano'], elemento['parcela'], elemento['passageiro'].replace("'",""), elemento['trecho'], elemento['lote'], elemento['ressarcimento'], elemento['restituicao'], elemento['numeroDeputadoID'], elemento['idDocumento'], "", ""
    if('idDeputado' not in elemento):
        return elemento['nomeParlamentar'].replace("'",""), elemento['cpf'], elemento['numeroCarteiraParlamentar'], elemento['legislatura'], elemento['siglaUF'], elemento['siglaPartido'], elemento['codigoLegislatura'], elemento['numeroSubCota'], elemento['descricao'], elemento['numeroEspecificacaoSubCota'], elemento['descricaoEspecificacao'], elemento['fornecedor'], elemento['cnpjCPF'], elemento['numero'], elemento['tipoDocumento'], elemento['dataEmissao'], elemento['valorDocumento'], elemento['valorGlosa'], elemento['valorLiquido'], elemento['mes'], elemento['ano'], elemento['parcela'], elemento['passageiro'].replace("'",""), elemento['trecho'], elemento['lote'], elemento['ressarcimento'], elemento['restituicao'], elemento['numeroDeputadoID'], elemento['idDocumento'], elemento['urlDocumento'], ""
    if('urlDocumento' not in elemento):
        return elemento['nomeParlamentar'].replace("'",""), elemento['cpf'], elemento['numeroCarteiraParlamentar'], elemento['legislatura'], elemento['siglaUF'], elemento['siglaPartido'], elemento['codigoLegislatura'], elemento['numeroSubCota'], elemento['descricao'], elemento['numeroEspecificacaoSubCota'], elemento['descricaoEspecificacao'], elemento['fornecedor'], elemento['cnpjCPF'], elemento['numero'], elemento['tipoDocumento'], elemento['dataEmissao'], elemento['valorDocumento'], elemento['valorGlosa'], elemento['valorLiquido'], elemento['mes'], elemento['ano'], elemento['parcela'], elemento['passageiro'].replace("'",""), elemento['trecho'], elemento['lote'], elemento['ressarcimento'], elemento['restituicao'], elemento['numeroDeputadoID'], elemento['idDocumento'], "", elemento['idDeputado']
    return elemento['nomeParlamentar'].replace("'",""), elemento['cpf'], elemento['numeroCarteiraParlamentar'], elemento['legislatura'], elemento['siglaUF'], elemento['siglaPartido'], elemento['codigoLegislatura'], elemento['numeroSubCota'], elemento['descricao'], elemento['numeroEspecificacaoSubCota'], elemento['descricaoEspecificacao'], elemento['fornecedor'], elemento['cnpjCPF'], elemento['numero'], elemento['tipoDocumento'], elemento['dataEmissao'], elemento['valorDocumento'], elemento['valorGlosa'], elemento['valorLiquido'], elemento['mes'], elemento['ano'], elemento['parcela'], elemento['passageiro'].replace("'",""), elemento['trecho'], elemento['lote'], elemento['ressarcimento'], elemento['restituicao'], elemento['numeroDeputadoID'], elemento['idDocumento'], elemento['urlDocumento'], elemento['idDeputado']

def busca_deputados_todas_legislaturas():
    id_legislatura = 1
    while (id_legislatura < 57):
        response = requests.get(f"https://dadosabertos.camara.leg.br/api/v2/deputados?idLegislatura={id_legislatura}&ordem=ASC&ordenarPor=nome")
        deputados_legislatura = json.loads(response.text)
        for deputado in deputados_legislatura['dados']:
            id_deputado, uri, nome, sigla_partido, uri_partido, sigla_uf, id_legislatura, url_foto, email = formata_dados_deputado(deputado)
            sql = f"INSERT INTO public.lista_deputados (id, uri, nome, sigla_partido, uri_partido, sigla_uf, id_legislatura, url_foto, email) VALUES('{id_deputado}', '{uri}', '{nome}', '{sigla_partido}', '{uri_partido}', '{sigla_uf}', '{id_legislatura}', '{url_foto}', '{email}');"
            conexao_db = conecta_db()
            cursor = conexao_db.cursor()
            try:
                cursor.execute(sql)
                conexao_db.commit()
            except(Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conexao_db.rollback()
                cursor.close()
        id_legislatura = id_legislatura + 1
    return 1
        

def salva_lista_deputados():
    with open("saida.json", "r") as arquivo:
        json_arquivo = json.load(arquivo)
        lista_deputados = json_arquivo['dados']
        conexao_db = conecta_db()
        cursor = conexao_db.cursor()
        for elemento in lista_deputados:
            id_deputado, uri, nome, sigla_partido, uri_partido, sigla_uf, id_legislatura, url_foto, email = formata_dados_deputado(elemento)
            sql = f"INSERT INTO public.lista_deputados (id, uri, nome, sigla_partido, uri_partido, sigla_uf, id_legislatura, url_foto, email) VALUES('{id_deputado}', '{uri}', '{nome}', '{sigla_partido}', '{uri_partido}', '{sigla_uf}', '{id_legislatura}', '{url_foto}', '{email}');"
            try:
                cursor.execute(sql)
                conexao_db.commit()
            except(Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conexao_db.rollback()
                cursor.close()
        return 1

def atualiza_dados_despesa(arquivo):
    with open(arquivo, "r", encoding="UTF-8") as arquivo_despesas:
        json_arquivo = json.load(arquivo_despesas)
        texto_arquivo = json.dumps(json_arquivo)
        texto_arquivo.replace("'","")
        json_arquivo = json.loads(texto_arquivo)
        dados_despesa = json_arquivo['dados']
        conexao_db = conecta_db()
        cursor = conexao_db.cursor()
        print("Iniciando atualização de DB")
        elementos_alterados = 0
        for elemento in dados_despesa:
            nome_parlamentar, cpf, numero_carteira_parlamentar, legislatura, sigla_uf, sigla_partido, codigo_legislatura, numero_sub_cota, descricao, numero_especificacao_sub_cota, descricao_especificacao, fornecedor, cnpj_cpf, numero, tipo_documento, data_emissao, valor_documento, valor_glosa, valor_liquido, mes, ano, parcela, passageiro, trecho, lote, ressarcimento, restituicao, numero_deputado_id, id_documento, url_documento, id_deputado = formata_dados_despesa(elemento)
            sql = f"INSERT INTO public.despesas (nome_parlamentar, cpf, numero_carteira_parlamentar, legislatura, sigla_uf, sigla_partido, codigo_legislatura, numero_sub_cota, descricao, numero_especificacao_sub_cota, descricao_especificacao, fornecedor, cnpj_cpf, numero, tipo_documento, data_emissao, valor_documento, valor_glosa, valor_liquido, mes, ano, parcela, passageiro, trecho, lote, ressarcimento, restituicao, numero_deputado_id, id_documento, url_documento, id_deputado) VALUES('{nome_parlamentar}', '{cpf}', '{numero_carteira_parlamentar}', '{legislatura}', '{sigla_uf}', '{sigla_partido}', '{codigo_legislatura}', '{numero_sub_cota}', '{descricao}', '{numero_especificacao_sub_cota}', '{descricao_especificacao}', '{fornecedor}', '{cnpj_cpf}', '{numero}', '{tipo_documento}', '{data_emissao}', '{valor_documento}', '{valor_glosa}', '{valor_liquido}', '{mes}', '{ano}', '{parcela}', '{passageiro}', '{trecho}', '{lote}', '{ressarcimento}', '{restituicao}', '{numero_deputado_id}', '{id_documento}', '{url_documento}', '{id_deputado}') ON CONFLICT DO NOTHING;"
            try:
                cursor.execute(sql)
                conexao_db.commit()
            except(Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conexao_db.rollback()
                cursor.close()
                return 1
            elementos_alterados = elementos_alterados + 1
            if (elementos_alterados % 1000 == 0): print(f"{elementos_alterados} elementos inseridos")
    return 0

def validar_campos_despesas(despesas_deputado):
    if (len(despesas_deputado) < 1 or despesas_deputado[0][0] != '2019'): despesas_deputado.insert(0, ('2019', Decimal(0.0)))
    if(len(despesas_deputado) < 2 or despesas_deputado[1][0] != '2020'): despesas_deputado.insert(1, ('2020', Decimal(0.0)))
    if(len(despesas_deputado) < 3 or despesas_deputado[2][0] != '2021'): despesas_deputado.insert(2, ('2021', Decimal(0.0)))
    if (len(despesas_deputado) < 4): despesas_deputado.append(('2022', Decimal(0.0)))
    return despesas_deputado

def alimenta_bd_despesas_deputados(legislatura):
    # seleciono os deputados da legislatura mostrada:
    conexao_db = conecta_db()
    cursor = conexao_db.cursor()
    sql_lista_deputados = f"select id from lista_deputados where id_legislatura = '{legislatura}';"
    cursor.execute(sql_lista_deputados)
    lista_deputados = cursor.fetchall()
    
    # com a lista de todos os deputados da legislatura, preciso pegar os valores e salvar na tabela
    for deputado in lista_deputados:
        sql_despesas_deputado = f"select ano, sum(valor_liquido) from despesas where id_deputado = \'{deputado[0]}\' group by id_deputado, ano;"
        cursor.execute(sql_despesas_deputado)
        despesas_deputado = cursor.fetchall() # essa linha devolve uma lista de tuplas ordenadas com o ano seguido pelo valor. ex:  ('2022', Decimal('198573.15')
        validar_campos_despesas(despesas_deputado)
        print(despesas_deputado)
        despesa_total = despesas_deputado[0][1] +  despesas_deputado[1][1] +  despesas_deputado[2][1] +  despesas_deputado[3][1] #soma os valores de despesas para salvar na tabela
        sql_atualizar_tabela = f"INSERT INTO public.despesas_deputados (id_deputado, legislatura, despesa_2019, despesa_2020, despesa_2021, despesa_2022, total_despesa) VALUES('{deputado[0]}', '{legislatura}', '{despesas_deputado[0][1]}', '{despesas_deputado[1][1]}', '{despesas_deputado[2][1]}', '{despesas_deputado[3][1]}', '{despesa_total}') on conflict (id_deputado, legislatura) do update set  despesa_2019 = '{despesas_deputado[0][1]}', despesa_2020 = '{despesas_deputado[1][1]}' , despesa_2021 = '{despesas_deputado[2][1]}', despesa_2022 = '{despesas_deputado[3][1]}', total_despesa = '{despesa_total}';"
        print(f"Dados atualizados para o deputado de id {deputado[0]} - Despesa total = {despesa_total}")
        cursor.execute(sql_atualizar_tabela)
        try:
            conexao_db.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conexao_db.rollback()
            cursor.close()
            return 1
    cursor.close()
    conexao_db.commit()
    return

def alimenta_tabela_proposicoes(arquivo):
    with open(arquivo, "r", encoding="UTF-8") as arquivo_proposicoes:
        json_arquivo_proposicoes = json.load(arquivo_proposicoes) #le o objeto json para um objeto do python
        dados_proposicoes = json_arquivo_proposicoes['dados'] #as informações importantes estão na seção dados do objeto
        for proposicao in dados_proposicoes: #os dados são uma lista de informações, então preciso trabalhar com cada uma das proposições que estão nesses dados
            ide_deputado = proposicao['ultimoStatus']['uriRelator']
            ide_deputado = ide_deputado.replace('https://dadosabertos.camara.leg.br/api/v2/deputados/', "")
            print(f"atualizando dados para o deputado {ide_deputado}")
            sql_alimentacao_proposicoes = f"INSERT INTO public.proposicoes (id, uri, sigla_tipo, numero, ano, cod_tipo, descricao_tipo, ementa, ementa_detalhada, keywords, data_apresentacao, uri_orgao_numerador, uri_prop_anterior, uri_prop_principal, uri_prop_posterior, url_inteiro_teor, data_ultimo_status, sequencia_ultimo_status, uri_relator_ultimo_status, cod_orgao_ultimo_status, sigla_orgao_ultimo_status, uri_orgao_ultimo_status, regime_ultimo_status, descricao_tramitacao_ultimo_status, id_tipo_tramitacao_ultimo_status, descricao_situacao_ultimo_status, id_situacao_ultimo_status, despacho_ultimo_status, url_ultimo_status, ide_deputado) VALUES({proposicao['id']}, '{proposicao['uri']}', '{proposicao['siglaTipo']}', {proposicao['numero']}, {proposicao['ano']}, {proposicao['codTipo']}, '{proposicao['descricaoTipo']}', '{proposicao['ementa']}', '{proposicao['ementaDetalhada']}', '{proposicao['keywords']}', '{proposicao['dataApresentacao']}', '{proposicao['uriOrgaoNumerador']}', '{proposicao['uriPropAnterior']}', '{proposicao['uriPropPrincipal']}', '{proposicao['uriPropPosterior']}', '{proposicao['urlInteiroTeor']}', '{proposicao['ultimoStatus']['data']}', '{proposicao['ultimoStatus']['sequencia']}', '{proposicao['ultimoStatus']['uriRelator']}', '{proposicao['ultimoStatus']['codOrgao']}', '{proposicao['ultimoStatus']['siglaOrgao']}', '{proposicao['ultimoStatus']['uriOrgao']}', '{proposicao['ultimoStatus']['regime']}', '{proposicao['ultimoStatus']['descricaoTramitacao']}', '{proposicao['ultimoStatus']['idTipoTramitacao']}', '{proposicao['ultimoStatus']['descricaoSituacao']}', '{proposicao['ultimoStatus']['idSituacao']}', '{proposicao['ultimoStatus']['despacho']}', '{proposicao['ultimoStatus']['url']}', '{ide_deputado}');"
            conexao_db = conecta_db()
            cursor = conexao_db.cursor()
            try:
                cursor.execute(sql_alimentacao_proposicoes)
                conexao_db.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conexao_db.rollback()
                cursor.close()
                return 1
        cursor.close()
        conexao_db.close()
        return
   
