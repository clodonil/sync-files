#!/usr/bin/python
""" Servidor
#
# Recebe os arquivos e confirma
# VERSAO: 03/10/12
"""

import socket
import cx_Oracle
import md5
import sys
import datetime
import os
from OpenSSL import SSL



class SyncServer:
    s=0
    conn=0
    params={}
    def __init__(self):
        try:
          # carregando certificado ssl
   	  context = SSL.Context(SSL.SSLv23_METHOD)
	  context.use_privatekey_file('keys/key')
	  context.use_certificate_file('keys/cert')
        except:
	  self.log(1,"Arquivos do certificado digital nao encontrado/n")
	  print("Arquivos do certificado digital nao encontrado")
	  sys.exit(1)

        try:
           fconfig = open("config")
        except:
           self.log(1,"Nao foi possivel abrir o arquivo de configuracao/n")
           print("Nao foi possivel abrir o arquivo de configuracao")
           sys.exit(1)


	for linha in fconfig.readlines():
		   (index,valor) = linha.split("=")
		   valor = valor.replace('\n','')
		   valor = valor.replace(' ','')
		   SyncServer.params[index]=valor


        try:
          # Iniciando o servidor
          SyncServer.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          # Carregando o ssl
	  SyncServer.s = SSL.Connection(context, SyncServer.s)
    	  SyncServer.s.bind((SyncServer.params['HOST'], int(SyncServer.params['PORT'])))
	  SyncServer.s.listen(5)
        except:
           self.log(1,"Impossivel iniciar o servidor/n")
           print("Impossivel iniciar o servidor")
           sys.exit(1)

    def log(self, id,str):
	if not os.path.exists(SyncServer.params['LOG_PATH']):
	   os.makedirs(SyncServer.params['LOG_PATH'])
	path_log=""

        if id == 1:
           path_log=("%s/sync-error.log") %(SyncServer.params['LOG_PATH'])
	elif id == 2:		 
	   path_log=("%s/sync.log") %(SyncServer.params['LOG_PATH'])

	try:
	   log = open(path_log,"a")
	except:
           print("Impossivel criar arquivo de log em %s") %(SyncClient.params['LOG_PATH'])
           sys.exit(1)
	now = datetime.datetime.now()
	text="[%s]: %s" %(now,str)
	log.write(text)
	log.close
              
    def dbconnect(self, id_sql,opcao=1,opcao2=1, opcao3=1,opcao4=1, opcao5=1,opcao6=1):
        result=True
  	tns="%s/%s@%s/orcl" %(SyncServer.params['DB_USER'],SyncServer.params['DB_PASS'],SyncServer.params['DB_HOST'])
        while True:
           try:
             db     = cx_Oracle.connect(tns)
             break
           except:
             self.log(1,"Impossivel conectar no banco de dados/n")
	cursor = db.cursor()           
        if  id_sql == 1:             
             # status = 0 arquivos recebidos mas nao confirmados
             sql="SELECT * from pasta_imagem where id_status = 2005 and ip_client = '%s'" %(opcao)
             cursor.execute(sql)
             result = cursor.fetchall()              
        elif id_sql == 2:
             sql="INSERT INTO pasta_imagem(id_imagem, nome_arquivo,path_interna, cod_md5,ip_client,id_status) VALUES ('%d' , '%s', '%s', '%s','%s','0')" %(int(id_file),filename,path,md5,ip)
             cursor.execute(sql)
             db.commit()             
	elif id_sql == 3:
             # Enviado o confirmacao do recebimento do arquivo
	     
             sql="UPDATE pasta_imagem SET id_status = 2005, path = '%s' where id_imagem_cliente = '%d' and ip_client = '%s'" % (opcao3,int(opcao),opcao2)
             cursor.execute(sql)
             db.commit()
	elif id_sql == 4: 
             #Pesquisando se o registro ja existe
	     sql="SELECT id_pasta FROM pasta WHERE id_pasta_cliente = %d AND id_pessoa = %d" %(int(opcao[1]), int(opcao[3]))
             cursor.execute(sql)
             result = cursor.fetchall()                      
	     
             if len(result) == 0:
                  # Fazendo o update da tabela pasta
	         sql="INSERT INTO projeto.pasta (id_tipo_item,id_pessoa,id_depto,id_caixa,n_cliente,dt_cadastro,dt_expurgo,id_status,id_user_implantacao,tipo_implantacao,id_caixa_anterior,conteudo,id_pasta_cliente) VALUES ('%d','%d','%d',NULL,'%s',SYSDATE,to_date('%s','dd/mm/yyyy hh24:mi:ss'),'%d','%d','%d',NULL,'%s','%d')" %(int(opcao[2]),int(opcao[3]),int(opcao[4]),opcao[6],opcao[7],int(opcao[8]),int(opcao[9]),int(opcao[10]),opcao[12],int(opcao[1]))           

                 cursor.execute(sql)
                 db.commit()
	         sql="SELECT id_pasta FROM pasta WHERE id_pasta_cliente = %d AND id_pessoa = %d" %(int(opcao[1]), int(opcao[3]))
                 cursor.execute(sql)
                 result = cursor.fetchall()
             else:
		 print("ID ja cadastrado")                           
	elif id_sql == 5:
             # Fazendo o update da tabela pasta
	     sql="SELECT id_coluna FROM pasta_coluna_extra WHERE id_pessoa = '%d' AND id_coluna = '%d' AND id_pasta = '%d'" %(int(opcao[1]),int(opcao[2]),int(opcao2))
             cursor.execute(sql)
             result = cursor.fetchall()          
	     
             if len(result) == 0:                
                sql="INSERT INTO projeto.pasta_coluna_extra (id_pessoa,id_coluna,id_pasta,conteudo) VALUES ('%d','%d','%d','%s')" %(int(opcao[1]),int(opcao[2]),int(opcao2),opcao[4])
	        # teste tirar esses comentarios
                cursor.execute(sql)
                db.commit()
	elif id_sql == 6:
             # Fazendo o update da tabela pasta
	     path="%s/%s" %(SyncServer.params['PATH_ARQ'],opcao[17])
 	     sql="INSERT INTO projeto.pasta_imagem(id_pasta,path,id_tipo_imagem,tamanho_imagem,path_interna,id_solicitacao,tipo_extensao,num_paginas_pdf,controlador,dt_controlador,id_user_controlador,visualizacao,id_user_digtalizacao,dt_alteracao,id_status,cod_md5,ip_client,nome_arquivo,id_imagem_cliente) VALUES ('%d','%s','%d','%d','%s',NULL,'%s','%d',NULL,NULL,NULL,'%d','%d',NULL,'%d','%s','%s','%s','%d')" %(opcao2,path,int(opcao[4]),int(opcao[5]),opcao[6],opcao[8],int(opcao[9]),int(opcao[13]),int(opcao[14]),int(opcao[15]),opcao[16],opcao3,opcao[17],int(opcao[1]))
             cursor.execute(sql)
             db.commit()
        elif id_sql == 7:
             # Enviado o confirmacao do recebimento do arquivo
             sql="UPDATE pasta_imagem SET id_status = 2006 where id_imagem_cliente = '%d' and ip_client = '%s'" % (int(opcao),opcao2)
             cursor.execute(sql)
             db.commit()
        elif id_sql == 7:
             sql="SELECT id_pasta FROM pasta_imagem WHERE id_imagem_cliente = '%d' AND id_pasta = '%d' AND ip_cliente = '%s'" %(int(opcao[1]),int(opcao[2]),int(opcao2))
	     curso.execute(sql)
	     dados = cursor.fetchall()
             result = True

        db.close()
        return result


    def verifyFile(self, ip):
        # Lista dos arquivos que nao foram enviados	  
        linha="" 
        linhas=[]
        result=self.dbconnect(1,ip)
        if len(result) > 0:
            for registro in result:
                if len(linha) > 1000:
                    linhas.append(linha)
                    linha=""
                linha="%s:%s" %(registro[21],linha)   
            linhas.append(linha)
            return linhas
        else:	
            return False 

    def receiveFile(self, filename):
        newfile = open(filename,"wb")
        while 1:
            line = self.network_read()
            if line == '*#FIM#*': break
            newfile.write(line)
        newfile.close()


    def confirmReceive(self,ip):
        # Tem que quebrar em linhas de 1024 caracteres
        arqs=self.verifyFile(ip)
        if arqs != False: 
           for arq in arqs:          
  	       self.network_send(arq)
	       self.log(2,"Confirmando o recebimento do arquivo s")
           self.network_send("*#FIM#*")

           for arq in arqs:
	       for id in arq.split(":"):
                   if len(id) > 0:
	              self.dbconnect(7,id,ip)
        else:
	    self.network_send("*#FIM#*")

    def sumfile(self, fobj):    
        m = md5.new()
        while True:
          d = fobj.read(8096)
          if not d:break
          m.update(d)
        return m.hexdigest()


    def md5sum(self, fname):    
       if fname == '-':
          ret = sumfile(sys.stdin)
       else:
         try:
             f = file(fname, 'rb')
         except:
             return 'Failed to open file'
       ret = self.sumfile(f)
       f.close()
       return ret

    def implantacao(self,ip):
          pasta         = self.network_read()
	  pasta_imagem  = self.network_read()
	  pasta_coluna_extra = []


	  pasta        =pasta.replace("'", "''") 
	  pasta_imagem = pasta_imagem.replace("'", "''")
	  


          while 1:
            line = self.network_read()
            if line == '*#FIM#*': break
            pasta_coluna_extra.append(line)
       

          # fazendo o insert da tabela pasta e retornando o ID
          mpasta = pasta.split('*:*')
          self.log(2,"Implantando pasta: d")
	  id_pasta = self.dbconnect(4,mpasta)
	  id_pasta=list(id_pasta[0])[0]             

	  # fazendo o insert da tabela pasta_coluna_extra
	  for item in pasta_coluna_extra:
            	item=item.replace("'", "''")
		mpasta_coluna_extra = item.split('*:*')		
		self.dbconnect(5,mpasta_coluna_extra,id_pasta)
	  
	  # Fazendo a insert da tabela pasta_imagem
	  mpasta_imagem= pasta_imagem.split('*:*')
	  # Fazer a validacao para nao incluir 2 imagens
          # id = dbconnect(7,mpasta_imagem,id_pasta,id)
          # if id == True
	  self.dbconnect(6,mpasta_imagem,id_pasta,ip)		

	  self.network_send("OK")

    def network_send(self, string):
       try:
          SyncServer.conn.send(string)
	  return True
       except:                
          self.log(1,"Erro no envio: %s\n" %(string))
	  return False

    def network_read(self,local="Sync"):
        try:
           result=SyncServer.conn.recv(1024)
           return result
        except:
           self.log(1,"(%s)Erro no recebimento de dados do servidor \n") %(local)
           return False

    def run(self):
         while 1:
	   SyncServer.conn, addr = SyncServer.s.accept()
           pack = self.network_read()
           acao,data = pack.split("*")
           if acao == "0":
	      if data != "OFF":
                 id_file,filename_tmp,md5 = data.split(":")
                 self.network_send('1')
                 filename="%s/%s" % (SyncServer.params['PATH_ARQ'],filename_tmp)
	         self.receiveFile(filename)
                 md5_new = self.md5sum(filename)

                 if md5 == md5_new.upper():              
        	    #self.dbconnect(2,"0",id_file,filename_tmp, md5_new,filename)
        	    self.dbconnect(3,id_file,addr[0],filename)
                    self.log(2,"Arquivo: s recebido com sucesso")
	         else:
                    self.log(1,"Problema no MD5 do arquivo %s (rede) ----%s (local)" %(filename,md5,md5_new.upper()))

              SyncServer.conn.close()
           elif acao == "1":
                self.confirmReceive(addr[0])		
                SyncServer.conn.close()                
           elif acao == "2":
 		# implantacao de dados
                self.implantacao(addr[0])
             
novo = SyncServer()
novo.run()
