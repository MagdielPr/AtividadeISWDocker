import pika
import json
import psycopg2
import redis
from datetime import datetime
import logging
import threading
import time
import signal
import sys
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.pg_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'appdb',
            'user': 'user',
            'password': 'password'
        }

        self.redis_config = {
            'host': 'redis',
            'port': 6379,
            'db': 0,
            'decode_responses': True
        }

        self.pg_connection = None
        self.redis_client = None
        self.pg_status = False
        self.redis_status = False
        self.lock = threading.Lock()

    def test_postgresql_connection(self):
        """Testa a conexão com PostgreSQL sem manter a conexão ativa"""
        try:
            test_conn = psycopg2.connect(**self.pg_config)
            test_conn.close()
            return True
        except Exception as e:
            logger.debug(f"PostgreSQL offline: {e}")
            return False

    def test_redis_connection(self):
        """Testa a conexão com Redis sem manter a conexão ativa"""
        try:
            test_client = redis.Redis(**self.redis_config, socket_connect_timeout=2)
            test_client.ping()
            test_client.close()
            return True
        except Exception as e:
            logger.debug(f"Redis offline: {e}")
            return False

    def check_databases_status(self):
        """Verifica o status de ambos os bancos de dados"""
        with self.lock:
            pg_status = self.test_postgresql_connection()
            redis_status = self.test_redis_connection()
            
            # Log apenas quando há mudança de status
            if pg_status != self.pg_status:
                if pg_status:
                    logger.info("PostgreSQL voltou a ficar online")
                else:
                    logger.warning("PostgreSQL ficou offline")
                self.pg_status = pg_status
                
            if redis_status != self.redis_status:
                if redis_status:
                    logger.info("Redis voltou a ficar online")
                else:
                    logger.warning("Redis ficou offline")
                self.redis_status = redis_status
                
            return pg_status and redis_status

    def connect_postgresql(self):
        """Conecta ao PostgreSQL com retry automático"""
        try:
            if self.pg_connection and not self.pg_connection.closed:
                return True
                
            self.pg_connection = psycopg2.connect(**self.pg_config)
            self.pg_connection.set_session(autocommit=False)
            logger.info("Conectado ao PostgreSQL com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar PostgreSQL: {e}")
            self.pg_connection = None
            return False

    def connect_redis(self):
        """Conecta ao Redis com retry automático"""
        try:
            if self.redis_client:
                try:
                    self.redis_client.ping()
                    return True
                except:
                    pass
                    
            self.redis_client = redis.Redis(**self.redis_config)
            self.redis_client.ping()
            logger.info("Conectado ao Redis com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar Redis: {e}")
            self.redis_client = None
            return False

    @contextmanager
    def get_pg_cursor(self):
        """Context manager para cursor PostgreSQL com tratamento de erro"""
        cursor = None
        try:
            if not self.pg_connection or self.pg_connection.closed:
                if not self.connect_postgresql():
                    raise Exception("Não foi possível conectar ao PostgreSQL")
            
            cursor = self.pg_connection.cursor()
            yield cursor
            self.pg_connection.commit()
        except Exception as e:
            if self.pg_connection:
                self.pg_connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()

    def insert_postgresql(self, dados):
        """Insere dados no PostgreSQL baseado no formulário HTML"""
        try:
            with self.get_pg_cursor() as cursor:
                # Mapear campos do formulário para a tabela
                nome = dados.get('nome', '')
                email = dados.get('email', '')
                telefone = dados.get('telefone', '')
                idade = int(dados.get('idade', 0))
                experiencia = dados.get('experiencia', '')
                criptomoeda = dados.get('criptomoeda', '')
                valor_investimento = float(dados.get('valor_investimento', 0))
                prazo_investimento = dados.get('prazo_investimento', '')
                perfil_risco = dados.get('perfil_risco', '')
                observacoes = dados.get('observacoes', '')

                # Criar tabela se não existir
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS investimentos_cripto (
                        id SERIAL PRIMARY KEY,
                        nome VARCHAR(255) NOT NULL,
                        email VARCHAR(255) NOT NULL,
                        telefone VARCHAR(50),
                        idade INTEGER,
                        experiencia VARCHAR(50),
                        criptomoeda VARCHAR(50),
                        valor_investimento DECIMAL(15,2),
                        prazo_investimento VARCHAR(50),
                        perfil_risco VARCHAR(50),
                        observacoes TEXT,
                        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                cursor.execute(create_table_query)

                # Inserir dados
                insert_query = """
                    INSERT INTO investimentos_cripto 
                    (nome, email, telefone, idade, experiencia, criptomoeda, 
                     valor_investimento, prazo_investimento, perfil_risco, observacoes) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    nome, email, telefone, idade, experiencia, criptomoeda,
                    valor_investimento, prazo_investimento, perfil_risco, observacoes
                ))

                logger.info(f"Dados inseridos no PostgreSQL: {nome}, {email}, {criptomoeda}")
                return True

        except Exception as e:
            logger.error(f"Erro ao inserir no PostgreSQL: {e}")
            return False

    def insert_redis(self, dados):
        """Insere dados no Redis baseado no formulário HTML"""
        try:
            if not self.redis_client:
                if not self.connect_redis():
                    return False

            # Criar chave única usando timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            key = f"investimento:{timestamp}"
            
            # Preparar dados completos para Redis
            redis_data = {
                'nome': dados.get('nome', ''),
                'email': dados.get('email', ''),
                'telefone': dados.get('telefone', ''),
                'idade': str(dados.get('idade', 0)),
                'experiencia': dados.get('experiencia', ''),
                'criptomoeda': dados.get('criptomoeda', ''),
                'valor_investimento': str(dados.get('valor_investimento', 0)),
                'prazo_investimento': dados.get('prazo_investimento', ''),
                'perfil_risco': dados.get('perfil_risco', ''),
                'observacoes': dados.get('observacoes', ''),
                'data_criacao': datetime.now().isoformat()
            }

            # Armazenar como hash no Redis
            self.redis_client.hset(key, mapping=redis_data)
            
            # Adicionar à lista de investimentos para facilitar busca
            self.redis_client.lpush("investimentos_list", key)
            
            # Definir TTL (30 dias)
            self.redis_client.expire(key, 2592000)

            logger.info(f"Dados inseridos no Redis com chave: {key}")
            return True

        except Exception as e:
            logger.error(f"Erro ao inserir no Redis: {e}")
            return False

    def close_connections(self):
        """Fecha todas as conexões de banco"""
        with self.lock:
            if self.pg_connection and not self.pg_connection.closed:
                self.pg_connection.close()
                logger.info("Conexão PostgreSQL fechada")

            if self.redis_client:
                try:
                    self.redis_client.close()
                    logger.info("Conexão Redis fechada")
                except:
                    pass


class DatabaseHealthChecker:
    def __init__(self, db_manager, rabbitmq_consumer):
        self.db_manager = db_manager
        self.rabbitmq_consumer = rabbitmq_consumer
        self.running = False
        self.thread = None
        self.databases_online = True

    def start_monitoring(self):
        """Inicia o monitoramento dos bancos de dados"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logger.info("Monitoramento de bancos de dados iniciado")

    def stop_monitoring(self):
        """Para o monitoramento dos bancos de dados"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        logger.info("Monitoramento de bancos de dados parado")

    def _monitor_loop(self):
        """Loop principal de monitoramento"""
        while self.running:
            try:
                databases_status = self.db_manager.check_databases_status()
                
                if databases_status != self.databases_online:
                    self.databases_online = databases_status
                    
                    if not databases_status:
                        logger.warning("Um ou mais bancos estão offline. Parando consumer...")
                        if self.rabbitmq_consumer:
                            self.rabbitmq_consumer.stop_consuming()
                        logger.info("Aguardando reconexão dos bancos...")
                
                time.sleep(2)  # Reduzido para 2 segundos
                
            except Exception as e:
                logger.error(f"Erro no monitoramento de bancos: {e}")
                time.sleep(2)


class RabbitmqConsumer:
    def __init__(self, callback, host="rabbit"):
        self.__host = host
        self.__port = 5672
        self.__username = "user"
        self.__password = "1234"
        self.__queue = "envio_fila1"
        self.__callback = callback
        self.__connection = None
        self.__channel = None
        self.consuming = False
        self.should_reconnect = True

    def __create_connection(self):
        """Cria conexão com RabbitMQ"""
        try:
            connection_parameters = pika.ConnectionParameters(
                host=self.__host,
                port=self.__port,
                credentials=pika.PlainCredentials(
                    username=self.__username,
                    password=self.__password
                ),
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            return pika.BlockingConnection(connection_parameters)
        except Exception as e:
            logger.error(f"Erro ao criar conexão RabbitMQ: {e}")
            return None

    def __create_channel(self):
        """Cria canal e declara a fila"""
        try:
            if not self.__connection or self.__connection.is_closed:
                self.__connection = self.__create_connection()
                if not self.__connection:
                    return None

            channel = self.__connection.channel()
            channel.queue_declare(queue=self.__queue, durable=True)
            channel.basic_qos(prefetch_count=1)
            
            return channel
        except Exception as e:
            logger.error(f"Erro ao criar canal RabbitMQ: {e}")
            return None

    def start_consuming(self):
        """Inicia o consumo das mensagens com reconexão automática"""
        while self.should_reconnect:
            try:
                self.__channel = self.__create_channel()
                if not self.__channel:
                    logger.error("Falha ao criar canal. Tentando reconectar em 5 segundos...")
                    time.sleep(5)
                    continue

                self.__channel.basic_consume(
                    queue=self.__queue,
                    on_message_callback=self._message_callback,
                    auto_ack=False
                )

                self.consuming = True
                logger.info("Consumer RabbitMQ iniciado. Aguardando mensagens...")
                
                try:
                    self.__channel.start_consuming()
                except KeyboardInterrupt:
                    logger.info("Parando consumer por interrupção do usuário...")
                    self.stop_consuming()
                    break
                    
            except pika.exceptions.ConnectionClosedByBroker as e:
                logger.warning(f"Conexão fechada pelo broker: {e}")
                self.consuming = False
                if self.should_reconnect:
                    logger.info("Tentando reconectar em 5 segundos...")
                    time.sleep(5)
                else:
                    break
                    
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Erro de conexão AMQP: {e}")
                self.consuming = False
                if self.should_reconnect:
                    logger.info("Tentando reconectar em 5 segundos...")
                    time.sleep(5)
                else:
                    break
                    
            except Exception as e:
                logger.error(f"Erro inesperado no consumer: {e}")
                self.consuming = False
                if self.should_reconnect:
                    logger.info("Tentando reconectar em 5 segundos...")
                    time.sleep(5)
                else:
                    break

    def _message_callback(self, ch, method, properties, body):
        """Callback interno que processa mensagem e faz ACK/NACK"""
        try:
            # Chama o callback do usuário
            success = self.__callback(self, ch, method, properties, body)
            
            # ACK se processamento foi bem-sucedido
            if success is not False:  # None ou True = sucesso
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                # NACK em caso de falha (rejeita e recoloca na fila)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
        except Exception as e:
            logger.error(f"Erro no callback da mensagem: {e}")
            # NACK em caso de exceção
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except:
                pass

    def stop_consuming(self):
        """Para o consumo das mensagens"""
        self.consuming = False
        self.should_reconnect = False
        
        try:
            if self.__channel and not self.__channel.is_closed:
                self.__channel.stop_consuming()
                logger.info("Consumo RabbitMQ parado")
        except Exception as e:
            logger.error(f"Erro ao parar consumo: {e}")

    def close_connection(self):
        """Fecha a conexão com RabbitMQ"""
        self.stop_consuming()
        try:
            if self.__connection and not self.__connection.is_closed:
                self.__connection.close()
                logger.info("Conexão RabbitMQ fechada")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão RabbitMQ: {e}")


def validar_dados(dados):
    """Valida os dados do formulário de investimento cripto"""
    if not isinstance(dados, dict):
        logger.error("O corpo da mensagem não é um dicionário.")
        return False

    # Campos obrigatórios do formulário
    required_fields = [
        'nome', 'email', 'telefone', 'idade', 'experiencia',
        'criptomoeda', 'valor_investimento', 'prazo_investimento', 'perfil_risco'
    ]
    
    for field in required_fields:
        if not dados.get(field):
            logger.error(f"Campo obrigatório ausente: {field}")
            return False

    # Validações específicas
    try:
        # Validar idade
        idade = int(dados.get('idade', 0))
        if idade < 18 or idade > 100:
            logger.error("Idade deve estar entre 18 e 100 anos")
            return False

        # Validar valor de investimento
        valor = float(dados.get('valor_investimento', 0))
        if valor < 100:
            logger.error("Valor de investimento deve ser pelo menos R$ 100")
            return False
            
    except (ValueError, TypeError) as e:
        logger.error(f"Erro na validação de tipos numéricos: {e}")
        return False

    return True


# Instância global do gerenciador de banco
db_manager = DatabaseManager()


def processar_callback(consumer, ch, method, properties, body):
    """Callback principal que processa as mensagens recebidas"""
    try:
        dados = json.loads(body)
        logger.info(f"Processando dados: {dados.get('nome', 'N/A')} - {dados.get('email', 'N/A')}")

        if not validar_dados(dados):
            logger.warning("Dados inválidos. Rejeitando mensagem.")
            return False  # NACK - rejeita mensagem

        # Tentar inserir nos bancos
        pg_success = db_manager.insert_postgresql(dados)
        redis_success = db_manager.insert_redis(dados)

        if pg_success and redis_success:
            logger.info("Dados inseridos com sucesso em ambos os bancos!")
            return True  # ACK - mensagem processada com sucesso
        else:
            error_msg = []
            if not pg_success:
                error_msg.append("PostgreSQL")
            if not redis_success:
                error_msg.append("Redis")
            logger.error(f"Falha ao inserir em: {', '.join(error_msg)}")
            return False  # NACK - falha no processamento

    except json.JSONDecodeError as e:
        logger.error(f"JSON inválido: {e}")
        return False  # NACK - JSON inválido
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")
        return False  # NACK - erro genérico


def signal_handler(signum, frame):
    """Handler para sinais de sistema"""
    logger.info(f"Sinal recebido: {signum}. Finalizando aplicação...")
    global running
    running = False


# Variável global para controle do loop principal
running = True


def main():
    """Função principal"""
    global running
    
    # Configurar handlers de sinal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    health_checker = None
    rabbitmq_consumer = None
    consumer_thread = None
    
    try:
        logger.info("=== INICIANDO APLICAÇÃO CONSUMER CRIPTO ===")
        
        while running:
            try:
                logger.info("Verificando conectividade dos bancos...")
                
                # Aguardar bancos ficarem online
                while running and not db_manager.check_databases_status():
                    logger.warning("Aguardando bancos ficarem online...")
                    time.sleep(5)
                
                if not running:
                    break
                
                logger.info("Bancos online! Estabelecendo conexões...")
                
                # Conectar aos bancos
                pg_connected = db_manager.connect_postgresql()
                redis_connected = db_manager.connect_redis()
                
                if not (pg_connected and redis_connected):
                    logger.error("Falha ao conectar aos bancos. Tentando novamente...")
                    time.sleep(5)
                    continue
                
                # Parar componentes anteriores se existirem
                if health_checker:
                    health_checker.stop_monitoring()
                if rabbitmq_consumer:
                    rabbitmq_consumer.close_connection()
                if consumer_thread and consumer_thread.is_alive():
                    consumer_thread.join(timeout=3)
                
                # Criar novos componentes
                rabbitmq_consumer = RabbitmqConsumer(processar_callback, host="rabbit")
                health_checker = DatabaseHealthChecker(db_manager, rabbitmq_consumer)
                
                # Iniciar monitoramento de bancos
                health_checker.start_monitoring()
                
                # Iniciar consumer em thread separada
                consumer_thread = threading.Thread(
                    target=rabbitmq_consumer.start_consuming,
                    daemon=False
                )
                consumer_thread.start()
                
                logger.info("Sistema operacional! Aguardando mensagens...")
                
                # Loop de monitoramento principal
                while running:
                    # Verificar se os bancos ainda estão online
                    if not db_manager.check_databases_status():
                        logger.warning("Bancos ficaram offline! Reiniciando...")
                        break
                    
                    # Verificar se o consumer ainda está ativo
                    if not consumer_thread.is_alive():
                        logger.warning("Thread do consumer morreu! Reiniciando...")
                        break
                    
                    time.sleep(3)
                
            except Exception as e:
                logger.error(f"Erro no loop principal: {e}")
                time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Aplicação interrompida pelo usuário")
    except Exception as e:
        logger.error(f"Erro crítico na aplicação: {e}")
    finally:
        logger.info("Finalizando aplicação...")
        running = False
        
        if health_checker:
            health_checker.stop_monitoring()
        if rabbitmq_consumer:
            rabbitmq_consumer.close_connection()
        if consumer_thread and consumer_thread.is_alive():
            consumer_thread.join(timeout=5)
        
        db_manager.close_connections()
        logger.info("Aplicação finalizada com sucesso")


if __name__ == "__main__":
    main()