from flask import Flask, request, render_template, redirect, url_for, flash
from typing import Dict
import pika
import json
import logging
from datetime import datetime

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'chave_secreta'  

class RabbitmqPublisher:
    def __init__(self, host="rabbit") -> None:
        self.__host = host
        self.__port = 5672
        self.__username = "user"
        self.__password = "1234"
        self.__exchange = "envio_fila1"
        self.__routing_key = ""
        self.__channel = None
        self.__connection = None

    def __create_channel(self):
        try:
            connection_parameters = pika.ConnectionParameters(
                host=self.__host,
                port=self.__port,
                credentials=pika.PlainCredentials(
                    username=self.__username,
                    password=self.__password
                ),
                heartbeat=600,
                blocked_connection_timeout=300
            )
            self.__connection = pika.BlockingConnection(connection_parameters)
            channel = self.__connection.channel()

            channel.exchange_declare(
                exchange=self.__exchange,
                exchange_type="direct",
                durable=True
            )
            channel.queue_declare(
                queue="calculadora_cripto_queue",
                durable=True
            )
            channel.queue_bind(
                exchange=self.__exchange,
                queue="calculadora_cripto_queue",
                routing_key=self.__routing_key
            )
            return channel
        except Exception as e:
            logger.error(f"Erro ao criar canal RabbitMQ: {e}")
            raise

    def send_message(self, body: Dict):
        try:
            if not self.__channel:
                self.__channel = self.__create_channel()

            body['timestamp'] = datetime.now().isoformat()
            body['tipo'] = 'calculadora_cripto'

            self.__channel.basic_publish(
                exchange=self.__exchange,
                routing_key=self.__routing_key,
                body=json.dumps(body, ensure_ascii=False, indent=2),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            logger.info(f"Mensagem publicada com sucesso: {body['nome']} - {body['email']}")
            return True
        except Exception as e:
            logger.error(f"Erro ao publicar mensagem: {e}")
            return False
        finally:
            if self.__connection and not self.__connection.is_closed:
                self.__connection.close()

    def __del__(self):
        try:
            if self.__connection and not self.__connection.is_closed:
                self.__connection.close()
        except:
            pass

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/enviar", methods=["POST"])
def enviar():
    try:
        nome = request.form.get("nome", "").strip()
        email = request.form.get("email", "").strip()
        telefone = request.form.get("telefone", "").strip()
        idade = request.form.get("idade", "").strip()
        experiencia = request.form.get("experiencia", "").strip()
        criptomoeda = request.form.get("criptomoeda", "").strip()
        valor_investimento = request.form.get("valor_investimento", "").strip()
        prazo_investimento = request.form.get("prazo_investimento", "").strip()
        perfil_risco = request.form.get("perfil_risco", "").strip()
        observacoes = request.form.get("observacoes", "").strip()

        campos_obrigatorios = {
            "nome": nome,
            "email": email,
            "telefone": telefone,
            "idade": idade,
            "experiencia": experiencia,
            "criptomoeda": criptomoeda,
            "valor_investimento": valor_investimento,
            "prazo_investimento": prazo_investimento,
            "perfil_risco": perfil_risco
        }

        campos_faltando = [campo for campo, valor in campos_obrigatorios.items() if not valor]
        if campos_faltando:
            logger.warning(f"Campos obrigatórios faltando: {campos_faltando}")
            flash(f"Preencha todos os campos obrigatórios: {', '.join(campos_faltando)}", "error")
            return redirect(url_for("index"))

        try:
            idade_int = int(idade)
            if idade_int < 18 or idade_int > 100:
                flash("A idade deve estar entre 18 e 100 anos.", "error")
                return redirect(url_for("index"))
        except ValueError:
            flash("Por favor, insira uma idade válida.", "error")
            return redirect(url_for("index"))

        try:
            valor_limpo = valor_investimento.replace(".", "").replace(",", ".")
            valor_float = float(valor_limpo)
            if valor_float < 100:
                flash("O valor mínimo de investimento é R$ 100,00.", "error")
                return redirect(url_for("index"))
        except ValueError:
            flash("Por favor, insira um valor de investimento válido.", "error")
            return redirect(url_for("index"))

        dados = {
            "nome": nome,
            "email": email,
            "telefone": telefone,
            "idade": idade_int,
            "experiencia": experiencia,
            "criptomoeda": criptomoeda,
            "valor_investimento": valor_float,
            "prazo_investimento": prazo_investimento,
            "perfil_risco": perfil_risco,
            "observacoes": observacoes,
            "ip_cliente": request.remote_addr,
            "user_agent": request.headers.get('User-Agent', ''),
            "origem": "calculadora_cripto_web"
        }

        publisher = RabbitmqPublisher(host="rabbit")
        sucesso = publisher.send_message(dados)

        if sucesso:
            logger.info(f"Dados enviados com sucesso: {nome} - {email}")
            flash("Análise enviada com sucesso! Entraremos em contato em breve.", "success")
        else:
            raise Exception("Falha ao publicar mensagem no RabbitMQ")

    except Exception as e:
        logger.error(f"Erro ao processar envio: {e}")
        flash("Erro ao processar a solicitação. Tente novamente mais tarde.", "error")
        return redirect(url_for("index"))

    return redirect(url_for("index"))

@app.route("/health")
def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "calculadora-cripto"
    }, 200

@app.errorhandler(404)
def not_found(error):
    return render_template("index.html"), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Erro interno do servidor: {error}")
    flash("Erro interno do servidor. Tente novamente mais tarde.", "error")
    return render_template("index.html"), 500

if __name__ == "__main__":
    logger.info("Iniciando aplicação Calculadora Cripto...")
    app.run(host='0.0.0.0', port=5000, debug=False)
