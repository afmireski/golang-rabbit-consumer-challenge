# Desafio Go Lang

Executar RabbitMQ como um container docker como o comando:
- docker run -d --hostname localhost --name local-rabbit -p 15672:15672 -p 5672:5672 -p 5552:5552 rabbitmq:3.12.13-management
- Dentro do container executar: rabbitmq-plugins enable rabbitmq_stream
Conectar com RabbitMQ usando a Lib: https://github.com/rabbitmq/rabbitmq-stream-go-client na porta 5552
Acessar a interface do RabbitMQ http://localhost:15672 Login: guest e senha: guest
- Criar a queue do tipo stream com o nome: secmob.request
Ler mensagem do RabbitMQ da queue: secmob.request
- Formato da mensagem JSON
```json
{
"name": "João Silva",
"email": "joao@gmail.com"
}
```
Inserir mensagem no banco (mysql:8)
- id: autoincrement
- email: string
- name: string

Publicar na queue secmob.result o id do registro inserido na tabela do BD.