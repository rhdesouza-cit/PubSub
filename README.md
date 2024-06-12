# Motivação:

## Pré-requisitos:
* Java Development Kit (JDK) 11+
* Apache Maven
* Google Cloud SDK
* Pub/Sub Emulator
* K6

Passos:
1. Configurar o Pub/Sub Emulator
   Primeiro, inicie o Pub/Sub Emulator em sua máquina local. No terminal, execute:

`sh gcloud beta emulators pubsub start --host-port=localhost:8085`

Para usar o emulador, você precisa definir a variável de ambiente PUBSUB_EMULATOR_HOST:

`sh export PUBSUB_EMULATOR_HOST=localhost:8085`


# Documentação de apio
https://bullla.atlassian.net/jira/software/c/projects/ONE/boards/9?selectedIssue=ONE-182

https://cloud.google.com/sdk/gcloud/reference/beta/emulators/pubsub/start

https://cloud.spring.io/spring-cloud-static/spring-cloud-gcp/current/reference/html/appendix.html

https://cloud.spring.io/spring-cloud-static/spring-cloud-gcp/current/reference/html/#cloud-pubsub