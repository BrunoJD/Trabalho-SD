import argparse
import threading
from concurrent import futures

import grpc
import numpy as np

import federated_pb2
import federated_pb2_grpc
from common_fl import (
    aggregate_fedavg,
    create_model_template,
    evaluate_global_model,
    get_model_params,
    load_all_test_sets,
)


# Dica para os alunos:
# O servidor tem três papéis principais:
# 1) disponibilizar o modelo global atual aos clientes;
# 2) receber atualizações locais enviadas pelos clientes;
# 3) consolidar uma rodada quando todos os clientes esperados responderem.


class FederatedLearningServicer(federated_pb2_grpc.FederatedLearningServicer):
    def __init__(self, num_clients: int, total_rounds: int):
        self.num_clients = num_clients
        self.total_rounds = total_rounds
        self.current_round = 1
        self.lock = threading.Lock()
        self.received_updates = {}
        self.training_finished = threading.Event()

        # O modelo global começa com um template vazio/inicial.
        self.model_template = create_model_template(num_clients)
        self.global_params = get_model_params(self.model_template)
        self.X_test_global, self.y_test_global = load_all_test_sets(num_clients)

    def _params_to_proto(self, params):
        """
        Converte [coef, intercept] para ModelParameters.

        Dica:
        - mesma lógica do cliente em params_to_proto
        - servidor e cliente devem serializar no mesmo formato
        """
        # TODO: implemente a serialização do modelo para protobuf
        raise NotImplementedError("Implemente _params_to_proto")


    def _proto_to_params(self, proto_params):
        """
        Converte ModelParameters para [coef, intercept].

        Dica:
        - mesma lógica do cliente em params_from_proto
        """
        # TODO: implemente a desserialização do protobuf para NumPy
        raise NotImplementedError("Implemente _proto_to_params")

    def GetGlobalModel(self, request, context):
        """
        Retorna a rodada atual e o modelo global atual.

        Dica:
        - done deve ser True quando current_round > total_rounds
        - a mensagem pode ser algo como "Treinamento encerrado" ou "Modelo global disponível"
        """
        with self.lock:
            # TODO: calcule done
            # TODO: monte proto a partir dos parâmetros globais
            # TODO: retorne federated_pb2.GlobalModel(...)
            raise NotImplementedError("Implemente GetGlobalModel")

    def _validate_update(self, request):
        """
        Verifica se uma atualização recebida do cliente é válida.

        Regras esperadas:
        1. Se o treinamento já terminou, rejeitar.
        2. Se request.round for diferente de current_round, rejeitar.
        3. Se o mesmo cliente já enviou atualização nesta rodada, rejeitar.
        4. Caso contrário aceite e mensagem "OK".
        Retorne uma tupla: (accepted: bool, message: str)
        """
        # TODO: implemente as quatro verificações acima
        raise NotImplementedError("Implemente _validate_update")


    def _consolidate_round_if_ready(self):
        """
        Consolida a rodada quando todos os clientes esperados já responderam.

        Passos esperados:
        - verificar se len(received_updates) == num_clients
        - Montar lista de updates
        - agregar com aggregate_fedavg(...)
        - avaliar o modelo global com evaluate_global_model(...)
        - imprimir métricas da rodada
        - limpar received_updates
        - avançar current_round
        - se a última rodada terminou, sinalizar training_finished
        """
        # TODO: implemente a consolidação da rodada
        raise NotImplementedError("Implemente _consolidate_round_if_ready")


    def SubmitUpdate(self, request, context):
        """
        Recebe a atualização de um cliente.

        Passos esperados:
        1. Validar a atualização com _validate_update
        2. Se inválida, retornar UpdateAck(accepted=False, ...)
        3. Se válida, armazenar em received_updates[request.cid]
        4. Tentar consolidar a rodada com _consolidate_round_if_ready
        5. Retornar UpdateAck(accepted=True, ...)
        """
        with self.lock:
            # TODO: validar a atualização
            
            # TODO: armazenar a atualização recebida
            
            # Dica: salve params, num_examples, train_loss e train_acc
            
            # TODO: imprimir um log curto identificando cliente e rodada

            # TODO: consolidar a rodada se todos os clientes já enviaram

            # TODO: retornar UpdateAck de sucesso
            raise NotImplementedError("Implemente SubmitUpdate")




def serve(host: str, port: int, num_clients: int, total_rounds: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = FederatedLearningServicer(num_clients, total_rounds)
    federated_pb2_grpc.add_FederatedLearningServicer_to_server(servicer, server)

    address = f"{host}:{port}"
    server.add_insecure_port(address)
    server.start()
    print(f"Servidor gRPC ouvindo em {address}")
    print(f"Aguardando {num_clients} clientes por rodada, total de {total_rounds} rodadas.\n")

    # Dica:
    # wait_for_termination() bloqueia para sempre.
    # Aqui queremos encerrar quando o treinamento terminar.
    servicer.training_finished.wait()
    print("[Servidor] Todas as rodadas foram concluídas. Encerrando servidor gRPC...")
    server.stop(grace=2).wait()
    print("[Servidor] Servidor encerrado com sucesso.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--num-clients", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()

    serve(args.host, args.port, args.num_clients, args.rounds)
