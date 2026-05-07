package com.mashnet.network.server;

import com.mashnet.network.client.RsocketClientManager;
import com.mashnet.network.control.ControlCommandHandler;
import com.mashnet.network.control.IStreamProvider;
import com.mashnet.network.topology.TopologyManager;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpServer;

import java.time.Duration;
import java.util.UUID;

/**
 * Клас відповідає за підняття серверної частини (TCP для нод та WebSocket для Дашборду).
 *
 * Реалізує SRP: не містить логіки обробки команд, лише конфігурує мережу.
 */
public class RsocketServerManager {

    private final TopologyManager topologyManager;
    private final RsocketClientManager clientManager;
    private final IStreamProvider streamProvider;

    public RsocketServerManager(TopologyManager topologyManager,
                                RsocketClientManager clientManager,
                                IStreamProvider streamProvider) {
        this.topologyManager = topologyManager;
        this.clientManager = clientManager;
        this.streamProvider = streamProvider;
    }

    /**
     * Запускає захищений SSL TCP-сервер для спілкування між Java-нодами та Python-датчиками.
     *
     * @param port порт, на якому буде слухати сервер (наприклад, 7000).
     */
    public void startTcpServer(int port){
        try {

            //генерація тимсасового ssl сертифіката для тестування в локалхості
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            SslContext sslContext = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();

            TcpServer tcpServer = TcpServer.create()
                    .host("localhost")
                    .port(port)
                    .secure(ssl -> ssl.sslContext(sslContext));

            RSocketServer.create()
                    .acceptor((setup, sendingSocket) -> {
                        String remoteNodeId = setup.getDataUtf8();

                        // Реєструємо підключення в менеджері топології
                        topologyManager.addConnection(remoteNodeId, sendingSocket);

                        // відстежуємо ВІДКЛЮЧЕННЯ ноди для Garbage Collector графа
                        sendingSocket.onClose().subscribe(
                                null,
                                err -> topologyManager.handleDisconnection(remoteNodeId),
                                () -> topologyManager.handleDisconnection(remoteNodeId)
                        );

                        // Повертаємо наш маршрутизатор як обробник усіх наступних повідомлень
                        return Mono.just(new ControlCommandHandler(topologyManager, clientManager, streamProvider));
                    })
                    .resume(new io.rsocket.core.Resume().sessionDuration(Duration.ofMinutes(5)))
                    .bind(TcpServerTransport.create(tcpServer))
                    .subscribe(v -> System.out.println(">>> [SERVER] SSL TCP Сервер слухає на порту " + port));

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Запускає WebSocket сервер БЕЗ шифрування спеціально для React-дашборду.
     *
     * @param wsPort порт для WebSockets (наприклад, 7001).
     */
    public void startWebSocketServer(int wsPort) {
        try {
            RSocketServer.create()
                    .acceptor((setup, sendingSocket) -> {
                        String rawId = setup.getDataUtf8();
                        // Якщо UI не передав ID, генеруємо його самі
                        final String remoteNodeId = (rawId == null || rawId.isEmpty())
                                ? "UI-Dashboard-" + UUID.randomUUID().toString().substring(0, 4)
                                : rawId;

                        topologyManager.addConnection(remoteNodeId, sendingSocket);

                        sendingSocket.onClose().subscribe(
                                null,
                                err -> topologyManager.handleDisconnection(remoteNodeId),
                                () -> topologyManager.handleDisconnection(remoteNodeId)
                        );

                        return Mono.just(new ControlCommandHandler(topologyManager, clientManager, streamProvider));
                    })
                    .bind(WebsocketServerTransport.create("localhost", wsPort))
                    .subscribe(v -> System.out.println(">>> [SERVER] WebSocket Сервер (UI) слухає на порту " + wsPort));
        } catch (Exception e) {
            System.err.println("!!! Помилка запуску WS сервера: " + e.getMessage());
        }
    }


}
