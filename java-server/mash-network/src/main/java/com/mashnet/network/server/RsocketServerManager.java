package com.mashnet.network.server;

import com.mashnet.network.client.RsocketClientManager;
import com.mashnet.network.control.ControlCommandHandler;
import com.mashnet.network.control.IStreamProvider;
import com.mashnet.network.topology.TopologyManager;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpServer;

import java.io.File;
import java.security.cert.X509Certificate;
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
     * Запускає захищений SSL TCP-сервер (mTLS).
     */
    public void startTcpServer(int port) {
        try {
            // 1. Вказуємо шляхи до згенерованих сертифікатів
            File serverCert = new File("certs/server1.crt");
            File serverKey = new File("certs/server1.key");
            File caCert = new File("certs/ca.crt");

            if (!serverCert.exists() || !serverKey.exists() || !caCert.exists()) {
                System.err.println("[FATAL] Сертифікати не знайдено у папці certs/. Запуск неможливий.");
                return;
            }

            System.out.println(">>> [SECURITY] Ініціалізація суворого mTLS для сервера...");

            // 2. Створюємо SSL Контекст з вимогою клієнтського сертифіката
            SslContext sslContext = SslContextBuilder.forServer(serverCert, serverKey)
                    .trustManager(caCert) // Довіряємо ТІЛЬКИ нашому Local CA
                    .clientAuth(ClientAuth.REQUIRE) // Блокуємо всіх, хто не має сертифіката
                    .build();

            // 3. Налаштовуємо Netty TCP Сервер
            TcpServer tcpServer = TcpServer.create()
                    // ВАЖЛИВО: 0.0.0.0 дозволяє приймати з'єднання ззовні (через IP Vagrant-мережі)
                    .host("0.0.0.0")
                    .port(port)
                    .secure(ssl -> ssl.sslContext(sslContext))
                    // Перехоплювач на рівні L6 (TLS), щоб дістати UUID до того, як включиться RSocket
                    .doOnConnection(connection -> {
                        connection.addHandlerLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
                                if (sslHandler != null) {
                                    sslHandler.handshakeFuture().addListener(future -> {
                                        if (future.isSuccess()) {
                                            try {
                                                // Витягуємо сертифікат сенсора
                                                X509Certificate clientCert = (X509Certificate) sslHandler.engine().getSession().getPeerCertificates()[0];
                                                String distinguishedName = clientCert.getSubjectDN().getName();
                                                System.out.println(">>> [SECURITY] mTLS Успішно! Підключено пристрій з DN: " + distinguishedName);
                                                // Тепер ми криптографічно впевнені, хто до нас підключився
                                            } catch (Exception e) {
                                                System.err.println("[SECURITY] Помилка читання сертифіката: " + e.getMessage());
                                            }
                                        }
                                    });
                                }
                                super.channelActive(ctx);
                            }
                        });
                    });

            // 4. Піднімаємо RSocket поверх безпечного TCP-тунелю
            RSocketServer.create()
                    .acceptor((setup, sendingSocket) -> {
                        // Оскільки ми вже пройшли mTLS, ми можемо довіряти ID, що прийшов у Setup
                        String remoteNodeId = setup.getDataUtf8();
                        topologyManager.addConnection(remoteNodeId, sendingSocket);

                        sendingSocket.onClose().subscribe(
                                null,
                                err -> topologyManager.handleDisconnection(remoteNodeId),
                                () -> topologyManager.handleDisconnection(remoteNodeId)
                        );

                        return Mono.just(new ControlCommandHandler(topologyManager, clientManager, streamProvider));
                        // ПРИМІТКА: Тут передається null для streamManager тимчасово, щоб код компілювався за попередньою структурою.
                        // Коли ми додамо StreamManager сюди повноцінно, ми його прокинемо.
                    })
                    .resume(new io.rsocket.core.Resume().sessionDuration(Duration.ofMinutes(5)))
                    .bind(TcpServerTransport.create(tcpServer))
                    .subscribe(v -> System.out.println(">>> [SERVER] SSL TCP Сервер слухає на порту " + port));

        } catch (Exception e) {
            throw new RuntimeException("Помилка запуску сервера: " + e.getMessage(), e);
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
                    .bind(WebsocketServerTransport.create("0.0.0.0", wsPort))
                    .subscribe(v -> System.out.println(">>> [SERVER] WebSocket Сервер (UI) слухає на порту " + wsPort));
        } catch (Exception e) {
            System.err.println("!!! Помилка запуску WS сервера: " + e.getMessage());
        }
    }


}
