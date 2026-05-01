import { useCallback, useEffect, useState, useRef } from 'react';
import { Buffer } from 'buffer';

import { RSocketClient } from 'rsocket-core';
import RSocketWebSocketClient from 'rsocket-websocket-client';

import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
} from '@xyflow/react';
import type { Connection, Edge, Node } from '@xyflow/react'; 
import '@xyflow/react/dist/style.css';

const initialNodes: Node[] = [];
const initialEdges: Edge[] = [];

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  
  const [isConnected, setIsConnected] = useState(false);
  const [isConnecting, setIsConnecting] = useState(false);

  const rsocketRef = useRef<any>(null);

  const onConnect = useCallback(
    (params: Connection | Edge) => setEdges((eds) => addEdge({ ...params, animated: true }, eds)),
    [setEdges],
  );

  // --- Запит топології з візуальним виокремленням та зв'язками ---
  const fetchTopology = (client: any) => {
    client.requestResponse({
      data: 'GET_TOPOLOGY',
      metadata: '',
    }).subscribe({
      onComplete: (payload: any) => {
        try {
          // Отримуємо комплексний об'єкт { nodes: [], edges: [] }
          const topologyData = JSON.parse(payload.data);
          const connectedIds: string[] = topologyData.nodes || [];
          const connectedEdges: any[] = topologyData.edges || [];
          
          // 1. Формуємо Ноди (цей код майже не змінився)
          const dynamicNodes: Node[] = connectedIds.map((id, index) => {
            let icon = '📦';
            let readableName = id;
            let customStyle = {};

            if (id.startsWith('NodePy')) {
              icon = '🐍';
              readableName = `Python Sensor [${id.slice(-4)}]`; 
            } else if (id.startsWith('Seed-')) {
              icon = '👑';
              readableName = `Java Seed`;
              customStyle = {
                border: '2px solid #ff0072',
                backgroundColor: '#2b1b24', color: '#fff',
                fontWeight: 'bold', boxShadow: '0 0 15px rgba(255, 0, 114, 0.4)'
              };
            } else if (id.startsWith('JavaNode-')) {
              icon = '☕';
              readableName = `Java Compute [${id.slice(-4)}]`;
              customStyle = { border: '1px solid #ffba08' };
            } else if (id.includes('UI') || id.includes('Dashboard')) {
              icon = '💻';
              readableName = `Dashboard [${id.slice(-4)}]`;
            }

            return {
              id: id,
              position: { 
                x: id.startsWith('Seed-') ? 350 : 50 + index * 220, 
                y: id.startsWith('Seed-') ? 50 : 250 
              }, 
              data: { label: `${icon} ${readableName}` },
              style: customStyle
            };
          });

          // 2. Формуємо Зв'язки (Edges)
          const dynamicEdges: Edge[] = connectedEdges.map((edgeInfo, index) => {
            return {
              id: `edge-${index}`,
              source: edgeInfo.source,
              target: edgeInfo.target,
              animated: false, // ТЕПЕР СТАТИЧНІ
              style: { stroke: '#555', strokeWidth: 1.5 }, // Сірий колір
            };
          });

          // Оновлюємо стан React
          setNodes(dynamicNodes);
          setEdges(dynamicEdges); // Малюємо лінії!
          
          console.log("Топологію оновлено!");

        } catch (error) {
          console.error("Помилка парсингу JSON:", error);
        }
      },
      onError: (error: any) => console.error("Помилка запиту топології:", error),
    });
  };


  // --- Слухаємо події в реальному часі ---
  const subscribeToEvents = (client: any) => {
    client.requestStream({
      data: 'SUBSCRIBE_EVENTS',
      metadata: '',
    }).subscribe({
      onSubscribe: (sub: any) => sub.request(2147483647), // Просимо відправляти ВСІ події
      onNext: (payload: any) => {
        try {
          const data = JSON.parse(payload.data);
          console.log("Отримано подію:", data);

          // Оновлюємо стилі ліній динамічно
          setEdges((eds) => eds.map((edge) => {
            // Якщо лінія торкається ноди, яка змінила стан
            if (edge.source === data.nodeId || edge.target === data.nodeId) {
              const isActive = data.event === 'STREAM_START';
              return {
                ...edge,
                animated: isActive,
                style: {
                  stroke: isActive ? '#00ff00' : '#555', // Зелений при роботі, інакше сірий
                  strokeWidth: isActive ? 3 : 1.5,
                }
              };
            }
            return edge;
          }));
        } catch (error) {
          console.error("Помилка парсингу події:", error);
        }
      },
      onError: (error: any) => console.error("Помилка підписки на події:", error),
    });
  };

  // --- Підключення з фіксом фантомних з'єднань ---
  useEffect(() => {
    let isMounted = true; // Прапорець життєвого циклу
    let localSocket: any = null;

    const connectToBackend = async () => {
      if (isConnecting) return;
      setIsConnecting(true);

      try {
        if (rsocketRef.current) rsocketRef.current.close();

        const TransportClass = (RSocketWebSocketClient as any).default || RSocketWebSocketClient;

        const client = new RSocketClient({
          setup: {
            keepAlive: 10000,
            lifetime: 30000,
            dataMimeType: 'text/plain',
            metadataMimeType: 'text/plain',
            data: 'Node-Dashboard',
            metadata: '3000', 
          },
          transport: new TransportClass({
            url: 'ws://localhost:7001',
          }),
        });

        const socket = await client.connect();

        // Якщо React встиг вбити компонент поки ми підключалися — закриваємо сокет
        if (!isMounted) {
          console.warn("Перехоплено фантомне з'єднання. Закриваємо...");
          socket.close();
          return; 
        }

        localSocket = socket;
        rsocketRef.current = socket;
        console.log("Успішно підключено до Java!");
        setIsConnected(true);

        fetchTopology(socket);
        subscribeToEvents(socket);

      } catch (error) {
        if (isMounted) {
          console.error("Помилка підключення до Java:", error);
          setIsConnected(false);
        }
      } finally {
        if (isMounted) {
          setIsConnecting(false);
        }
      }
    };

    connectToBackend();

    return () => {
      isMounted = false; // Сигналізуємо, що компонент знищено
      if (localSocket) {
        localSocket.close();
      } else if (rsocketRef.current) {
        rsocketRef.current.close();
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); 

  return (
    <div style={{ width: '100vw', height: '100vh', backgroundColor: '#1a192b' }}>
      
      <div style={{
        position: 'absolute', top: 20, left: 20, zIndex: 10, 
        padding: '10px 20px', borderRadius: '8px',
        backgroundColor: isConnected ? '#28a745' : '#343a40',
        color: 'white', fontWeight: 'bold', fontFamily: 'sans-serif',
        display: 'flex', alignItems: 'center', gap: '15px',
        boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
      }}>
        <span>
          {isConnected 
            ? '🟢 Підключено до Java Меш-мережі' 
            : '🔴 Немає з\'єднання з сервером'}
        </span>
        
        {/* Кнопка перепідключення використовує ту саму функцію, але через реф, бо вона тепер всередині useEffect.
            Щоб не ускладнювати код виносом функції, ми просто перезавантажуємо сторінку при ручному перепідключенні. */}
        {!isConnected && (
          <button 
            onClick={() => window.location.reload()}
            style={{
              padding: '6px 12px', borderRadius: '4px', border: 'none',
              backgroundColor: '#007bff', color: 'white', cursor: 'pointer',
              fontWeight: 'bold'
            }}
          >
            🔄 Перепідключитись
          </button>
        )}
      </div>

      <ReactFlow 
        nodes={nodes} edges={edges} 
        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect}         
        colorMode="dark" fitView 
      >
        <Background color="#fff" gap={16} />
        <Controls />
        <MiniMap nodeColor="#00ff00" nodeStrokeWidth={3} zoomable pannable />
      </ReactFlow>
    </div>
  );
}