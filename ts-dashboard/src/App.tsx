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
  Handle,
  Position
} from '@xyflow/react';
import type { Connection, Edge, Node } from '@xyflow/react'; 
import '@xyflow/react/dist/style.css';

// ==========================================
// 1. СТВОРЮЄМО КАСТОМНИЙ ВУЗОЛ (За ескізом)
// ==========================================
const MeshNode = ({ data }: any) => {
  return (
    <div style={{
      padding: '15px 20px',
      borderRadius: '8px',
      background: data.customStyle?.backgroundColor || '#2b2b36',
      border: data.customStyle?.border || '2px solid #555',
      boxShadow: data.customStyle?.boxShadow || '0 4px 6px rgba(0,0,0,0.3)',
      color: data.customStyle?.color || '#fff',
      minWidth: '160px',
      textAlign: 'center',
      fontFamily: 'monospace',
      position: 'relative'
    }}>
      {/* INPUT (Ліва сторона) */}
      <Handle 
        type="target" 
        position={Position.Left} 
        style={{ width: '10px', height: '10px', background: '#00ff00', border: '2px solid #222' }} 
      />

      <div style={{ fontSize: '24px', marginBottom: '8px' }}>{data.icon}</div>
      <div style={{ fontSize: '14px', fontWeight: 'bold' }}>{data.label}</div>

      {/* OUTPUT (Права сторона) */}
      <Handle 
        type="source" 
        position={Position.Right} 
        style={{ width: '10px', height: '10px', background: '#ff0072', border: '2px solid #222' }} 
      />
    </div>
  );
};

// Реєструємо наш кастомний вузол
const nodeTypes = { meshNode: MeshNode };

// ==========================================
// ГОЛОВНИЙ КОМПОНЕНТ APP
// ==========================================
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
          const topologyData = JSON.parse(payload.data);
          const connectedIds: string[] = topologyData.nodes || [];
          const connectedEdges: any[] = topologyData.edges || [];
          const activeStreams: string[] = topologyData.activeStreams || []; 
          
          // 1. Формуємо Ноди зі ЗБЕРЕЖЕННЯМ ПОЗИЦІЙ
          setNodes((prevNodes) => {
            return connectedIds.map((id, index) => {
              // Шукаємо, чи була ця нода вже на екрані
              const existingNode = prevNodes.find((n) => n.id === id);

              let icon = '📦';
              let readableName = id;
              let customStyle: any = {};

              if (id.startsWith('NodePy')) {
                icon = '🐍';
                readableName = `Python Sensor\n[${id.slice(-4)}]`; 
                customStyle = { border: '2px solid #3572A5' };
              } else if (id.startsWith('Seed-')) {
                icon = '👑';
                readableName = `Java Seed`;
                customStyle = {
                  border: '2px solid #ff0072',
                  backgroundColor: '#3b1c28', 
                  boxShadow: '0 0 20px rgba(255, 0, 114, 0.4)'
                };
              } else if (id.startsWith('JavaNode-')) {
                icon = '☕';
                readableName = `Java Compute\n[${id.slice(-4)}]`;
                customStyle = { border: '2px solid #ffba08' };
              }

              // ВИПРАВЛЕННЯ: Якщо нода існує, залишаємо її координати. 
              // Якщо нова — вираховуємо стартову позицію.
              const position = existingNode 
                ? existingNode.position 
                : { 
                    x: id.startsWith('Seed-') ? 400 : 50 + index * 250, 
                    y: id.startsWith('Seed-') ? 100 : 300 
                  };

              return {
                id: id,
                type: 'meshNode', // Використовуємо наш новий дизайн
                position: position, 
                data: { label: readableName, icon: icon, customStyle: customStyle }
              };
            });
          });

          // 2. Формуємо Зв'язки (Edges)
          const dynamicEdges: Edge[] = connectedEdges.map((edgeInfo) => {
            const forwardKey = `${edgeInfo.source}->${edgeInfo.target}`;
            const reverseKey = `${edgeInfo.target}->${edgeInfo.source}`;

            const isActiveForward = activeStreams.includes(forwardKey);
            const isActiveReverse = activeStreams.includes(reverseKey);
            const isActive = isActiveForward || isActiveReverse;

            let finalSource = edgeInfo.source;
            let finalTarget = edgeInfo.target;

            if (isActiveReverse && !isActiveForward) {
                finalSource = edgeInfo.target;
                finalTarget = edgeInfo.source;
            }

            return {
              id: `edge-${edgeInfo.source}-${edgeInfo.target}`,
              source: finalSource,
              target: finalTarget,
              animated: isActive, 
              style: { 
                stroke: isActive ? '#00ff00' : '#555', 
                strokeWidth: isActive ? 3 : 2 
              }, 
            };
          });

          setEdges(dynamicEdges);
          
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
      onSubscribe: (sub: any) => sub.request(2147483647),
      onNext: (payload: any) => {
        try {
          const data = JSON.parse(payload.data);
          if (data.event === 'TOPOLOGY_CHANGED') {
             fetchTopology(client);
          }
        } catch (error) {
          console.error("Помилка парсингу події:", error);
        }
      },
      onError: (error: any) => console.error("Помилка підписки на події:", error),
    });
  };

  // --- Підключення ---
  useEffect(() => {
    let isMounted = true; 
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

        if (!isMounted) {
          socket.close();
          return; 
        }

        localSocket = socket;
        rsocketRef.current = socket;
        setIsConnected(true);

        fetchTopology(socket);
        subscribeToEvents(socket);

      } catch (error) {
        if (isMounted) setIsConnected(false);
      } finally {
        if (isMounted) setIsConnecting(false);
      }
    };

    connectToBackend();

    return () => {
      isMounted = false; 
      if (localSocket) localSocket.close();
      else if (rsocketRef.current) rsocketRef.current.close();
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
        nodeTypes={nodeTypes} /* РЕЄСТРАЦІЯ КАСТОМНИХ НОД */
        colorMode="dark" fitView 
      >
        <Background color="#555" gap={16} />
        <Controls />
        <MiniMap nodeColor="#00ff00" nodeStrokeWidth={3} zoomable pannable />
      </ReactFlow>
    </div>
  );
}