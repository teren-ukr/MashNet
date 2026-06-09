import { useCallback, useEffect, useState, useRef } from 'react';
import { useNodesState, useEdgesState, addEdge, type Connection, type Edge, type Node } from '@xyflow/react';
import { RSocketClient } from 'rsocket-core';
import RSocketWebSocketClient from 'rsocket-websocket-client';

// Імпортуємо інтерфейс ActiveTask з нашого нового компонента
import { type ActiveTask } from '../components/ActiveTasksPanel';

export const useMeshNetwork = () => {
  // ==========================================
  // СТАН ДОДАТКУ (State)
  // ==========================================
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [isConnected, setIsConnected] = useState(false);
  const rsocketRef = useRef<any>(null);
  
  // ТИМЧАСОВА ЗАГЛУШКА: Список активних задач. 
  // Пізніше ми будемо отримувати ці дані від Java сервера.
  const [activeTasks, setActiveTasks] = useState<ActiveTask[]>([
    { schema_id: 'task-1234', operation: 'AVERAGE', input_source: 'NodePy-5007', output_sink: 'JavaNode-7007' },
    { schema_id: 'task-5678', operation: 'MAX', input_source: 'JavaNode-7007', output_sink: 'Dashboard-UI' }
  ]);

  // ==========================================
  // ФУНКЦІЇ КЕРУВАННЯ МЕРЕЖЕЮ
  // ==========================================
  
  const onConnectManual = useCallback(
    (params: Connection | Edge) => setEdges((eds) => addEdge({ ...params, animated: true }, eds)),
    [setEdges],
  );

  // Відправка будь-яких команд на бекенд (Java)
  const sendCommand = (command: string, payloadStr: string = '') => {
    if (!rsocketRef.current || !isConnected) {
      alert("Немає підключення до сервера!");
      return;
    }

    const payload = { data: command, metadata: payloadStr };

    if (command === 'STOP_ALL' || command === 'NEW_EDGE' || command === 'STREAM_EVENT' || command === 'STOP_SCHEMA') {
      rsocketRef.current.fireAndForget(payload);
      console.log(`[FIRE_AND_FORGET] Відправлено: ${command}`);
    } else {
      rsocketRef.current.requestResponse(payload).subscribe({
        onComplete: (res: any) => console.log(`[ВІДПОВІДЬ] ${command}:`, res.data),
        onError: (err: any) => console.error(`[ПОМИЛКА] ${command}:`, err)
      });
    }
  };

  // Функція для зупинки конкретної задачі (за її schema_id)
  const stopTask = (schemaId: string) => {
    console.log(`Відправка команди на зупинку задачі: ${schemaId}`);
    
    // Відправляємо команду на Java сервер
    sendCommand('STOP_SCHEMA', schemaId);
    
    // Видаляємо задачу з візуального списку на дашборді
    setActiveTasks(prevTasks => prevTasks.filter(t => t.schema_id !== schemaId));
  };

  // ==========================================
  // ВЗАЄМОДІЯ З JAVA СЕРВЕРОМ (Топологія та Події)
  // ==========================================

  const fetchTopology = (client: any) => {
    client.requestResponse({ data: 'GET_TOPOLOGY', metadata: '' }).subscribe({
      onComplete: (payload: any) => {
        try {
          const topologyData = JSON.parse(payload.data);
          const connectedIds: string[] = topologyData.nodes || [];
          const connectedEdges: any[] = topologyData.edges || [];
          const activeStreams: string[] = topologyData.activeStreams || []; 
          
          setNodes((prevNodes) => {
            return connectedIds.map((id, index) => {
              const existingNode = prevNodes.find((n) => n.id === id);
              let icon = '📦'; let readableName = id; let customStyle: any = {};

              if (id.startsWith('NodePy')) {
                icon = '🐍'; readableName = `Python Sensor\n[${id.slice(-4)}]`; 
                customStyle = { border: '2px solid #3572A5' };
              } else if (id.startsWith('Seed-')) {
                icon = '👑'; readableName = `Java Seed`;
                customStyle = { border: '2px solid #ff0072', backgroundColor: '#3b1c28', boxShadow: '0 0 20px rgba(255, 0, 114, 0.4)' };
              } else if (id.startsWith('JavaNode-')) {
                icon = '☕'; readableName = `Java Compute\n[${id.slice(-4)}]`;
                customStyle = { border: '2px solid #ffba08' };
              } else if (id.startsWith('UI-Dashboard')) {
                icon = '📊'; readableName = `Dashboard`;
                customStyle = { border: '2px solid #17a2b8' };
              }

              const position = existingNode 
                ? existingNode.position 
                : { x: id.startsWith('Seed-') ? 400 : 50 + index * 250, y: id.startsWith('Seed-') ? 100 : 300 };

              return { id, type: 'meshNode', position, data: { label: readableName, icon, customStyle } };
            });
          });

          const dynamicEdges: Edge[] = connectedEdges.map((edgeInfo) => {
            const forwardKey = `${edgeInfo.source}->${edgeInfo.target}`;
            const reverseKey = `${edgeInfo.target}->${edgeInfo.source}`;
            const isActive = activeStreams.includes(forwardKey) || activeStreams.includes(reverseKey);

            let finalSource = edgeInfo.source;
            let finalTarget = edgeInfo.target;
            if (activeStreams.includes(reverseKey) && !activeStreams.includes(forwardKey)) {
                finalSource = edgeInfo.target;
                finalTarget = edgeInfo.source;
            }

            return {
              id: `edge-${edgeInfo.source}-${edgeInfo.target}`,
              source: finalSource, target: finalTarget, animated: isActive, 
              style: { stroke: isActive ? '#00ff00' : '#555', strokeWidth: isActive ? 3 : 2 }, 
            };
          });

          setEdges(dynamicEdges);
        } catch (error) { console.error("Помилка JSON:", error); }
      }
    });
  };

  const subscribeToEvents = (client: any) => {
    client.requestStream({ data: 'SUBSCRIBE_EVENTS', metadata: '' }).subscribe({
      onSubscribe: (sub: any) => sub.request(2147483647),
      onNext: (payload: any) => {
        try {
          const data = JSON.parse(payload.data);
          if (data.event === 'TOPOLOGY_CHANGED') fetchTopology(client);
        } catch (error) { console.error("Помилка події:", error); }
      }
    });
  };

  const connectToBackend = async () => {
    try {
      if (rsocketRef.current) rsocketRef.current.close();
      const TransportClass = (RSocketWebSocketClient as any).default || RSocketWebSocketClient;
      
      const client = new RSocketClient({
        setup: { 
          keepAlive: 10000, 
          lifetime: 30000, 
          dataMimeType: 'text/plain', 
          metadataMimeType: 'text/plain', 
          payload: { data: 'UI-Dashboard-Main', metadata: '' } 
        },
        transport: new TransportClass({ url: 'ws://192.168.50.11:7001' }),
      });

      const socket = await client.connect();
      rsocketRef.current = socket;
      setIsConnected(true);
      fetchTopology(socket);
      subscribeToEvents(socket);
    } catch (error) {
      setIsConnected(false);
    }
  };

  useEffect(() => {
    connectToBackend();
    return () => { if (rsocketRef.current) rsocketRef.current.close(); };
  }, []);

  // ==========================================
  // ЕКСПОРТ ДАНИХ ТА ФУНКЦІЙ У КОМПОНЕНТИ
  // ==========================================
  return { 
    nodes, edges, 
    onNodesChange, onEdgesChange, onConnectManual, 
    isConnected, reconnect: connectToBackend, 
    sendCommand,
    activeTasks, // Експортуємо задачі
    stopTask     // Експортуємо функцію зупинки
  };
};