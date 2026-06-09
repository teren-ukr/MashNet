import { ReactFlow, Background, Controls, MiniMap } from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { MeshNode } from './components/MashNode';
import { StatusBar } from './components/StatusBar';
import { ControlPanel } from './components/ControlPanel'; // <--- Імпорт
import { useMeshNetwork } from './hooks/useMashNetwork';

const nodeTypes = { meshNode: MeshNode };

export default function App() {
  const { 
    nodes, edges, 
    onNodesChange, onEdgesChange, onConnectManual, 
    isConnected, reconnect, sendCommand 
  } = useMeshNetwork();

  return (
    <div style={{ width: '100vw', height: '100vh', backgroundColor: '#1a192b' }}>
      
      <StatusBar isConnected={isConnected} onReconnect={reconnect} />
      
      {/* Додаємо панель керування */}
      <ControlPanel nodes={nodes} onSendCommand={sendCommand} />

      <ReactFlow 
        nodes={nodes} edges={edges} 
        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnectManual}
        nodeTypes={nodeTypes}
        colorMode="dark" fitView 
      >
        <Background color="#555" gap={16} />
        <Controls />
        <MiniMap nodeColor="#00ff00" nodeStrokeWidth={3} zoomable pannable />
      </ReactFlow>
    </div>
  );
}