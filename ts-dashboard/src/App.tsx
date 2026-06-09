import { useState } from 'react';
import { ReactFlow, Background, Controls, MiniMap } from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { MeshNode } from './components/MashNode';
import { StatusBar } from './components/StatusBar';
import { ControlPanel } from './components/ControlPanel';
import { PipelineEditor } from './components/PipelineEditor';
import { useMeshNetwork } from './hooks/useMashNetwork';

const nodeTypes = { meshNode: MeshNode };

export default function App() {
  const { 
    nodes, edges, 
    onNodesChange, onEdgesChange, onConnectManual, 
    isConnected, reconnect, sendCommand 
  } = useMeshNetwork();

  // Стан для керування відображенням: null = Макро-рівень (Топологія), string = ID вузла (Мікро-рівень)
  const [selectedNodeForEdit, setSelectedNodeForEdit] = useState<string | null>(null);

  // Обробник подвійного кліку по вузлу
  const onNodeDoubleClick = (event: React.MouseEvent, node: any) => {
    // Дозволяємо редагувати лише обчислювальні вузли (Java), а не сенсори чи дашборд
    if (node.id.startsWith('JavaNode') || node.id.startsWith('Seed')) {
      setSelectedNodeForEdit(node.id);
    }
  };

  return (
    <div style={{ width: '100vw', height: '100vh', backgroundColor: '#1a192b' }}>
      
      <StatusBar isConnected={isConnected} onReconnect={reconnect} />
      
      {selectedNodeForEdit ? (
        /* МІКРО-РІВЕНЬ: Ізольований простір для редагування DSP-графа */
        <PipelineEditor 
          nodeId={selectedNodeForEdit} 
          onClose={() => setSelectedNodeForEdit(null)} 
          onDeploy={(schemaJson) => {
             // Формуємо структуру, яку очікує Java бекенд для DEPLOY_SCHEMA
             const requestPayload = JSON.stringify({
                 targetNode: selectedNodeForEdit,
                 schema: JSON.parse(schemaJson)
             });
             sendCommand('DEPLOY_SCHEMA', requestPayload);
          }}
        />
      ) : (
        /* МАКРО-РІВЕНЬ: Глобальна топологія мережі */
        <>
          <ControlPanel nodes={nodes} onSendCommand={sendCommand} />
          <ReactFlow 
            nodes={nodes} 
            edges={edges} 
            onNodesChange={onNodesChange} 
            onEdgesChange={onEdgesChange} 
            onConnect={onConnectManual}
            onNodeDoubleClick={onNodeDoubleClick}
            nodeTypes={nodeTypes}
            colorMode="dark" 
            fitView 
          >
            <Background color="#555" gap={16} />
            <Controls />
            <MiniMap nodeColor="#00ff00" nodeStrokeWidth={3} zoomable pannable />
          </ReactFlow>
        </>
      )}
    </div>
  );
}