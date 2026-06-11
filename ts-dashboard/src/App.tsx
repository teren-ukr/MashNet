import { useState } from 'react';
import { ReactFlow, Background, Controls, MiniMap, type Node, type Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { MeshNode } from './components/MashNode';
import { StatusBar } from './components/StatusBar';
import { ControlPanel } from './components/ControlPanel';
import { PipelineEditor } from './components/PipelineEditor';
import { useMeshNetwork } from './hooks/useMashNetwork';

const nodeTypes = { meshNode: MeshNode };

// Структура для зберігання стану одного ізольованого конвеєра
interface StoredPipeline {
  nodes: Node[];
  edges: Edge[];
}

export default function App() {
  const { 
    nodes: macroNodes, edges: macroEdges, 
    onNodesChange, onEdgesChange, onConnectManual, 
    isConnected, reconnect, sendCommand 
  } = useMeshNetwork();

  const [selectedNodeForEdit, setSelectedNodeForEdit] = useState<string | null>(null);

  // Глобальний реєстр пайплайнів для всіх нод мережі (Key: nodeId, Value: StoredPipeline)
  const [savedPipelines, setSavedPipelines] = useState<Record<string, StoredPipeline>>({});

  const onNodeDoubleClick = (_event: React.MouseEvent, node: any) => {
    if (node.id.startsWith('JavaNode') || node.id.startsWith('Seed')) {
      setSelectedNodeForEdit(node.id);
    }
  };

  // Метод для збереження стану графа конкретної ноди в пам'яті дашборду
  const handleSavePipelineLayout = (nodeId: string, currentNodes: Node[], currentEdges: Edge[]) => {
    setSavedPipelines((prev) => ({
      ...prev,
      [nodeId]: { nodes: currentNodes, edges: currentEdges }
    }));
  };

  return (
    <div style={{ width: '100vw', height: '100vh', backgroundColor: '#1a192b' }}>
      
      <StatusBar isConnected={isConnected} onReconnect={reconnect} />
      
      {selectedNodeForEdit ? (
        <PipelineEditor 
          nodeId={selectedNodeForEdit}
          // Передаємо раніше збережені вузли або порожній масив, якщо нода редагується вперше
          initialNodes={savedPipelines[selectedNodeForEdit]?.nodes || []}
          initialEdges={savedPipelines[selectedNodeForEdit]?.edges || []}
          macroNodes={macroNodes} // Передаємо макро-вузли для отримання списку сенсорів
          onClose={(currentNodes, currentEdges) => {
            // Перед закриттям фіксуємо візуальну структуру в батьківському стані
            handleSavePipelineLayout(selectedNodeForEdit, currentNodes, currentEdges);
            setSelectedNodeForEdit(null);
          }} 
          onDeploy={(schemaJson) => {
            const requestPayload = JSON.stringify({
                targetNode: selectedNodeForEdit,
                schema: JSON.parse(schemaJson)
            });
            sendCommand('DEPLOY_SCHEMA', requestPayload);
          }}
        />
      ) : (
        <>
          <ControlPanel nodes={macroNodes} onSendCommand={sendCommand} />
          <ReactFlow 
            nodes={macroNodes} 
            edges={macroEdges} 
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