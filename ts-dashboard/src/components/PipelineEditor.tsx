import React, { useState, useCallback } from 'react';
import { 
    ReactFlow, Background, BackgroundVariant, Controls, 
    addEdge, applyNodeChanges, applyEdgeChanges, 
    type Node, type Edge, type Connection,
    ReactFlowProvider
} from '@xyflow/react';

import { SensorNode } from '../nodes/SensorNode';
import { MathNode } from '../nodes/MathNode';
import { CorrelationNode } from '../nodes/CorrelationNode';
import { generateComputationSchema } from '../utils/schemaGenerator';

const pipelineNodeTypes = {
    sensorNode: SensorNode,
    mathNode: MathNode,
    correlationNode: CorrelationNode
};

interface PipelineEditorProps {
    nodeId: string;
    onClose: () => void;
    onDeploy: (schemaJson: string) => void;
}

// Внутрішній компонент, який використовує контекст ReactFlowProvider
function EditorCanvas({ nodeId, onClose, onDeploy }: PipelineEditorProps) {
    // Початковий стан тепер порожній
    const [nodes, setNodes] = useState<Node[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);

    const onNodesChange = useCallback((changes: any) => setNodes((nds) => applyNodeChanges(changes, nds)), []);
    const onEdgesChange = useCallback((changes: any) => setEdges((eds) => applyEdgeChanges(changes, eds)), []);
    const onConnect = useCallback((connection: Connection) => setEdges((eds) => addEdge(connection, eds)), []);

    // Функція-фабрика для створення нових вузлів
    const addNode = (type: string, operationLabel: string) => {
        const newNode: Node = {
            id: `node-${Date.now()}`,
            type: type,
            // Додаємо вузли по центру екрана з невеликим випадковим зсувом
            position: { x: 250 + Math.random() * 50, y: 150 + Math.random() * 50 },
            data: { operation: operationLabel }
        };
        setNodes((nds) => [...nds, newNode]);
    };

    const handleDeploy = () => {
        const schema = generateComputationSchema(nodes, edges);
        console.log("[DASHBOARD] Згенерована схема:", schema);
        onDeploy(JSON.stringify(schema));
    };

    const handleClear = () => {
        setNodes([]);
        setEdges([]);
    };

    return (
        <div style={{ width: '100%', height: 'calc(100vh - 40px)', position: 'relative' }}>
            {/* Головна панель керування */}
            <div style={{ 
                position: 'absolute', top: 10, left: 10, zIndex: 10, 
                display: 'flex', gap: '10px', alignItems: 'center',
                background: '#222', padding: '10px', borderRadius: '8px', border: '1px solid #444'
            }}>
                <button onClick={onClose} style={{ padding: '6px 12px', background: '#444', color: 'white', border: '1px solid #666', borderRadius: '4px' }}>
                    ← Назад
                </button>
                <div style={{ width: '1px', height: '24px', background: '#555', margin: '0 5px' }}></div>
                
                {/* Панель генерації блоків */}
                <button onClick={() => addNode('sensorNode', 'SENSOR')} style={{ padding: '6px 12px', background: '#2E7D32', color: 'white', border: 'none', borderRadius: '4px' }}>
                    + Сенсор
                </button>
                <button onClick={() => addNode('correlationNode', 'CORRELATION')} style={{ padding: '6px 12px', background: '#C62828', color: 'white', border: 'none', borderRadius: '4px' }}>
                    + Корелятор
                </button>
                <button onClick={() => addNode('mathNode', 'AVERAGE')} style={{ padding: '6px 12px', background: '#1565C0', color: 'white', border: 'none', borderRadius: '4px' }}>
                    + Математика
                </button>

                <div style={{ width: '1px', height: '24px', background: '#555', margin: '0 5px' }}></div>
                <button onClick={handleClear} style={{ padding: '6px 12px', background: '#FF9800', color: 'white', border: 'none', borderRadius: '4px' }}>
                    Очистити
                </button>
                <button onClick={handleDeploy} style={{ padding: '6px 16px', background: '#4CAF50', color: 'white', border: 'none', borderRadius: '4px', fontWeight: 'bold' }}>
                    Deploy
                </button>
            </div>

            <ReactFlow
                nodes={nodes} edges={edges}
                onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect}
                nodeTypes={pipelineNodeTypes}
                colorMode="dark" fitView
            >
                <Background color="#333" gap={16} variant={BackgroundVariant.Dots} />
                <Controls />
            </ReactFlow>
        </div>
    );
}

// Обгортка провайдера для доступу до хуків React Flow всередині кастомних нод
export function PipelineEditor(props: PipelineEditorProps) {
    return (
        <ReactFlowProvider>
            <EditorCanvas {...props} />
        </ReactFlowProvider>
    );
}