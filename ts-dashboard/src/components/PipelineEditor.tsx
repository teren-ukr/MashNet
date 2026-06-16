import React, { useState, useCallback, useMemo } from 'react';
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
import { NetworkSinkNode } from '../nodes/NetworkSinkNode';
import { NetworkSourceNode } from '../nodes/NetworkSourceNode';
import { VisualizerNode } from '../nodes/VisualizerNode';

const pipelineNodeTypes = {
    sensorNode:         SensorNode,
    mathNode:           MathNode,
    correlationNode:    CorrelationNode,
    networkSinkNode:    NetworkSinkNode,
    networkSourceNode:  NetworkSourceNode,
    visualizerNode:     VisualizerNode
};

interface PipelineEditorProps {
    nodeId: string;
    initialNodes: Node[];
    initialEdges: Edge[];
    macroNodes: Node[]; 
    onClose: (currentNodes: Node[], currentEdges: Edge[]) => void;
    onDeploy: (schemaJson: string) => void;
    onStartStream?: (sourceId: string, visualizerId: string) => void; 
    onStopStream?: (visualizerId: string) => void;
}

function EditorCanvas({ nodeId, initialNodes, initialEdges, macroNodes, onClose, onDeploy, onStartStream, onStopStream }: PipelineEditorProps) {
    const [nodes, setNodes] = useState<Node[]>(initialNodes);
    const [edges, setEdges] = useState<Edge[]>(initialEdges);

    // Патерн Auto-discovery для Python-сенсорів
    const availableSensors = useMemo(() => {
        return macroNodes
            .filter(n => n.id.startsWith('NodePy'))
            .map(n => n.id);
    }, [macroNodes]);

    // Патерн Auto-discovery для Java-серверів
    const availableJavaNodes = useMemo(() => {
        return macroNodes
            .filter(n => n.id.startsWith('JavaNode') || n.id.startsWith('Seed'))
            .map(n => n.id);
    }, [macroNodes]);

    const onNodesChange = useCallback((changes: any) => setNodes((nds) => applyNodeChanges(changes, nds)), []);
    const onEdgesChange = useCallback((changes: any) => setEdges((eds) => applyEdgeChanges(changes, eds)), []);
    const onConnect = useCallback((connection: Connection) => setEdges((eds) => addEdge(connection, eds)), []);

    // Фабричний метод створення вузлів обробки
    const addNode = (type: string, operationLabel: string) => {
        const newNode: Node = {
            id: `node-${Date.now()}`,
            type: type,
            position: { x: 250 + Math.random() * 50, y: 150 + Math.random() * 50 },
            data: { 
                operation: operationLabel,
                parameters: {}
            }
        };
        setNodes((nds) => [...nds, newNode]);
    };

    const updateNodeField = useCallback((id: string, field: string, value: any) => {
        setNodes(nds => nds.map(n => n.id === id ? { ...n, data: { ...n.data, [field]: value } } : n));
    }, []);

    // Декоратор (Data Mapper): динамічно насичує вузли актуальними реєстрами Discovery
    const nodesWithDynamicData = useMemo(() => {
        return nodes.map(node => {
            if (node.type === 'sensorNode') {
                return { ...node, data: { ...node.data, availableSensors, onChange: (field: string, value: string) => updateNodeField(node.id, field, value) } };
            }
            if (node.type === 'networkSourceNode') {
                return { ...node, data: { ...node.data, availableNodes: availableJavaNodes, onChange: (field: string, value: string) => updateNodeField(node.id, field, value) } };
            }
            
            // ВЧИМО ОСЦИЛОГРАФ ЧИТАТИ ДРОТИ
            if (node.type === 'visualizerNode') {
                const incomingEdge = edges.find(e => e.target === node.id);
                let sourceName = '';

                if (incomingEdge) {
                    const sourceNode = nodes.find(n => n.id === incomingEdge.source);
                    if (sourceNode) {
                        if (sourceNode.type === 'sensorNode') {
                            sourceName = `Сенсор ${(sourceNode.data.sensorId as string || '').slice(-4)}`;
                        } else if (sourceNode.type === 'mathNode') {
                            sourceName = `${sourceNode.data.operation || 'Обробка'}`;
                        } else {
                            sourceName = `Вузол підключено`;
                        }
                    }
                }

                return {
                    ...node,
                    data: { 
                        ...node.data, 
                        // ОСЬ ГОЛОВНА ЗМІНА: Кажемо осцилографу слухати свій власний 
                        // прихований потік, який згенерував schemaGenerator!
                        connectedSourceId: `vis-${node.id}`, 
                        connectedSourceName: sourceName, 
                        availableSources: availableJavaNodes, 
                        onStart: onStartStream,
                        onStop: onStopStream
                    }
                };
            }
            return node;
        });
    }, [nodes, edges, availableSensors, availableJavaNodes, updateNodeField, onStartStream, onStopStream]);

    const handleDeploy = () => {
        const schema = generateComputationSchema(nodes, edges);
        console.log("[DASHBOARD] Згенерована схема (Об'єкт):", schema);
        
        onDeploy(JSON.stringify(schema));
    };

    const handleClear = () => {
        setNodes([]);
        setEdges([]);
    };

    return (
        <div style={{ width: '100%', height: 'calc(100vh - 40px)', position: 'relative' }}>
            <div style={{ 
                position: 'absolute', top: 10, left: 10, zIndex: 10, 
                display: 'flex', gap: '10px', alignItems: 'center',
                background: '#222', padding: '10px', borderRadius: '8px', border: '1px solid #444'
            }}>
                <button 
                    onClick={() => onClose(nodes, edges)} 
                    style={{ padding: '6px 12px', background: '#444', color: 'white', border: '1px solid #666', borderRadius: '4px', cursor: 'pointer' }}
                >
                    ← Назад до топології
                </button>
                <div style={{ width: '1px', height: '24px', background: '#555', margin: '0 5px' }}></div>
                
                <button onClick={() => addNode('sensorNode', 'SENSOR')} style={{ padding: '6px 12px', background: '#2E7D32', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                    + Сенсор
                </button>
                <button onClick={() => addNode('correlationNode', 'CORRELATION')} style={{ padding: '6px 12px', background: '#C62828', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                    + Корелятор
                </button>
                <button onClick={() => addNode('visualizerNode', 'VISUALIZE')} style={{ padding: '6px 12px', background: '#FBC02D', color: '#000', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold' }}>
                    + Осцилограф
                </button>
                <button onClick={() => addNode('mathNode', 'AVERAGE')} style={{ padding: '6px 12px', background: '#1565C0', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                    + Математика
                </button>
                <button onClick={() => addNode('networkSinkNode', 'NETWORK_SINK')} style={{ padding: '6px 12px', background: '#7B1FA2', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                    + Мережевий Sink
                </button>
                <button onClick={() => addNode('networkSourceNode', 'NETWORK_SOURCE')} style={{ padding: '6px 12px', background: '#0097A7', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                    + Мережеве Джерело
                </button>

                <div style={{ width: '1px', height: '24px', background: '#555', margin: '0 5px' }}></div>
                <button onClick={handleClear} style={{ padding: '6px 12px', background: '#FF9800', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}>
                    Очистити
                </button>
                <button onClick={handleDeploy} style={{ padding: '6px 16px', background: '#4CAF50', color: 'white', border: 'none', borderRadius: '4px', fontWeight: 'bold', cursor: 'pointer' }}>
                    Deploy
                </button>
                <span style={{ color: '#aaa', marginLeft: '10px', fontFamily: 'monospace', fontSize: '12px' }}>
                    Вузол: {nodeId}
                </span>
            </div>

            <ReactFlow
                nodes={nodesWithDynamicData} 
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                nodeTypes={pipelineNodeTypes}
                colorMode="dark"
                fitView
            >
                <Background color="#333" gap={16} variant={BackgroundVariant.Dots} />
                <Controls />
            </ReactFlow>
        </div>
    );
}

export function PipelineEditor(props: PipelineEditorProps) {
    return (
        <ReactFlowProvider>
            <EditorCanvas {...props} />
        </ReactFlowProvider>
    );
}