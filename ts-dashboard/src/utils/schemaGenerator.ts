import { type Node, type Edge } from '@xyflow/react';

export function generateComputationSchema(nodes: Node[], edges: Edge[]) {
    const inputSources: Record<string, string> = {};
    const pipelineStages: any[] = [];

    // 1. Знаходимо всі вузли-джерела
    const sourceNodes = nodes.filter(n => n.type === 'sensorNode' || n.type === 'networkSourceNode');
    
    sourceNodes.forEach(node => {
        let sourceValue = '';
        if (node.type === 'sensorNode') {
            sourceValue = node.data.sensorId as string;
        } else if (node.type === 'networkSourceNode') {
            sourceValue = `STREAM:${node.data.targetNode}:${node.data.streamId}`;
        }

        const outgoingEdges = edges.filter(e => e.source === node.id);
        
        outgoingEdges.forEach(edge => {
            const portId = edge.targetHandle || 'default-input';
            inputSources[portId] = sourceValue;
        });
    });

    // 2. Блоки обробки (ТУТ МИ БІЛЬШЕ НЕ ВИКЛЮЧАЄМО ВІЗУАЛІЗАТОР)
    const processingNodes = nodes.filter(n => n.type !== 'sensorNode' && n.type !== 'networkSourceNode');

    processingNodes.forEach(node => {
        if (node.type === 'visualizerNode') {
            // МАГІЯ: Перетворюємо Осцилограф на прихований NETWORK_SINK для Java-сервера
            pipelineStages.push({
                stage_id: node.id,
                operation: 'NETWORK_SINK',
                parameters: { streamId: `vis-${node.id}` }
            });
        } else {
            pipelineStages.push({
                stage_id: node.id,
                operation: node.data.operation || 'NETWORK_SINK',
                parameters: node.data.parameters || {}
            });
        }
    });

    return {
        schema_id: `pipeline-${Date.now()}`,
        input_sources: inputSources,
        output_sink: "Dashboard-UI",
        pipeline_stages: pipelineStages
    };
}