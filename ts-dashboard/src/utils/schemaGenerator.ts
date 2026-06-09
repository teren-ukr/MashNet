import { type Node, type Edge } from '@xyflow/react';

export function generateComputationSchema(nodes: Node[], edges: Edge[]) {
    const inputSources: Record<string, string> = {};
    const pipelineStages: any[] = [];

    // 1. Знаходимо всі вузли-сенсори
    const sensorNodes = nodes.filter(n => n.type === 'sensorNode');
    
    // 2. Формуємо карту джерел (inputSources)
    sensorNodes.forEach(sensor => {
        const sensorId = sensor.data.sensorId as string;
        // Шукаємо ребро, яке виходить з цього сенсора
        const outgoingEdges = edges.filter(e => e.source === sensor.id);
        
        outgoingEdges.forEach(edge => {
            // targetHandle - це ID порту (наприклад, 'input-A' або 'default-input')
            const portId = edge.targetHandle || 'default-input';
            inputSources[portId] = sensorId;
        });
    });

    // 3. Формуємо масив етапів обробки (найпростіше топологічне сортування)
    // Виключаємо сенсори, залишаємо лише обчислювальні блоки
    const processingNodes = nodes.filter(n => n.type !== 'sensorNode');

    // Для надійності необхідно відсортувати вузли за порядком їх з'єднання.
    // У базовому варіанті (якщо користувач додає їх послідовно) можна використати існуючий масив.
    processingNodes.forEach(node => {
        pipelineStages.push({
            stage_id: node.id,
            operation: node.data.operation,
            parameters: node.data.parameters || {}
        });
    });

    return {
        schema_id: `pipeline-${Date.now()}`,
        input_sources: inputSources,
        output_sink: "Dashboard-UI",
        pipeline_stages: pipelineStages
    };
}