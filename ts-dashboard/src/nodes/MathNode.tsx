import { Handle, Position, type NodeProps, useReactFlow } from '@xyflow/react';
import React from 'react';

export function MathNode({ id, data }: NodeProps) {
    const { updateNodeData } = useReactFlow();

    const handleOperationChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
        // Оновлення внутрішнього стану графа при виборі нової операції
        updateNodeData(id, { operation: event.target.value });
    };

    return (
        <div style={{ padding: 10, border: '1px solid #2196F3', borderRadius: 5, background: '#222', color: '#fff', width: 150 }}>
            <Handle type="target" position={Position.Top} id="default-input" />
            
            <div style={{ fontWeight: 'bold', fontSize: '12px', textAlign: 'center', marginBottom: 8, color: '#2196F3' }}>
                Математична операція
            </div>
            
            <select 
                value={data.operation as string || 'AVERAGE'} 
                onChange={handleOperationChange}
                style={{ width: '100%', padding: '4px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '3px' }}
            >
                <option value="AVERAGE">AVERAGE</option>
                <option value="MAX">MAX</option>
                <option value="MULTIPLY">MULTIPLY</option>
            </select>
            
            <Handle type="source" position={Position.Bottom} id="default-output" />
        </div>
    );
}