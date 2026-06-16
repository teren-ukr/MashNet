import { Handle, Position, type NodeProps, useReactFlow } from '@xyflow/react';
import React from 'react';

export function MathNode({ id, data }: NodeProps) {
    // Дістаємо функцію оновлення безпосередньо з React Flow
    const { updateNodeData } = useReactFlow();

    const handleOperationChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
        // Оновлюємо стан вузла (його operation) напряму
        updateNodeData(id, { operation: event.target.value });
    };

    return (
        <div style={{ padding: 10, border: '1px solid #2196F3', borderRadius: 5, background: '#222', color: '#fff', width: 150 }}>
            {/* Вхід ліворуч */}
            <Handle type="target" position={Position.Left} id="default-input" style={{ top: '50%' }} />
            
            <div style={{ fontWeight: 'bold', fontSize: '11px', textAlign: 'center', marginBottom: 8, color: '#2196F3' }}>
                Математична операція
            </div>
            
            <select 
                value={data.operation as string || 'AVERAGE'} 
                onChange={handleOperationChange}
                style={{ width: '100%', padding: '4px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '3px', fontSize: '11px' }}
            >
                <option value="AVERAGE">AVERAGE</option>
                <option value="MAX">MAX</option>
                <option value="MULTIPLY">MULTIPLY</option>
            </select>
            
            {/* Вихід праворуч */}
            <Handle type="source" position={Position.Right} id="default-output" style={{ top: '50%' }} />
        </div>
    ); 
}