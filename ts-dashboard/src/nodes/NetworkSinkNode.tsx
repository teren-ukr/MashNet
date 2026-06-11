import { Handle, Position, type NodeProps, useReactFlow } from '@xyflow/react';
import React from 'react';

export function NetworkSinkNode({ id, data }: NodeProps) {
    const { updateNodeData } = useReactFlow();

    const handleStreamIdChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        // Зберігаємо введене ім'я потоку в параметри вузла
        updateNodeData(id, { 
            parameters: { ...((data.parameters as object) || {}), streamId: event.target.value } 
        });
    };

    return (
        <div style={{ padding: 10, border: '1px solid #9C27B0', borderRadius: 5, background: '#222', color: '#fff', width: 170 }}>
            <Handle type="target" position={Position.Left} id="default-input" />
            
            <div style={{ fontWeight: 'bold', fontSize: '12px', marginBottom: 8, color: '#9C27B0', textAlign: 'right' }}>
                Мережевий Вихід (Sink)
            </div>
            
            <div style={{ fontSize: '10px' }}>
                <label>Stream ID (Токен):</label>
                <input 
                    type="text" 
                    placeholder="напр., my-calc-stream"
                    defaultValue={(data.parameters as any)?.streamId || ''}
                    onChange={handleStreamIdChange}
                    style={{ width: '100%', marginTop: 4, background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '3px' }}
                />
            </div>
            {/* Цей вузол не має вихідного порту, оскільки він передає дані в мережу, а не на наступний блок */}
        </div>
    );
}