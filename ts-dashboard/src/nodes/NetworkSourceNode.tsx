import { Handle, Position, type NodeProps } from '@xyflow/react';
import React from 'react';

export function NetworkSourceNode({ id, data }: NodeProps) {
    // Отримуємо список доступних обчислювальних вузлів з макро-топології
    const availableNodes: string[] = (data.availableNodes as string[]) || [];

    return (
        <div style={{ padding: 10, border: '1px solid #00BCD4', borderRadius: 5, background: '#222', color: '#fff', width: 180 }}>
            <div style={{ fontWeight: 'bold', fontSize: '12px', marginBottom: 8, color: '#00BCD4' }}>
                Мережеве Джерело (Source)
            </div>
            
            <div style={{ fontSize: '10px', marginBottom: 6 }}>
                <label>Цільовий Java-Вузол:</label>
                <select 
                    value={data.targetNode as string || ''}
                    onChange={(e) => data.onChange && (data.onChange as Function)('targetNode', e.target.value)}
                    style={{ width: '100%', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '3px', marginTop: '2px' }}
                >
                    <option value="">-- Оберіть сервер --</option>
                    {availableNodes.map(nodeId => (
                        <option key={nodeId} value={nodeId}>{nodeId}</option>
                    ))}
                </select>
            </div>
            
            <div style={{ fontSize: '10px' }}>
                <label>Stream ID (Токен):</label>
                <input 
                    type="text" 
                    placeholder="напр., my-calc-stream"
                    value={data.streamId as string || ''}
                    onChange={(e) => data.onChange && (data.onChange as Function)('streamId', e.target.value)}
                    style={{ width: '100%', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '3px', marginTop: '2px' }}
                />
            </div>
            
            <Handle type="source" position={Position.Right} id="default-output" />
        </div>
    );
}