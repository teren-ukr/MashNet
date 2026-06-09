import { Handle, Position } from '@xyflow/react';
import React from 'react';

export function DspOperationNode({ data }: { data: any }) {
    return (
        <div style={{ padding: 10, border: '1px solid #777', borderRadius: 5, background: '#222', color: '#fff' }}>
            <Handle type="target" position={Position.Top} />
            
            <div style={{ fontWeight: 'bold', marginBottom: 8 }}>
                {data.label || 'DSP Operation'}
            </div>
            
            {/* Приклад поля для параметризації */}
            <div style={{ fontSize: '12px' }}>
                <label>Threshold:</label>
                <input 
                    type="number" 
                    defaultValue={data.parameters?.threshold || 0}
                    onChange={(e) => {
                        // Оновлення внутрішнього стану вузла для подальшої серіалізації
                        if (data.onParameterChange) {
                            data.onParameterChange('threshold', parseFloat(e.target.value));
                        }
                    }}
                    style={{ width: '100%', marginTop: 4, background: '#333', color: '#fff', border: '1px solid #555' }}
                />
            </div>

            <Handle type="source" position={Position.Bottom} />
        </div>
    );
}