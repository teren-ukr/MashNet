import { Handle, Position, type NodeProps } from '@xyflow/react';

export function SensorNode({ data }: NodeProps) {
    return (
        <div style={{ padding: 10, border: '1px solid #4CAF50', borderRadius: 5, background: '#222', color: '#fff', width: 150 }}>
            <div style={{ fontWeight: 'bold', fontSize: '12px', marginBottom: 8, color: '#4CAF50' }}>
                Джерело (Сенсор)
            </div>
            <div style={{ fontSize: '10px' }}>
                <label>ID Вузла:</label>
                <input 
                    type="text" 
                    defaultValue={data.sensorId as string || 'NodePy-XXXX'}
                    onChange={(e) => data.onChange && (data.onChange as Function)('sensorId', e.target.value)}
                    style={{ width: '100%', marginTop: 4, background: '#333', color: '#fff', border: '1px solid #555' }}
                />
            </div>
            <Handle type="source" position={Position.Bottom} id="default-output" />
        </div>
    );
}