import { Handle, Position, type NodeProps } from '@xyflow/react';

export function SensorNode({ data }: NodeProps) {
    // Отримуємо список сенсорів, які дашборд знайшов у глобальній топології
    const availableSensors = (data.availableSensors as string[]) || [];

    return (
        <div style={{ 
            padding: 10, 
            border: '1px solid #4CAF50', 
            borderRadius: 5, 
            background: '#222', 
            color: '#fff', 
            width: 180 
        }}>
            <div style={{ fontWeight: 'bold', fontSize: '12px', marginBottom: 8, color: '#4CAF50' }}>
                Джерело (Сенсор)
            </div>
            <div style={{ fontSize: '10px' }}>
                <label>Оберіть активний вузол:</label>
                <select 
                    value={data.sensorId as string || ''}
                    onChange={(e) => data.onChange && (data.onChange as Function)('sensorId', e.target.value)}
                    style={{ 
                        width: '100%', 
                        marginTop: 4, 
                        background: '#333', 
                        color: '#fff', 
                        border: '1px solid #555',
                        padding: '4px',
                        borderRadius: '3px'
                    }}
                >
                    <option value="">-- Оберіть ID --</option>
                    {availableSensors.map(id => (
                        <option key={id} value={id}>{id}</option>
                    ))}
                </select>
            </div>
            
            {/* Міняємо позицію Handle на Right для уніфікації Left-to-Right */}
            <Handle type="source" position={Position.Right} id="default-output" />
        </div>
    );
}