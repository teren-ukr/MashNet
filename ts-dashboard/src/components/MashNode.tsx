import { Handle, Position } from '@xyflow/react';

export const MeshNode = ({ id, data }: any) => {
  // Визначаємо тип ноди безпосередньо за ID
  const isPython = id.startsWith('NodePy');
  const isDashboard = id.includes('Dashboard') || id.includes('UI');
  const isSeed = id.startsWith('Seed');

  const inputs = data.inputs || [{ id: 'default-in', label: 'In' }];
  const outputs = data.outputs || [{ id: 'default-out', label: 'Out' }];

  // Визначаємо іконку та колір, якщо вони не прийшли в data
  const icon = isDashboard ? '📊' : (isPython ? '🐍' : '☕');
  const borderColor = isDashboard ? '#9C27B0' : (isPython ? '#4CAF50' : '#2196F3');

  return (
    <div style={{
      padding: '15px 20px',
      borderRadius: '8px',
      background: '#2b2b36',
      border: `2px solid ${borderColor}`,
      boxShadow: '0 4px 6px rgba(0,0,0,0.3)',
      color: '#fff',
      minWidth: '200px',
      textAlign: 'center',
      fontFamily: 'monospace',
      position: 'relative'
    }}>
      
      {/* ВХОДИ */}
      {!isPython && inputs.map((input: any, index: number) => {
        const topPosition = inputs.length === 1 ? '50%' : `${20 + (index * 60 / (inputs.length - 1))}%`;
        return (
          <div key={`in-${input.id}`}>
            <Handle 
              type="target" 
              position={Position.Left} 
              id={input.id} 
              style={{ top: topPosition, width: '12px', height: '12px', background: '#00ff00', border: '2px solid #222' }} 
            />
          </div>
        );
      })}

      <div style={{ fontSize: '24px', marginBottom: '8px', marginTop: '5px' }}>{icon}</div>
      <div style={{ fontSize: '14px', fontWeight: 'bold', marginBottom: '5px' }}>
        {isDashboard ? 'Dashboard UI' : data.label}
      </div>

      {/* ПОВНИЙ ID ВУЗЛА (Системний) */}
      <div style={{ 
          fontSize: '10px', 
          color: '#aaa', 
          background: '#1a1a24', 
          padding: '4px', 
          borderRadius: '4px',
          wordBreak: 'break-all',
          marginTop: '5px'
      }}>
        {id}
      </div>

      {/* ВИХОДИ */}
      {!isDashboard && outputs.map((output: any, index: number) => {
        const topPosition = outputs.length === 1 ? '50%' : `${20 + (index * 60 / (outputs.length - 1))}%`;
        return (
          <div key={`out-${output.id}`}>
            <Handle 
              type="source" 
              position={Position.Right} 
              id={output.id} 
              style={{ top: topPosition, width: '12px', height: '12px', background: '#ff0072', border: '2px solid #222' }} 
            />
          </div>
        );
      })}
    </div>
  );
};