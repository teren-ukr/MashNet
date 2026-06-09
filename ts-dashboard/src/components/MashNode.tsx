import { Handle, Position } from '@xyflow/react';

export const MeshNode = ({ data }: any) => {
  // За замовчуванням робимо хоча б один вхід/вихід, якщо даних ще немає
  const inputs = data.inputs || [{ id: 'default-in', label: 'In' }];
  const outputs = data.outputs || [{ id: 'default-out', label: 'Out' }];

  return (
    <div style={{
      padding: '15px 20px',
      borderRadius: '8px',
      background: data.customStyle?.backgroundColor || '#2b2b36',
      border: data.customStyle?.border || '2px solid #555',
      boxShadow: data.customStyle?.boxShadow || '0 4px 6px rgba(0,0,0,0.3)',
      color: data.customStyle?.color || '#fff',
      minWidth: '160px',
      textAlign: 'center',
      fontFamily: 'monospace',
      position: 'relative'
    }}>
      
      {/* ГЕНЕРУЄМО N ВХОДІВ (Зліва) */}
      {inputs.map((input: any, index: number) => {
        // Вираховуємо відступ зверху, щоб вони були рівномірно розподілені
        const topPosition = inputs.length === 1 ? '50%' : `${20 + (index * 60 / (inputs.length - 1))}%`;
        
        return (
          <div key={`in-${input.id}`}>
            <Handle 
              type="target" 
              position={Position.Left} 
              id={input.id} // ВАЖЛИВО: Унікальний ID для конектора
              style={{ top: topPosition, width: '12px', height: '12px', background: '#00ff00', border: '2px solid #222' }} 
            />
            {/* Маленький підпис біля конектора (опціонально) */}
            <span style={{ position: 'absolute', left: '10px', top: `calc(${topPosition} - 8px)`, fontSize: '10px', color: '#888' }}>
              {input.label}
            </span>
          </div>
        );
      })}

      <div style={{ fontSize: '24px', marginBottom: '8px', marginTop: '10px' }}>{data.icon}</div>
      <div style={{ fontSize: '14px', fontWeight: 'bold', marginBottom: '10px' }}>{data.label}</div>

      {/* ГЕНЕРУЄМО N ВИХОДІВ (Справа) */}
      {outputs.map((output: any, index: number) => {
        const topPosition = outputs.length === 1 ? '50%' : `${20 + (index * 60 / (outputs.length - 1))}%`;
        
        return (
          <div key={`out-${output.id}`}>
            <Handle 
              type="source" 
              position={Position.Right} 
              id={output.id} // ВАЖЛИВО: Унікальний ID
              style={{ top: topPosition, width: '12px', height: '12px', background: '#ff0072', border: '2px solid #222' }} 
            />
            <span style={{ position: 'absolute', right: '10px', top: `calc(${topPosition} - 8px)`, fontSize: '10px', color: '#888' }}>
              {output.label}
            </span>
          </div>
        );
      })}
    </div>
  );
};