import { Handle, Position, type NodeProps } from '@xyflow/react';

export function CorrelationNode({ data }: NodeProps) {
    return (
        <div style={{ padding: 10, border: '1px solid #f44336', borderRadius: 5, background: '#222', color: '#fff', width: 180 }}>
            <div style={{ position: 'relative', height: '60px' }}>
                <div style={{ position: 'absolute', top: '10px', left: '-15px', fontSize: '10px' }}>
                    <Handle type="target" position={Position.Left} id="input-A" style={{ top: '15px' }} />
                    Вхід A
                </div>
                <div style={{ position: 'absolute', top: '35px', left: '-15px', fontSize: '10px' }}>
                    <Handle type="target" position={Position.Left} id="input-B" style={{ top: '40px' }} />
                    Вхід B
                </div>
                
                <div style={{ textAlign: 'center', fontWeight: 'bold', fontSize: '12px', paddingTop: '20px' }}>
                    {data.operation as string || 'CORRELATION'}
                </div>
            </div>
            
            <Handle type="source" position={Position.Right} id="tdoa-output" style={{ top: '30px' }} />
        </div>
    );
}