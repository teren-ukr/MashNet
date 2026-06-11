import React, { useState } from 'react';
import { type Node } from '@xyflow/react';

interface ControlPanelProps {
    nodes: Node[];
    onSendCommand: (command: string, payload?: string, targetNodeId?: string) => void;
}

export function ControlPanel({ nodes, onSendCommand }: ControlPanelProps) {
    const [selectedNode, setSelectedNode] = useState<string>('');

    const handleGlobalStop = () => {
        if (window.confirm('Ви впевнені, що хочете зупинити всі обчислення в мережі?')) {
            // Відправляємо команду STOP_ALL. Наш Java-бекенд має обробити її як FireAndForget
            onSendCommand('STOP_ALL');
        }
    };

    const handleSync = () => {
        // Примусове оновлення топології
        onSendCommand('GET_TOPOLOGY');
    };

    const handleNodeAction = (action: string) => {
        if (!selectedNode) {
            alert('Будь ласка, оберіть цільовий вузол.');
            return;
        }
        // Відправляємо команду (STOP або RESET) конкретному вузлу через RSocket-маршрутизатор
        onSendCommand(action, '', selectedNode);
    };

    // Фільтруємо вузли: прибираємо сам дашборд з випадаючого списку
    const availableTargets = nodes.filter(n => !n.id.includes('Dashboard') && !n.id.includes('UI'));

    return (
        <div style={{ 
            position: 'absolute', top: 20, right: 20, zIndex: 10, 
            background: '#222', padding: '16px', borderRadius: '8px', 
            border: '1px solid #444', color: '#fff', width: '260px',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
        }}>
            <h3 style={{ margin: '0 0 16px 0', fontSize: '14px', borderBottom: '1px solid #555', paddingBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px', color: '#aaa' }}>
                Control Plane
            </h3>
            
            {/* Секція глобальних команд */}
            <div style={{ marginBottom: '24px' }}>
                <div style={{ fontSize: '11px', color: '#888', marginBottom: '8px' }}>ГЛОБАЛЬНІ ОПЕРАЦІЇ</div>
                <button 
                    onClick={handleGlobalStop} 
                    style={{ width: '100%', padding: '8px', background: '#C62828', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer', marginBottom: '8px', fontWeight: 'bold' }}
                >
                    🛑 Emergency Stop
                </button>
                <button 
                    onClick={handleSync} 
                    style={{ width: '100%', padding: '8px', background: '#1565C0', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                >
                    🔄 Sync Topology
                </button>
            </div>

            {/* Секція точкових команд */}
            <div>
                <div style={{ fontSize: '11px', color: '#888', marginBottom: '8px' }}>КЕРУВАННЯ ВУЗЛОМ</div>
                <select 
                    value={selectedNode} 
                    onChange={(e) => setSelectedNode(e.target.value)}
                    style={{ width: '100%', padding: '6px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '4px', marginBottom: '12px' }}
                >
                    <option value="">-- Оберіть ціль --</option>
                    {availableTargets.map(n => (
                        <option key={n.id} value={n.id}>{n.id}</option>
                    ))}
                </select>
                
                <div style={{ display: 'flex', gap: '8px' }}>
                    <button 
                        onClick={() => handleNodeAction('STOP')} 
                        style={{ flex: 1, padding: '8px', background: '#EF6C00', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                    >
                        STOP
                    </button>
                    <button 
                        onClick={() => handleNodeAction('RESET')} 
                        style={{ flex: 1, padding: '8px', background: '#6A1B9A', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                    >
                        RESET
                    </button>
                </div>
            </div>
        </div>
    );
}