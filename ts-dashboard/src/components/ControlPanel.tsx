import { useState, useEffect } from 'react';
import type { Node } from '@xyflow/react';

interface ControlPanelProps {
  nodes: Node[];
  onSendCommand: (command: string, payload?: string) => void;
}

export const ControlPanel = ({ nodes, onSendCommand }: ControlPanelProps) => {
  const [targetNode, setTargetNode] = useState(''); // Хто буде рахувати (Java)
  const [inputSource, setInputSource] = useState(''); // Хто дає дані (Python)
  const [operation, setOperation] = useState('AVERAGE');
  const [outputSink, setOutputSink] = useState('Dashboard-UI');

  // Автоматичний вибір нод для зручності
  useEffect(() => {
    if (nodes.length > 0) {
      if (!inputSource) {
        const sensor = nodes.find(n => n.id.startsWith('NodePy'));
        if (sensor) setInputSource(sensor.id);
      }
      if (!targetNode) {
        // За замовчуванням обираємо будь-яку Java ноду (або Seed)
        const compute = nodes.find(n => n.id.startsWith('Java') || n.id.startsWith('Seed'));
        if (compute) setTargetNode(compute.id);
      }
    }
  }, [nodes]);

  const handleDeploy = () => {
    if (!targetNode || !inputSource) {
      alert("⚠️ Оберіть Виконавця (Java) та Джерело (Python)!");
      return;
    }

    // 1. Формуємо саму схему обчислень З ПРАВИЛЬНИМИ КЛЮЧАМИ
    const schemaObj = {
      schema_id: `task-${Math.floor(Math.random() * 10000)}`,
      operation: operation,
      input_source: inputSource, // Змінили ключ на input_source
      output_sink: outputSink    // Змінили ключ на output_sink
    };

    // 2. Формуємо "конверт" для маршрутизації
    const deployRequest = {
      targetNode: targetNode, 
      schema: schemaObj
    };

    console.log("Відправка DEPLOY_SCHEMA:", deployRequest);
    onSendCommand('DEPLOY_SCHEMA', JSON.stringify(deployRequest));
  };

  return (
    <div style={{
      position: 'absolute', top: 80, left: 20, zIndex: 10,
      backgroundColor: '#2b2b36', padding: '15px', borderRadius: '8px',
      border: '1px solid #444', color: 'white', fontFamily: 'sans-serif',
      boxShadow: '0 4px 10px rgba(0,0,0,0.5)', width: '300px'
    }}>
      <h3 style={{ margin: '0 0 15px 0', fontSize: '16px', borderBottom: '1px solid #555', paddingBottom: '10px' }}>
        ⚙️ Редактор схеми
      </h3>
      
      <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
        
        {/* ХТО БУДЕ ОБЧИСЛЮВАТИ (JAVA) */}
        <div>
          <label style={{ fontSize: '12px', color: '#ffba08' }}>1. Виконавець (Compute Node):</label>
          <select value={targetNode} onChange={(e) => setTargetNode(e.target.value)} style={{ width: '100%', padding: '6px', marginTop: '4px', backgroundColor: '#1a192b', color: '#ffba08', border: '1px solid #555', borderRadius: '4px' }}>
            <option value="">-- Оберіть сервер --</option>
            {nodes.filter(n => n.id.startsWith('Java') || n.id.startsWith('Seed')).map(node => (
              <option key={node.id} value={node.id}>☕ {node.id}</option>
            ))}
          </select>
        </div>

        {/* ЗВІДКИ БЕРЕМО ДАНІ (PYTHON) */}
        <div>
          <label style={{ fontSize: '12px', color: '#aaa' }}>2. Джерело (Input Source):</label>
          <select value={inputSource} onChange={(e) => setInputSource(e.target.value)} style={{ width: '100%', padding: '6px', marginTop: '4px', backgroundColor: '#1a192b', color: '#00ff00', border: '1px solid #555', borderRadius: '4px' }}>
            <option value="">-- Оберіть сенсор --</option>
            {nodes.filter(n => n.id.startsWith('NodePy')).map(node => (
              <option key={node.id} value={node.id}>🐍 {node.id}</option>
            ))}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '12px', color: '#aaa' }}>3. Операція:</label>
          <select value={operation} onChange={(e) => setOperation(e.target.value)} style={{ width: '100%', padding: '6px', marginTop: '4px', backgroundColor: '#1a192b', color: '#fff', border: '1px solid #555', borderRadius: '4px' }}>
            <option value="AVERAGE">Усереднення (AVERAGE)</option>
            <option value="MAX">Максимум (MAX)</option>
            <option value="MULTIPLY">Множення (MULTIPLY)</option>
          </select>
        </div>

        <hr style={{ borderColor: '#444', margin: '5px 0' }} />

        <button onClick={handleDeploy} style={{ padding: '8px', backgroundColor: '#007bff', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold' }}>
          🚀 Відправити схему (Deploy)
        </button>
        <button onClick={() => onSendCommand('STOP_ALL')} style={{ padding: '8px', backgroundColor: '#dc3545', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold' }}>
          🛑 Зупинити потоки (Stop)
        </button>
        <button onClick={() => onSendCommand('RESET')} style={{ padding: '8px', backgroundColor: '#ffc107', color: 'black', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold' }}>
          ♻️ Скинути (Reset)
        </button>

      </div>
    </div>
  );
};