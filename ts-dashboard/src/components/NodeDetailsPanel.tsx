import React from 'react';

/**
 * Структура конфігураційної схеми обчислень.
 */
export interface ActiveTask {
  schema_id: string;
  operation: string;
  input_source: string;
  output_sink: string;
  executor_node: string;
}

interface NodeDetailsPanelProps {
  selectedNodeId: string | null;
  tasks: ActiveTask[];
  onStopTask: (schemaId: string) => void;
  onClose: () => void;
}

/**
 * Панель детальної інформації про вузол.
 * Виконує фільтрацію глобального списку задач за ідентифікатором обраного вузла.
 */
export const NodeDetailsPanel = ({ selectedNodeId, tasks, onStopTask, onClose }: NodeDetailsPanelProps) => {
  if (!selectedNodeId) return null;

  // Фільтрація задач, до яких залучено даний вузол
  const nodeTasks = tasks.filter(t => 
    t.input_source === selectedNodeId || 
    t.output_sink === selectedNodeId || 
    t.executor_node === selectedNodeId
  );

  return (
    <div style={{
      position: 'absolute', top: 20, right: 20, zIndex: 10,
      backgroundColor: '#2b2b36', padding: '15px', borderRadius: '8px',
      border: '1px solid #444', color: 'white', fontFamily: 'sans-serif',
      boxShadow: '0 4px 15px rgba(0,0,0,0.7)', width: '340px',
      maxHeight: '80vh', overflowY: 'auto'
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid #555', paddingBottom: '10px', marginBottom: '15px' }}>
        <h3 style={{ margin: 0, fontSize: '14px', textTransform: 'uppercase' }}>
          Деталі вузла
        </h3>
        <button 
          onClick={onClose}
          style={{ background: 'transparent', border: 'none', color: '#aaa', cursor: 'pointer', fontSize: '14px', fontWeight: 'bold' }}
        >
          [X]
        </button>
      </div>

      <div style={{ marginBottom: '20px' }}>
        <div style={{ fontSize: '11px', color: '#aaa' }}>Ідентифікатор:</div>
        <div style={{ fontSize: '13px', fontWeight: 'bold', color: '#00ff00', fontFamily: 'monospace' }}>{selectedNodeId}</div>
      </div>

      <h4 style={{ fontSize: '12px', marginBottom: '10px', color: '#ccc', textTransform: 'uppercase' }}>Схеми та Потоки:</h4>

      {nodeTasks.length === 0 ? (
        <div style={{ fontSize: '12px', color: '#888', textAlign: 'center', padding: '10px 0', border: '1px dashed #555', borderRadius: '4px' }}>
          Активні потоки відсутні.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
          {nodeTasks.map((task) => {
            let role = '';
            if (task.executor_node === selectedNodeId) role = 'Виконавець обчислень';
            else if (task.input_source === selectedNodeId) role = 'Джерело даних';
            else if (task.output_sink === selectedNodeId) role = 'Приймач даних';

            return (
              <div key={task.schema_id} style={{
                backgroundColor: '#1a192b', padding: '10px', borderRadius: '6px', border: '1px solid #555'
              }}>
                <div style={{ fontSize: '10px', color: '#17a2b8', marginBottom: '5px', fontWeight: 'bold', textTransform: 'uppercase' }}>
                  [{role}]
                </div>
                
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                  <strong style={{ fontSize: '13px', color: '#ffba08' }}>{task.operation}</strong>
                  <span style={{ fontSize: '10px', color: '#aaa', fontFamily: 'monospace' }}>{task.schema_id}</span>
                </div>
                
                <div style={{ fontSize: '11px', color: '#ccc', marginBottom: '10px', display: 'flex', alignItems: 'center', gap: '5px', flexWrap: 'wrap' }}>
                  <span style={{ color: '#00ff00' }}>{task.input_source}</span>
                  <span>-&gt;</span>
                  <span style={{ color: '#ffba08' }}>{task.executor_node}</span>
                  <span>-&gt;</span>
                  <span style={{ color: '#ff0072' }}>{task.output_sink}</span>
                </div>

                {task.executor_node === selectedNodeId && (
                  <button 
                    onClick={() => onStopTask(task.schema_id)}
                    style={{ 
                      width: '100%', padding: '6px', backgroundColor: '#dc3545', 
                      color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold', fontSize: '11px', textTransform: 'uppercase'
                    }}
                  >
                    Зупинити потік
                  </button>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};