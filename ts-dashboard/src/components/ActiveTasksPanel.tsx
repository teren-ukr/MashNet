import React from 'react';

// ==========================================
// ІНТЕРФЕЙС: Описує структуру нашої задачі
// ==========================================
export interface ActiveTask {
  schema_id: string;   // Унікальний ID потоку (наприклад, task-4512)
  operation: string;   // Математична операція (AVERAGE, MAX)
  input_source: string; // Звідки беремо дані
  output_sink: string;  // Куди відправляємо результат
}

interface ActiveTasksPanelProps {
  tasks: ActiveTask[];                  // Масив активних задач
  onStopTask: (schemaId: string) => void; // Функція, яка викликається при натисканні "Зупинити"
}

// ==========================================
// КОМПОНЕНТ: Панель активних потоків
// ==========================================
export const ActiveTasksPanel = ({ tasks, onStopTask }: ActiveTasksPanelProps) => {
  return (
    <div style={{
      position: 'absolute', top: 20, right: 20, zIndex: 10,
      backgroundColor: '#2b2b36', padding: '15px', borderRadius: '8px',
      border: '1px solid #444', color: 'white', fontFamily: 'sans-serif',
      boxShadow: '0 4px 10px rgba(0,0,0,0.5)', width: '320px',
      maxHeight: '400px', overflowY: 'auto' // Додаємо скрол, якщо задач буде багато
    }}>
      
      {/* Заголовок панелі */}
      <h3 style={{ margin: '0 0 15px 0', fontSize: '16px', borderBottom: '1px solid #555', paddingBottom: '10px' }}>
        📋 Активні потоки (Задачі)
      </h3>

      {/* Якщо задач немає, показуємо повідомлення */}
      {tasks.length === 0 ? (
        <div style={{ fontSize: '13px', color: '#888', textAlign: 'center', padding: '10px 0' }}>
          Немає активних обчислень.
        </div>
      ) : (
        /* Якщо задачі є, виводимо їх списком */
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
          {tasks.map((task) => (
            <div key={task.schema_id} style={{
              backgroundColor: '#1a192b', padding: '10px', borderRadius: '6px', border: '1px solid #555'
            }}>
              
              {/* Верхній рядок: Операція та ID схеми */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                <strong style={{ fontSize: '14px', color: '#ffba08' }}>{task.operation}</strong>
                <span style={{ fontSize: '11px', color: '#aaa', fontFamily: 'monospace' }}>{task.schema_id}</span>
              </div>
              
              {/* Середній рядок: Маршрут (Джерело -> Приймач) */}
              <div style={{ fontSize: '12px', color: '#ccc', marginBottom: '10px', display: 'flex', alignItems: 'center', gap: '5px' }}>
                <span style={{ color: '#00ff00' }}>{task.input_source}</span>
                <span>➔</span>
                <span style={{ color: '#ff0072' }}>{task.output_sink}</span>
              </div>

              {/* Кнопка зупинки конкретного потоку */}
              <button 
                onClick={() => onStopTask(task.schema_id)}
                style={{ 
                  width: '100%', padding: '6px', backgroundColor: '#dc3545', 
                  color: 'white', border: 'none', borderRadius: '4px', 
                  cursor: 'pointer', fontWeight: 'bold', fontSize: '12px',
                  transition: 'background-color 0.2s'
                }}
                onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#c82333'}
                onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#dc3545'}
              >
                🛑 Зупинити потік
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};