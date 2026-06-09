interface StatusBarProps {
  isConnected: boolean;
  onReconnect: () => void;
}

export const StatusBar = ({ isConnected, onReconnect }: StatusBarProps) => {
  return (
    <div style={{
      position: 'absolute', top: 20, left: 20, zIndex: 10, 
      padding: '10px 20px', borderRadius: '8px',
      backgroundColor: isConnected ? '#28a745' : '#343a40',
      color: 'white', fontWeight: 'bold', fontFamily: 'sans-serif',
      display: 'flex', alignItems: 'center', gap: '15px',
      boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
    }}>
      <span>
        {isConnected 
          ? '🟢 Підключено до Java Меш-мережі' 
          : '🔴 Немає з\'єднання з сервером'}
      </span>
      
      {!isConnected && (
        <button 
          onClick={onReconnect}
          style={{
            padding: '6px 12px', borderRadius: '4px', border: 'none',
            backgroundColor: '#007bff', color: 'white', cursor: 'pointer',
            fontWeight: 'bold'
          }}
        >
          🔄 Перепідключитись
        </button>
      )}
    </div>
  );
};