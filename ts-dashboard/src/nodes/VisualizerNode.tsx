import { Handle, Position, type NodeProps } from '@xyflow/react';
import React, { useEffect, useRef, useState } from 'react';

export function VisualizerNode({ id, data }: NodeProps) {
    const canvasRef = useRef<HTMLCanvasElement>(null);
    const [selectedSource, setSelectedSource] = useState<string>('');
    const [isStreaming, setIsStreaming] = useState(false);
    
    // Стан для налаштування шкали
    const [isAutoScale, setIsAutoScale] = useState(true);
    const [manualMin, setManualMin] = useState(-50);
    const [manualMax, setManualMax] = useState(50);
    
    // Зберігаємо налаштування в ref, щоб Canvas-цикл бачив свіжі дані без перезапуску
    const scaleConfig = useRef({ auto: true, min: -50, max: 50 });
    
    useEffect(() => {
        scaleConfig.current = { auto: isAutoScale, min: manualMin, max: manualMax };
    }, [isAutoScale, manualMin, manualMax]);

    // Масив для зберігання останніх значень
    const dataBuffer = useRef<number[]>(new Array(260).fill(0));
    const connectedSourceId = data.connectedSourceId as string;
    const connectedSourceName = data.connectedSourceName as string;

    // Якщо підключили дріт - автоматично обираємо це джерело
    useEffect(() => {
        if (connectedSourceId) setSelectedSource(connectedSourceId);
    }, [connectedSourceId]);

    // Ловимо дані з RSocket
    useEffect(() => {
        const handleNewData = (e: any) => {
            if (e.detail.visualizerId === id) {
                dataBuffer.current.push(e.detail.value);
                dataBuffer.current.shift();
            }
        };
        window.addEventListener('visualizer-data', handleNewData);
        return () => window.removeEventListener('visualizer-data', handleNewData);
    }, [id]);

    // Анімація та Малювання Canvas
    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        let animationFrameId: number;

        const render = () => {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            const buffer = dataBuffer.current;
            
            // 1. Визначаємо межі шкали (Авто або Ручна)
            let maxVal = 1;
            let minVal = -1;

            if (scaleConfig.current.auto) {
                maxVal = Math.max(...buffer);
                minVal = Math.min(...buffer);
                // Якщо сигнал плоский (всі нулі), даємо базовий діапазон
                if (maxVal === minVal) {
                    maxVal += 1;
                    minVal -= 1;
                }
            } else {
                maxVal = scaleConfig.current.max;
                minVal = scaleConfig.current.min;
                // Захист від некоректного введення
                if (maxVal <= minVal) maxVal = minVal + 1;
            }
            
            const padding = scaleConfig.current.auto ? (maxVal - minVal) * 0.1 : 0;
            const topLimit = maxVal + padding;
            const bottomLimit = minVal - padding;
            const range = topLimit - bottomLimit;

            // 2. Малюємо поточне числове значення на фоні
            const currentValue = buffer[buffer.length - 1];
            if (currentValue !== undefined) {
                ctx.fillStyle = 'rgba(255, 255, 255, 0.1)'; // Напівпрозорий білий текст
                ctx.font = 'bold 32px monospace';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                // Виводимо число по центру екрану
                ctx.fillText(currentValue.toFixed(2), canvas.width / 2 + 15, canvas.height / 2);
            }

            // Відновлюємо налаштування тексту для шкали
            ctx.textAlign = 'left';
            ctx.textBaseline = 'alphabetic';

            // 3. Сітка та текст шкали
            ctx.strokeStyle = '#333';
            ctx.fillStyle = '#888';
            ctx.font = '9px monospace';
            ctx.lineWidth = 1;
            const leftPadding = 35; 

            for (let i = 0; i <= 4; i++) {
                const y = (canvas.height / 4) * i;
                if (i > 0 && i < 4) {
                    ctx.beginPath(); ctx.moveTo(leftPadding, y); ctx.lineTo(canvas.width, y); ctx.stroke();
                }
                const val = topLimit - (i / 4) * range;
                ctx.fillText(val.toFixed(1), 2, i === 0 ? y + 10 : (i === 4 ? y - 2 : y + 3));
            }

            // 4. Малюємо хвилю
            ctx.beginPath();
            ctx.strokeStyle = isStreaming ? '#00ff00' : '#888';
            ctx.lineWidth = 1.5;

            for (let i = 0; i < buffer.length; i++) {
                const x = leftPadding + (i * ((canvas.width - leftPadding) / buffer.length));
                // Обмежуємо значення, щоб лінія не виходила за межі Canvas при ручному режимі
                let clampedValue = buffer[i];
                if (!scaleConfig.current.auto) {
                    if (clampedValue > topLimit) clampedValue = topLimit;
                    if (clampedValue < bottomLimit) clampedValue = bottomLimit;
                }

                const normalized = (clampedValue - bottomLimit) / range;
                const y = canvas.height - (normalized * canvas.height);
                
                if (i === 0) ctx.moveTo(x, y);
                else ctx.lineTo(x, y);
            }
            
            ctx.stroke();
            animationFrameId = requestAnimationFrame(render);
        };

        render();
        return () => cancelAnimationFrame(animationFrameId);
    }, [isStreaming]);

    const handleToggleStream = () => {
        if (isStreaming) {
            if (data.onStop) (data.onStop as Function)(id);
            setIsStreaming(false);
        } else {
            const targetSource = connectedSourceId || selectedSource;
            if (!targetSource) { alert('Підключіть дріт або оберіть джерело!'); return; }
            if (data.onStart) (data.onStart as Function)(targetSource, id);
            setIsStreaming(true);
        }
    };

    const availableSources = (data.availableSources as string[]) || [];

    return (
        <div style={{ padding: 10, border: '1px solid #FFEB3B', borderRadius: 5, background: '#222', color: '#fff', width: 280 }}>
            <Handle type="target" position={Position.Left} id="signal-input" />
            
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <strong style={{ color: '#FFEB3B', fontSize: '12px' }}>Осцилограф</strong>
                <span style={{ fontSize: '10px', color: '#aaa', fontFamily: 'monospace' }}>{id.split('-')[1] || id}</span>
            </div>

            {/* Панель вибору джерела та кнопка PLAY */}
            <div style={{ display: 'flex', gap: '5px', marginBottom: '8px' }}>
                {connectedSourceId ? (
                    <div style={{ flex: 1, background: '#111', color: '#00ff00', border: '1px solid #555', fontSize: '10px', padding: '4px', borderRadius: '3px', textAlign: 'center', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        🔗 {connectedSourceName || connectedSourceId}
                    </div>
                ) : (
                    <select 
                        value={selectedSource} 
                        onChange={e => setSelectedSource(e.target.value)}
                        disabled={isStreaming}
                        style={{ flex: 1, background: '#333', color: '#fff', border: '1px solid #555', fontSize: '10px', padding: '2px' }}
                    >
                        <option value="">-- Джерело --</option>
                        <option value="NodePy-Test">Python Sensor (Тест)</option>
                        {availableSources.map(src => <option key={src} value={src}>{src}</option>)}
                    </select>
                )}
                
                <button 
                    onClick={handleToggleStream}
                    style={{ background: isStreaming ? '#f44336' : '#4CAF50', color: 'white', border: 'none', borderRadius: '3px', cursor: 'pointer', fontSize: '10px', fontWeight: 'bold', padding: '2px 8px' }}
                >
                    {isStreaming ? 'STOP' : 'PLAY'}
                </button>
            </div>

            {/* Нова панель налаштування шкали */}
            <div style={{ display: 'flex', gap: '5px', marginBottom: '8px', alignItems: 'center', fontSize: '10px' }}>
                <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', color: isAutoScale ? '#00ff00' : '#aaa' }}>
                    <input 
                        type="checkbox" 
                        checked={isAutoScale} 
                        onChange={e => setIsAutoScale(e.target.checked)} 
                        style={{ margin: '0 4px 0 0' }}
                    /> 
                    Auto-Scale
                </label>
                 
                {!isAutoScale && (
                    <div style={{ display: 'flex', gap: '4px', alignItems: 'center', marginLeft: 'auto' }}>
                        Min: 
                        <input 
                            type="number" 
                            value={manualMin} 
                            onChange={e => setManualMin(Number(e.target.value))} 
                            style={{ width: '40px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '2px', fontSize: '10px', padding: '2px' }} 
                        />
                        Max: 
                        <input 
                            type="number" 
                            value={manualMax} 
                            onChange={e => setManualMax(Number(e.target.value))} 
                            style={{ width: '40px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '2px', fontSize: '10px', padding: '2px' }} 
                        />
                    </div>
                )}
            </div>
            
            <canvas ref={canvasRef} width={260} height={120} style={{ background: '#111', borderRadius: 3, display: 'block' }} />
            <Handle type="source" position={Position.Right} id="signal-output" />
        </div>
    );
}