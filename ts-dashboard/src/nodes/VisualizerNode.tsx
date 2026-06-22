import { Handle, Position, type NodeProps } from '@xyflow/react';
import React, { useEffect, useRef, useState } from 'react';

export function VisualizerNode({ id, data }: NodeProps) {
    const [selectedSource, setSelectedSource] = useState<string>('');
    const [isStreaming, setIsStreaming] = useState(false);
    const [mode, setMode] = useState<'single' | 'correlation'>('single');
    const [isAudioEnabled, setIsAudioEnabled] = useState(false);
    
    const [isAutoScale, setIsAutoScale] = useState(true);
    const [manualMin, setManualMin] = useState(-1);
    const [manualMax, setManualMax] = useState(1);
    const scaleConfig = useRef({ auto: true, min: -1, max: 1 });
    
    const canvas1Ref = useRef<HTMLCanvasElement>(null); 
    const canvas2Ref = useRef<HTMLCanvasElement>(null); 
    const canvas3Ref = useRef<HTMLCanvasElement>(null); 
    const canvas4Ref = useRef<HTMLCanvasElement>(null); 
    
    const audioCtxRef = useRef<AudioContext | null>(null);
    const analyserRef = useRef<AnalyserNode | null>(null);
    const nextAudioTime = useRef<number>(0);

    const dataBuffer = useRef<number[]>(new Array(260).fill(0));
    const delayBuffer = useRef<number[]>(new Array(260).fill(0));
    const latestDataRef = useRef<any>(null);

    const connectedSourceId = data.connectedSourceId as string;
    const connectedSourceName = data.connectedSourceName as string;

    useEffect(() => { scaleConfig.current = { auto: isAutoScale, min: manualMin, max: manualMax }; }, [isAutoScale, manualMin, manualMax]);
    useEffect(() => { if (connectedSourceId) setSelectedSource(connectedSourceId); }, [connectedSourceId]);

    // ВИПРАВЛЕННЯ БАГУ З ПАМ'ЯТТЮ: Очищаємо аудіо-контекст при закритті ноди
    useEffect(() => {
        return () => {
            if (isStreaming && data.onStop) (data.onStop as Function)(id);
            if (audioCtxRef.current) {
                audioCtxRef.current.close();
                audioCtxRef.current = null;
            }
        };
    }, []);

    const handleAudioToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
        const enabled = e.target.checked;
        setIsAudioEnabled(enabled);
        nextAudioTime.current = 0;
        
        if (enabled && isStreaming && !audioCtxRef.current) {
            const AudioContextClass = window.AudioContext || (window as any).webkitAudioContext;
            const ctx = new AudioContextClass();
            audioCtxRef.current = ctx;
            const analyser = ctx.createAnalyser();
            analyser.fftSize = 128;
            analyserRef.current = analyser;
        } else if (!enabled && audioCtxRef.current) {
            // ВБИВАЄМО контекст повністю замість suspend, щоб очистити пам'ять
            audioCtxRef.current.close();
            audioCtxRef.current = null;
        }
    };

    useEffect(() => {
        const handleNewData = (e: any) => {
            if (e.detail.visualizerId !== id) return;
            const val = e.detail.value;
            latestDataRef.current = val;

            // Автоматичне перемикання режиму панелі
            if (val && val.correlation !== undefined && mode !== 'correlation') {
                setMode('correlation');
            }

            // Наповнення буферів для малювання графіків
            if (typeof val === 'number') {
                dataBuffer.current.push(val); 
                dataBuffer.current.shift();
            } else if (val && val.delay !== undefined) {
                delayBuffer.current.push(val.delay); 
                delayBuffer.current.shift();
            }

            // --- БЛОК ВІДТВОРЕННЯ ЗВУКУ ---
            if (isAudioEnabled && audioCtxRef.current && audioCtxRef.current.state === 'running') {
                let audioArray: number[] | null = null;
                if (Array.isArray(val)) audioArray = val;
                else if (val && val.waveA) audioArray = val.waveA;
                
                if (audioArray) {
                    const ctx = audioCtxRef.current;
                    const buffer = ctx.createBuffer(1, audioArray.length, 8000);
                    buffer.getChannelData(0).set(audioArray);
                    const source = ctx.createBufferSource();
                    source.buffer = buffer;
                    
                    if (analyserRef.current) source.connect(analyserRef.current);
                    source.connect(ctx.destination);
                    
                    // --- ВИПРАВЛЕНИЙ ANTI-BLOAT ЗАХИСТ ---
                    if (nextAudioTime.current < ctx.currentTime) {
                        // Якщо буфер порожній (немає що грати), починаємо майже одразу
                        nextAudioTime.current = ctx.currentTime + 0.05;
                    } 
                    else if (nextAudioTime.current > ctx.currentTime + 0.4) {
                        // Якщо в черзі зібралося більше 400мс звуку (мережа "насипала" зайвого)
                        console.warn("Audio buffer overflow! Dropping frame.");
                        // Ми ПРОСТО ВИХОДИМО і не відтворюємо цей кадр!
                        // Старий час залишається, старі кадри дограють, черга розчиститься.
                        return; 
                    }
                    // -------------------------------------

                    source.start(nextAudioTime.current);
                    nextAudioTime.current += buffer.duration;
                }
            }
        };

        window.addEventListener('visualizer-data', handleNewData);
        return () => window.removeEventListener('visualizer-data', handleNewData);
    }, [id, isAudioEnabled, mode, isStreaming]); // ВАЖЛИВО: isStreaming додано в залежності

    useEffect(() => {
        let animationFrameId: number;
        const render = () => {
            const val = latestDataRef.current;
            let waves: {arr: number[], color: string}[] = [];
            let delayText = '';

            if (typeof val === 'number' || val === null) {
                waves.push({ arr: dataBuffer.current, color: '#00ff00' });
            } else if (Array.isArray(val)) {
                waves.push({ arr: val, color: '#00ff00' });
            } else if (val.waveA && val.waveB) {
                waves.push({ arr: val.waveA, color: '#00ff00' });
                waves.push({ arr: val.waveB, color: '#ff0072' });
                delayText = `Delay: ${val.delay} ms`;
            }

            let maxVal = 1, minVal = -1;
            if (scaleConfig.current.auto && waves.length > 0 && waves[0].arr.length > 0) {
                maxVal = Math.max(...waves[0].arr);
                minVal = Math.min(...waves[0].arr);
                if (maxVal === minVal) { maxVal += 0.1; minVal -= 0.1; }
            } else {
                maxVal = scaleConfig.current.max; 
                minVal = scaleConfig.current.min;
                if (maxVal <= minVal) maxVal = minVal + 1;
            }
            
            const padding = scaleConfig.current.auto ? (maxVal - minVal) * 0.1 : 0;
            const topLimit = maxVal + padding;
            const bottomLimit = minVal - padding;
            const range = (topLimit - bottomLimit) || 1;
            const leftPadding = 35; 

            const c1 = canvas1Ref.current;
            if (c1) {
                const ctx = c1.getContext('2d')!;
                ctx.clearRect(0, 0, c1.width, c1.height);
                drawGrid(ctx, c1.width, c1.height, topLimit, bottomLimit);
                
                waves.forEach(({arr, color}) => {
                    ctx.beginPath(); ctx.strokeStyle = isStreaming ? color : '#555'; ctx.lineWidth = 1.5;
                    const step = (c1.width - leftPadding) / arr.length;
                    for (let i = 0; i < arr.length; i++) {
                        const x = leftPadding + (i * step);
                        let clamped = arr[i];
                        if (!scaleConfig.current.auto) {
                            if (clamped > topLimit) clamped = topLimit;
                            if (clamped < bottomLimit) clamped = bottomLimit;
                        }
                        const normalized = (clamped - bottomLimit) / range;
                        const y = c1.height - (normalized * c1.height);
                        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
                    }
                    ctx.stroke();
                });
            }

            if (mode === 'correlation' && val) {
                const c2 = canvas2Ref.current;
                if (c2 && val.correlation) {
                    const ctx2 = c2.getContext('2d')!;
                    ctx2.clearRect(0, 0, c2.width, c2.height);
                    drawGrid(ctx2, c2.width, c2.height, 1, 0);

                    ctx2.beginPath(); ctx2.strokeStyle = '#00BCD4'; ctx2.lineWidth = 2;
                    const arr = val.correlation;
                    const step = (c2.width - leftPadding) / arr.length;
                    for (let i = 0; i < arr.length; i++) {
                        const x = leftPadding + (i * step);
                        const y = c2.height - (arr[i] * c2.height * 0.9);
                        if (i === 0) ctx2.moveTo(x, y); else ctx2.lineTo(x, y);
                    }
                    ctx2.stroke();
                    
                    ctx2.fillStyle = '#FFEB3B'; ctx2.font = 'bold 12px monospace';
                    ctx2.fillText(delayText, leftPadding + 10, 20);
                }

                const c3 = canvas3Ref.current;
                const analyser = analyserRef.current;
                if (c3 && analyser && isStreaming) {
                    const ctx3 = c3.getContext('2d')!;
                    ctx3.drawImage(c3, -2, 0);
                    const freqData = new Uint8Array(analyser.frequencyBinCount);
                    analyser.getByteFrequencyData(freqData);
                    
                    const barH = c3.height / freqData.length;
                    for (let i = 0; i < freqData.length; i++) {
                        const v = freqData[i];
                        const hue = 240 - (v / 255) * 240;
                        const lightness = v === 0 ? 0 : 50;
                        ctx3.fillStyle = `hsl(${hue}, 100%, ${lightness}%)`;
                        ctx3.fillRect(c3.width - 2, c3.height - (i * barH) - barH, 2, barH);
                    }
                }

                const c4 = canvas4Ref.current;
                if (c4) {
                    const ctx4 = c4.getContext('2d')!;
                    ctx4.clearRect(0, 0, c4.width, c4.height);
                    drawGrid(ctx4, c4.width, c4.height, 50, -50);

                    ctx4.beginPath(); ctx4.strokeStyle = '#FFEB3B'; ctx4.lineWidth = 2;
                    const hist = delayBuffer.current;
                    const step = (c4.width - leftPadding) / hist.length;
                    for (let i = 0; i < hist.length; i++) {
                        const x = leftPadding + (i * step);
                        const normalized = (hist[i] + 50) / 100;
                        const y = c4.height - (normalized * c4.height);
                        if (i === 0) ctx4.moveTo(x, y); else ctx4.lineTo(x, y);
                    }
                    ctx4.stroke();
                }
            }
            if (isStreaming) animationFrameId = requestAnimationFrame(render);
        };

        if (isStreaming) render();
        return () => cancelAnimationFrame(animationFrameId);
    }, [isStreaming, mode]);

    const drawGrid = (ctx: CanvasRenderingContext2D, w: number, h: number, max: number, min: number) => {
        ctx.strokeStyle = '#333'; ctx.fillStyle = '#888'; ctx.font = '9px monospace'; ctx.lineWidth = 1;
        ctx.textAlign = 'left'; ctx.textBaseline = 'alphabetic';
        for (let i = 0; i <= 4; i++) {
            const y = (h / 4) * i;
            if (i > 0 && i < 4) { ctx.beginPath(); ctx.moveTo(35, y); ctx.lineTo(w, y); ctx.stroke(); }
            const val = max - (i / 4) * (max - min);
            ctx.fillText(val.toFixed(1), 2, i === 0 ? y + 10 : (i === 4 ? y - 2 : y + 3));
        }
    };

    const handleToggleStream = () => {
        if (isStreaming) {
            if (data.onStop) (data.onStop as Function)(id);
            setIsStreaming(false);
            nextAudioTime.current = 0;
            // ВБИВАЄМО контекст при зупинці графіка!
            if (audioCtxRef.current) {
                audioCtxRef.current.close();
                audioCtxRef.current = null;
            }
        } else {
            const targetSource = connectedSourceId || selectedSource;
            if (!targetSource) { alert('Підключіть дріт!'); return; }
            if (data.onStart) (data.onStart as Function)(targetSource, id);
            setIsStreaming(true);
            nextAudioTime.current = 0;
            // Створюємо чистий контекст з нуля, якщо звук увімкнено
            if (isAudioEnabled) {
                const AudioContextClass = window.AudioContext || (window as any).webkitAudioContext;
                const ctx = new AudioContextClass();
                audioCtxRef.current = ctx;
                const analyser = ctx.createAnalyser();
                analyser.fftSize = 128;
                analyserRef.current = analyser;
            }
        }
    };

    const availableSources = (data.availableSources as string[]) || [];

    return (
        <div style={{ padding: 10, border: '1px solid #FFEB3B', borderRadius: 5, background: '#222', color: '#fff', width: mode === 'single' ? 280 : 580, transition: 'width 0.3s' }}>
            <Handle type="target" position={Position.Left} id="signal-input" />
            
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, borderBottom: '1px solid #444', paddingBottom: 5 }}>
                <strong style={{ color: '#FFEB3B', fontSize: '12px' }}>
                    {mode === 'single' ? 'Осцилограф' : 'Акустична Панель (GCC-PHAT)'}
                </strong>
                <span style={{ fontSize: '10px', color: '#aaa', fontFamily: 'monospace' }}>{id.split('-')[1] || id}</span>
            </div>

            <div style={{ display: 'flex', gap: '8px', marginBottom: '10px', alignItems: 'center' }}>
                {connectedSourceId ? (
                    <div style={{ flex: 1, background: '#111', color: '#00ff00', border: '1px solid #555', fontSize: '10px', padding: '4px', borderRadius: '3px' }}>🔗 {connectedSourceName || connectedSourceId}</div>
                ) : (
                    <select value={selectedSource} onChange={e => setSelectedSource(e.target.value)} disabled={isStreaming} style={{ flex: 1, background: '#333', color: '#fff', border: '1px solid #555', fontSize: '10px', padding: '2px' }}>
                        <option value="">-- Джерело --</option>
                        {availableSources.map(src => <option key={src} value={src}>{src}</option>)}
                    </select>
                )}
                
                <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', color: isAudioEnabled ? '#2196F3' : '#aaa', fontSize: '11px', fontWeight: 'bold' }}>
                    <input type="checkbox" checked={isAudioEnabled} onChange={handleAudioToggle} style={{ margin: '0 4px 0 0' }} /> 🎧 Звук
                </label>

                <button onClick={handleToggleStream} style={{ background: isStreaming ? '#f44336' : '#4CAF50', color: 'white', border: 'none', borderRadius: '3px', cursor: 'pointer', fontSize: '10px', fontWeight: 'bold', padding: '4px 12px' }}>
                    {isStreaming ? 'STOP' : 'START'}
                </button>
            </div>

            <div style={{ display: 'flex', gap: '8px', marginBottom: '8px', alignItems: 'center', fontSize: '10px' }}>
                <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', color: isAutoScale ? '#00ff00' : '#aaa' }}>
                    <input type="checkbox" checked={isAutoScale} onChange={e => setIsAutoScale(e.target.checked)} style={{ margin: '0 4px 0 0' }} /> Auto-Scale
                </label>
                {!isAutoScale && (
                    <div style={{ display: 'flex', gap: '4px', alignItems: 'center', marginLeft: 'auto' }}>
                        Min: <input type="number" value={manualMin} onChange={e => setManualMin(Number(e.target.value))} style={{ width: '40px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '2px', padding: '2px' }} />
                        Max: <input type="number" value={manualMax} onChange={e => setManualMax(Number(e.target.value))} style={{ width: '40px', background: '#333', color: '#fff', border: '1px solid #555', borderRadius: '2px', padding: '2px' }} />
                    </div>
                )}
            </div>

            {mode === 'single' ? (
                <canvas ref={canvas1Ref} width={260} height={120} style={{ background: '#111', borderRadius: 3, display: 'block' }} />
            ) : (
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
                    <div>
                        <div style={{ fontSize: 10, color: '#aaa', marginBottom: 2 }}>1. Сигнали (Time-Domain)</div>
                        <canvas ref={canvas1Ref} width={270} height={120} style={{ background: '#111', borderRadius: 3, display: 'block' }} />
                    </div>
                    <div>
                        <div style={{ fontSize: 10, color: '#aaa', marginBottom: 2 }}>2. Функція Крос-Кореляції</div>
                        <canvas ref={canvas2Ref} width={270} height={120} style={{ background: '#111', borderRadius: 3, display: 'block' }} />
                    </div>
                    <div>
                        <div style={{ fontSize: 10, color: '#aaa', marginBottom: 2 }}>3. Спектрограма (Waterfall)</div>
                        <canvas ref={canvas3Ref} width={270} height={120} style={{ background: '#000', borderRadius: 3, display: 'block' }} />
                    </div>
                    <div>
                        <div style={{ fontSize: 10, color: '#aaa', marginBottom: 2 }}>4. Історія затримки (Trend)</div>
                        <canvas ref={canvas4Ref} width={270} height={120} style={{ background: '#111', borderRadius: 3, display: 'block' }} />
                    </div>
                </div>
            )}
            
            <Handle type="source" position={Position.Right} id="signal-output" />
        </div>
    );
}