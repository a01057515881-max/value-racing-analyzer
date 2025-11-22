import React from 'react';
import { Maximize2, Minimize2, Home, Image } from 'lucide-react';

const MainPanel = ({ isFullscreen, toggleFullscreen, onReset, analysisData }) => {
    return (
        <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            {/* Header Toolbar */}
            <div style={{
                padding: '16px 24px',
                borderBottom: '1px solid var(--border-color)',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                background: 'rgba(30, 30, 30, 0.8)',
                backdropFilter: 'blur(10px)',
                position: 'sticky',
                top: 0,
                zIndex: 10
            }}>
                <div>
                    {analysisData ? (
                        <div style={{ display: 'flex', alignItems: 'baseline', gap: '12px' }}>
                            <h2 style={{ margin: 0, color: 'white' }}>{analysisData.region} {analysisData.raceNo}경주</h2>
                            <span style={{ color: 'var(--primary)', fontSize: '0.9rem' }}>
                                {analysisData.trackCondition || '주로 상태: 자동'}
                            </span>
                        </div>
                    ) : (
                        <h2 style={{ margin: 0, color: 'var(--text-muted)' }}>분석을 시작하세요</h2>
                    )}
                </div>

                <div style={{ display: 'flex', gap: '12px' }}>
                    <button onClick={onReset} style={toolBtnStyle} title="새 분석"><Home size={20} /></button>
                    <button style={toolBtnStyle} title="이미지 저장"><Image size={20} /></button>
                    <button onClick={toggleFullscreen} style={toolBtnStyle} title="전체 화면 전환">
                        {isFullscreen ? <Minimize2 size={20} /> : <Maximize2 size={20} />}
                    </button>
                </div>
            </div>

            {/* Content Area */}
            <div style={{ flex: 1, padding: '24px', overflowY: 'auto' }}>
                {analysisData ? (
                    <div style={{ textAlign: 'center', marginTop: '100px' }}>
                        <h3 style={{ color: 'var(--primary)' }}>분석 완료</h3>
                        <p style={{ color: 'var(--text-muted)' }}>상세 경주 분석 결과가 여기에 표시됩니다.</p>
                        {/* RaceView 탭 자리 */}
                    </div>
                ) : (
                    <div style={{
                        height: '100%',
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'center',
                        justifyContent: 'center',
                        color: '#424242'
                    }}>
                        <div style={{ fontSize: '4rem', marginBottom: '20px', opacity: 0.2 }}>🏇</div>
                        <p>지역을 선택하고 경주카드를 업로드하면 분석이 시작됩니다.</p>
                    </div>
                )}
            </div>
        </div>
    );
};

const toolBtnStyle = {
    background: 'transparent',
    border: 'none',
    color: 'var(--text-muted)',
    cursor: 'pointer',
    padding: '8px',
    borderRadius: '50%',
    transition: 'background 0.2s',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center'
};

export default MainPanel;
