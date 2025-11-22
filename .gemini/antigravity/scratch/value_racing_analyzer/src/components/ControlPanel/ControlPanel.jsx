import React, { useState } from 'react';
import { Upload, Play, Save, BookOpen, FlaskConical } from 'lucide-react';

const regions = ['서울', '부산', '제주'];

const ControlPanel = ({ onAnalyze }) => {
    const [activeRegion, setActiveRegion] = useState('서울');
    const [raceNo, setRaceNo] = useState(1);
    const [trackCondition, setTrackCondition] = useState('');
    const [file, setFile] = useState(null);

    const handleFileChange = (e) => {
        if (e.target.files && e.target.files[0]) {
            setFile(e.target.files[0]);
        }
    };

    const isReady = file && raceNo;

    return (
        <div style={{ padding: '20px', display: 'flex', flexDirection: 'column', gap: '24px', height: '100%' }}>
            {/* Header */}
            <div>
                <h2 style={{ color: 'var(--primary)', margin: '0 0 8px 0' }}>경주마 분석</h2>
                <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem', margin: 0 }}>프로페셔널 경주마 심층 분석 도구</p>
            </div>

            {/* Region Tabs */}
            <div style={{ display: 'flex', gap: '8px', background: '#2C2C2C', padding: '4px', borderRadius: '8px' }}>
                {regions.map(region => (
                    <button
                        key={region}
                        onClick={() => setActiveRegion(region)}
                        style={{
                            flex: 1,
                            padding: '8px',
                            border: 'none',
                            borderRadius: '6px',
                            background: activeRegion === region ? 'var(--primary)' : 'transparent',
                            color: activeRegion === region ? '#000' : 'var(--text-muted)',
                            cursor: 'pointer',
                            fontWeight: 'bold',
                            transition: 'all 0.2s'
                        }}
                    >
                        {region}
                    </button>
                ))}
            </div>

            {/* Inputs */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                {/* File Upload */}
                <div style={{ border: '2px dashed var(--border-color)', borderRadius: '8px', padding: '20px', textAlign: 'center', cursor: 'pointer' }}>
                    <input type="file" id="race-file" style={{ display: 'none' }} onChange={handleFileChange} accept="image/*,.pdf" />
                    <label htmlFor="race-file" style={{ cursor: 'pointer', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' }}>
                        <Upload size={24} color="var(--primary)" />
                        <span style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                            {file ? file.name : 'Upload Race Card (Img/PDF)'}
                        </span>
                    </label>
                </div>

                {/* Race Number */}
                <div>
                    <label style={{ display: 'block', color: 'var(--text-muted)', marginBottom: '8px', fontSize: '0.9rem' }}>Race Number</label>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <input
                            type="number"
                            min="1"
                            max="15"
                            value={raceNo}
                            onChange={(e) => setRaceNo(parseInt(e.target.value))}
                            style={{
                                width: '100%',
                                padding: '10px',
                                background: '#2C2C2C',
                                border: '1px solid var(--border-color)',
                                borderRadius: '6px',
                                color: 'white',
                                fontSize: '1rem'
                            }}
                        />
                    </div>
                </div>

                {/* Track Condition */}
                <div>
                    <label style={{ display: 'block', color: 'var(--text-muted)', marginBottom: '8px', fontSize: '0.9rem' }}>Track Condition (Optional)</label>
                    <select
                        value={trackCondition}
                        onChange={(e) => setTrackCondition(e.target.value)}
                        style={{
                            width: '100%',
                            padding: '10px',
                            background: '#2C2C2C',
                            border: '1px solid var(--border-color)',
                            borderRadius: '6px',
                            color: 'white'
                        }}
                    >
                        <option value="">Auto (Google Search)</option>
                        <option value="dry">Dry (건조)</option>
                        <option value="good">Good (양호)</option>
                        <option value="wet">Wet (다습)</option>
                        <option value="bad">Bad (불량)</option>
                    </select>
                </div>
            </div>

            {/* Analyze Button */}
            <button
                onClick={() => onAnalyze({ region: activeRegion, raceNo, trackCondition, file })}
                disabled={!isReady}
                style={{
                    marginTop: 'auto',
                    padding: '16px',
                    background: isReady ? 'var(--primary)' : '#424242',
                    color: isReady ? '#000' : '#757575',
                    border: 'none',
                    borderRadius: '8px',
                    fontSize: '1.1rem',
                    fontWeight: 'bold',
                    cursor: isReady ? 'pointer' : 'not-allowed',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    gap: '8px',
                    transition: 'all 0.2s'
                }}
            >
                <Play size={20} />
                Run Analysis
            </button>

            {/* Footer Nav */}
            <div style={{ display: 'flex', justifyContent: 'space-between', paddingTop: '20px', borderTop: '1px solid var(--border-color)' }}>
                <button style={footerBtnStyle}><BookOpen size={18} /></button>
                <button style={footerBtnStyle}><Save size={18} /></button>
                <button style={footerBtnStyle}><FlaskConical size={18} /></button>
            </div>
        </div>
    );
};

const footerBtnStyle = {
    background: 'transparent',
    border: '1px solid var(--border-color)',
    color: 'var(--text-muted)',
    padding: '10px',
    borderRadius: '6px',
    cursor: 'pointer',
    flex: 1,
    margin: '0 4px',
    display: 'flex',
    justifyContent: 'center'
};

export default ControlPanel;
