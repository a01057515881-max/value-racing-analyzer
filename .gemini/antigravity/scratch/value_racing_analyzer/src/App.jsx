import React, { useState } from 'react';
import SplitLayout from './components/Layout/SplitLayout';
import { db } from './firebase';
import { collection, addDoc } from 'firebase/firestore';
import ControlPanel from './components/ControlPanel/ControlPanel';
import MainPanel from './components/MainPanel/MainPanel';

function App() {
    const [isFullscreen, setIsFullscreen] = useState(false);
    const [analysisData, setAnalysisData] = useState(null);

    const handleAnalyze = async (data) => {
        setAnalysisData(data);
        setIsFullscreen(true); // Auto fullscreen on analysis
        // 분석 결과를 Firestore에 저장
        try {
            await addDoc(collection(db, 'analysisResults'), {
                ...data,
                timestamp: new Date().toISOString()
            });
        } catch (e) {
            console.error('분석 결과 저장 실패:', e);
        }
    };

    const handleReset = () => {
        setAnalysisData(null);
        setIsFullscreen(false);
    };

    return (
        <SplitLayout
            isFullscreen={isFullscreen}
            left={
                <ControlPanel onAnalyze={handleAnalyze} />
            }
            right={
                <MainPanel
                    isFullscreen={isFullscreen}
                    toggleFullscreen={() => setIsFullscreen(!isFullscreen)}
                    onReset={handleReset}
                    analysisData={analysisData}
                />
            }
        />
    );
}

export default App;
