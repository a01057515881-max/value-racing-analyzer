import React from 'react';

const SplitLayout = ({ left, right, isFullscreen }) => {
    return (
        <div style={{ display: 'flex', height: '100vh', width: '100vw', overflow: 'hidden' }}>
            {/* Left Panel (Control) - Hidden in Fullscreen */}
            <div
                style={{
                    width: isFullscreen ? '0' : '25%',
                    minWidth: isFullscreen ? '0' : '300px',
                    transition: 'width 0.5s ease-in-out',
                    borderRight: '1px solid var(--border-color)',
                    backgroundColor: 'var(--bg-panel)',
                    overflowY: 'auto',
                    opacity: isFullscreen ? 0 : 1,
                    pointerEvents: isFullscreen ? 'none' : 'auto'
                }}
            >
                {left}
            </div>

            {/* Right Panel (Main) - Expands in Fullscreen */}
            <div
                style={{
                    width: isFullscreen ? '100%' : '75%',
                    flex: 1,
                    transition: 'width 0.5s ease-in-out',
                    backgroundColor: 'var(--bg-dark)',
                    position: 'relative',
                    overflowY: 'auto'
                }}
            >
                {right}
            </div>
        </div>
    );
};

export default SplitLayout;
