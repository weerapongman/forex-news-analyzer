// WebSocket connection
let ws;
let reconnectAttempts = 0;
const maxReconnectAttempts = 5;

// State
let allNews = [];
let filters = {
    currency: 'all',
    impact: 'all',
    direction: 'all',
    confidence: 0  // NEW: Minimum confidence filter
};

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initializeWebSocket();
    setupEventListeners();
    fetchInitialData();
});

// WebSocket Setup
function initializeWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;
    
    console.log('Connecting to WebSocket:', wsUrl);
    ws = new WebSocket(wsUrl);
    
    ws.onopen = () => {
        console.log('✅ WebSocket connected');
        reconnectAttempts = 0;
        updateConnectionStatus(true);
    };
    
    ws.onmessage = (event) => {
        console.log('📥 WebSocket message received');
        try {
            const message = JSON.parse(event.data);
            console.log('Message type:', message.type);
            console.log('Message data:', message.data);
            handleWebSocketMessage(message);
        } catch (error) {
            console.error('Error parsing WebSocket message:', error);
        }
    };
    
    ws.onclose = () => {
        console.log('❌ WebSocket disconnected');
        updateConnectionStatus(false);
        attemptReconnect();
    };
    
    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        updateConnectionStatus(false);
    };
}

function attemptReconnect() {
    if (reconnectAttempts < maxReconnectAttempts) {
        reconnectAttempts++;
        console.log(`Reconnecting... Attempt ${reconnectAttempts}`);
        setTimeout(initializeWebSocket, 3000);
    }
}

function updateConnectionStatus(connected) {
    const indicator = document.getElementById('connectionStatus');
    const statusText = document.getElementById('statusText');
    
    if (indicator && statusText) {
        if (connected) {
            indicator.classList.add('connected');
            indicator.classList.remove('disconnected');
            statusText.textContent = 'Connected';
        } else {
            indicator.classList.add('disconnected');
            indicator.classList.remove('connected');
            statusText.textContent = 'Disconnected';
        }
    }
}

// Handle WebSocket Messages
function handleWebSocketMessage(message) {
    console.log('Handling message:', message);
    
    if (message.type === 'initial' || message.type === 'update') {
        // Extract the analyzed news from the data
        if (message.data && message.data.analyzed) {
            allNews = message.data.analyzed;
            console.log(`📊 Received ${allNews.length} news items`);
            updateLastUpdateTime(message.data.lastUpdate);
            renderNews();
            updateStatistics();
            
            // Show alert for high impact news
            if (message.type === 'update' && message.data.highImpact && message.data.highImpact.length > 0) {
                const latestHighImpact = message.data.highImpact[0];
                showAlert(`⚡ High Impact: ${latestHighImpact.title} - ${latestHighImpact.direction} (${latestHighImpact.confidence}% confidence)`);
            }
        } else {
            console.warn('No analyzed data in message:', message.data);
        }
    }
}

// Fetch Initial Data
async function fetchInitialData() {
    try {
        console.log('Fetching initial data from API...');
        const response = await fetch('/api/news');
        const data = await response.json();
        console.log('API response:', data);
        
        if (data.analyzed) {
            allNews = data.analyzed;
            console.log(`📊 Loaded ${allNews.length} news items from API`);
            updateLastUpdateTime(data.lastUpdate);
            renderNews();
            updateStatistics();
        } else {
            console.warn('No analyzed data in API response');
        }
    } catch (error) {
        console.error('Error fetching initial data:', error);
        showError('Failed to load initial data');
    }
}

// Event Listeners
function setupEventListeners() {
    const currencyFilter = document.getElementById('currencyFilter');
    const impactFilter = document.getElementById('impactFilter');
    const directionFilter = document.getElementById('directionFilter');
    const confidenceFilter = document.getElementById('confidenceFilter');
    
    if (currencyFilter) {
        currencyFilter.addEventListener('change', (e) => {
            filters.currency = e.target.value;
            renderNews();
        });
    }
    
    if (impactFilter) {
        impactFilter.addEventListener('change', (e) => {
            filters.impact = e.target.value;
            renderNews();
        });
    }
    
    if (directionFilter) {
        directionFilter.addEventListener('change', (e) => {
            filters.direction = e.target.value;
            renderNews();
        });
    }
    
    if (confidenceFilter) {
        confidenceFilter.addEventListener('change', (e) => {
            filters.confidence = parseInt(e.target.value);
            console.log(`Confidence filter set to: ${filters.confidence}%`);
            renderNews();
        });
    }
}

// Filter News
function filterNews() {
    return allNews.filter(news => {
        if (filters.currency !== 'all' && news.currency !== filters.currency) {
            return false;
        }
        
        if (filters.impact === 'high' && !news.isHighImpact) {
            return false;
        }
        
        if (filters.impact === 'medium' && news.impact === 'low') {
            return false;
        }
        
        if (filters.direction !== 'all' && news.direction !== filters.direction) {
            return false;
        }
        
        // NEW: Confidence filter
        if (news.confidence < filters.confidence) {
            return false;
        }
        
        return true;
    });
}

// Render News
function renderNews() {
    const newsFeed = document.getElementById('newsFeed');
    if (!newsFeed) {
        console.error('News feed element not found');
        return;
    }
    
    const filteredNews = filterNews();
    console.log(`Rendering ${filteredNews.length} news items (filtered from ${allNews.length})`);
    
    if (filteredNews.length === 0) {
        newsFeed.innerHTML = `
            <div class="loading">
                <p style="color: var(--gray);">No news items match the current filters.</p>
            </div>
        `;
        return;
    }
    
    // Sort by confidence and time
    const sortedNews = filteredNews.sort((a, b) => {
        if (b.isHighImpact !== a.isHighImpact) {
            return b.isHighImpact ? 1 : -1;
        }
        return b.confidence - a.confidence;
    });
    
    newsFeed.innerHTML = sortedNews.map(news => createNewsCard(news)).join('');
}

// Create News Card HTML
function createNewsCard(news) {
    const impactClass = news.isHighImpact ? 'high-impact' : '';
    const directionClass = news.direction.toLowerCase();
    
    const actualNum = parseFloat(String(news.actual).replace(/[^0-9.-]/g, ''));
    const forecastNum = parseFloat(String(news.forecast).replace(/[^0-9.-]/g, ''));
    
    let actualClass = '';
    if (!isNaN(actualNum) && !isNaN(forecastNum)) {
        actualClass = actualNum > forecastNum ? 'positive' : actualNum < forecastNum ? 'negative' : '';
    }
    
    // Group signals by type for better organization
    const groupedSignals = {
        FOREX: [],
        COMMODITY: [],
        INDEX: [],
        CRYPTO: [],
        BOND: []
    };
    
    if (news.tradingSignals && news.tradingSignals.length > 0) {
        news.tradingSignals.forEach(signal => {
            if (groupedSignals[signal.type]) {
                groupedSignals[signal.type].push(signal);
            }
        });
    }
    
    const tradingRecsHTML = news.tradingSignals && news.tradingSignals.length > 0 ? `
        <div class="trading-recs">
            <h4>
                <svg width="16" height="16" viewBox="0 0 20 20" fill="currentColor">
                    <path d="M2 11a1 1 0 011-1h2a1 1 0 011 1v5a1 1 0 01-1 1H3a1 1 0 01-1-1v-5zM8 7a1 1 0 011-1h2a1 1 0 011 1v9a1 1 0 01-1 1H9a1 1 0 01-1-1V7zM14 4a1 1 0 011-1h2a1 1 0 011 1v12a1 1 0 01-1 1h-2a1 1 0 01-1-1V4z"/>
                </svg>
                Trading Recommendations (${news.tradingSignals.length} Products)
            </h4>
            
            ${groupedSignals.FOREX.length > 0 ? `
                <div class="product-group">
                    <h5 class="product-group-title">💱 Forex Pairs (${groupedSignals.FOREX.length})</h5>
                    <div class="trading-grid">
                        ${groupedSignals.FOREX.slice(0, 8).map(rec => createTradingCard(rec)).join('')}
                    </div>
                </div>
            ` : ''}
            
            ${groupedSignals.COMMODITY.length > 0 ? `
                <div class="product-group">
                    <h5 class="product-group-title">🪙 Commodities (${groupedSignals.COMMODITY.length})</h5>
                    <div class="trading-grid">
                        ${groupedSignals.COMMODITY.slice(0, 6).map(rec => createTradingCard(rec)).join('')}
                    </div>
                </div>
            ` : ''}
            
            ${groupedSignals.INDEX.length > 0 ? `
                <div class="product-group">
                    <h5 class="product-group-title">📊 Indices (${groupedSignals.INDEX.length})</h5>
                    <div class="trading-grid">
                        ${groupedSignals.INDEX.slice(0, 4).map(rec => createTradingCard(rec)).join('')}
                    </div>
                </div>
            ` : ''}
            
            ${groupedSignals.CRYPTO.length > 0 ? `
                <div class="product-group">
                    <h5 class="product-group-title">₿ Crypto (${groupedSignals.CRYPTO.length})</h5>
                    <div class="trading-grid">
                        ${groupedSignals.CRYPTO.slice(0, 6).map(rec => createTradingCard(rec)).join('')}
                    </div>
                </div>
            ` : ''}
            
            ${groupedSignals.BOND.length > 0 ? `
                <div class="product-group">
                    <h5 class="product-group-title">📈 Bonds (${groupedSignals.BOND.length})</h5>
                    <div class="trading-grid">
                        ${groupedSignals.BOND.slice(0, 4).map(rec => createTradingCard(rec)).join('')}
                    </div>
                </div>
            ` : ''}
        </div>
    ` : '';
    
    return `
        <div class="news-card ${impactClass} ${directionClass}">
            <div class="news-header">
                <div class="news-title">
                    <div class="news-datetime">
                        <span class="news-date">${formatDateBangkok(news.time)}</span>
                        <span class="news-time-bangkok">${formatTimeBangkok(news.time)}</span>
                        ${!news.actual || news.actual === '-' ? '<span class="forecast-badge">📊 FORECAST ANALYSIS</span>' : '<span class="actual-badge">✅ ACTUAL DATA</span>'}
                    </div>
                    <h3>${news.title}</h3>
                    <div class="news-meta">
                        <span class="badge currency">${news.currency}</span>
                        <span class="badge impact ${news.isHighImpact ? 'high' : 'medium'}">
                            ${news.isHighImpact ? 'HIGH' : 'MED'} IMPACT
                        </span>
                        <span class="badge source">${news.source}</span>
                    </div>
                </div>
            </div>
            
            <div class="data-row">
                <div class="data-item">
                    <div class="data-label">Actual</div>
                    <div class="data-value ${actualClass}">${news.actual || '-'}</div>
                </div>
                <div class="data-item">
                    <div class="data-label">Forecast</div>
                    <div class="data-value">${news.forecast || '-'}</div>
                </div>
                <div class="data-item">
                    <div class="data-label">Previous</div>
                    <div class="data-value">${news.previous || '-'}</div>
                </div>
            </div>
            
            <div class="analysis">
                <div class="direction-badge ${news.direction}">
                    ${getDirectionIcon(news.direction)} ${news.direction}
                </div>
                
                <div class="confidence-bar">
                    <div class="confidence-label">
                        <span>Analysis Confidence</span>
                        <span>${news.confidence}%</span>
                    </div>
                    <div class="confidence-progress">
                        <div class="confidence-fill" style="width: ${news.confidence}%"></div>
                    </div>
                </div>
                
                <div class="analysis-text">
                    ${news.analysis || 'Analysis in progress...'}
                </div>
            </div>
            
            ${tradingRecsHTML}
        </div>
    `;
}

// Create Trading Card HTML
function createTradingCard(rec) {
    // Map product names to proper format (e.g., Gold -> XAU/USD)
    const productName = mapProductName(rec.product);
    
    return `
        <div class="trading-card">
            <div class="trading-pair">${productName}</div>
            <div class="trading-type">${rec.type}</div>
            <div class="trading-direction ${rec.direction.toLowerCase()}">${rec.direction}</div>
            <div class="trading-action ${rec.action.includes('BUY') ? 'buy' : rec.action.includes('SELL') ? 'sell' : ''}">${rec.action}</div>
            <div class="confidence-mini">Confidence: ${rec.confidence}%</div>
            <div class="timeframe">${rec.timeframe}</div>
        </div>
    `;
}

// Map product names to trading symbols
function mapProductName(product) {
    const productMap = {
        'Gold': 'XAU/USD',
        'Silver': 'XAG/USD',
        'Platinum': 'XPT/USD',
        'Crude Oil': 'CL (WTI)',
        'Brent Oil': 'Brent',
        'Natural Gas': 'NG',
        'Copper': 'HG',
        'Iron Ore': 'Iron Ore',
        'Steel': 'Steel',
        'Aluminum': 'Aluminum',
        'Coal': 'Coal'
    };
    
    return productMap[product] || product;
}

// Get Direction Icon
function getDirectionIcon(direction) {
    switch (direction) {
        case 'BULLISH':
            return '📈';
        case 'BEARISH':
            return '📉';
        case 'NEUTRAL':
            return '➡️';
        case 'WATCH':
            return '👁️';
        default:
            return '❓';
    }
}

// Update Statistics
function updateStatistics() {
    const totalNews = allNews.length;
    const highImpactCount = allNews.filter(n => n.isHighImpact).length;
    const bullishCount = allNews.filter(n => n.direction === 'BULLISH').length;
    const bearishCount = allNews.filter(n => n.direction === 'BEARISH').length;
    
    const totalNewsEl = document.getElementById('totalNews');
    const highImpactEl = document.getElementById('highImpactCount');
    const bullishEl = document.getElementById('bullishCount');
    const bearishEl = document.getElementById('bearishCount');
    
    if (totalNewsEl) totalNewsEl.textContent = totalNews;
    if (highImpactEl) highImpactEl.textContent = highImpactCount;
    if (bullishEl) bullishEl.textContent = bullishCount;
    if (bearishEl) bearishEl.textContent = bearishCount;
    
    console.log(`📊 Stats updated: Total=${totalNews}, High=${highImpactCount}, Bullish=${bullishCount}, Bearish=${bearishCount}`);
}

// Update Last Update Time
function updateLastUpdateTime(timestamp) {
    if (!timestamp) return;
    
    const lastUpdateEl = document.getElementById('lastUpdate');
    if (!lastUpdateEl) return;
    
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now - date;
    const diffSecs = Math.floor(diffMs / 1000);
    
    let timeText;
    if (diffSecs < 60) {
        timeText = 'Just now';
    } else if (diffSecs < 3600) {
        const mins = Math.floor(diffSecs / 60);
        timeText = `${mins} min${mins > 1 ? 's' : ''} ago`;
    } else {
        timeText = date.toLocaleTimeString();
    }
    
    lastUpdateEl.textContent = timeText;
}

// Format Time
function formatTime(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
        hour: '2-digit', 
        minute: '2-digit',
        hour12: true 
    });
}

// Format Date (Bangkok Time)
function formatDateBangkok(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    // Bangkok is UTC+7
    const bangkokDate = new Date(date.getTime() + (7 * 60 * 60 * 1000));
    return bangkokDate.toLocaleDateString('en-US', {
        weekday: 'short',
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        timeZone: 'Asia/Bangkok'
    });
}

// Format Time (Bangkok Time) - LARGER FONT
function formatTimeBangkok(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZone: 'Asia/Bangkok'
    }) + ' ICT';
}

// Show Alert
function showAlert(message) {
    const alertBanner = document.getElementById('alertBanner');
    const alertMessage = document.getElementById('alertMessage');
    
    if (alertBanner && alertMessage) {
        alertMessage.textContent = message;
        alertBanner.style.display = 'block';
        
        // Auto-hide after 10 seconds
        setTimeout(() => {
            alertBanner.style.display = 'none';
        }, 10000);
    }
}

// Show Error
function showError(message) {
    const newsFeed = document.getElementById('newsFeed');
    if (newsFeed) {
        newsFeed.innerHTML = `
            <div class="loading">
                <p style="color: var(--danger); font-weight: 600;">⚠️ ${message}</p>
                <button onclick="fetchInitialData()" style="margin-top: 15px; padding: 10px 20px; background: var(--primary); color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: 600;">
                    Retry
                </button>
            </div>
        `;
    }
}

// Refresh data every minute for time updates
setInterval(() => {
    if (allNews.length > 0 && allNews[0].timestamp) {
        updateLastUpdateTime(allNews[0].timestamp);
    }
}, 60000);