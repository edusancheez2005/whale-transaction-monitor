#!/usr/bin/env python3
"""
🏛️ INSTITUTIONAL WHALE INTELLIGENCE API
Production-grade FastAPI backend for real-time whale activity aggregation

Features:
- Real-time whale buy/sell signal aggregation from Supabase
- Token-specific whale activity analysis
- Professional-grade confidence scoring
- Responsive HTML dashboard with auto-refresh
- Market cap tier-based filtering
- Comprehensive error handling and logging
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import asyncio
from datetime import datetime, timedelta
import os
import sys
import json
import logging
from supabase import create_client, Client

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    from config.settings import TEST_MODE
    print(f"✅ Configuration imported successfully. TEST_MODE: {TEST_MODE}")
except ImportError as e:
    print(f"❌ Could not import configuration: {e}")
    # Check if we can import from parent directory
    try:
        sys.path.insert(0, '..')
        from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
        from config.settings import TEST_MODE
        print(f"✅ Configuration imported from parent directory. TEST_MODE: {TEST_MODE}")
    except ImportError as e2:
        print(f"❌ Parent directory import also failed: {e2}")
        SUPABASE_URL = "https://demo.supabase.co" 
        SUPABASE_SERVICE_ROLE_KEY = "demo-key"
        TEST_MODE = True

# Initialize Supabase client
supabase_client: Optional[Client] = None
if not TEST_MODE:
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        print(f"✅ Supabase client initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize Supabase client: {e}")
        supabase_client = None

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for API responses
class WhaleSignal(BaseModel):
    token_symbol: str
    buy_percentage: float
    sell_percentage: float
    whale_count: int
    transaction_count: int  # NEW: Total transaction count per token
    total_volume_usd: float
    avg_confidence: float
    avg_whale_score: float
    trend: str  # "BUY", "SELL", "MIXED"
    market_cap_tier: str  # "mega_cap", "large_cap", etc.

class TokenActivity(BaseModel):
    token_symbol: str
    transactions: List[Dict[str, Any]]
    summary: Dict[str, Any]

class SystemStats(BaseModel):
    total_tokens_monitored: int
    active_whale_signals: int
    last_24h_transactions: int
    system_status: str

# FastAPI app initialization
app = FastAPI(
    title="🐋 Institutional Whale Intelligence API",
    description="Professional-grade whale activity monitoring and aggregation",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection management
def get_supabase_client():
    """
    🔗 SUPABASE CLIENT CONNECTION
    Returns the initialized Supabase client for database operations
    """
    if TEST_MODE:
        logger.info("🧪 TEST_MODE: Database disabled")
        raise HTTPException(status_code=503, detail="Database disabled in TEST_MODE")
    
    if supabase_client is None:
        logger.error("Supabase client not initialized")
        raise HTTPException(status_code=500, detail="Database connection unavailable")
    
    return supabase_client

@app.get("/whale-signals", response_model=List[WhaleSignal])
async def get_whale_signals(
    timeframe: str = Query("1h", description="Time window: 1h, 4h, 24h"),
    min_whale_score: float = Query(0, description="Minimum whale score filter"),
    tier_filter: Optional[str] = Query(None, description="Filter by market cap tier")
):
    """
    🐋 GET AGGREGATED WHALE BUY/SELL SIGNALS
    
    Returns percentage of whale transactions that are BUY vs SELL
    for each token in the specified timeframe.
    
    Professional-grade aggregation with confidence weighting and
    market cap tier filtering for institutional decision making.
    """
    
    # Validate timeframe
    time_windows = {
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4), 
        "24h": timedelta(hours=24)
    }
    
    if timeframe not in time_windows:
        raise HTTPException(status_code=400, detail="Invalid timeframe. Use: 1h, 4h, 24h")
    
    since_time = datetime.utcnow() - time_windows[timeframe]
    
    try:
        supabase = get_supabase_client()
        
        # Query whale transactions using Supabase client
        result = supabase.table('whale_transactions').select('*').gte('timestamp', since_time.isoformat()).in_('classification', ['BUY', 'SELL']).eq('blockchain', 'ethereum').gte('whale_score', min_whale_score).not_.is_('token_symbol', 'null').order('timestamp', desc=True).limit(1000).execute()
        
        transactions = result.data
        
        # Aggregate transactions by token symbol
        token_stats = {}
        for tx in transactions:
            symbol = tx['token_symbol']
            if not symbol:
                continue
                
            if symbol not in token_stats:
                token_stats[symbol] = {
                    'buys': 0,
                    'sells': 0,
                    'total_volume': 0,
                    'confidences': [],
                    'whale_scores': [],
                    'transaction_count': 0
                }
            
            stats = token_stats[symbol]
            if tx['classification'] == 'BUY':
                stats['buys'] += 1
            elif tx['classification'] == 'SELL':
                stats['sells'] += 1
            
            stats['total_volume'] += float(tx['usd_value'] or 0)
            stats['confidences'].append(float(tx['confidence'] or 0))
            stats['whale_scores'].append(float(tx['whale_score'] or 0))
            stats['transaction_count'] += 1
        
        signals = []
        for symbol, stats in token_stats.items():
            total_trades = stats['buys'] + stats['sells']
            if total_trades < 2:  # Filter out tokens with less than 2 trades
                continue
                
            buy_pct = (stats['buys'] / total_trades) * 100 if total_trades > 0 else 0
            sell_pct = (stats['sells'] / total_trades) * 100 if total_trades > 0 else 0
            
            # Determine trend
            if buy_pct > 70:
                trend = "BUY"
            elif sell_pct > 70:
                trend = "SELL"
            elif abs(buy_pct - sell_pct) <= 10:
                trend = "MIXED"
            elif buy_pct > sell_pct:
                trend = "BUY"
            else:
                trend = "SELL"
            
            # Determine market cap tier
            symbol_upper = symbol.upper()
            if symbol_upper in ['USDT', 'USDC', 'WETH', 'WBTC', 'DAI', 'SHIB']:
                tier = "mega_cap"
            elif symbol_upper in ['UNI', 'LINK', 'MATIC', 'AAVE', 'MKR', 'LDO', 'APE', 'GRT', 'MANA', 'SAND', 'ARB', 'OP']:
                tier = "large_cap"
            elif symbol_upper in ['CRV', 'YFI', 'COMP', 'SUSHI', 'SNX', 'BAL', 'CVX', '1INCH', 'ENS', 'PEPE', 'FLOKI']:
                tier = "mid_cap"
            elif symbol_upper in ['FET', 'OCEAN', 'AGIX', 'RNDR', 'BLUR', 'RPL', 'SSV']:
                tier = "small_cap"
            else:
                tier = "micro_cap"
            
            # Apply tier filter if specified
            if tier_filter and tier != tier_filter:
                continue
            
            avg_confidence = sum(stats['confidences']) / len(stats['confidences']) if stats['confidences'] else 0
            avg_whale_score = sum(stats['whale_scores']) / len(stats['whale_scores']) if stats['whale_scores'] else 0
            
            signals.append(WhaleSignal(
                token_symbol=symbol_upper,
                buy_percentage=round(buy_pct, 1),
                sell_percentage=round(sell_pct, 1),
                whale_count=total_trades,
                transaction_count=stats['transaction_count'],
                total_volume_usd=round(stats['total_volume'], 2),
                avg_confidence=round(avg_confidence, 2),
                avg_whale_score=round(avg_whale_score, 2),
                trend=trend,
                market_cap_tier=tier
            ))
        
        # Sort by total volume descending
        signals.sort(key=lambda x: x.total_volume_usd, reverse=True)
        
        return signals
        
    except Exception as e:
        logger.error(f"Error fetching whale signals: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/token/{symbol}/activity", response_model=TokenActivity)
async def get_token_activity(symbol: str, limit: int = Query(50, le=200)):
    """
    📊 GET DETAILED WHALE ACTIVITY FOR SPECIFIC TOKEN
    
    Returns recent whale transactions for a specific token with
    comprehensive analysis and summary statistics.
    """
    
    symbol = symbol.upper()
    
    try:
        supabase = get_supabase_client()
        
        # Query for detailed token activity using Supabase client
        since_time = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        result = supabase.table('whale_transactions').select('*').eq('token_symbol', symbol).gte('timestamp', since_time).eq('blockchain', 'ethereum').order('timestamp', desc=True).limit(limit).execute()
        
        transactions = []
        for row in result.data:
            transactions.append({
                "transaction_hash": row['transaction_hash'],
                "classification": row['classification'],
                "confidence": float(row['confidence'] or 0),
                "usd_value": float(row['usd_value'] or 0),
                "whale_score": float(row['whale_score'] or 0),
                "from_address": row['from_address'],
                "to_address": row['to_address'],
                "timestamp": row['timestamp'],
                "reasoning": row['reasoning'],
                "analysis_phases": row['analysis_phases'] or 0
            })
        
        # Calculate summary statistics
        total = len(transactions)
        buy_count = sum(1 for tx in transactions if tx['classification'] == 'BUY')
        sell_count = sum(1 for tx in transactions if tx['classification'] == 'SELL')
        
        summary = {
            "total_transactions": total,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "buy_percentage": round((buy_count / max(buy_count + sell_count, 1)) * 100, 1),
            "sell_percentage": round((sell_count / max(buy_count + sell_count, 1)) * 100, 1),
            "avg_confidence": round(sum(tx['confidence'] for tx in transactions) / max(total, 1), 2),
            "total_volume": round(sum(tx['usd_value'] for tx in transactions), 2),
            "avg_whale_score": round(sum(tx['whale_score'] for tx in transactions) / max(total, 1), 2)
        }
        
        return TokenActivity(
            token_symbol=symbol,
            transactions=transactions,
            summary=summary
        )
        
    except Exception as e:
        logger.error(f"Error fetching token activity for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/system-stats", response_model=SystemStats)
async def get_system_stats():
    """
    📈 GET SYSTEM STATISTICS AND HEALTH
    
    Returns overall system performance metrics and status.
    """
    
    try:
        supabase = get_supabase_client()
        
        # Get system statistics using Supabase client
        since_24h = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        since_1h = (datetime.utcnow() - timedelta(hours=1)).isoformat()
        
        # Get all transactions from last 24 hours
        result_24h = supabase.table('whale_transactions').select('*').eq('blockchain', 'ethereum').gte('timestamp', since_24h).execute()
        result_1h = supabase.table('whale_transactions').select('*').eq('blockchain', 'ethereum').gte('timestamp', since_1h).execute()
        
        # Calculate statistics
        transactions_24h = result_24h.data
        transactions_1h = result_1h.data
        
        unique_tokens = len(set(tx['token_symbol'] for tx in transactions_24h if tx['token_symbol']))
        last_24h_txs = len(transactions_24h)
        last_hour_txs = len(transactions_1h)
        active_signals = len([tx for tx in transactions_1h if tx['classification'] in ['BUY', 'SELL']])
        
        return SystemStats(
            total_tokens_monitored=unique_tokens,
            active_whale_signals=active_signals,
            last_24h_transactions=last_24h_txs,
            system_status="operational" if last_hour_txs > 0 else "low_activity"
        )
        
    except Exception as e:
        logger.error(f"Error fetching system stats: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard():
    """
    🖥️ SERVE INSTITUTIONAL WHALE INTELLIGENCE DASHBOARD
    
    Returns a comprehensive HTML dashboard with real-time whale signals,
    auto-refresh functionality, and professional styling.
    """
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🐋 Institutional Whale Intelligence Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            .trend-buy { color: #10b981; background-color: rgba(16, 185, 129, 0.1); }
            .trend-sell { color: #ef4444; background-color: rgba(239, 68, 68, 0.1); }
            .trend-mixed { color: #f59e0b; background-color: rgba(245, 158, 11, 0.1); }
            .tier-mega { border-left: 4px solid #8b5cf6; }
            .tier-large { border-left: 4px solid #06b6d4; }
            .tier-mid { border-left: 4px solid #10b981; }
            .tier-small { border-left: 4px solid #f59e0b; }
            .tier-micro { border-left: 4px solid #ef4444; }
            .pulse-dot { animation: pulse 2s infinite; }
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.5; }
            }
        </style>
    </head>
    <body class="bg-gray-900 text-white min-h-screen">
        <div class="container mx-auto px-4 py-8">
            <!-- Header -->
            <header class="mb-8">
                <div class="text-center">
                    <h1 class="text-5xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent mb-4">
                        🐋 Whale Intelligence Dashboard
                    </h1>
                    <p class="text-xl text-gray-400 mb-6">Institutional-Grade Blockchain Intelligence</p>
                    
                    <!-- Status Bar -->
                    <div class="flex justify-center items-center space-x-6 mb-6">
                        <div class="flex items-center">
                            <div id="status-dot" class="w-3 h-3 bg-green-500 rounded-full pulse-dot mr-2"></div>
                            <span id="status-text" class="text-sm font-medium">🟢 Live - Updates every 10s</span>
                        </div>
                        <div class="text-sm text-gray-400">
                            <span id="last-update">Last updated: --:--:--</span>
                        </div>
                        <div class="text-sm text-gray-400">
                            <span id="total-tokens">0 tokens monitored</span>
                        </div>
                    </div>
                    
                    <!-- Quick Stats -->
                    <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
                        <div class="bg-gray-800 rounded-lg p-4">
                            <div class="text-2xl font-bold text-green-400" id="active-signals">--</div>
                            <div class="text-sm text-gray-400">Active Signals</div>
                        </div>
                        <div class="bg-gray-800 rounded-lg p-4">
                            <div class="text-2xl font-bold text-blue-400" id="total-volume">--</div>
                            <div class="text-sm text-gray-400">24h Volume</div>
                        </div>
                        <div class="bg-gray-800 rounded-lg p-4">
                            <div class="text-2xl font-bold text-purple-400" id="avg-confidence">--</div>
                            <div class="text-sm text-gray-400">Avg Confidence</div>
                        </div>
                        <div class="bg-gray-800 rounded-lg p-4">
                            <div class="text-2xl font-bold text-yellow-400" id="whale-count">--</div>
                            <div class="text-sm text-gray-400">Whale Transactions</div>
                        </div>
                    </div>
                </div>
            </header>

            <!-- Filter Controls -->
            <div class="bg-gray-800 rounded-lg p-4 mb-6">
                <div class="flex flex-wrap items-center gap-4">
                    <select id="timeframe-filter" class="bg-gray-700 text-white rounded px-3 py-2">
                        <option value="1h">Last 1 Hour</option>
                        <option value="4h">Last 4 Hours</option>
                        <option value="24h">Last 24 Hours</option>
                    </select>
                    <select id="tier-filter" class="bg-gray-700 text-white rounded px-3 py-2">
                        <option value="">All Tiers</option>
                        <option value="mega_cap">Mega Cap</option>
                        <option value="large_cap">Large Cap</option>
                        <option value="mid_cap">Mid Cap</option>
                        <option value="small_cap">Small Cap</option>
                        <option value="micro_cap">Micro Cap</option>
                    </select>
                    <input type="range" id="whale-score-filter" min="0" max="100" value="0" 
                           class="w-32" title="Minimum Whale Score">
                    <span class="text-sm text-gray-400">Whale Score: <span id="whale-score-value">0</span></span>
                </div>
            </div>

            <!-- Main Whale Signals Table -->
            <div class="bg-gray-800 rounded-lg overflow-hidden shadow-2xl">
                <div class="px-6 py-4 bg-gray-700 border-b border-gray-600">
                    <h2 class="text-2xl font-semibold">Live Whale Signals</h2>
                </div>
                
                <div class="overflow-x-auto">
                    <table class="w-full text-sm">
                        <thead class="bg-gray-700">
                            <tr>
                                <th class="px-6 py-3 text-left cursor-pointer hover:bg-gray-600" onclick="sortBy('token_symbol')">
                                    Token 📊
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('market_cap_tier')">
                                    Tier 🏆
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('buy_percentage')">
                                    Buy % 🟢
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('sell_percentage')">
                                    Sell % 🔴
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('whale_count')">
                                    Whales 🐋
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('transaction_count')">
                                    Transactions 📊
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('total_volume_usd')">
                                    Volume (USD) 💰
                                </th>
                                <th class="px-6 py-3 text-center cursor-pointer hover:bg-gray-600" onclick="sortBy('avg_confidence')">
                                    Confidence ⚡
                                </th>
                                <th class="px-6 py-3 text-center">
                                    Trend 📈
                                </th>
                            </tr>
                        </thead>
                        <tbody id="whale-signals-body" class="divide-y divide-gray-600">
                            <!-- Auto-populated via JavaScript -->
                        </tbody>
                    </table>
                </div>
                
                <div class="px-6 py-4 bg-gray-700 text-center text-gray-400">
                    <p class="text-sm">
                        Powered by institutional-grade whale intelligence | 
                        <span id="signal-count">0</span> signals detected |
                        <span id="system-status">System operational</span>
                    </p>
                </div>
            </div>
        </div>

        <script>
            let whaleData = [];
            let sortColumn = 'total_volume_usd';
            let sortDirection = 'desc';
            let updateInterval;

            // Initialize dashboard
            async function initDashboard() {
                await fetchSystemStats();
                await fetchWhaleSignals();
                startAutoRefresh();
            }

            async function fetchSystemStats() {
                try {
                    const response = await fetch('/system-stats');
                    const stats = await response.json();
                    
                    document.getElementById('total-tokens').textContent = `${stats.total_tokens_monitored} tokens monitored`;
                    document.getElementById('active-signals').textContent = stats.active_whale_signals;
                    document.getElementById('system-status').textContent = 
                        stats.system_status === 'operational' ? 'System operational' : 'Low activity detected';
                } catch (error) {
                    console.error('Error fetching system stats:', error);
                }
            }

            async function fetchWhaleSignals() {
                try {
                    const timeframe = document.getElementById('timeframe-filter').value;
                    const tierFilter = document.getElementById('tier-filter').value;
                    const whaleScore = document.getElementById('whale-score-filter').value;
                    
                    let url = `/whale-signals?timeframe=${timeframe}&min_whale_score=${whaleScore}`;
                    if (tierFilter) url += `&tier_filter=${tierFilter}`;
                    
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    whaleData = data;
                    updateTable();
                    updateQuickStats();
                    updateStatus(true);
                    
                } catch (error) {
                    console.error('Error fetching whale signals:', error);
                    updateStatus(false);
                }
            }

            function updateTable() {
                const tbody = document.getElementById('whale-signals-body');
                
                // Sort data
                whaleData.sort((a, b) => {
                    let aVal = a[sortColumn];
                    let bVal = b[sortColumn];
                    
                    if (typeof aVal === 'string') {
                        return sortDirection === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                    }
                    
                    return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
                });
                
                tbody.innerHTML = whaleData.map(signal => `
                    <tr class="hover:bg-gray-700 tier-${signal.market_cap_tier}">
                        <td class="px-6 py-4 font-bold text-lg">${signal.token_symbol}</td>
                        <td class="px-6 py-4 text-center">
                            <span class="px-2 py-1 rounded text-xs font-medium bg-gray-600">
                                ${formatTier(signal.market_cap_tier)}
                            </span>
                        </td>
                        <td class="px-6 py-4 text-center">
                            <span class="text-green-400 font-bold text-lg">${signal.buy_percentage}%</span>
                        </td>
                        <td class="px-6 py-4 text-center">
                            <span class="text-red-400 font-bold text-lg">${signal.sell_percentage}%</span>
                        </td>
                        <td class="px-6 py-4 text-center font-semibold">${signal.whale_count}</td>
                        <td class="px-6 py-4 text-center">${signal.transaction_count}</td>
                        <td class="px-6 py-4 text-center">$${formatNumber(signal.total_volume_usd)}</td>
                        <td class="px-6 py-4 text-center">${signal.avg_confidence}</td>
                        <td class="px-6 py-4 text-center">
                            <span class="px-3 py-1 rounded font-bold trend-${signal.trend.toLowerCase()}">
                                ${signal.trend} ${getTrendEmoji(signal.trend)}
                            </span>
                        </td>
                    </tr>
                `).join('');
                
                document.getElementById('signal-count').textContent = whaleData.length;
            }

            function updateQuickStats() {
                const totalVolume = whaleData.reduce((sum, signal) => sum + signal.total_volume_usd, 0);
                const avgConfidence = whaleData.reduce((sum, signal) => sum + signal.avg_confidence, 0) / Math.max(whaleData.length, 1);
                const totalWhales = whaleData.reduce((sum, signal) => sum + signal.whale_count, 0);
                
                document.getElementById('total-volume').textContent = `$${formatNumber(totalVolume)}`;
                document.getElementById('avg-confidence').textContent = avgConfidence.toFixed(2);
                document.getElementById('whale-count').textContent = totalWhales;
            }

            function updateStatus(success) {
                const statusDot = document.getElementById('status-dot');
                const statusText = document.getElementById('status-text');
                const lastUpdate = document.getElementById('last-update');
                
                if (success) {
                    statusDot.className = 'w-3 h-3 bg-green-500 rounded-full pulse-dot mr-2';
                    statusText.textContent = '🟢 Live - Updates every 10s';
                } else {
                    statusDot.className = 'w-3 h-3 bg-red-500 rounded-full mr-2';
                    statusText.textContent = '🔴 Connection Error';
                }
                
                lastUpdate.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
            }

            function startAutoRefresh() {
                if (updateInterval) clearInterval(updateInterval);
                updateInterval = setInterval(fetchWhaleSignals, 10000); // 10 seconds
            }

            function sortBy(column) {
                if (sortColumn === column) {
                    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    sortColumn = column;
                    sortDirection = column === 'token_symbol' ? 'asc' : 'desc';
                }
                updateTable();
            }

            function formatNumber(num) {
                if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
                if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
                return num.toFixed(0);
            }

            function formatTier(tier) {
                const tiers = {
                    'mega_cap': 'MEGA',
                    'large_cap': 'LARGE',
                    'mid_cap': 'MID',
                    'small_cap': 'SMALL',
                    'micro_cap': 'MICRO'
                };
                return tiers[tier] || tier.toUpperCase();
            }

            function getTrendEmoji(trend) {
                switch(trend) {
                    case 'BUY': return '🚀';
                    case 'SELL': return '📉';
                    case 'MIXED': return '⚖️';
                    default: return '🔄';
                }
            }

            // Event listeners
            document.getElementById('timeframe-filter').addEventListener('change', fetchWhaleSignals);
            document.getElementById('tier-filter').addEventListener('change', fetchWhaleSignals);
            document.getElementById('whale-score-filter').addEventListener('input', function() {
                document.getElementById('whale-score-value').textContent = this.value;
                clearTimeout(this.timeout);
                this.timeout = setTimeout(fetchWhaleSignals, 500);
            });

            // Initialize dashboard on load
            initDashboard();
        </script>
    </body>
    </html>
    """

@app.get("/health")
async def health_check():
    """🔍 Health check endpoint for monitoring"""
    if TEST_MODE:
        return {
            "status": "healthy", 
            "mode": "test",
            "database": "disabled",
            "timestamp": datetime.utcnow().isoformat()
        }
    
    try:
        supabase = get_supabase_client()
        # Test connection by fetching count of whale transactions
        result = supabase.table('whale_transactions').select('id', count='exact').limit(1).execute()
        return {
            "status": "healthy", 
            "database": "connected",
            "total_transactions": result.count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "degraded", 
            "database": "disconnected",
            "error": str(e)[:100],  # Truncate error message
            "timestamp": datetime.utcnow().isoformat()
        }

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Institutional Whale Intelligence API...")
    print("📊 Dashboard: http://localhost:8000/dashboard")
    print("📚 API Docs: http://localhost:8000/docs")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info",
        access_log=True
    ) 