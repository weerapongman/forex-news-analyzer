import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import { WebSocketServer } from 'ws';
import http from 'http';
import axios from 'axios';
import * as cheerio from 'cheerio';
import cron from 'node-cron';
import Sentiment from 'sentiment';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const sentiment = new Sentiment();

// Store connected clients
const clients = new Set();

// Enhanced cache
let newsCache = {
  rawNews: [],
  analyzed: [],
  highImpact: [],
  byProduct: {},
  signals: [],
  lastUpdate: null,
  sourcesCount: 0,
  analysisTime: 0,
  dataQuality: 'unknown',
  activeSources: []
};

// Track API health
let apiHealth = {
  forexFactory: { working: true, lastSuccess: null, failCount: 0 },
  investing: { working: true, lastSuccess: null, failCount: 0 },
  tradingView: { working: true, lastSuccess: null, failCount: 0 },
  myfxbook: { working: true, lastSuccess: null, failCount: 0 },
  fxstreet: { working: true, lastSuccess: null, failCount: 0 }
};

// ============================================
// TRADING PRODUCTS MAPPING
// ============================================
const TRADING_PRODUCTS = {
  FOREX: {
    'USD': ['EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CNY', 'USD/HKD', 'USD/SGD', 'DXY'],
    'EUR': ['EUR/USD', 'EUR/GBP', 'EUR/JPY', 'EUR/CHF', 'EUR/AUD', 'EUR/CAD', 'EUR/NZD', 'EUR/CNY'],
    'GBP': ['GBP/USD', 'EUR/GBP', 'GBP/JPY', 'GBP/AUD', 'GBP/CHF', 'GBP/CAD', 'GBP/NZD'],
    'JPY': ['USD/JPY', 'EUR/JPY', 'GBP/JPY', 'AUD/JPY', 'CAD/JPY', 'CHF/JPY', 'NZD/JPY'],
    'AUD': ['AUD/USD', 'EUR/AUD', 'GBP/AUD', 'AUD/JPY', 'AUD/NZD', 'AUD/CAD', 'AUD/CHF'],
    'NZD': ['NZD/USD', 'NZD/JPY', 'EUR/NZD', 'GBP/NZD', 'AUD/NZD', 'NZD/CAD', 'NZD/CHF'],
    'CAD': ['USD/CAD', 'CAD/JPY', 'EUR/CAD', 'GBP/CAD', 'AUD/CAD', 'NZD/CAD', 'CAD/CHF'],
    'CHF': ['USD/CHF', 'EUR/CHF', 'GBP/CHF', 'CHF/JPY', 'AUD/CHF', 'NZD/CHF', 'CAD/CHF'],
    'CNY': ['USD/CNY', 'EUR/CNY', 'GBP/CNY', 'CNY/JPY']
  },
  COMMODITIES: {
    'USD': ['Gold', 'Silver', 'Crude Oil', 'Natural Gas', 'Copper', 'Platinum'],
    'EUR': ['Gold', 'Silver', 'Crude Oil'],
    'CNY': ['Copper', 'Iron Ore', 'Steel', 'Aluminum'],
    'AUD': ['Gold', 'Iron Ore', 'Coal'],
    'CAD': ['Crude Oil', 'Natural Gas', 'Gold'],
    'GLOBAL': ['Gold', 'Silver', 'Crude Oil', 'Brent Oil', 'Natural Gas', 'Copper']
  },
  INDICES: {
    'USD': ['S&P 500', 'Dow Jones', 'NASDAQ', 'Russell 2000'],
    'EUR': ['DAX', 'CAC 40', 'EURO STOXX 50'],
    'GBP': ['FTSE 100'],
    'JPY': ['Nikkei 225', 'TOPIX'],
    'AUD': ['ASX 200'],
    'CNY': ['Shanghai Composite', 'Hang Seng', 'CSI 300'],
    'GLOBAL': ['VIX', 'MSCI World']
  },
  CRYPTO: {
    'USD': ['BTC/USD', 'ETH/USD', 'BNB/USD', 'XRP/USD', 'ADA/USD', 'SOL/USD'],
    'GLOBAL': ['BTC/USD', 'ETH/USD', 'Crypto Market Cap']
  },
  BONDS: {
    'USD': ['US 10Y Treasury', 'US 2Y Treasury', 'US 30Y Treasury'],
    'EUR': ['German 10Y Bund', 'EU Bonds'],
    'GBP': ['UK 10Y Gilt'],
    'JPY': ['Japan 10Y Bond']
  }
};

const HIGH_IMPACT_INDICATORS = [
  'NFP', 'NON-FARM', 'PAYROLL', 'EMPLOYMENT', 'UNEMPLOYMENT', 'JOBLESS', 'CLAIMS',
  'ADP', 'LABOR FORCE', 'JOB OPENINGS', 'JOLTS', 'WAGE', 'EARNINGS',
  'FOMC', 'FED', 'FEDERAL RESERVE', 'INTEREST RATE', 'RATE DECISION', 'RATE CUT', 'RATE HIKE',
  'ECB', 'EUROPEAN CENTRAL BANK', 'BOE', 'BANK OF ENGLAND', 'BOJ', 'BANK OF JAPAN',
  'RBA', 'RESERVE BANK AUSTRALIA', 'RBNZ', 'SNB', 'BOC', 'BANK OF CANADA', 'PBOC',
  'MONETARY POLICY', 'QUANTITATIVE EASING', 'QE', 'TIGHTENING',
  'GDP', 'GROSS DOMESTIC', 'GROWTH', 'ECONOMIC GROWTH', 'RECESSION',
  'CPI', 'CONSUMER PRICE', 'INFLATION', 'CORE INFLATION', 'DEFLATION',
  'PPI', 'PRODUCER PRICE', 'PCE', 'PRICE INDEX',
  'TRADE BALANCE', 'CURRENT ACCOUNT', 'RETAIL SALES', 'CONSUMER SPENDING',
  'CONSUMER CONFIDENCE', 'CONSUMER SENTIMENT', 'MICHIGAN',
  'PMI', 'PURCHASING MANAGERS', 'MANUFACTURING', 'SERVICES', 'ISM',
  'INDUSTRIAL PRODUCTION', 'HOUSING STARTS', 'BUILDING PERMITS'
];

// Serve static files
app.use(express.static(__dirname));
app.use(express.json());

// Root route - serve index.html
app.get('/', (req, res) => {
  res.sendFile(join(__dirname, 'index.html'));
});

// WebSocket handler
wss.on('connection', (ws) => {
  console.log('✅ Client connected');
  clients.add(ws);
  if (newsCache.lastUpdate) {
    ws.send(JSON.stringify({ type: 'initial', data: newsCache }));
  }
  ws.on('close', () => { console.log('❌ Client disconnected'); clients.delete(ws); });
  ws.on('error', (error) => { console.error('WebSocket error:', error); clients.delete(ws); });
});

function broadcast(data) {
  const message = JSON.stringify(data);
  let successCount = 0;
  clients.forEach(client => {
    if (client.readyState === 1) {
      try { client.send(message); successCount++; } 
      catch (error) { console.error('Error broadcasting:', error); }
    }
  });
  if (successCount > 0) console.log(`📡 Broadcast to ${successCount} client(s)`);
}

// ============================================
// API SOURCE 1: ForexFactory (Primary)
// ============================================
async function fetchForexFactory() {
  try {
    console.log('📡 Source 1: ForexFactory...');
    const url = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json';
    
    const response = await axios.get(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
      }
    });
    
    if (response.status === 429) {
      console.log('⚠️  ForexFactory: Rate limited');
      apiHealth.forexFactory.working = false;
      apiHealth.forexFactory.failCount++;
      return [];
    }
    
    if (!response.data || !Array.isArray(response.data)) {
      console.log('⚠️  ForexFactory: Invalid format');
      return [];
    }
    
    const events = processForexFactoryData(response.data);
    apiHealth.forexFactory.working = true;
    apiHealth.forexFactory.lastSuccess = new Date();
    apiHealth.forexFactory.failCount = 0;
    
    console.log(`✅ ForexFactory: ${events.length} events`);
    return events;
    
  } catch (error) {
    console.log(`⚠️  ForexFactory: Unavailable`);
    apiHealth.forexFactory.working = false;
    apiHealth.forexFactory.failCount++;
    return [];
  }
}

function processForexFactoryData(data) {
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const nextWeek = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
  
  return data
    .filter(event => {
      if (!event || !event.date) return false;
      const eventDate = new Date(event.date);
      return eventDate >= yesterday && eventDate <= nextWeek;
    })
    .map(event => ({
      id: `ff_${event.date}_${event.title}`.replace(/[^a-zA-Z0-9_]/g, '_'),
      time: event.date,
      currency: (event.country || 'USD').toUpperCase(),
      title: event.title || 'Economic Event',
      impact: event.impact?.toLowerCase() === 'high' ? 'high' : 
              event.impact?.toLowerCase() === 'medium' ? 'medium' : 'low',
      actual: event.actual || '-',
      forecast: event.forecast || '-',
      previous: event.previous || '-',
      source: 'ForexFactory',
      description: event.title || ''
    }));
}

// ============================================
// API SOURCE 2: Investing.com Scraper
// ============================================
async function fetchInvesting() {
  try {
    console.log('📡 Source 2: Investing.com...');
    const url = 'https://www.investing.com/economic-calendar/';
    
    const response = await axios.get(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html'
      }
    });
    
    const $ = cheerio.load(response.data);
    const events = [];
    
    $('#economicCalendarData tr').each((i, elem) => {
      if (i >= 30) return false;
      
      const $row = $(elem);
      const time = $row.find('.time').text().trim();
      const currency = $row.find('.flagCur').text().trim();
      const title = $row.find('.event a').text().trim();
      const actual = $row.find('.act').text().trim();
      const forecast = $row.find('.fore').text().trim();
      const previous = $row.find('.prev').text().trim();
      const importance = $row.attr('data-event-importance');
      
      if (title) {
        events.push({
          id: `inv_${Date.now()}_${i}`,
          time: new Date().toISOString(),
          currency: currency || 'USD',
          title: title,
          impact: importance >= 2 ? 'high' : 'medium',
          actual: actual || '-',
          forecast: forecast || '-',
          previous: previous || '-',
          source: 'Investing.com',
          description: title
        });
      }
    });
    
    apiHealth.investing.working = true;
    apiHealth.investing.lastSuccess = new Date();
    console.log(`✅ Investing.com: ${events.length} events`);
    return events;
    
  } catch (error) {
    console.log(`⚠️  Investing.com: Unavailable`);
    apiHealth.investing.working = false;
    return [];
  }
}

// ============================================
// API SOURCE 3: TradingView Calendar
// ============================================
async function fetchTradingView() {
  try {
    console.log('📡 Source 3: TradingView...');
    
    // TradingView uses a different API structure
    const url = 'https://economic-calendar.tradingview.com/events';
    const params = {
      from: new Date(Date.now() - 86400000).toISOString(),
      to: new Date(Date.now() + 604800000).toISOString(),
      countries: 'US,EU,GB,JP,AU,NZ,CA,CH,CN',
      importance: '1,2,3'
    };
    
    const response = await axios.get(url, {
      timeout: 15000,
      params: params,
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
      }
    });
    
    if (!response.data || !response.data.result) return [];
    
    const events = response.data.result.slice(0, 30).map((e, i) => ({
      id: `tv_${e.id || i}`,
      time: e.date || new Date().toISOString(),
      currency: e.country || 'USD',
      title: e.title || 'Economic Event',
      impact: e.importance >= 2 ? 'high' : 'medium',
      actual: e.actual || '-',
      forecast: e.forecast || '-',
      previous: e.previous || '-',
      source: 'TradingView',
      description: e.title || ''
    }));
    
    apiHealth.tradingView.working = true;
    apiHealth.tradingView.lastSuccess = new Date();
    console.log(`✅ TradingView: ${events.length} events`);
    return events;
    
  } catch (error) {
    console.log(`⚠️  TradingView: Unavailable`);
    apiHealth.tradingView.working = false;
    return [];
  }
}

// ============================================
// API SOURCE 4: MyFXBook Calendar
// ============================================
async function fetchMyFXBook() {
  try {
    console.log('📡 Source 4: MyFXBook...');
    const url = 'https://www.myfxbook.com/forex-economic-calendar';
    
    const response = await axios.get(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/html'
      }
    });
    
    const $ = cheerio.load(response.data);
    const events = [];
    
    $('.calendar-row').each((i, elem) => {
      if (i >= 20) return false;
      
      const $row = $(elem);
      const title = $row.find('.calendar-event').text().trim();
      const currency = $row.find('.calendar-currency').text().trim();
      const actual = $row.find('.calendar-actual').text().trim();
      const forecast = $row.find('.calendar-forecast').text().trim();
      const previous = $row.find('.calendar-previous').text().trim();
      
      if (title) {
        events.push({
          id: `mfx_${Date.now()}_${i}`,
          time: new Date().toISOString(),
          currency: currency || 'USD',
          title: title,
          impact: 'medium',
          actual: actual || '-',
          forecast: forecast || '-',
          previous: previous || '-',
          source: 'MyFXBook',
          description: title
        });
      }
    });
    
    apiHealth.myfxbook.working = true;
    apiHealth.myfxbook.lastSuccess = new Date();
    console.log(`✅ MyFXBook: ${events.length} events`);
    return events;
    
  } catch (error) {
    console.log(`⚠️  MyFXBook: Unavailable`);
    apiHealth.myfxbook.working = false;
    return [];
  }
}

// ============================================
// API SOURCE 5: FXStreet Calendar API
// ============================================
async function fetchFXStreet() {
  try {
    console.log('📡 Source 5: FXStreet...');
    const url = 'https://calendar-api.fxstreet.com/en/api/v1/eventDates';
    
    const response = await axios.get(url, {
      timeout: 15000,
      params: {
        timezone: 'GMT',
        rows: 30
      },
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
      }
    });
    
    if (!response.data || !response.data.events) return [];
    
    const events = response.data.events.slice(0, 30).map((e, i) => ({
      id: `fxs_${e.dateUtc}_${e.name}`.replace(/[^a-zA-Z0-9_]/g, '_'),
      time: e.dateUtc || new Date().toISOString(),
      currency: e.countryCode || 'USD',
      title: e.name || 'Economic Event',
      impact: e.volatility >= 2 ? 'high' : 'medium',
      actual: e.actual || '-',
      forecast: e.forecast || '-',
      previous: e.previous || '-',
      source: 'FXStreet',
      description: e.name || ''
    }));
    
    apiHealth.fxstreet.working = true;
    apiHealth.fxstreet.lastSuccess = new Date();
    console.log(`✅ FXStreet: ${events.length} events`);
    return events;
    
  } catch (error) {
    console.log(`⚠️  FXStreet: Unavailable`);
    apiHealth.fxstreet.working = false;
    return [];
  }
}

// ============================================
// FALLBACK: Enhanced Sample Real Economic Data
// ============================================
function getSampleRealData() {
  console.log('📊 Using enhanced sample economic data (APIs unavailable)');
  const now = new Date();
  const addHours = (hours) => new Date(now.getTime() + hours * 3600000);
  
  return [
    // === US EVENTS ===
    {
      id: 'sample_usd_1',
      time: addHours(8).toISOString(),
      currency: 'USD',
      title: 'Non-Farm Payrolls (NFP)',
      impact: 'high',
      actual: '-',
      forecast: '185K',
      previous: '187K',
      source: 'Sample Data',
      description: 'Monthly change in US employment'
    },
    {
      id: 'sample_usd_2',
      time: addHours(8).toISOString(),
      currency: 'USD',
      title: 'Unemployment Rate',
      impact: 'high',
      actual: '-',
      forecast: '3.8%',
      previous: '3.7%',
      source: 'Sample Data',
      description: 'Percentage of unemployed workers'
    },
    {
      id: 'sample_usd_3',
      time: addHours(14).toISOString(),
      currency: 'USD',
      title: 'Consumer Price Index (CPI)',
      impact: 'high',
      actual: '-',
      forecast: '3.2%',
      previous: '3.4%',
      source: 'Sample Data',
      description: 'Inflation measure'
    },
    {
      id: 'sample_usd_4',
      time: addHours(20).toISOString(),
      currency: 'USD',
      title: 'FOMC Meeting Minutes',
      impact: 'high',
      actual: '-',
      forecast: '-',
      previous: '-',
      source: 'Sample Data',
      description: 'Federal Reserve policy meeting'
    },
    {
      id: 'sample_usd_5',
      time: addHours(26).toISOString(),
      currency: 'USD',
      title: 'Retail Sales',
      impact: 'medium',
      actual: '-',
      forecast: '0.3%',
      previous: '0.1%',
      source: 'Sample Data',
      description: 'Consumer spending indicator'
    },
    {
      id: 'sample_usd_6',
      time: addHours(32).toISOString(),
      currency: 'USD',
      title: 'GDP Growth Rate',
      impact: 'high',
      actual: '-',
      forecast: '2.4%',
      previous: '2.1%',
      source: 'Sample Data',
      description: 'Quarterly economic growth'
    },
    {
      id: 'sample_usd_7',
      time: addHours(38).toISOString(),
      currency: 'USD',
      title: 'ISM Manufacturing PMI',
      impact: 'medium',
      actual: '-',
      forecast: '48.5',
      previous: '48.1',
      source: 'Sample Data',
      description: 'Manufacturing sector health'
    },
    {
      id: 'sample_usd_8',
      time: addHours(44).toISOString(),
      currency: 'USD',
      title: 'Initial Jobless Claims',
      impact: 'medium',
      actual: '-',
      forecast: '215K',
      previous: '220K',
      source: 'Sample Data',
      description: 'Weekly unemployment claims'
    },
    {
      id: 'sample_usd_9',
      time: addHours(50).toISOString(),
      currency: 'USD',
      title: 'Core PCE Price Index',
      impact: 'high',
      actual: '-',
      forecast: '2.8%',
      previous: '2.9%',
      source: 'Sample Data',
      description: 'Fed\'s preferred inflation gauge'
    },
    {
      id: 'sample_usd_10',
      time: addHours(56).toISOString(),
      currency: 'USD',
      title: 'Fed Chair Powell Speech',
      impact: 'high',
      actual: '-',
      forecast: '-',
      previous: '-',
      source: 'Sample Data',
      description: 'Federal Reserve Chairman speech'
    },
    
    // === EUROZONE EVENTS ===
    {
      id: 'sample_eur_1',
      time: addHours(12).toISOString(),
      currency: 'EUR',
      title: 'ECB Interest Rate Decision',
      impact: 'high',
      actual: '-',
      forecast: '4.50%',
      previous: '4.50%',
      source: 'Sample Data',
      description: 'European Central Bank rate'
    },
    {
      id: 'sample_eur_2',
      time: addHours(18).toISOString(),
      currency: 'EUR',
      title: 'German Manufacturing PMI',
      impact: 'medium',
      actual: '-',
      forecast: '43.2',
      previous: '42.5',
      source: 'Sample Data',
      description: 'German factory activity'
    },
    {
      id: 'sample_eur_3',
      time: addHours(24).toISOString(),
      currency: 'EUR',
      title: 'Eurozone CPI',
      impact: 'high',
      actual: '-',
      forecast: '2.4%',
      previous: '2.6%',
      source: 'Sample Data',
      description: 'Eurozone inflation rate'
    },
    {
      id: 'sample_eur_4',
      time: addHours(36).toISOString(),
      currency: 'EUR',
      title: 'ECB President Lagarde Speech',
      impact: 'high',
      actual: '-',
      forecast: '-',
      previous: '-',
      source: 'Sample Data',
      description: 'ECB policy communication'
    },
    {
      id: 'sample_eur_5',
      time: addHours(48).toISOString(),
      currency: 'EUR',
      title: 'German GDP',
      impact: 'high',
      actual: '-',
      forecast: '0.1%',
      previous: '-0.1%',
      source: 'Sample Data',
      description: 'German economic growth'
    },
    
    // === UK EVENTS ===
    {
      id: 'sample_gbp_1',
      time: addHours(10).toISOString(),
      currency: 'GBP',
      title: 'BoE Interest Rate Decision',
      impact: 'high',
      actual: '-',
      forecast: '5.25%',
      previous: '5.25%',
      source: 'Sample Data',
      description: 'Bank of England rate'
    },
    {
      id: 'sample_gbp_2',
      time: addHours(22).toISOString(),
      currency: 'GBP',
      title: 'UK GDP Growth Rate',
      impact: 'high',
      actual: '-',
      forecast: '0.2%',
      previous: '0.1%',
      source: 'Sample Data',
      description: 'UK economic growth'
    },
    {
      id: 'sample_gbp_3',
      time: addHours(34).toISOString(),
      currency: 'GBP',
      title: 'UK CPI',
      impact: 'high',
      actual: '-',
      forecast: '3.9%',
      previous: '4.0%',
      source: 'Sample Data',
      description: 'UK inflation rate'
    },
    {
      id: 'sample_gbp_4',
      time: addHours(46).toISOString(),
      currency: 'GBP',
      title: 'UK Unemployment Rate',
      impact: 'medium',
      actual: '-',
      forecast: '4.2%',
      previous: '4.3%',
      source: 'Sample Data',
      description: 'UK jobless rate'
    },
    
    // === JAPAN EVENTS ===
    {
      id: 'sample_jpy_1',
      time: addHours(6).toISOString(),
      currency: 'JPY',
      title: 'BoJ Policy Rate',
      impact: 'high',
      actual: '-',
      forecast: '0.25%',
      previous: '0.25%',
      source: 'Sample Data',
      description: 'Bank of Japan policy rate'
    },
    {
      id: 'sample_jpy_2',
      time: addHours(28).toISOString(),
      currency: 'JPY',
      title: 'Japan CPI',
      impact: 'medium',
      actual: '-',
      forecast: '2.5%',
      previous: '2.6%',
      source: 'Sample Data',
      description: 'Japan inflation'
    },
    {
      id: 'sample_jpy_3',
      time: addHours(40).toISOString(),
      currency: 'JPY',
      title: 'Japan GDP',
      impact: 'high',
      actual: '-',
      forecast: '0.3%',
      previous: '0.2%',
      source: 'Sample Data',
      description: 'Japanese economic growth'
    },
    
    // === AUSTRALIA EVENTS ===
    {
      id: 'sample_aud_1',
      time: addHours(4).toISOString(),
      currency: 'AUD',
      title: 'RBA Interest Rate Decision',
      impact: 'high',
      actual: '-',
      forecast: '4.35%',
      previous: '4.35%',
      source: 'Sample Data',
      description: 'Reserve Bank of Australia rate'
    },
    {
      id: 'sample_aud_2',
      time: addHours(16).toISOString(),
      currency: 'AUD',
      title: 'Australia Employment Change',
      impact: 'medium',
      actual: '-',
      forecast: '25K',
      previous: '28K',
      source: 'Sample Data',
      description: 'Australian jobs report'
    },
    {
      id: 'sample_aud_3',
      time: addHours(30).toISOString(),
      currency: 'AUD',
      title: 'Australia CPI',
      impact: 'high',
      actual: '-',
      forecast: '3.5%',
      previous: '3.8%',
      source: 'Sample Data',
      description: 'Australian inflation'
    },
    
    // === CANADA EVENTS ===
    {
      id: 'sample_cad_1',
      time: addHours(15).toISOString(),
      currency: 'CAD',
      title: 'BoC Interest Rate Decision',
      impact: 'high',
      actual: '-',
      forecast: '5.00%',
      previous: '5.00%',
      source: 'Sample Data',
      description: 'Bank of Canada rate'
    },
    {
      id: 'sample_cad_2',
      time: addHours(27).toISOString(),
      currency: 'CAD',
      title: 'Canada Employment Change',
      impact: 'medium',
      actual: '-',
      forecast: '18K',
      previous: '20K',
      source: 'Sample Data',
      description: 'Canadian jobs'
    },
    {
      id: 'sample_cad_3',
      time: addHours(42).toISOString(),
      currency: 'CAD',
      title: 'Canada CPI',
      impact: 'high',
      actual: '-',
      forecast: '3.1%',
      previous: '3.3%',
      source: 'Sample Data',
      description: 'Canadian inflation'
    },
    
    // === NEW ZEALAND ===
    {
      id: 'sample_nzd_1',
      time: addHours(2).toISOString(),
      currency: 'NZD',
      title: 'RBNZ Interest Rate Decision',
      impact: 'high',
      actual: '-',
      forecast: '5.50%',
      previous: '5.50%',
      source: 'Sample Data',
      description: 'Reserve Bank of New Zealand rate'
    },
    
    // === SWITZERLAND ===
    {
      id: 'sample_chf_1',
      time: addHours(11).toISOString(),
      currency: 'CHF',
      title: 'SNB Policy Rate',
      impact: 'high',
      actual: '-',
      forecast: '1.75%',
      previous: '1.75%',
      source: 'Sample Data',
      description: 'Swiss National Bank rate'
    }
  ];
}

// ============================================
// INTELLIGENT MULTI-SOURCE AGGREGATION
// ============================================
async function aggregateAndDeepAnalyze() {
  console.log('\n' + '='.repeat(70));
  console.log('📊 MULTI-SOURCE DATA FETCH WITH INTELLIGENT FALLBACK');
  console.log('='.repeat(70));
  
  const startTime = Date.now();
  
  try {
    console.log('📡 Fetching from MULTIPLE sources in parallel...');
    const fetchStart = Date.now();
    
    // Try all 5 sources simultaneously
    const [ff, inv, tv, mfx, fxs] = await Promise.allSettled([
      fetchForexFactory(),
      fetchInvesting(),
      fetchTradingView(),
      fetchMyFXBook(),
      fetchFXStreet()
    ]);
    
    // Collect successful results
    const allEvents = [
      ...(ff.status === 'fulfilled' ? ff.value : []),
      ...(inv.status === 'fulfilled' ? inv.value : []),
      ...(tv.status === 'fulfilled' ? tv.value : []),
      ...(mfx.status === 'fulfilled' ? mfx.value : []),
      ...(fxs.status === 'fulfilled' ? fxs.value : [])
    ];
    
    const fetchTime = Date.now() - fetchStart;
    console.log(`✅ Multi-source fetch completed in ${fetchTime}ms`);
    
    // Count working sources
    const workingSources = [
      apiHealth.forexFactory.working,
      apiHealth.investing.working,
      apiHealth.tradingView.working,
      apiHealth.myfxbook.working,
      apiHealth.fxstreet.working
    ].filter(Boolean).length;
    
    console.log(`📊 Working sources: ${workingSources}/5`);
    console.log(`📥 Total events from all sources: ${allEvents.length}`);
    
    // If no data from any source, use enhanced sample data
    let finalEvents = allEvents;
    if (allEvents.length === 0) {
      console.log('⚠️  All APIs unavailable');
      console.log('📊 Using enhanced sample data (30 realistic economic events)');
      finalEvents = getSampleRealData();
    }
    
    // Remove duplicates (same event from multiple sources)
    const uniqueEvents = removeDuplicates(finalEvents);
    console.log(`📊 Unique events after deduplication: ${uniqueEvents.length}`);
    
    // Track active sources
    const activeSources = [];
    if (apiHealth.forexFactory.working) activeSources.push('ForexFactory');
    if (apiHealth.investing.working) activeSources.push('Investing.com');
    if (apiHealth.tradingView.working) activeSources.push('TradingView');
    if (apiHealth.myfxbook.working) activeSources.push('MyFXBook');
    if (apiHealth.fxstreet.working) activeSources.push('FXStreet');
    
    // DEEP ANALYSIS
    console.log('🔍 Running deep analysis...');
    const analysisStart = Date.now();
    
    const analyzedNews = uniqueEvents.map((news, index) => {
      const analysis = deepAnalyzeNewsImpact(news, uniqueEvents);
      console.log(`   ${index + 1}/${uniqueEvents.length}. ${news.title} → ${analysis.direction} (${analysis.confidence}%) [${analysis.analysisTime}]`);
      return { ...news, ...analysis };
    });
    
    const analysisTime = Date.now() - analysisStart;
    console.log(`✅ Analysis completed in ${analysisTime}ms`);

    const highImpact = analyzedNews.filter(n => n.isHighImpact && n.confidence >= 50);
    const urgentSignals = analyzedNews.filter(n => n.urgency === 'IMMEDIATE');
    
    const byProduct = { forex: [], commodities: [], indices: [], crypto: [], bonds: [] };
    analyzedNews.forEach(news => {
      if (news.affectedProducts) {
        byProduct.forex.push(...news.affectedProducts.forex);
        byProduct.commodities.push(...news.affectedProducts.commodities);
        byProduct.indices.push(...news.affectedProducts.indices);
        byProduct.crypto.push(...news.affectedProducts.crypto);
        byProduct.bonds.push(...news.affectedProducts.bonds);
      }
    });

    const allSignals = analyzedNews
      .flatMap(n => n.tradingSignals || [])
      .sort((a, b) => b.confidence - a.confidence);

    newsCache = {
      rawNews: uniqueEvents,
      analyzed: analyzedNews.sort((a, b) => b.confidence - a.confidence),
      highImpact: highImpact.sort((a, b) => b.confidence - a.confidence),
      urgentSignals: urgentSignals,
      byProduct: byProduct,
      signals: allSignals.slice(0, 50),
      lastUpdate: new Date().toISOString(),
      sourcesCount: workingSources,
      activeSources: activeSources,
      analysisTime: analysisTime,
      totalTime: Date.now() - startTime,
      dataQuality: workingSources >= 2 ? 'high' : workingSources === 1 ? 'medium' : 'sample',
      stats: {
        total: uniqueEvents.length,
        highImpact: highImpact.length,
        urgent: urgentSignals.length,
        avgConfidence: analyzedNews.length > 0 ? Math.round(analyzedNews.reduce((a, b) => a + b.confidence, 0) / analyzedNews.length) : 0,
        totalSignals: allSignals.length,
        workingSources: workingSources
      }
    };

    broadcast({ type: 'update', data: newsCache });

    const totalTime = Date.now() - startTime;
    console.log('\n📊 CYCLE SUMMARY:');
    console.log(`   ⏱️  Total time: ${totalTime}ms`);
    console.log(`   📡 Working sources: ${workingSources}/5`);
    console.log(`   📌 Active sources: ${activeSources.join(', ')}`);
    console.log(`   📰 Events analyzed: ${uniqueEvents.length}`);
    console.log(`   ⚡ High impact: ${highImpact.length}`);
    console.log(`   🚨 Urgent signals: ${urgentSignals.length}`);
    console.log(`   📈 Trading signals: ${allSignals.length}`);
    console.log(`   🎯 Avg confidence: ${newsCache.stats.avgConfidence}%`);
    console.log(`   🔒 Data quality: ${newsCache.dataQuality.toUpperCase()}`);
    console.log('='.repeat(70) + '\n');

  } catch (error) {
    console.error('❌ Error in analysis cycle:', error);
  }
}

// Remove duplicate events from multiple sources
function removeDuplicates(events) {
  const seen = new Map();
  
  events.forEach(event => {
    const key = `${event.title}_${new Date(event.time).toDateString()}_${event.currency}`;
    const existing = seen.get(key);
    
    if (!existing) {
      seen.set(key, event);
    } else {
      // Prefer event with actual data over forecast
      if (event.actual !== '-' && existing.actual === '-') {
        seen.set(key, event);
      }
    }
  });
  
  return Array.from(seen.values());
}

// [INCLUDE ALL YOUR EXISTING ANALYSIS FUNCTIONS HERE]
// deepAnalyzeNewsImpact, calculateImpactScore, mapToAllProducts, 
// generateTradingSignals, getActionRecommendation, etc.
// (I'll keep them the same as before to save space)

function deepAnalyzeNewsImpact(newsItem, allNewsContext = []) {
  const startTime = Date.now();
  const { title, currency, actual, forecast, previous, impact, description, source, time } = newsItem;

  const textToAnalyze = (title || '') + ' ' + (description || '');
  const textAnalysis = sentiment.analyze(textToAnalyze);
  const titleUpper = (title || '').toUpperCase();
  
  const isHighImpact = HIGH_IMPACT_INDICATORS.some(indicator => titleUpper.includes(indicator)) || impact === 'high';
  const impactScore = calculateImpactScore(newsItem);
  
  const actualNum = parseFloat(String(actual).replace(/[^0-9.-]/g, ''));
  const forecastNum = parseFloat(String(forecast).replace(/[^0-9.-]/g, ''));
  const previousNum = parseFloat(String(previous).replace(/[^0-9.-]/g, ''));

  let direction = 'NEUTRAL';
  let confidence = 0;
  let analysis = '';
  let tradingSignals = [];
  let affectedProducts = { forex: [], commodities: [], indices: [], crypto: [], bonds: [] };

  if (!isNaN(actualNum) && (!isNaN(forecastNum) || !isNaN(previousNum))) {
    const reference = !isNaN(forecastNum) ? forecastNum : previousNum;
    const referenceName = !isNaN(forecastNum) ? 'forecast' : 'previous';
    const difference = actualNum - reference;
    const percentDiff = reference !== 0 ? (difference / Math.abs(reference)) * 100 : 0;

    const betterWhenHigher = checkIfBetterWhenHigher(title);
    
    if (Math.abs(percentDiff) > 0.05) {
      if (difference > 0) {
        direction = betterWhenHigher ? 'BULLISH' : 'BEARISH';
      } else {
        direction = betterWhenHigher ? 'BEARISH' : 'BULLISH';
      }

      let baseConfidence = Math.min(50, Math.abs(percentDiff) * 8);
      if (isHighImpact) baseConfidence += 25; else baseConfidence += 10;
      if (Math.abs(percentDiff) > 10) baseConfidence += 15;
      else if (Math.abs(percentDiff) > 5) baseConfidence += 10;
      else if (Math.abs(percentDiff) > 2) baseConfidence += 5;
      if ((direction === 'BULLISH' && textAnalysis.score > 0) || (direction === 'BEARISH' && textAnalysis.score < 0)) {
        baseConfidence += Math.abs(textAnalysis.score) * 3;
      }
      if (!isNaN(previousNum)) {
        const trend = actualNum > previousNum ? 'UP' : 'DOWN';
        const expectedTrend = direction === 'BULLISH' ? 'UP' : 'DOWN';
        if (trend === expectedTrend) baseConfidence += 5;
      }
      if (source === 'ForexFactory' || source === 'FXStreet') baseConfidence += 3;
      
      confidence = Math.min(98, Math.round(baseConfidence));

      affectedProducts = mapToAllProducts(currency, direction, confidence, titleUpper);
      tradingSignals = generateTradingSignals(affectedProducts, direction, confidence, newsItem);

      const betterOrWorse = (difference > 0 && betterWhenHigher) || (difference < 0 && !betterWhenHigher) ? '✅ BETTER' : '⚠️ WORSE';
      
      analysis = `🔍 DEEP ANALYSIS [${source}]: `;
      analysis += `Actual ${actualNum} is ${betterOrWorse} than ${referenceName} ${reference}. `;
      analysis += `Deviation: ${percentDiff > 0 ? '+' : ''}${percentDiff.toFixed(2)}%. `;
      analysis += `Market Direction: ${direction} for ${currency}. `;
      
      if (isHighImpact) {
        analysis += `⚡ HIGH IMPACT - Expect significant volatility across ${tradingSignals.length} products. `;
      }
      
      const correlations = getCorrelationInsights(currency, direction, titleUpper);
      if (correlations) analysis += `📊 ${correlations} `;

    } else {
      analysis = `📊 Actual ${actualNum} matches ${referenceName} ${reference}. Minimal market impact. `;
      confidence = 25;
    }
    
  } else if (actual && actual !== '-') {
    analysis = `📢 EVENT: ${title}. `;
    if (titleUpper.includes('SPEECH') || titleUpper.includes('SPEAKS')) {
      analysis += 'Central bank communication - Monitor for policy hints. ';
      confidence = isHighImpact ? 65 : 45;
    } else if (titleUpper.includes('MEETING') || titleUpper.includes('DECISION')) {
      analysis += 'Policy meeting - High attention to outcomes. ';
      confidence = 70;
    } else {
      confidence = isHighImpact ? 55 : 35;
    }
    direction = 'WATCH';
    affectedProducts = mapToAllProducts(currency, direction, confidence, titleUpper);
    tradingSignals = generateTradingSignals(affectedProducts, direction, confidence, newsItem);
  } else {
    analysis = `📊 FORECAST ANALYSIS: Expecting ${forecast || 'data'} vs previous ${previous || '-'}. `;
    confidence = 45;
    direction = 'WATCH';
    affectedProducts = mapToAllProducts(currency, direction, confidence, titleUpper);
    tradingSignals = generateTradingSignals(affectedProducts, direction, confidence, newsItem);
  }

  const analysisTime = Date.now() - startTime;

  return {
    direction, confidence, analysis, isHighImpact, impactScore,
    tradingSignals: tradingSignals.slice(0, 10),
    affectedProducts,
    sentimentScore: textAnalysis.score,
    analysisTime: `${analysisTime}ms`,
    timestamp: new Date().toISOString(),
    volatilityExpected: confidence > 70 ? 'HIGH' : confidence > 50 ? 'MEDIUM' : 'LOW',
    urgency: isHighImpact && confidence > 70 ? 'IMMEDIATE' : 'NORMAL',
    reliability: confidence
  };
}

function calculateImpactScore(newsItem) {
  let score = 0;
  const titleUpper = (newsItem.title || '').toUpperCase();
  if (titleUpper.includes('NFP') || titleUpper.includes('NON-FARM')) score += 10;
  if (titleUpper.includes('FOMC') || titleUpper.includes('FED')) score += 10;
  if (titleUpper.includes('GDP')) score += 9;
  if (titleUpper.includes('CPI') || titleUpper.includes('INFLATION')) score += 9;
  if (titleUpper.includes('INTEREST RATE')) score += 8;
  if (titleUpper.includes('UNEMPLOYMENT')) score += 7;
  if (titleUpper.includes('RETAIL SALES')) score += 6;
  if (titleUpper.includes('PMI')) score += 6;
  if (newsItem.impact === 'high') score += 5;
  else if (newsItem.impact === 'medium') score += 3;
  return Math.min(10, score);
}

function mapToAllProducts(currency, direction, confidence, titleUpper) {
  const products = { forex: [], commodities: [], indices: [], crypto: [], bonds: [] };
  const forexPairs = TRADING_PRODUCTS.FOREX[currency] || [];
  forexPairs.forEach(pair => {
    const [base, quote] = pair.split('/');
    let pairDirection = direction;
    if (quote === currency) {
      pairDirection = direction === 'BULLISH' ? 'BEARISH' : direction === 'BEARISH' ? 'BULLISH' : 'NEUTRAL';
    }
    products.forex.push({ product: pair, type: 'FOREX', direction: pairDirection, confidence });
  });
  const commodities = TRADING_PRODUCTS.COMMODITIES[currency] || TRADING_PRODUCTS.COMMODITIES.GLOBAL || [];
  commodities.forEach(commodity => {
    let commodityDirection = direction;
    if ((commodity === 'Gold' || commodity === 'Silver') && currency === 'USD') {
      commodityDirection = direction === 'BULLISH' ? 'BEARISH' : direction === 'BEARISH' ? 'BULLISH' : 'NEUTRAL';
    }
    products.commodities.push({ product: commodity, type: 'COMMODITY', direction: commodityDirection, confidence: Math.round(confidence * 0.8) });
  });
  const indices = TRADING_PRODUCTS.INDICES[currency] || [];
  indices.forEach(index => {
    products.indices.push({ product: index, type: 'INDEX', direction: direction, confidence: Math.round(confidence * 0.75) });
  });
  if (currency === 'USD' || titleUpper.includes('RISK')) {
    const cryptos = TRADING_PRODUCTS.CRYPTO.USD || TRADING_PRODUCTS.CRYPTO.GLOBAL || [];
    cryptos.forEach(crypto => {
      let cryptoDirection = direction;
      if (currency === 'USD') {
        cryptoDirection = direction === 'BULLISH' ? 'BEARISH' : direction === 'BEARISH' ? 'BULLISH' : 'NEUTRAL';
      }
      products.crypto.push({ product: crypto, type: 'CRYPTO', direction: cryptoDirection, confidence: Math.round(confidence * 0.7) });
    });
  }
  const bonds = TRADING_PRODUCTS.BONDS[currency] || [];
  bonds.forEach(bond => {
    let bondDirection = 'NEUTRAL';
    if (titleUpper.includes('RATE') && direction === 'BULLISH') bondDirection = 'BEARISH';
    else if (titleUpper.includes('RATE') && direction === 'BEARISH') bondDirection = 'BULLISH';
    products.bonds.push({ product: bond, type: 'BOND', direction: bondDirection, confidence: Math.round(confidence * 0.65) });
  });
  return products;
}

function generateTradingSignals(affectedProducts, direction, confidence, newsItem) {
  const signals = [];
  const allProducts = [
    ...affectedProducts.forex,
    ...affectedProducts.commodities,
    ...affectedProducts.indices,
    ...affectedProducts.crypto,
    ...affectedProducts.bonds
  ];
  allProducts.forEach(item => {
    const action = getActionRecommendation(item.direction, item.confidence);
    const timeframe = confidence > 80 ? 'IMMEDIATE (0-5 min)' : confidence > 60 ? 'SHORT-TERM (5-30 min)' : 'MEDIUM-TERM (30+ min)';
    signals.push({
      product: item.product,
      type: item.type,
      direction: item.direction,
      action: action,
      confidence: item.confidence,
      timeframe: timeframe,
      reason: `${newsItem.title} - ${direction} ${newsItem.currency}`,
      timestamp: new Date().toISOString()
    });
  });
  signals.sort((a, b) => b.confidence - a.confidence);
  return signals;
}

function getActionRecommendation(direction, confidence) {
  if (direction === 'WATCH') return '👁️ MONITOR';
  if (direction === 'NEUTRAL') return '⏸️ WAIT';
  if (confidence < 40) return '⏸️ WAIT';
  if (confidence >= 40 && confidence < 60) return direction === 'BULLISH' ? '🟡 CONSIDER BUY' : '🟡 CONSIDER SELL';
  if (confidence >= 60 && confidence < 80) return direction === 'BULLISH' ? '🟢 BUY' : '🔴 SELL';
  if (confidence >= 80) return direction === 'BULLISH' ? '🔥 STRONG BUY' : '🔥 STRONG SELL';
  return '⏸️ WAIT';
}

function checkIfBetterWhenHigher(title) {
  const lowerIsBetter = ['UNEMPLOYMENT', 'JOBLESS', 'CLAIMS', 'INFLATION', 'CPI', 'PPI', 'CORE', 'DEFICIT', 'TRADE DEFICIT', 'VIX', 'VOLATILITY'];
  const upperTitle = (title || '').toUpperCase();
  return !lowerIsBetter.some(indicator => upperTitle.includes(indicator));
}

function getCorrelationInsights(currency, direction, titleUpper) {
  let insights = '';
  if (currency === 'USD' && direction === 'BULLISH') insights = 'Correlations: Gold↓ Oil↓ Stocks↓ Crypto↓';
  else if (currency === 'USD' && direction === 'BEARISH') insights = 'Correlations: Gold↑ Oil↑ Stocks↑ Crypto↑';
  if (titleUpper.includes('INFLATION') && direction === 'BULLISH') insights += ' → Bonds↓ Gold↑';
  if (titleUpper.includes('RATE') && direction === 'BULLISH') insights += ' → Bonds↓ Stocks↓';
  return insights;
}

// REST API
app.get('/api/news', async (req, res) => {
  if (!newsCache.lastUpdate) await aggregateAndDeepAnalyze();
  res.json(newsCache);
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    uptime: process.uptime(),
    clients: clients.size,
    lastUpdate: newsCache.lastUpdate,
    stats: newsCache.stats,
    apiHealth: apiHealth,
    activeSources: newsCache.activeSources
  });
});

// Schedule: Every 3 minutes (avoid rate limiting)
console.log('⏰ Scheduling updates every 3 minutes (multi-source, avoid rate limiting)...');
cron.schedule('*/180 * * * * *', async () => { await aggregateAndDeepAnalyze(); });

// Initial fetch
setTimeout(async () => {
  console.log('🚀 Performing initial multi-source fetch...');
  await aggregateAndDeepAnalyze();
}, 3000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log('\n' + '='.repeat(70));
  console.log('🎉 FOREX NEWS ANALYZER - MULTI-SOURCE EDITION');
  console.log('='.repeat(70));
  console.log(`📡 Server: http://localhost:${PORT}`);
  console.log(`🔌 WebSocket: READY`);
  console.log(`🔍 Deep Analysis: ACTIVE`);
  console.log(`📊 Data Sources: 5 independent APIs + Sample fallback`);
  console.log(`🔄 Update Interval: 3 minutes (avoid rate limits)`);
  console.log(`⚡ Trading Products: Forex, Commodities, Indices, Crypto, Bonds`);
  console.log('='.repeat(70));
  console.log('\n💡 Open browser: http://localhost:' + PORT + '\n');
});

process.on('SIGTERM', () => {
  console.log('📴 Shutting down...');
  server.close(() => { console.log('✅ Server closed'); process.exit(0); });
});
