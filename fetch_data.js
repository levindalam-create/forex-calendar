const https = require('https');
const fs = require('fs');

const TD_KEY   = process.env.TD_KEY   || '6d917abe390a4491801e0bacc78b9006';
const AV_KEY   = process.env.AV_KEY   || '66VWX9UM4I5IGEK2';
const NEWS_KEY = process.env.NEWS_KEY  || '388fa2f196244bd4b3e12912e1b6d350';

// ── CONFIG ──
const PAIRS = ['EUR/USD','GBP/USD','USD/JPY','GBP/JPY','XAU/USD','AUD/USD','USD/CAD','USD/CHF'];
const TF    = ['15min','1h','4h','1day'];

const TF_MAP = { '15min':'M15', '1h':'H1', '4h':'H4', '1day':'D1' };
const OUTPUT_SIZE = { '15min':96, '1h':48, '4h':60, '1day':90 };

function get(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 15000 }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try { resolve(JSON.parse(d)); }
        catch(e) { reject(new Error('JSON parse error: ' + e.message)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── TWELVE DATA FETCH ──
async function fetchTD(symbol, interval) {
  const size = OUTPUT_SIZE[interval] || 60;
  const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=${size}&timezone=UTC&apikey=${TD_KEY}`;
  try {
    const data = await get(url);
    if (!data || !data.values || data.values.length === 0) return null;
    if (data.code === 429 || (data.message && /limit|quota|exceed/i.test(data.message))) {
      console.log(`TD quota hit for ${symbol} ${interval}`);
      return null;
    }
    // Twelve Data retourne du plus récent au plus ancien — on inverse
    const vals = data.values.slice().reverse();
    const opens=[], highs=[], lows=[], closes=[], timestamps=[];
    for (const v of vals) {
      const o=parseFloat(v.open), h=parseFloat(v.high), l=parseFloat(v.low), c=parseFloat(v.close);
      if (isNaN(o)||isNaN(h)||isNaN(l)||isNaN(c)) continue;
      opens.push(o); highs.push(h); lows.push(l); closes.push(c);
      timestamps.push(v.datetime);
    }
    if (closes.length < 5) return null;
    return { opens, highs, lows, closes, timestamps, source: 'Twelve Data', real: true };
  } catch(e) {
    console.warn(`TD error ${symbol} ${interval}: ${e.message}`);
    return null;
  }
}

// ── ALPHA VANTAGE FALLBACK ──
async function fetchAV(symbol, interval) {
  const sym = symbol.replace('/', '');
  const from = sym.slice(0, 3);
  const to   = sym.slice(3, 6);
  const avInterval = { '15min':'15min','1h':'60min','4h':'60min','1day':null }[interval];
  const func = interval === '1day' ? 'FX_DAILY' : 'FX_INTRADAY';
  const fromSym = symbol === 'XAU/USD' ? 'XAU' : from;
  const toSym   = symbol === 'XAU/USD' ? 'USD' : to;
  const url = `https://www.alphavantage.co/query?function=${func}&from_symbol=${fromSym}&to_symbol=${toSym}${avInterval?'&interval='+avInterval:''}&outputsize=compact&apikey=${AV_KEY}`;
  try {
    const data = await get(url);
    if (!data || data['Note'] || data['Information'] || data['Error Message']) return null;
    const tsKey = Object.keys(data).find(k => k.includes('Time Series'));
    if (!tsKey) return null;
    const entries = Object.keys(data[tsKey]).sort();
    const opens=[], highs=[], lows=[], closes=[], timestamps=[];
    for (const k of entries) {
      const b = data[tsKey][k];
      opens.push(parseFloat(b['1. open']));
      highs.push(parseFloat(b['2. high']));
      lows.push(parseFloat(b['3. low']));
      closes.push(parseFloat(b['4. close']));
      timestamps.push(k);
    }
    if (closes.length < 5) return null;
    return { opens, highs, lows, closes, timestamps, source: 'Alpha Vantage', real: true };
  } catch(e) {
    console.warn(`AV error ${symbol} ${interval}: ${e.message}`);
    return null;
  }
}

// ── NEWS FETCH ──
async function fetchNews() {
  // NewsAPI fonctionne côté serveur (pas de CORS ici)
  const since = new Date(Date.now() - 86400000).toISOString().split('T')[0];
  // Essaie plusieurs endpoints NewsAPI
  const urls = [
    'https://newsapi.org/v2/top-headlines?category=business&language=en&pageSize=20&apiKey='+NEWS_KEY,
    'https://newsapi.org/v2/everything?q=forex+dollar+fed&language=en&from='+since+'&pageSize=20&apiKey='+NEWS_KEY,
    'https://newsapi.org/v2/top-headlines?sources=bloomberg,reuters,the-wall-street-journal&pageSize=20&apiKey='+NEWS_KEY
  ];
  let data = null;
  for (const url of urls) {
    try {
      const d = await get(url);
      console.log('NewsAPI:', d && d.status, 'articles:', d && d.articles && d.articles.length, d && d.code);
      if (d && d.articles && d.articles.length > 0) { data = d; break; }
    } catch(e) { console.warn('NewsAPI attempt failed:', e.message); }
  }
  try {
    if (!data || !data.articles) return null;
    const bull = ['rise','rally','gain','surge','bullish','strong','beat','above','higher','hawkish'];
    const bear = ['fall','drop','decline','bearish','weak','below','miss','lower','concern','dovish'];
    function getSent(articles, ccy) {
      let sc = 0;
      for (const a of articles) {
        const t = ((a.title||'')+(a.description||'')).toLowerCase();
        if (ccy && a.currency && a.currency !== ccy) continue;
        bull.forEach(w => { if(t.includes(w)) sc++; });
        bear.forEach(w => { if(t.includes(w)) sc--; });
      }
      return sc > 2 ? 'haussier' : sc < -2 ? 'baissier' : 'neutre';
    }
    function getImpact(text) {
      const t = text.toLowerCase();
      if (/fed|fomc|nfp|ecb|bce|rate decision|boj|cpi|central bank/i.test(t)) return 'VH';
      if (/pmi|ism|retail sales|gdp|payroll|jobless/i.test(t)) return 'H';
      return 'M';
    }
    function getCcy(text) {
      const t = text.toLowerCase();
      if (/eur|euro|ecb|lagarde/i.test(t)) return 'EUR';
      if (/gbp|pound|boe|bailey/i.test(t)) return 'GBP';
      if (/jpy|yen|boj|ueda/i.test(t)) return 'JPY';
      if (/gold|xau|bullion/i.test(t)) return 'XAU';
      return 'USD';
    }
    const articles = data.articles.map(a => ({
      title: a.title || '',
      description: a.description || '',
      source: (a.source && a.source.name) || 'NewsAPI',
      publishedAt: a.publishedAt || new Date().toISOString(),
      impact: getImpact((a.title||'')+(a.description||'')),
      currency: getCcy((a.title||'')+(a.description||''))
    }));
    const sentiment = {};
    ['USD','EUR','GBP','JPY','XAU'].forEach(c => { sentiment[c] = getSent(articles, c); });
    return { articles, sentiment };
  } catch(e) {
    console.warn('News error:', e.message);
    return null;
  }
}

// ── COT DATA — CFTC direct CSV (source officielle) ──
async function fetchCOT() {
  const cotData = { updated: new Date().toISOString(), pairs: {} };

  // CFTC f_disagg.txt — Disaggregated Futures Only
  // Headers utilisent des underscores et guillemets doubles
  const cotUrl = 'https://www.cftc.gov/dea/newcot/f_disagg.txt';

  // Noms exacts dans le fichier CFTC (avec exchange)
  const COT_CODES = {
    'EURUSD': 'EURO FX',
    'GBPUSD': 'BRITISH POUND STERLING',
    'USDJPY': 'JAPANESE YEN',
    'AUDUSD': 'AUSTRALIAN DOLLAR',
    'USDCAD': 'CANADIAN DOLLAR',
    'USDCHF': 'SWISS FRANC',
    'XAUUSD': 'GOLD'
  };

  try {
    const csvData = await new Promise((resolve, reject) => {
      const req = require('https').get(cotUrl, { timeout: 20000, headers: {'User-Agent':'Mozilla/5.0'} }, res => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => resolve(d));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    });

    const lines = csvData.split('\n').filter(l => l.trim());
    if (lines.length < 2) throw new Error('CSV too short: ' + lines.length + ' lines');

    // Parser une ligne CSV avec guillemets
    function parseCSVLine(line) {
      const result = [];
      let cur = '', inQ = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') { inQ = !inQ; }
        else if (ch === ',' && !inQ) { result.push(cur.trim()); cur = ''; }
        else { cur += ch; }
      }
      result.push(cur.trim());
      return result;
    }

    const headers = parseCSVLine(lines[0]);
    console.log('COT CSV:', lines.length, 'lines,', headers.length, 'columns');
    console.log('First headers:', headers.slice(0,5).join(' | '));

    // Trouver les colonnes par contenu partiel (insensible casse)
    const findCol = (keyword) => headers.findIndex(h => h.toLowerCase().includes(keyword.toLowerCase()));

    let iName  = findCol('market_and_exchange') !== -1 ? findCol('market_and_exchange') : findCol('market and exchange');
    let iDate  = findCol('as_of_date') !== -1 ? findCol('as_of_date') : findCol('as of date');
    // Colonnes Managed Money (Asset Managers dans disaggregated = institutionnels)
    // Ou NonComm dans legacy — chercher "money_long" ou "noncomm_long"
    let iLong  = findCol('money_positions_long');
    let iShort = findCol('money_positions_short');
    let iChg   = findCol('change_in_money_long');
    // Fallback: Prod/Merch positions
    if (iLong  < 0) iLong  = findCol('prod_merc_positions_long');
    if (iShort < 0) iShort = findCol('prod_merc_positions_short');
    if (iChg   < 0) iChg   = findCol('change_in_prod_merc_long');
    // Dernier fallback: colonnes 6,7,8
    if (iLong  < 0) iLong  = 6;
    if (iShort < 0) iShort = 7;
    if (iDate  < 0) iDate  = 2;
    if (iName  < 0) iName  = 0;

    console.log('COT cols: name=' + iName + ' date=' + iDate + ' long=' + iLong + ' short=' + iShort);

    for (const [sym, codeName] of Object.entries(COT_CODES)) {
      const line = lines.find(l => l.toUpperCase().includes(codeName.toUpperCase()));
      if (!line) {
        console.warn('COT: "' + codeName + '" not found in ' + lines.length + ' lines');
        cotData.pairs[sym] = { netPosition:0, change:0, direction:'neutre', momentum:'stable', source:'Non trouvé' };
        continue;
      }
      const cols = parseCSVLine(line);
      const longPos  = parseInt((cols[iLong]  || '0').replace(/,/g,''), 10) || 0;
      const shortPos = parseInt((cols[iShort] || '0').replace(/,/g,''), 10) || 0;
      const net      = longPos - shortPos;
      const chg      = iChg >= 0 ? (parseInt((cols[iChg] || '0').replace(/,/g,''), 10) || 0) : 0;
      const date     = cols[iDate] || '';
      const usdBase  = ['USDJPY','USDCAD','USDCHF'].includes(sym);
      const finalNet = usdBase ? -net : net;
      cotData.pairs[sym] = {
        netPosition: finalNet, change: usdBase ? -chg : chg,
        direction: finalNet > 10000 ? 'haussier' : finalNet < -10000 ? 'baissier' : 'neutre',
        momentum: Math.abs(chg) > 3000 ? (chg > 0 ? 'renforcement' : 'affaiblissement') : 'stable',
        date, longPos, shortPos, source: 'CFTC officiel'
      };
      console.log('COT ' + sym + ': net=' + finalNet + ' (' + cotData.pairs[sym].direction + ') date=' + date);
    }
  } catch(e) {
    console.warn('COT CFTC error:', e.message);
    const fallback = {
      'EURUSD':{net:42000,dir:'haussier'},'GBPUSD':{net:16000,dir:'haussier'},
      'USDJPY':{net:-58000,dir:'baissier'},'AUDUSD':{net:-11000,dir:'baissier'},
      'XAUUSD':{net:195000,dir:'haussier'},'USDCAD':{net:22000,dir:'haussier'},
      'USDCHF':{net:15000,dir:'haussier'}
    };
    for (const [sym, v] of Object.entries(fallback)) {
      cotData.pairs[sym] = { netPosition:v.net, change:0, direction:v.dir, momentum:'stable', source:'Statique (CFTC indispo)' };
    }
  }
  return cotData;
}

// ── SPOT PRICES ──
async function fetchSpots() {
  const spots = {};
  const pairs = ['eur','gbp','usd','jpy','aud','cad','chf'];
  try {
    const data = await get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json');
    if (data && data.usd) {
      spots['EURUSD'] = data.usd['eur'] ? +(1/data.usd['eur']).toFixed(5) : null;
      spots['GBPUSD'] = data.usd['gbp'] ? +(1/data.usd['gbp']).toFixed(5) : null;
      spots['USDJPY'] = data.usd['jpy'] ? +data.usd['jpy'].toFixed(3) : null;
      spots['AUDUSD'] = data.usd['aud'] ? +(1/data.usd['aud']).toFixed(5) : null;
      spots['USDCAD'] = data.usd['cad'] ? +data.usd['cad'].toFixed(5) : null;
      spots['USDCHF'] = data.usd['chf'] ? +data.usd['chf'].toFixed(5) : null;
      spots['EURGBP'] = (spots['EURUSD'] && spots['GBPUSD'])
        ? +(spots['EURUSD']/spots['GBPUSD']).toFixed(5) : null;
      spots['GBPJPY'] = (spots['GBPUSD'] && spots['USDJPY'])
        ? +(spots['GBPUSD']*spots['USDJPY']).toFixed(3) : null;
    }
  } catch(e) { console.warn('Spot price error:', e.message); }
  // XAU
  try {
    const xauData = await get('https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/xau.json');
    if (xauData && xauData.xau && xauData.xau.usd) {
      spots['XAUUSD'] = +xauData.xau.usd.toFixed(2);
    }
  } catch(e) {}
  return spots;
}

// ── MAIN ──
async function main() {
  console.log('=== FX PRO Data Fetch ===', new Date().toISOString());

  const now    = new Date();
  const day    = now.getUTCDay();
  const hour   = now.getUTCHours();
  const isWeekend = day === 0 || day === 6;

  // Déterminer heure Paris
  const month    = now.getUTCMonth() + 1;
  const isDST    = month >= 3 && month <= 10;
  const parisHour = (hour + (isDST ? 2 : 1)) % 24;

  // ── SPOTS (toujours) ──
  console.log('Fetching spot prices...');
  const spots = await fetchSpots();
  console.log('Spots:', JSON.stringify(spots));

  // ── NEWS (toujours) ──
  console.log('Fetching news...');
  const newsData = await fetchNews();

  // ── COT (une fois par jour le vendredi ou si manquant) ──
  let cotData = null;
  const cotFile = 'cot.json';
  let cotNeedsUpdate = true;
  if (fs.existsSync(cotFile)) {
    try {
      const existing = JSON.parse(fs.readFileSync(cotFile, 'utf8'));
      const age = Date.now() - new Date(existing.updated).getTime();
      if (age < 7 * 86400000) cotNeedsUpdate = false; // moins d'une semaine
    } catch(e) {}
  }
  if (cotNeedsUpdate || day === 5) {
    console.log('Fetching COT data...');
    cotData = await fetchCOT();
  }

  // ── OHLC (uniquement en heures de marché) ──
  const ohlcData = {};
  if (!isWeekend) {
    // Priorité TF selon heure: toujours M15 + H1, H4 + D1 moins souvent
    const minute = now.getUTCMinutes();
    const fetchD1 = minute === 0;     // seulement à H:00
    const fetchH4 = minute === 0 || minute === 30; // à H:00 et H:30

    // XAU/USD: TD gratuit ne supporte pas XAU — on utilise Yahoo Finance GC=F (Gold Futures)
    // Tous les autres via Twelve Data
    const pairsToFetch = ['EUR/USD','GBP/USD','USD/JPY','GBP/JPY','AUD/USD','USD/CAD'];
    const yahooOnlyPairs = ['XAU/USD']; // Gold via Yahoo Finance (TD plan gratuit ne supporte pas XAU)
    const yahooSymMap = { 'XAU/USD':'GC=F','AUD/USD':'AUDUSD=X','USD/CAD':'CAD=X' };
    let reqCount = 0;
    const REQ_LIMIT = 25; // Conservative pour rester dans quota gratuit

    // Fetch standard pairs via Twelve Data
    for (const pair of pairsToFetch) {
      const sym = pair.replace('/','');
      ohlcData[sym] = {};
      for (const tf of TF) {
        if (tf === '1day' && !fetchD1) continue;
        if (tf === '4h'   && !fetchH4)  continue;
        if (reqCount >= REQ_LIMIT) { console.log('Request limit reached, skipping'); break; }
        console.log(`Fetching ${pair} ${tf}...`);
        let candles = await fetchTD(pair, tf);
        reqCount++;
        await sleep(500);
        if (!candles) {
          console.log(`TD failed, trying AV for ${pair} ${tf}...`);
          candles = await fetchAV(pair, tf);
          if (candles) await sleep(13000);
        }
        // Si AV échoue aussi, essayer Yahoo Finance
        if (!candles) {
          const ySym = yahooSymMap[pair];
          if (ySym) {
            try {
              await sleep(1500);
              const yI = { '15min':'15m','1h':'1h','4h':'1h','1day':'1d' }[tf];
              const yR = { '15min':'2d','1h':'7d','4h':'30d','1day':'90d' }[tf];
              const yUrl = 'https://query2.finance.yahoo.com/v8/finance/chart/'+ySym+'?interval='+yI+'&range='+yR+'&includePrePost=false';
              const yData = await get(yUrl);
              const res = yData && yData.chart && yData.chart.result && yData.chart.result[0];
              if (res && res.timestamp) {
                const q = res.indicators && res.indicators.quote && res.indicators.quote[0];
                if (q && q.close) {
                  const opens=[],highs=[],lows=[],closes=[];
                  for (let i=0;i<res.timestamp.length;i++) {
                    if (q.open[i]===null||isNaN(q.open[i])) continue;
                    opens.push(+q.open[i]);highs.push(+q.high[i]);lows.push(+q.low[i]);closes.push(+q.close[i]);
                  }
                  if (closes.length >= 5) {
                    candles = { opens, highs, lows, closes, source:'Yahoo Finance', real:true };
                    console.log(`✓ ${sym} ${tf}: ${closes.length} candles (Yahoo fallback)`);
                  }
                }
              }
            } catch(e) { console.warn(`Yahoo fallback error ${pair} ${tf}: ${e.message}`); }
          }
        }
        if (candles) {
          const sp = spots[sym];
          if (sp && !isNaN(sp)) {
            const last = candles.closes.length-1;
            candles.closes[last]=sp; candles.highs[last]=Math.max(candles.highs[last],sp); candles.lows[last]=Math.min(candles.lows[last],sp);
          }
          ohlcData[sym][TF_MAP[tf]] = { ...candles, fetchedAt: new Date().toISOString() };
          console.log(`✓ ${sym} ${tf}: ${candles.closes.length} candles (${candles.source})`);
        } else { console.warn(`✗ ${sym} ${tf}: no data`); }
      }
      if (reqCount >= REQ_LIMIT) break;
    }

    // Fetch XAU/USD via Yahoo Finance (Gold Futures GC=F)
    for (const pair of yahooOnlyPairs) {
      const sym = pair.replace('/','');
      ohlcData[sym] = {};
      const yahooMap = { '15min':'15m','1h':'1h','4h':'1h','1day':'1d' };
      const rangeMap  = { '15min':'2d','1h':'7d','4h':'30d','1day':'90d' };
      for (const tf of TF) {
        if (tf === '1day' && !fetchD1) continue;
        if (tf === '4h'   && !fetchH4)  continue;
        console.log(`Fetching ${pair} ${tf} via Yahoo...`);
        try {
          // Yahoo rate limits — attendre entre les requêtes
          await sleep(2000);
          // Essayer query1 et query2 (Yahoo a 2 endpoints en rotation)
          const yUrls = [
            'https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval='+yahooMap[tf]+'&range='+rangeMap[tf]+'&includePrePost=false',
            'https://query2.finance.yahoo.com/v8/finance/chart/GC=F?interval='+yahooMap[tf]+'&range='+rangeMap[tf]+'&includePrePost=false'
          ];
          let yData = null;
          for (const yUrl of yUrls) {
            try { yData = await get(yUrl); if (yData && yData.chart) break; } catch(e) { await sleep(1000); }
          }
          const res = yData && yData.chart && yData.chart.result && yData.chart.result[0];
          if (!res || !res.timestamp) { console.warn(`✗ ${sym} ${tf}: Yahoo no data`); continue; }
          const q = res.indicators && res.indicators.quote && res.indicators.quote[0];
          if (!q || !q.close) { console.warn(`✗ ${sym} ${tf}: Yahoo no quotes`); continue; }
          const opens=[],highs=[],lows=[],closes=[],timestamps=[];
          const blockSize = tf==='4h' ? 4 : 1;
          let block=[];
          for (let i=0;i<res.timestamp.length;i++) {
            const o=q.open[i],h=q.high[i],l=q.low[i],c=q.close[i];
            if (o===null||o===undefined||isNaN(o)) continue;
            if (blockSize===1) { opens.push(+o);highs.push(+h);lows.push(+l);closes.push(+c);timestamps.push(res.timestamp[i]); }
            else {
              block.push({o:+o,h:+h,l:+l,c:+c,t:res.timestamp[i]});
              if (block.length===blockSize) {
                opens.push(block[0].o); highs.push(Math.max(...block.map(b=>b.h)));
                lows.push(Math.min(...block.map(b=>b.l))); closes.push(block[blockSize-1].c);
                timestamps.push(block[blockSize-1].t); block=[];
              }
            }
          }
          if (closes.length < 5) { console.warn(`✗ ${sym} ${tf}: too few candles`); continue; }
          const sp = spots[sym];
          if (sp && !isNaN(sp)) { const last=closes.length-1; closes[last]=sp; highs[last]=Math.max(highs[last],sp); lows[last]=Math.min(lows[last],sp); }
          ohlcData[sym][TF_MAP[tf]] = { opens,highs,lows,closes,timestamps, source:'Yahoo Finance (Gold)', real:true, fetchedAt:new Date().toISOString() };
          console.log(`✓ ${sym} ${tf}: ${closes.length} candles (Yahoo Finance)`);
          await sleep(500);
        } catch(e) { console.warn(`✗ ${sym} ${tf} Yahoo error: ${e.message}`); }
      }
    }
  } else {
    console.log('Weekend — skipping OHLC fetch');
  }

  // ── ÉCRIRE LES FICHIERS ──
  const meta = {
    updated:    new Date().toISOString(),
    parisHour,
    weekend:    isWeekend,
    marketOpen: !isWeekend && hour >= 0 && hour < 22,
    spots,
    fetchStats: {
      ohlcPairs:  Object.keys(ohlcData).length,
      newsCount:  newsData ? newsData.articles.length : 0,
      cotUpdated: !!cotData
    }
  };

  const WORKDIR = process.env.GITHUB_WORKSPACE || process.cwd();
  const path = require('path');
  console.log('Writing JSON to:', WORKDIR);
  fs.writeFileSync(path.join(WORKDIR, 'meta.json'),    JSON.stringify(meta,    null, 2));
  fs.writeFileSync(path.join(WORKDIR, 'ohlc.json'),    JSON.stringify(ohlcData, null, 2));
  // Toujours écrire news.json même si fetch échoue
  const newsOut = newsData || { articles: [], sentiment: {}, updated: new Date().toISOString() };
  fs.writeFileSync(path.join(WORKDIR, 'news.json'), JSON.stringify(newsOut, null, 2));
  if (cotData)  fs.writeFileSync(path.join(WORKDIR, 'cot.json'),  JSON.stringify(cotData,  null, 2));

  console.log('✅ Files written:');
  console.log('  meta.json —', JSON.stringify(meta.fetchStats));
  console.log('  ohlc.json —', Object.keys(ohlcData).length, 'pairs');
  if (newsData) console.log('  news.json —', newsData.articles.length, 'articles');
  if (cotData)  console.log('  cot.json  — COT updated');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
