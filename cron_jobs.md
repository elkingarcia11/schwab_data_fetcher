# Cron Jobs Configuration for Schwab Market Data

## Market Hours: 9:30 AM - 4:00 PM ET (Monday - Friday)

## Symbols: SPY, META, AMZN

## Frequencies: 5m, 10m, 15m, 30m

### Complete Workflow Per Execution:

✅ **Fetch new data** → ✅ **Add to existing CSV** → ✅ **Calculate new indicators** → ✅ **Analyze signals** → ✅ **Email if triggered**

---

## 📅 Complete Cron Job Configuration

### **5-Minute Jobs (Every 5 minutes during market hours)**

```bash
# SPY 5-minute data collection
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 5m >> /var/log/schwab_SPY_5m.log 2>&1

# META 5-minute data collection
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 5m >> /var/log/schwab_META_5m.log 2>&1

# AMZN 5-minute data collection
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 5m >> /var/log/schwab_AMZN_5m.log 2>&1
```

### **10-Minute Jobs (Every 10 minutes during market hours)**

```bash
# SPY 10-minute data collection
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 10m >> /var/log/schwab_SPY_10m.log 2>&1

# META 10-minute data collection
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 10m >> /var/log/schwab_META_10m.log 2>&1

# AMZN 10-minute data collection
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 10m >> /var/log/schwab_AMZN_10m.log 2>&1
```

### **15-Minute Jobs (Every 15 minutes during market hours)**

```bash
# SPY 15-minute data collection
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 15m >> /var/log/schwab_SPY_15m.log 2>&1

# META 15-minute data collection
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 15m >> /var/log/schwab_META_15m.log 2>&1

# AMZN 15-minute data collection
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 15m >> /var/log/schwab_AMZN_15m.log 2>&1
```

### **30-Minute Jobs (Every 30 minutes during market hours)**

```bash
# SPY 30-minute data collection
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 30m >> /var/log/schwab_SPY_30m.log 2>&1

# META 30-minute data collection
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 30m >> /var/log/schwab_META_30m.log 2>&1

# AMZN 30-minute data collection
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 30m >> /var/log/schwab_AMZN_30m.log 2>&1
```

### **Bootstrap Jobs (Run once at market open to fill any gaps)**

```bash
# Bootstrap all symbols and frequencies at market open
31 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
32 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
33 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
34 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1

35 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
36 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
37 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
38 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1

39 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
40 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
41 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
42 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
```

---

## 📋 Complete Crontab Installation

**Add all jobs to your crontab using `crontab -e`:**

```bash
# Schwab Market Data Collection - SPY, META, AMZN
# Runs during market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
# Each job: Fetch → Add to CSV → Calculate indicators → Analyze signals → Email if triggered

# ====== 5-MINUTE JOBS ======
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 5m >> /var/log/schwab_SPY_5m.log 2>&1
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 5m >> /var/log/schwab_META_5m.log 2>&1
*/5 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 5m >> /var/log/schwab_AMZN_5m.log 2>&1

# ====== 10-MINUTE JOBS ======
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 10m >> /var/log/schwab_SPY_10m.log 2>&1
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 10m >> /var/log/schwab_META_10m.log 2>&1
0,10,20,30,40,50 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 10m >> /var/log/schwab_AMZN_10m.log 2>&1

# ====== 15-MINUTE JOBS ======
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 15m >> /var/log/schwab_SPY_15m.log 2>&1
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 15m >> /var/log/schwab_META_15m.log 2>&1
0,15,30,45 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 15m >> /var/log/schwab_AMZN_15m.log 2>&1

# ====== 30-MINUTE JOBS ======
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 30m >> /var/log/schwab_SPY_30m.log 2>&1
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 30m >> /var/log/schwab_META_30m.log 2>&1
0,30 9-16 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 30m >> /var/log/schwab_AMZN_30m.log 2>&1

# ====== BOOTSTRAP JOBS (Market Open) ======
31 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
32 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
33 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
34 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py SPY 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
35 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
36 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
37 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
38 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py META 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
39 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 5m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
40 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 10m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
41 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 15m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
42 9 * * 1-5 cd /Users/elkingarcia/Documents/python/schwab_market_api && python scheduled_coordinator.py AMZN 30m --mode bootstrap >> /var/log/schwab_bootstrap.log 2>&1
```

---

## 🔍 Monitoring Your 12 Data Streams

### **Check Individual Logs**

```bash
# Monitor specific symbol/frequency
tail -f /var/log/schwab_SPY_5m.log
tail -f /var/log/schwab_META_15m.log
tail -f /var/log/schwab_AMZN_30m.log

# Monitor all at once
tail -f /var/log/schwab_*.log
```

### **Data Files Generated**

Your `data/` directory will contain:

```
SPY_5m.csv, SPY_5m_INVERSE.csv
SPY_10m.csv, SPY_10m_INVERSE.csv
SPY_15m.csv, SPY_15m_INVERSE.csv
SPY_30m.csv, SPY_30m_INVERSE.csv

META_5m.csv, META_5m_INVERSE.csv
META_10m.csv, META_10m_INVERSE.csv
META_15m.csv, META_15m_INVERSE.csv
META_30m.csv, META_30m_INVERSE.csv

AMZN_5m.csv, AMZN_5m_INVERSE.csv
AMZN_10m.csv, AMZN_10m_INVERSE.csv
AMZN_15m.csv, AMZN_15m_INVERSE.csv
AMZN_30m.csv, AMZN_30m_INVERSE.csv
```

### **Position Tracking**

Each symbol/frequency combination maintains independent position states:

- **SPY**: 4 timeframes × 2 position types (LONG/SHORT) = 8 position trackers
- **META**: 4 timeframes × 2 position types = 8 position trackers
- **AMZN**: 4 timeframes × 2 position types = 8 position trackers
- **Total**: 24 independent position trackers

---

## ⚡ **What Each Cron Job Does:**

1. **✅ Validates authentication** (skip if failed)
2. **✅ Checks market hours** (skip if outside)
3. **✅ Fetches only NEW data** since last run
4. **✅ Appends to existing CSV files** (incremental)
5. **✅ Calculates indicators** for new candles only
6. **✅ Analyzes latest signals** (LONG/SHORT positions)
7. **✅ Sends email alerts** when positions open/close
8. **✅ Logs everything** to separate files

**Total: 12 concurrent data collection streams + 12 bootstrap jobs = 24 cron jobs running your complete trading system!** 🚀
