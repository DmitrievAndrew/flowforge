db = db.getSiblingDB('flowforge');
db.raw_trades.createIndex({ ticker: 1, timestamp: -1 }, { background: true });
db.raw_quotes.createIndex({ ticker: 1, timestamp: -1 }, { background: true });
print("MongoDB indexes created.");
