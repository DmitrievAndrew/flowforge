db = db.getSiblingDB('flowforge');

db.createCollection('raw_trades');
db.createCollection('raw_quotes');
db.createCollection('enriched_events');
db.createCollection('predictions');

db.raw_trades.createIndex({ ticker: 1, timestamp: -1 });
db.raw_trades.createIndex({ timestamp: -1 }, { expireAfterSeconds: 2592000 });
db.raw_quotes.createIndex({ ticker: 1, timestamp: -1 });
db.enriched_events.createIndex({ ticker: 1, timestamp: -1 });
db.predictions.createIndex({ ticker: 1, timestamp: -1 });
