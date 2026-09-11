use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
    pub mongodb_uri: String,
    pub mongodb_database: String,
    pub mongodb_collection: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            kafka_brokers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
            kafka_topic: std::env::var("KAFKA_TOPIC_RAW_TRADES")
                .unwrap_or_else(|_| "raw-trades".into()),
            kafka_group_id: std::env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "flowforge-consumer".into()),
            mongodb_uri: std::env::var("MONGODB_URI")
                .unwrap_or_else(|_| "mongodb://admin:password@localhost:27017".into()),
            mongodb_database: std::env::var("MONGODB_DATABASE")
                .unwrap_or_else(|_| "flowforge".into()),
            mongodb_collection: "processed_aggregates".into(),
        })
    }
}
