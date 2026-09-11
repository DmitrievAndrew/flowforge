use anyhow::Result;
use mongodb::{Client, Collection};

use crate::models::Aggregate;

pub struct MongoWriter {
    collection: Collection<Aggregate>,
}

impl MongoWriter {
    pub async fn new(uri: &str, database: &str, collection: &str) -> Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        let coll = client.database(database).collection(collection);
        Ok(Self { collection: coll })
    }

    pub async fn insert(&self, agg: &Aggregate) -> Result<()> {
        self.collection.insert_one(agg).await?;
        Ok(())
    }
}
