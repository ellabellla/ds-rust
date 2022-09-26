use std::{str::FromStr};

use sqlx::{sqlite::{SqlitePool, SqliteConnectOptions, SqliteRow}, QueryBuilder, Row, pool::PoolConnection, Sqlite};


struct KVLite {
    pool: SqlitePool,
    kv_name: String,
}

impl KVLite {
    async fn create_store_table(conn: &mut PoolConnection<Sqlite>, kv_name: &str) -> Result<(), sqlx::Error>{
        QueryBuilder::new(format!(r#"
                CREATE TABLE {} (key TEXT PRIMARY KEY, value TEXT);
            "#, kv_name))
            .build()
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn new(filename: &str, kv_name: &str, read_only: bool, create_new: bool) -> Result<KVLite, sqlx::Error> {
        let options = SqliteConnectOptions::from_str(filename)?
            .create_if_missing(create_new)
            .read_only(read_only);

        let pool = SqlitePool::connect_with(options).await?;

        let mut conn = pool.acquire().await?;
        match QueryBuilder::new(r#"
                SELECT name FROM sqlite_master WHERE type="table" AND name=
            "#)
            .push_bind(kv_name)
            .build()
            .fetch_optional(&mut conn)
            .await? 
        {
            Some(_) => (),
            None => {
                KVLite::create_store_table(&mut conn, kv_name).await?;
            },
        }

        Ok(KVLite { pool, kv_name: kv_name.to_string() })
    }

    pub async fn get(&self, key: &str) -> Result<String, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        
        let row = QueryBuilder::new(format!(r#"
                SELECT value FROM {} WHERE key=
            "#, self.kv_name))
            .push_bind(key)
            .build()
            .fetch_one(&mut conn)
            .await?;
        
        Ok(row.get("value"))
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        
        QueryBuilder::new(format!(r#"
                INSERT OR REPLACE INTO {} VALUES (
            "#, self.kv_name))
            .push_bind(key)
            .push(",")
            .push_bind(value)
            .push(")")
            .build()
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    pub async fn keys(&self) -> Result<Vec<SqliteRow>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        
        let rows = QueryBuilder::new(format!(r#"
                SELECT key FROM {}
            "#, self.kv_name))
            .build()
            .fetch_all(&mut conn)
            .await?;

        Ok(rows)
    }

    pub async fn values(&self) -> Result<Vec<SqliteRow>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        
        let rows = QueryBuilder::new(format!(r#"
                SELECT value FROM {}
            "#, self.kv_name))
            .build()
            .fetch_all(&mut conn)
            .await?;

        Ok(rows)
    }

    pub async fn items(&self) -> Result<Vec<SqliteRow>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        
        let rows = QueryBuilder::new(format!(r#"
                SELECT key,value FROM {}
            "#, self.kv_name))
            .build()
            .fetch_all(&mut conn)
            .await?;

        Ok(rows)
    }

    pub async fn contains(&self, key: &str) -> Result<bool, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;

        match QueryBuilder::new(format!(r#"
                SELECT value FROM {} WHERE key=
            "#, self.kv_name))
            .push_bind(key)
            .build()
            .fetch_optional(&mut conn)
            .await? 
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{QueryBuilder, Row};

    use crate::KVLite;

    async fn setup_store() -> Result<KVLite, sqlx::Error> {
        let kv_name = "store";
        let store = KVLite::new("sqlite://tmp.db", kv_name, false, true).await?;
        
        
        let mut conn = store.pool.acquire().await?;
        QueryBuilder::new(format!(r#"
                DROP TABLE IF EXISTS {};
            "#, kv_name))
            .build()
            .execute(&mut conn)
            .await?;

        KVLite::create_store_table(&mut conn, kv_name).await?;

        Ok(store)
    }

    #[tokio::test]
    async fn test_KVLite() -> Result<(), sqlx::Error> {
        let store = setup_store().await?;

        for i in 0..100 {
            assert!(matches!(store.set(&format!("key{}", i), &format!("value{}", i)).await, Ok(())))
        }

        assert!(store.get("not in store").await.is_err());
        for i in 0..100 {
            assert_eq!(store.get(&format!("key{}", i)).await.unwrap(), format!("value{}", i))
        }

        let mut keys = store.keys().await.unwrap();
        keys.sort_by(|a, b| 
                a.get::<String,&str>("key")[3..].parse::<i32>().unwrap()
            .cmp(
                &b.get::<String,&str>("key")[3..].parse::<i32>().unwrap()
            ));
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(key.get::<String, &str>("key"), format!("key{}", i))
        }

        let mut values = store.values().await.unwrap();
        values.sort_by(|a, b| 
            a.get::<String,&str>("value")[5..].parse::<i32>().unwrap()
        .cmp(
            &b.get::<String,&str>("value")[5..].parse::<i32>().unwrap()
        ));
        for (i, value) in values.iter().enumerate() {
            assert_eq!(value.get::<String, &str>("value"), format!("value{}", i))
        }

        let mut items = store.items().await.unwrap();
        items.sort_by(|a, b| 
            a.get::<String,&str>("key")[3..].parse::<i32>().unwrap()
        .cmp(
            &b.get::<String,&str>("key")[3..].parse::<i32>().unwrap()
        ));
        for (i, item) in items.iter().enumerate() {
            assert_eq!(item.get::<String, &str>("key"), format!("key{}", i));
            assert_eq!(item.get::<String, &str>("value"), format!("value{}", i));
        }

        assert!(matches!(store.contains("not in store").await, Ok(false)));
        for i in 0..100 {
            assert!(matches!(store.contains(&format!("key{}", i)).await, Ok(true)))
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let store = KVLite::new("sqlite://tmp.db", "store", false, true).await?;
    store.set("test", "testing value").await?;

    println!("{}", store.get("test").await?);

    Ok(())
}
