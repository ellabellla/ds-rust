use std::{str::FromStr, fs};
use sqlx::{sqlite::{SqlitePool, SqliteConnectOptions, SqliteRow}, QueryBuilder, Row, pool::PoolConnection, Sqlite};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(long)]
    /// Specify datastore location
    ds: Option<String>,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Set the value of a record
    Set { key: String, value: String },
    /// Check if a record exists
    Contains { key: String },
    /// Get the value of a record
    Get { key: String },
    /// Delete a record
    Delete { key: String },
    /// Get a list of all keys in the datastore
    Keys,
    /// Get a list of all values in the datastore
    Values,
    /// Get a list of all records in the datastore
    Records,
}

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

    pub async fn del(&self, key: &str) -> Result<(), sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        
        QueryBuilder::new(format!(r#"
                DELETE FROM {} WHERE key=
            "#, self.kv_name))
            .push_bind(key)
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

    pub async fn records(&self) -> Result<Vec<SqliteRow>, sqlx::Error> {
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
    async fn test_kv_lite() -> Result<(), sqlx::Error> {
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

        let mut records = store.records().await.unwrap();
        records.sort_by(|a, b| 
            a.get::<String,&str>("key")[3..].parse::<i32>().unwrap()
        .cmp(
            &b.get::<String,&str>("key")[3..].parse::<i32>().unwrap()
        ));
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.get::<String, &str>("key"), format!("key{}", i));
            assert_eq!(record.get::<String, &str>("value"), format!("value{}", i));
        }

        assert!(matches!(store.contains("not in store").await, Ok(false)));
        for i in 0..100 {
            assert!(matches!(store.contains(&format!("key{}", i)).await, Ok(true)))
        }

        assert!(matches!(store.del("key1").await, Ok(())));
        assert!(matches!(store.contains("key1").await, Ok(false)));

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let default_db_dir = "ds-rust/";
    let default_db_name = "ds.db";
    let default_db_prefix = "sqlite://";

    let args = Cli::parse();

    let db_path = match args.ds {
        Some(db_path) => db_path,
        None => {
            let mut db_path = dirs::config_dir().expect("couldn't find a default db location");
            db_path.push(default_db_dir);
            fs::create_dir_all(&db_path)?;
            db_path.push(default_db_name);
            let db_path = db_path.to_str().expect("couldn't find a default db location").to_string();
            format!("{}{}", default_db_prefix, db_path)
        },
    };

    let store = KVLite::new(&db_path, "store", false, true).await?;
    
    
    match args.command {
        Commands::Set { key, value } => {
            match store.set(&key, &value).await {
                Ok(_) => println!("ok"),
                Err(e) => println!("{:?}", e),
            } 
        },
        Commands::Contains { key } => {
            match store.contains(&key).await {
                Ok(res) => println!("{}", res),
                Err(e) => println!("{:?}", e),
            } 
        },
        Commands::Get { key } => {
            match store.get(&key).await {
                Ok(res) => println!("{}", res),
                Err(e) => println!("{:?}", e),
            } 
        },
        Commands::Delete { key } => {
            match store.del(&key).await {
                Ok(_) => println!("ok"),
                Err(e) => println!("{:?}", e),
            } 
        },
        Commands::Keys =>  {
            match store.keys().await {
                Ok(res) => for key in res {
                    println!("{}", key.get::<String, &str>("key"))
                },
                Err(e) => println!("{:?}", e),
            } 
        },
        Commands::Values =>  {
            match store.values().await {
                Ok(res) => for value in res {
                    println!("{}", value.get::<String, &str>("value"))
                },
                Err(e) => println!("{:?}", e),
            } 
        },
        Commands::Records =>  {
            match store.records().await {
                Ok(res) => for record in res {
                    print!("{},", record.get::<String, &str>("key"));
                    println!("{}", record.get::<String, &str>("value"))
                },
                Err(e) => println!("{:?}", e),
            } 
        },
    }

    Ok(())
}
