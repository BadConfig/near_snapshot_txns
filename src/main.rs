use sqlx;
use sqlx::postgres::PgPool;
use sqlx::Executor;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::sync::Arc;

async fn proc(txn: Arc<PgRow>, conn: PgPool) -> anyhow::Result<Option<(String,u64)>> {
        let rec = txn.get::<String,&str>("converted_into_receipt_id");
        let hash = txn.get::<String,&str>("transaction_hash");
        let signer = txn.get::<String,&str>("signer_account_id");
        //println!("{}",hash);
        let args = conn.fetch_all(&format!("
                select 
                (args->'args_json'->>'number_to_mint')::INT 
                as arg from transaction_actions 
                where transaction_hash = {} and (
                args->>'method_name' = 'nft_mint_free' or
                args->>'method_name' = 'nft_mint_paid');
            ",hash)[..])
            .await?;
    if args.len() == 0 {
        println!("txn {} terminated",hash);
        return Ok(None);
    }
        let eo = conn.fetch_all(&format!("
                select * from execution_outcomes 
                where receipt_id = '{}' and status = 'SUCCESS_VALUE';
            ",rec)[..])
            .await?;
    if eo.len() == 0 {
        println!("txn {} terminated",hash);
        return Ok(None);
    }

        if eo.len() != 0 && args.len() != 0 {
            let nv = args[0].get::<String,&str>("arg")
                .parse::<u64>().unwrap();
            println!("txn {} success",hash);
            println!("quan {} addr {}",nv,signer);
            return Ok(Some((signer,nv)));
        }
        println!("txn {} terminated",hash);
        Ok(None)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let mut conn = pool.acquire().await?;
    let transactions = conn.fetch_all("
            select transaction_hash, converted_into_receipt_id, signer_account_id from transactions 
            where receiver_account_id = 'spin-nft-contract.near';
        ")
        .await?;

    let mut result = std::collections::HashMap::<String,u64>::new();
    let mut handles = Vec::with_capacity(transactions.len());
    for txn in transactions {
        let pool = pool.clone();
        let txn = Arc::new(txn);
        handles.push(tokio::spawn(async move {
            let mut res = proc(txn.clone(),pool.clone()).await;
            while res.is_err() {
                res = proc(txn.clone(),pool.clone()).await;
            } 
            res
        }))
    }
    for h in handles {
        let res = h.await.unwrap();
        if let Some((user,q)) = res.unwrap() {
            let prev = result.get(&user).unwrap_or(&0u64);
            result.insert(user, prev+q);
        }
    } 
    serde_json::to_writer(&std::fs::File::create("data.json")?, &result)?;

    
    Ok(())
}
