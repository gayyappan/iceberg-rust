use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogConfig};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProviderFactory};
use std::collections::HashMap;
use std::sync::Arc;

use iceberg_catalog_s3tables::*;
use tokio::*;

const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const AWS_REGION_NAME: &str = "REGION_NAME";
const S3TABLES_BUCKET_ARN: &str = "arn:aws:s3tables:us-east-1:142548018081:bucket/gayathri-demo";

async fn load_s3tables_catalog_from_env() -> Result<Option<S3TablesCatalog>> {
    // see io/s3_storage.rs. The aws variable naming is not consistent
    //between that and S3Tables catalog
    let mut config_props = HashMap::<String, String>::new();
    if let Some(aws_access_key_id) = std::env::var(AWS_ACCESS_KEY_ID).ok() {
        config_props.insert("s3.access_key_id".to_string(), aws_access_key_id.clone());
        config_props.insert(AWS_ACCESS_KEY_ID.to_string(), aws_access_key_id);
    }
    if let Some(aws_secret_access_key) = std::env::var(AWS_SECRET_ACCESS_KEY).ok() {
        config_props.insert(
            "s3.secret_access_key".to_string(),
            aws_secret_access_key.clone(),
        );
        config_props.insert(AWS_SECRET_ACCESS_KEY.to_string(), aws_secret_access_key);
    }
    if let Some(aws_region) = std::env::var(AWS_REGION_NAME).ok() {
        config_props.insert("s3.region".to_string(), aws_region.clone());
        config_props.insert(AWS_REGION_NAME.to_string(), aws_region);
    }

    let config = S3TablesCatalogConfig::builder()
        .table_bucket_arn(S3TABLES_BUCKET_ARN.to_string())
        //.table_bucket_arn(table_bucket_arn)
        .endpoint_url_opt(None)
        .properties(config_props)
        .build();

    let cat = S3TablesCatalog::new(config).await.unwrap();
    Ok(Some(cat))
}

async fn test_s3tables_load_table(catalog: Arc<dyn Catalog>) {
    let ns = catalog.list_namespaces(None).await.unwrap();
    println!("all namespaces here {:?}", ns);
    let tabnm = TableIdent::new(
        NamespaceIdent::new("gayathridb".to_string()),
        "demotab".to_string(),
    );

    let sch = NamespaceIdent::new("gayathridb".to_string());
    let tblist = catalog.list_tables(&sch).await.unwrap();
    println!("tbllist for gayathridb is {:?}", tblist);
    println!("{:?}", tabnm);

    let tab = catalog.table_exists(&tabnm).await.unwrap();
    //    println!("tabl exists {:?}", tab);
    let res = catalog.load_table(&tabnm).await;
    match res {
        Ok(table) => {
            //println!(" HERE **** Table loaded: {:?}", table);
            println!(" HERE **** Table loaded: {:?}", tabnm);
        }
        Err(e) => {
            println!("Error loading table: {:?}", e);
        }
    }
}

async fn test_s3tables_with_df(catalog: Arc<dyn Catalog>) -> datafusion::error::Result<()> {
    // Create a new session context
    let mut state = SessionStateBuilder::new().with_default_features().build();

    // Register the IcebergTableProviderFactory in the session
    state.table_factories_mut().insert(
        "ICEBERG".to_string(),
        Arc::new(IcebergTableProviderFactory::new()),
    );

    // let catalog_provider = get_rest_catalog_provider().await;
    //let catalog_provider = get_s3tables_catalog().await;
    let catalog_provider = IcebergCatalogProvider::try_new_with_schema(catalog, "gayathridb")
        .await
        .unwrap();

    // register the catalog
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog("s3tables", Arc::new(catalog_provider));

    // XXX: Can't create tables from `iceberg-rust` atm
    // let df = ctx.sql("CREATE TABLE s3tables.gayathridb.demotab (id INT, ts TIMESTAMP, device_id INT, val FLOAT)").await?;

    let df = ctx.sql("SHOW COLUMNS FROM s3tables.gayathridb.demotab").await?;
    df.show().await?;

    let df = ctx.sql("SELECT * FROM s3tables.gayathridb.demotab").await?;
    df.show().await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    let catalog = match load_s3tables_catalog_from_env().await {
        Ok(Some(catalog)) => catalog,
        Ok(None) => return,
        Err(e) => panic!("Error loading catalog: {}", e),
    };
    let catalog = Arc::new(catalog);
//    test_s3tables_load_table( catalog.clone()).await;
    test_s3tables_with_df(catalog.clone()).await;
}
