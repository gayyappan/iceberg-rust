use std::collections::HashMap;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProviderFactory};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogConfig};
use std::sync::Arc;

use iceberg_catalog_s3tables::*;
use tokio::*;

const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const AWS_REGION_NAME: &str = "REGION_NAME";
const S3TABLES_BUCKET_ARN: &str = "arn:aws:s3tables:us-east-1:142548018081:bucket/gayathri-demo";

async fn load_s3tables_catalog_from_env() -> Result< Option<S3TablesCatalog> >
{
    // see io/s3_storage.rs. The aws variable naming is not consistent
    //between that and S3Tables catalog
    let mut config_props = HashMap::<String, String>::new();
    if let Some(aws_access_key_id) = std::env::var(AWS_ACCESS_KEY_ID).ok() {
        config_props.insert("s3.access_key_id".to_string(), aws_access_key_id.clone());
        config_props.insert(AWS_ACCESS_KEY_ID.to_string(), aws_access_key_id);
    }
    if let Some(aws_secret_access_key) = std::env::var(AWS_SECRET_ACCESS_KEY).ok() {
        config_props.insert("s3.secret_access_key".to_string(), aws_secret_access_key.clone());
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

async fn test_s3tables_load_table( catalog: Arc< dyn Catalog>) {

    let ns = catalog.list_namespaces(None).await.unwrap();
    println!("{:?}", ns);
    let tabnm = TableIdent::new(
                NamespaceIdent::new("demodb".to_string()),
                "demotab".to_string());


    let sch = NamespaceIdent::new("demodb".to_string());
    let tblist = catalog.list_tables( &sch).await.unwrap();
    println!("{:?}", tblist);
    println!("{:?}", tabnm);

    let tab = catalog
        .table_exists(&tabnm)
        .await
        .unwrap();
//    println!("tabl exists {:?}", tab);
    let res = catalog
        .load_table(&tabnm)
        .await;
    match res {
        Ok(table) => {
            println!(" HERE **** Table loaded: {:?}", table);
        }
        Err(e) => {
            println!("Error loading table: {:?}", e);
        }
    }
}

#[tokio::main]
async fn main() {
    let catalog = match load_s3tables_catalog_from_env().await {
        Ok(Some(catalog)) => catalog,
        Ok(None) => return,
        Err(e) => panic!("Error loading catalog: {}", e),
    };
    let catalog = Arc::new(catalog);
    test_s3tables_load_table( catalog.clone()).await;
}
