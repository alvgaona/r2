use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "A CLI tool for managing Cloudflare R2 storage buckets and objects"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
#[command(about = "R2 storage operations for managing buckets and objects")]
enum Commands {
    #[command(
        about = "List buckets or objects in a bucket",
        long_about = "List all buckets when no bucket specified, or list objects in the specified bucket\n\
        Examples:\n\
        - List all buckets: r2 ls\n\
        - List objects in bucket: r2 ls my-bucket"
    )]
    Ls {
        #[arg(help = "Name of the bucket to list objects from (optional)")]
        bucket: Option<String>,
    },
    #[command(
        about = "Move/rename objects within a bucket",
        long_about = "Move or rename objects within the same bucket using source and destination paths\n\
        Example: r2 mv my-bucket file1.txt folder/file2.txt"
    )]
    Mv {
        #[arg(help = "Name of the bucket containing the object")]
        bucket: String,
        #[arg(help = "Source object key (path to existing object)")]
        src: String,
        #[arg(help = "Destination object key (new path/name)")]
        dst: String,
    },
    #[command(
        about = "Copy files to R2",
        long_about = "Copy local files to R2 storage. The destination must be specified in 'bucket/key' format\n\
        Example: r2 cp local/file.txt my-bucket/remote/file.txt"
    )]
    Cp {
        #[arg(help = "Local file path to upload")]
        src: String,
        #[arg(
            help = "Destination path in format 'bucket/key' (e.g., 'my-bucket/folder/file.txt')"
        )]
        dst: String,
    },
    #[command(
        about = "Delete an object from a bucket",
        long_about = "Delete a single object from the specified bucket\n\
        Example: r2 rm my-bucket file.txt"
    )]
    Rm {
        #[arg(help = "Name of the bucket containing the object")]
        bucket: String,
        #[arg(help = "Object key to delete")]
        key: String,
    },
}

#[derive(Deserialize)]
struct Credentials {
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Deserialize)]
struct Metadata {
    account_id: String,
}

#[derive(Deserialize)]
struct Config {
    credentials: Credentials,
    metadata: Metadata,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let (access_key_id, secret_access_key, account_id) = match (
        env::var("R2_ACCESS_KEY_ID"),
        env::var("R2_SECRET_ACCESS_KEY"),
        env::var("R2_ACCOUNT_ID"),
    ) {
        (Ok(key), Ok(secret), Ok(account)) => (key, secret, account),
        _ => {
            let home = env::var("HOME")?;
            let config_path = PathBuf::from(home).join(".r2").join("config");
            let mut contents = String::new();
            File::open(config_path)?.read_to_string(&mut contents)?;

            let config: Config = toml::from_str(&contents)?;

            (
                config.credentials.access_key_id,
                config.credentials.secret_access_key,
                config.metadata.account_id,
            )
        }
    };

    let r2_endpoint = format!("https://{}.r2.cloudflarestorage.com", account_id);

    let region_provider = RegionProviderChain::first_try(Region::new("auto"));

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(r2_endpoint)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            &access_key_id,
            &secret_access_key,
            None,
            None,
            "r2",
        ))
        .load()
        .await;

    let client = Client::new(&config);

    match args.command {
        None | Some(Commands::Ls { bucket: None }) => {
            let buckets = client.list_buckets().send().await?;
            for bucket in buckets.buckets() {
                println!("{}", bucket.name().unwrap_or_default());
            }
        }
        Some(Commands::Ls {
            bucket: Some(bucket),
        }) => {
            let objects = client.list_objects_v2().bucket(bucket).send().await?;
            for object in objects.contents() {
                println!("{}", object.key().unwrap_or_default());
            }
        }
        Some(Commands::Mv { bucket, src, dst }) => {
            client
                .copy_object()
                .bucket(&bucket)
                .copy_source(format!("{}/{}", bucket, src))
                .key(&dst)
                .send()
                .await?;

            client
                .delete_object()
                .bucket(&bucket)
                .key(&src)
                .send()
                .await?;
        }
        Some(Commands::Cp { src, dst }) => {
            let parts: Vec<&str> = dst.splitn(2, '/').collect();
            if parts.len() != 2 {
                return Err("Destination must be in format bucket/key".into());
            }
            let (bucket, key) = (parts[0], parts[1]);

            let body = tokio::fs::read(src).await?;
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body.into())
                .send()
                .await?;
        }
        Some(Commands::Rm { bucket, key }) => {
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await?;
        }
    }

    Ok(())
}
