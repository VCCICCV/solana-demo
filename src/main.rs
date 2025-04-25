use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::SubscribeRequestPing;
use yellowstone_grpc_proto::prelude::SubscribeRequest;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;
use yellowstone_grpc_proto::prelude::CommitmentLevel;
use std::collections::HashMap;
use solana_sdk::signature::Signature;
use dotenvy::dotenv;
use log::info;
use log::error;
use chrono::Local;
use solana_sdk::pubkey::Pubkey;
#[tokio::main]
async fn main() {
    env_logger::init();
    // 加载环境变量
    dotenv().ok();
    let url = std::env::var("YELLOWSTONE_GRPC_URL").expect("YELLOWSTONE_GRPC_URL must be set");

    // 创建gRPC连接
    let mut grpc = GeyserGrpc::new(url).await.expect("发送出错");
    let (mut subscribe_tx, mut stream) = grpc.subscribe().await.expect("发送出错");

    // 订阅配置
    let addrs = vec!["53gas6nwoz3GjbdbmiZ5ywLdfnhqUQNEKvF5ErpXUc3S".to_string()];
    let subscribe_request = SubscribeRequest {
        accounts: HashMap::from([
            (
                "client".to_string(),
                SubscribeRequestFilterAccounts {
                    account: addrs,
                    ..Default::default()
                },
            ),
        ]),
        commitment: Some(CommitmentLevel::Processed.into()),
        ..Default::default()
    };

    // 发送订阅请求
    subscribe_tx.send(subscribe_request).await.expect("发送出错");

    // 处理数据流
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => handle_message(msg, &mut subscribe_tx).await.expect("处理出错"),
            Err(e) => {
                error!("Stream error: {:?}", e);
                break;
            }
        }
    }
}

async fn handle_message(
    msg: yellowstone_grpc_proto::prelude::SubscribeUpdate,
    subscribe_tx: &mut tokio::sync::mpsc::Sender<SubscribeRequest>
) -> Result<()> {
    match msg.update_oneof {
        Some(UpdateOneof::Account(subscribe_account)) => {
            if let Some(account) = subscribe_account.account {
                // 解析账户地址
                let account_pubkey = Pubkey::try_from(account.pubkey.as_slice())?;
                info!("Account: {}", account_pubkey);

                // 解析所有者地址
                let owner = Pubkey::try_from(account.owner.as_slice())?;
                info!("Owner: {}", owner);

                // 解析交易签名
                match Signature::try_from(account.txn_signature.as_slice()) {
                    Ok(sig) => info!("Signature: {}", sig),
                    Err(e) => error!("Signature error: {}", e),
                }

                // 解析代币账户数据
                match TokenAccount::unpack(&account.data) {
                    Ok(token_account) => info!("Token account: {:?}", token_account),
                    Err(e) => error!("Unpack error: {}", e),
                }
            }
        }
        Some(UpdateOneof::Ping(_)) => {
            subscribe_tx.send(SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: 1 }),
                ..Default::default()
            }).await?;
            info!("Sent ping at {}", Local::now());
        }
        Some(UpdateOneof::Pong(_)) => {
            info!("Received pong at {}", Local::now());
        }
        _ => {}
    }
    Ok(())
}
