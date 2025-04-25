use ::{
    anyhow::Context,
    backoff::{ future::retry, ExponentialBackoff },
    dotenvy::dotenv,
    futures::{ future::TryFutureExt, stream::StreamExt },
    log::{ error, info },
    std::{ collections::HashMap, env, sync::Arc, time::{ Duration, SystemTime } },
    tokio::sync::Mutex,
    yellowstone_grpc_client::{ ClientTlsConfig, GeyserGrpcClient, Interceptor },
    yellowstone_grpc_proto::{
        prelude::{ SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots },
        tonic::transport::Certificate,
    },
};

#[derive(Debug, Clone)]
struct Config {
    // 端点
    endpoint: String,
    // cs证书
    ca_certificate: Option<String>,
    // x_token
    x_token: Option<String>,
    // 链接超时时间
    connect_timeout_ms: Option<u64>,
    //TCP套接字缓冲区大小（单位：字节）
    buffer_size: Option<usize>,
    // 是否启用HTTP/2自适应窗口控制（自动调整流量窗口大小）
    http2_adaptive_window: Option<bool>,

    // HTTP/2心跳包发送间隔（单位：毫秒），保持连接活跃
    http2_keep_alive_interval_ms: Option<u64>,

    // HTTP/2连接级初始流量控制窗口大小（单位：字节）
    initial_connection_window_size: Option<u32>,

    // HTTP/2流级初始流量控制窗口大小（单位：字节）
    initial_stream_window_size: Option<u32>,

    // HTTP/2保持连接超时时间（单位：毫秒）
    keep_alive_timeout_ms: Option<u64>,

    // 是否在空闲时保持HTTP/2连接活跃
    keep_alive_while_idle: Option<bool>,

    // TCP keepalive探测间隔（单位：毫秒）
    tcp_keepalive_ms: Option<u64>,

    // 是否禁用Nagle算法（true=禁用，减少延迟）
    tcp_nodelay: Option<bool>,

    // 请求超时时间（单位：毫秒）
    timeout_ms: Option<u64>,

    // 最大解码消息尺寸（防止内存溢出），默认1GiB
    max_decoding_message_size: usize,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        dotenv().ok(); // 加载.env文件，忽略错误如果文件不存在

        Ok(Self {
            endpoint: env
                ::var("YELLOWSTONE_GRPC_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:10000".into()),
            ca_certificate: env::var("CA_CERTIFICATE").ok(),
            x_token: env::var("X_TOKEN").ok(),
            connect_timeout_ms: env
                ::var("CONNECT_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            buffer_size: env
                ::var("BUFFER_SIZE")
                .ok()
                .and_then(|s| s.parse().ok()),
            http2_adaptive_window: env
                ::var("HTTP2_ADAPTIVE_WINDOW")
                .ok()
                .and_then(|s| s.parse().ok()),
            http2_keep_alive_interval_ms: env
                ::var("HTTP2_KEEP_ALIVE_INTERVAL_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            initial_connection_window_size: env
                ::var("INITIAL_CONNECTION_WINDOW_SIZE")
                .ok()
                .and_then(|s| s.parse().ok()),
            initial_stream_window_size: env
                ::var("INITIAL_STREAM_WINDOW_SIZE")
                .ok()
                .and_then(|s| s.parse().ok()),
            keep_alive_timeout_ms: env
                ::var("KEEP_ALIVE_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            keep_alive_while_idle: env
                ::var("KEEP_ALIVE_WHILE_IDLE")
                .ok()
                .and_then(|s| s.parse().ok()),
            tcp_keepalive_ms: env
                ::var("TCP_KEEPALIVE_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            tcp_nodelay: env
                ::var("TCP_NODELAY")
                .ok()
                .and_then(|s| s.parse().ok()),
            timeout_ms: env
                ::var("TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok()),
            max_decoding_message_size: env
                ::var("MAX_DECODING_MESSAGE_SIZE")
                .map(|s| s.parse().unwrap_or(1024 * 1024 * 1024))
                .unwrap_or(1024 * 1024 * 1024),
        })
    }

    async fn connect(&self) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        let mut tls_config = ClientTlsConfig::new().with_native_roots();
        if let Some(path) = &self.ca_certificate {
            let bytes = tokio::fs::read(path).await?;
            tls_config = tls_config.ca_certificate(Certificate::from_pem(bytes));
        }
        let mut builder = GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(self.x_token.clone())?
            .tls_config(tls_config)?
            .max_decoding_message_size(self.max_decoding_message_size);

        if let Some(duration) = self.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(duration));
        }
        if let Some(sz) = self.buffer_size {
            builder = builder.buffer_size(sz);
        }
        if let Some(enabled) = self.http2_adaptive_window {
            builder = builder.http2_adaptive_window(enabled);
        }
        if let Some(duration) = self.http2_keep_alive_interval_ms {
            builder = builder.http2_keep_alive_interval(Duration::from_millis(duration));
        }
        if let Some(sz) = self.initial_connection_window_size {
            builder = builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.initial_stream_window_size {
            builder = builder.initial_stream_window_size(sz);
        }
        if let Some(duration) = self.keep_alive_timeout_ms {
            builder = builder.keep_alive_timeout(Duration::from_millis(duration));
        }
        if let Some(enabled) = self.keep_alive_while_idle {
            builder = builder.keep_alive_while_idle(enabled);
        }
        if let Some(duration) = self.tcp_keepalive_ms {
            builder = builder.tcp_keepalive(Some(Duration::from_millis(duration)));
        }
        if let Some(enabled) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(enabled);
        }
        if let Some(duration) = self.timeout_ms {
            builder = builder.timeout(Duration::from_millis(duration));
        }

        builder.connect().await.map_err(Into::into)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe {
        env::set_var(
            env_logger::DEFAULT_FILTER_ENV,
            env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into())
        );
    }

    // 初始化日志
    env_logger::init();

    // 初始化配置
    let config = Config::from_env()?;
    let zero_attempts = Arc::new(Mutex::new(true));

    // 构造订阅请求（订阅所有账户和槽位更新）
    let request = SubscribeRequest {
        accounts: {
            let mut map = HashMap::new();
            map.insert("all".to_string(), SubscribeRequestFilterAccounts {
                nonempty_txn_signature: None,
                account: vec![],
                owner: vec![],
                filters: vec![],
            });
            map
        },
        slots: {
            let mut map = HashMap::new();
            map.insert("all".to_string(), SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            });
            map
        },
        ..Default::default()
    };

    retry(ExponentialBackoff::default(), move || {
        let config = config.clone();
        let request = request.clone();
        let zero_attempts = Arc::clone(&zero_attempts);

        (
            async move {
                let mut zero_attempts = zero_attempts.lock().await;
                if *zero_attempts {
                    *zero_attempts = false;
                } else {
                    info!("Retry to connect to the server");
                }
                drop(zero_attempts);

                let mut client = config.connect().await.map_err(backoff::Error::transient)?;
                info!("Connected to {}", config.endpoint);

                let (mut subscribe_tx, mut stream) = client
                    .subscribe_with_request(Some(request.clone())).await
                    .expect("订阅失败");
                info!("Stream opened");

                while let Some(message) = stream.next().await {
                    match message {
                        Ok(msg) => {
                            let created_at: SystemTime = msg.created_at
                                .ok_or(anyhow::anyhow!("no created_at in the message"))?
                                .try_into()
                                .context("failed to parse created_at")?;

                            // 直接打印原始消息
                            info!("Received update: {:?}", msg);
                        }
                        Err(error) => {
                            error!("Stream error: {:?}", error);
                            break;
                        }
                    }
                }

                info!("Stream closed");
                Ok::<(), backoff::Error<anyhow::Error>>(())
            }
        ).inspect_err(|error| error!("Connection error: {error}"))
    }).await.map_err(Into::into)
}
