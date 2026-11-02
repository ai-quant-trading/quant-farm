use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy)]
pub enum AlertLevel {
    Info,
    Signal,
    Warning,
    Critical,
}

impl AlertLevel {
    pub fn tag(&self) -> &'static str {
        match self {
            AlertLevel::Info => "[INFO]",
            AlertLevel::Signal => "[SIGNAL]",
            AlertLevel::Warning => "[WARN]",
            AlertLevel::Critical => "[CRITICAL]",
        }
    }
}

#[derive(Clone)]
pub struct Notifier {
    client: Client,
    webhook_url: Option<String>,
    log_path: String,
}

impl Notifier {
    pub fn from_env() -> Self {
        let webhook_url = std::env::var("NOTIFY_WEBHOOK_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let client = Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .unwrap_or_default();

        let _ = fs::create_dir_all("logs");

        Self {
            client,
            webhook_url,
            log_path: "logs/alerts.log".to_string(),
        }
    }

    pub async fn send(&self, level: AlertLevel, title: &str, message: &str) -> Result<()> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
        let formatted = format!("{} {} {} | {}\n", now, level.tag(), title, message);

        // 1. 始终写入本地持久化审计日志 logs/alerts.log
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        {
            let _ = file.write_all(formatted.as_bytes());
        }

        info!("{} {} - {}", level.tag(), title, message);

        // 2. 若配置了飞书 / Telegram / 通用 Webhook，异步推送到手机
        if let Some(ref url) = self.webhook_url {
            let payload = if url.contains("feishu.cn") || url.contains("larksuite.com") {
                json!({
                    "msg_type": "text",
                    "content": {
                        "text": format!("【Coin-AI {}】{}\n时间: {}\n{}", level.tag(), title, now, message)
                    }
                })
            } else if url.contains("api.telegram.org") {
                json!({
                    "text": format!("*【Coin-AI {}】{}*\n`{}`\n{}", level.tag(), title, now, message),
                    "parse_mode": "Markdown"
                })
            } else {
                json!({
                    "level": level.tag(),
                    "title": title,
                    "timestamp": now,
                    "message": message
                })
            };

            if let Err(e) = self.client.post(url).json(&payload).send().await {
                warn!("推送远程 Webhook 告警失败 (已记录本地日志): {}", e);
            }
        }

        Ok(())
    }
}
