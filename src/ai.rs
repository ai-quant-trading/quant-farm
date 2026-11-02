use crate::strategy::SignalCandidate;
use crate::types::{Candle, Side};
use anyhow::Result;
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiRiskVerdict {
    pub approved: bool,
    pub risk_score: u8, // 0 ~ 100 (越高越危险，>= 70 一票否决)
    pub source: String,
    pub reason: String,
}

pub struct AiRiskGuard {
    client: Client,
    api_base_url: String,
    api_key: Option<String>,
    model: String,
}

impl AiRiskGuard {
    pub fn from_env() -> Self {
        let api_base_url = std::env::var("LLM_API_BASE_URL")
            .unwrap_or_else(|_| "https://api.deepseek.com/v1".to_string());
        let api_key = std::env::var("LLM_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && s != "your_llm_api_key_here");
        let model = std::env::var("LLM_MODEL").unwrap_or_else(|_| "deepseek-chat".to_string());

        let client = Client::builder()
            .timeout(Duration::from_secs(6))
            .build()
            .unwrap_or_default();

        Self {
            client,
            api_base_url,
            api_key,
            model,
        }
    }

    /// 第四层漏斗：在信号下单前执行 AI 风险审查与量化微观结构异常否决
    pub async fn evaluate_signal(
        &self,
        signal: &SignalCandidate,
        recent_candles: &[Candle],
        external_news_digest: Option<&str>,
    ) -> AiRiskVerdict {
        // 1. 本地毫秒级硬规则异常检测 (防插针、防极端拥挤费率接盘)
        if let Some(last) = recent_candles.last() {
            // 检查单根 K 线是否出现极端天地针 (振幅 > 12%)
            if last.low > Decimal::ZERO {
                let bar_amp = (last.high - last.low) / last.low;
                if bar_amp > dec!(0.12) {
                    return AiRiskVerdict {
                        approved: false,
                        risk_score: 88,
                        source: "LocalAnomalyRule".to_string(),
                        reason: format!(
                            "当根 1h K 线振幅高达 {:.2}% (>12% 阈值)，盘口流动性异常或插针风险极高，一票否决",
                            bar_amp * dec!(100)
                        ),
                    };
                }
            }

            // 检查做多时资金费率是否极度过热 (> +0.15% / 8h)
            if signal.side == Side::Long && last.funding_rate > dec!(0.0015) {
                return AiRiskVerdict {
                    approved: false,
                    risk_score: 80,
                    source: "LocalAnomalyRule".to_string(),
                    reason: format!(
                        "多头资金费率严重过热 ({:.4}%)，散户多头极度拥挤，拒绝高位追多",
                        last.funding_rate * dec!(100)
                    ),
                };
            }
        }

        // 2. 若配置了 LLM_API_KEY，调用大模型对快讯/公告与行情上下文进行语义一票否决评估
        if let Some(ref key) = self.api_key {
            match self
                .query_llm_veto(key, signal, recent_candles, external_news_digest)
                .await
            {
                Ok(verdict) => return verdict,
                Err(e) => {
                    warn!("LLM 风险评估接口调用超时或异常 ({})，自动回退至本地规则保护", e);
                }
            }
        }

        AiRiskVerdict {
            approved: true,
            risk_score: 15,
            source: "QuantRuleEngine".to_string(),
            reason: "通过波动率收敛、量价突破与费率健康度检查，未发现异常利空特征".to_string(),
        }
    }

    async fn query_llm_veto(
        &self,
        api_key: &str,
        signal: &SignalCandidate,
        recent_candles: &[Candle],
        news_digest: Option<&str>,
    ) -> Result<AiRiskVerdict> {
        let last_close = recent_candles
            .last()
            .map(|c| c.close)
            .unwrap_or(signal.signal_close);
        let news_text = news_digest.unwrap_or("暂无突发交易所下架或黑客攻击公告。");

        let prompt = format!(
            "你是数字货币量化系统的首席风控官。系统正准备开仓：\
            币种={}, 方向={}, 信号价={}, 预设止损价={}, 截面动量得分={:.4}。\
            近期消息面摘要：{}。\
            请判断是否存在致命风险（如交易所下架、黑客被盗、监管重锤、大额解锁砸盘或明显诱多）。\
            严格仅输出 JSON 格式：{{\"approved\": true/false, \"risk_score\": 0-100, \"reason\": \"简短原因\"}}",
            signal.symbol,
            signal.side,
            last_close,
            signal.proposed_stop,
            signal.score,
            news_text
        );

        let url = format!("{}/chat/completions", self.api_base_url.trim_end_matches('/'));
        let body = json!({
            "model": self.model,
            "temperature": 0.1,
            "response_format": { "type": "json_object" },
            "messages": [
                {"role": "system", "content": "You are a strict crypto quantitative risk control AI. Output valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        });

        let resp = self
            .client
            .post(&url)
            .bearer_auth(api_key)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;

        let v: Value = resp.json().await?;
        let content = v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("{}");
        let parsed: Value = serde_json::from_str(content)?;

        let risk_score = parsed["risk_score"].as_u64().unwrap_or(20) as u8;
        let approved = parsed["approved"].as_bool().unwrap_or(risk_score < 70) && risk_score < 70;
        let reason = parsed["reason"]
            .as_str()
            .unwrap_or("LLM 语义审查完成")
            .to_string();

        info!(
            "AI 风控审查 [{} {}]: approved={}, score={}, reason={}",
            signal.symbol, signal.side, approved, risk_score, reason
        );

        Ok(AiRiskVerdict {
            approved,
            risk_score,
            source: format!("LLM({})", self.model),
            reason,
        })
    }
}
