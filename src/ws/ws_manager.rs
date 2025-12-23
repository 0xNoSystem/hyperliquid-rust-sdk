use std::{
    borrow::BorrowMut,
    collections::HashMap,
    ops::DerefMut,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use alloy::primitives::Address;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    spawn,
    sync::{mpsc::UnboundedSender, Mutex},
    time,
};
use tokio_tungstenite::{connect_async, tungstenite::protocol, MaybeTlsStream, WebSocketStream};

use crate::{
    prelude::*,
    ws::message_types::{
        ActiveAssetData, ActiveSpotAssetCtx, AllMids, Bbo, Candle, L2Book, OrderUpdates, Trades,
        User,
    },
    ActiveAssetCtx, BaseUrl, Error, Notification, UserFills, UserFundings,
    UserNonFundingLedgerUpdates, WebData2,
};

#[derive(Debug)]
struct SubscriptionData {
    sending_channel: UnboundedSender<Message>,
    subscription_id: u32,
    id: String,
}

#[derive(Clone, Copy, Debug)]
struct LivenessConfig {
    ping_after: Duration,
    pong_grace: Duration,
    check_interval: Duration,
}
#[derive(Debug)]
pub(crate) struct WsManager {
    stop_flag: Arc<AtomicBool>,
    writer: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>>>,
    subscriptions: Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    subscription_id: u32,
    subscription_identifiers: HashMap<u32, String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Subscription {
    AllMids,
    Notification { user: Address },
    WebData2 { user: Address },
    Candle { coin: String, interval: String },
    L2Book { coin: String },
    Trades { coin: String },
    OrderUpdates { user: Address },
    UserEvents { user: Address },
    UserFills { user: Address },
    UserFundings { user: Address },
    UserNonFundingLedgerUpdates { user: Address },
    ActiveAssetCtx { coin: String },
    ActiveAssetData { user: Address, coin: String },
    Bbo { coin: String },
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "channel")]
#[serde(rename_all = "camelCase")]
pub enum Message {
    NoData,
    HyperliquidError(String),
    AllMids(AllMids),
    Trades(Trades),
    L2Book(L2Book),
    User(User),
    UserFills(UserFills),
    Candle(Candle),
    SubscriptionResponse,
    OrderUpdates(OrderUpdates),
    UserFundings(UserFundings),
    UserNonFundingLedgerUpdates(UserNonFundingLedgerUpdates),
    Notification(Notification),
    WebData2(WebData2),
    ActiveAssetCtx(ActiveAssetCtx),
    ActiveAssetData(ActiveAssetData),
    ActiveSpotAssetCtx(ActiveSpotAssetCtx),
    Bbo(Bbo),
    Pong,
}

#[derive(Serialize)]
pub(crate) struct SubscriptionSendData<'a> {
    method: &'static str,
    subscription: &'a serde_json::Value,
}

#[derive(Serialize)]
pub(crate) struct Ping {
    method: &'static str,
}

impl WsManager {
    const MAINNET_PING_AFTER_SECS: u64 = 60;
    const MAINNET_PONG_GRACE_SECS: u64 = 30;
    const TESTNET_PING_AFTER_SECS: u64 = 300;
    const TESTNET_PONG_GRACE_SECS: u64 = 60;
    const LIVENESS_CHECK_INTERVAL_SECS: u64 = 5;

    pub(crate) async fn new(url: String, reconnect: bool, base_url: BaseUrl) -> Result<WsManager> {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let base_instant = Instant::now();
        let last_rx = Arc::new(AtomicU64::new(0));
        let last_pong = Arc::new(AtomicU64::new(0));
        let last_ping = Arc::new(AtomicU64::new(0));
        let awaiting_pong = Arc::new(AtomicBool::new(false));
        let force_reconnect = Arc::new(AtomicBool::new(false));

        let (writer, mut reader) = Self::connect(&url).await?.split();
        let writer = Arc::new(Mutex::new(writer));

        let subscriptions_map: HashMap<String, Vec<SubscriptionData>> = HashMap::new();
        let subscriptions = Arc::new(Mutex::new(subscriptions_map));
        let subscriptions_copy = Arc::clone(&subscriptions);

        let liveness = Self::liveness_config(base_url);
        {
            let writer = writer.clone();
            let stop_flag = Arc::clone(&stop_flag);
            let last_rx = Arc::clone(&last_rx);
            let last_pong = Arc::clone(&last_pong);
            let last_ping = Arc::clone(&last_ping);
            let awaiting_pong = Arc::clone(&awaiting_pong);
            let force_reconnect = Arc::clone(&force_reconnect);
            let reader_fut = async move {
                while !stop_flag.load(Ordering::Relaxed) {
                    let mut should_reconnect = false;
                    let next = if let Some(cfg) = liveness {
                        time::timeout(cfg.check_interval, reader.next()).await
                    } else {
                        Ok(reader.next().await)
                    };
                    let now_ms = base_instant.elapsed().as_millis() as u64;

                    match next {
                        Ok(Some(Ok(data))) => {
                            last_rx.store(now_ms, Ordering::Relaxed);
                            if awaiting_pong.load(Ordering::Relaxed) {
                                awaiting_pong.store(false, Ordering::Relaxed);
                            }
                            match data {
                                protocol::Message::Text(text) => {
                                    match WsManager::parse_and_send_data(text, &subscriptions_copy)
                                        .await
                                    {
                                        Ok(message) => {
                                            if let Some(Message::Pong) = message {
                                                last_pong.store(now_ms, Ordering::Relaxed);
                                            }
                                        }
                                        Err(err) => {
                                            error!(
                                                "Error processing data received by WsManager reader: {err}"
                                            );
                                            should_reconnect = true;
                                        }
                                    }
                                }
                                protocol::Message::Close(_) => {
                                    warn!("WsManager received close frame");
                                    should_reconnect = true;
                                }
                                protocol::Message::Pong(_) => {
                                    last_pong.store(now_ms, Ordering::Relaxed);
                                }
                                protocol::Message::Binary(_) | protocol::Message::Ping(_) => {}
                                _ => {}
                            }
                        }
                        Ok(Some(Err(err))) => {
                            error!("WsManager reader error: {err}");
                            let error = Error::GenericReader(err.to_string());
                            if let Err(err) = WsManager::send_to_all_subscriptions(
                                &subscriptions_copy,
                                Message::HyperliquidError(error.to_string()),
                            )
                            .await
                            {
                                warn!("Error sending reader notification err={err}");
                            }
                            should_reconnect = true;
                        }
                        Ok(None) => {
                            warn!("WsManager disconnected");
                            should_reconnect = true;
                        }
                        Err(_) => {
                            if let Some(cfg) = liveness {
                                let last_ping_ms = last_ping.load(Ordering::Relaxed);
                                let last_rx_ms = last_rx.load(Ordering::Relaxed);
                                let last_pong_ms = last_pong.load(Ordering::Relaxed);
                                if awaiting_pong.load(Ordering::Relaxed)
                                    && now_ms.saturating_sub(last_ping_ms)
                                        >= cfg.pong_grace.as_millis() as u64
                                    && last_rx_ms <= last_ping_ms
                                    && last_pong_ms <= last_ping_ms
                                {
                                    warn!("WsManager pong timeout");
                                    should_reconnect = true;
                                }
                            }
                        }
                    }

                    if !should_reconnect && force_reconnect.swap(false, Ordering::Relaxed) {
                        should_reconnect = true;
                    }
                    if should_reconnect {
                        if let Err(err) = WsManager::send_to_all_subscriptions(
                            &subscriptions_copy,
                            Message::NoData,
                        )
                        .await
                        {
                            warn!("Error sending disconnection notification err={err}");
                        }
                        if reconnect {
                            // Always sleep for 1 second before attempting to reconnect so it does not spin during reconnecting. This could be enhanced with exponential backoff.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            info!("WsManager attempting to reconnect");
                            match Self::connect(&url).await {
                                Ok(ws) => {
                                    let (new_writer, new_reader) = ws.split();
                                    reader = new_reader;
                                    let mut writer_guard = writer.lock().await;
                                    *writer_guard = new_writer;
                                    for (identifier, v) in subscriptions_copy.lock().await.iter() {
                                        // TODO should these special keys be removed and instead use the simpler direct identifier mapping?
                                        if identifier.eq("userEvents")
                                            || identifier.eq("orderUpdates")
                                        {
                                            for subscription_data in v {
                                                if let Err(err) = Self::subscribe(
                                                    writer_guard.deref_mut(),
                                                    &subscription_data.id,
                                                )
                                                .await
                                                {
                                                    error!(
                                                        "Could not resubscribe {identifier}: {err}"
                                                    );
                                                }
                                            }
                                        } else if let Err(err) =
                                            Self::subscribe(writer_guard.deref_mut(), identifier)
                                                .await
                                        {
                                            error!("Could not resubscribe correctly {identifier}: {err}");
                                        }
                                    }
                                    info!("WsManager reconnect finished");
                                }
                                Err(err) => error!("Could not connect to websocket {err}"),
                            }
                        } else {
                            error!("WsManager reconnection disabled. Will not reconnect and exiting reader task.");
                            break;
                        }
                    }
                }
                warn!("ws message reader task stopped");
            };
            spawn(reader_fut);
        }

        if let Some(liveness) = liveness {
            let stop_flag = Arc::clone(&stop_flag);
            let writer = Arc::clone(&writer);
            let last_rx = Arc::clone(&last_rx);
            let last_ping = Arc::clone(&last_ping);
            let awaiting_pong = Arc::clone(&awaiting_pong);
            let force_reconnect = Arc::clone(&force_reconnect);
            let ping_fut = async move {
                while !stop_flag.load(Ordering::Relaxed) {
                    let now_ms = base_instant.elapsed().as_millis() as u64;
                    let last_rx_ms = last_rx.load(Ordering::Relaxed);
                    if !awaiting_pong.load(Ordering::Relaxed)
                        && now_ms.saturating_sub(last_rx_ms)
                            >= liveness.ping_after.as_millis() as u64
                    {
                        match serde_json::to_string(&Ping { method: "ping" }) {
                            Ok(payload) => {
                                let mut writer = writer.lock().await;
                                if let Err(err) =
                                    writer.send(protocol::Message::Text(payload)).await
                                {
                                    error!("Error pinging server: {err}");
                                    force_reconnect.store(true, Ordering::Relaxed);
                                } else {
                                    awaiting_pong.store(true, Ordering::Relaxed);
                                    last_ping.store(now_ms, Ordering::Relaxed);
                                }
                            }
                            Err(err) => error!("Error serializing ping message: {err}"),
                        }
                    }
                    time::sleep(liveness.check_interval).await;
                }
                warn!("ws ping task stopped");
            };
            spawn(ping_fut);
        }

        Ok(WsManager {
            stop_flag,
            writer,
            subscriptions,
            subscription_id: 0,
            subscription_identifiers: HashMap::new(),
        })
    }

    async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        Ok(connect_async(url)
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?
            .0)
    }

    fn liveness_config(base_url: BaseUrl) -> Option<LivenessConfig> {
        match base_url {
            BaseUrl::Testnet => Some(LivenessConfig {
                ping_after: Duration::from_secs(Self::TESTNET_PING_AFTER_SECS),
                pong_grace: Duration::from_secs(Self::TESTNET_PONG_GRACE_SECS),
                check_interval: Duration::from_secs(Self::LIVENESS_CHECK_INTERVAL_SECS),
            }),
            BaseUrl::Mainnet => Some(LivenessConfig {
                ping_after: Duration::from_secs(Self::MAINNET_PING_AFTER_SECS),
                pong_grace: Duration::from_secs(Self::MAINNET_PONG_GRACE_SECS),
                check_interval: Duration::from_secs(Self::LIVENESS_CHECK_INTERVAL_SECS),
            }),
            BaseUrl::Localhost => None,
        }
    }

    fn get_identifier(message: &Message) -> Result<String> {
        match message {
            Message::AllMids(_) => serde_json::to_string(&Subscription::AllMids)
                .map_err(|e| Error::JsonParse(e.to_string())),
            Message::User(_) => Ok("userEvents".to_string()),
            Message::UserFills(fills) => serde_json::to_string(&Subscription::UserFills {
                user: fills.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Trades(trades) => {
                if trades.data.is_empty() {
                    Ok(String::default())
                } else {
                    serde_json::to_string(&Subscription::Trades {
                        coin: trades.data[0].coin.clone(),
                    })
                    .map_err(|e| Error::JsonParse(e.to_string()))
                }
            }
            Message::L2Book(l2_book) => serde_json::to_string(&Subscription::L2Book {
                coin: l2_book.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Candle(candle) => serde_json::to_string(&Subscription::Candle {
                coin: candle.data.coin.clone(),
                interval: candle.data.interval.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::OrderUpdates(_) => Ok("orderUpdates".to_string()),
            Message::UserFundings(fundings) => serde_json::to_string(&Subscription::UserFundings {
                user: fundings.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::UserNonFundingLedgerUpdates(user_non_funding_ledger_updates) => {
                serde_json::to_string(&Subscription::UserNonFundingLedgerUpdates {
                    user: user_non_funding_ledger_updates.data.user,
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Notification(_) => Ok("notification".to_string()),
            Message::WebData2(web_data2) => serde_json::to_string(&Subscription::WebData2 {
                user: web_data2.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::ActiveAssetCtx(active_asset_ctx) => {
                serde_json::to_string(&Subscription::ActiveAssetCtx {
                    coin: active_asset_ctx.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::ActiveSpotAssetCtx(active_spot_asset_ctx) => {
                serde_json::to_string(&Subscription::ActiveAssetCtx {
                    coin: active_spot_asset_ctx.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::ActiveAssetData(active_asset_data) => {
                serde_json::to_string(&Subscription::ActiveAssetData {
                    user: active_asset_data.data.user,
                    coin: active_asset_data.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Bbo(bbo) => serde_json::to_string(&Subscription::Bbo {
                coin: bbo.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::SubscriptionResponse | Message::Pong => Ok(String::default()),
            Message::NoData => Ok("".to_string()),
            Message::HyperliquidError(err) => Ok(format!("hyperliquid error: {err:?}")),
        }
    }

    async fn parse_and_send_data(
        data: String,
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    ) -> Result<Option<Message>> {
        if !data.starts_with('{') {
            return Ok(None);
        }
        let message =
            serde_json::from_str::<Message>(&data).map_err(|e| Error::JsonParse(e.to_string()))?;
        let identifier = WsManager::get_identifier(&message)?;
        if identifier.is_empty() {
            return Ok(Some(message));
        }

        let mut subscriptions = subscriptions.lock().await;
        let mut res = Ok(());
        if let Some(subscription_datas) = subscriptions.get_mut(&identifier) {
            for subscription_data in subscription_datas {
                if let Err(e) = subscription_data
                    .sending_channel
                    .send(message.clone())
                    .map_err(|e| Error::WsSend(e.to_string()))
                {
                    res = Err(e);
                }
            }
        }
        res.map(|_| Some(message))
    }

    async fn send_to_all_subscriptions(
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
        message: Message,
    ) -> Result<()> {
        let mut subscriptions = subscriptions.lock().await;
        let mut res = Ok(());
        for subscription_datas in subscriptions.values_mut() {
            for subscription_data in subscription_datas {
                if let Err(e) = subscription_data
                    .sending_channel
                    .send(message.clone())
                    .map_err(|e| Error::WsSend(e.to_string()))
                {
                    res = Err(e);
                }
            }
        }
        res
    }

    async fn send_subscription_data(
        method: &'static str,
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(&SubscriptionSendData {
            method,
            subscription: &serde_json::from_str::<serde_json::Value>(identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?,
        })
        .map_err(|e| Error::JsonParse(e.to_string()))?;

        writer
            .send(protocol::Message::Text(payload))
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        Self::send_subscription_data("subscribe", writer, identifier).await
    }

    async fn unsubscribe(
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        Self::send_subscription_data("unsubscribe", writer, identifier).await
    }

    pub(crate) async fn add_subscription(
        &mut self,
        identifier: String,
        sending_channel: UnboundedSender<Message>,
    ) -> Result<u32> {
        let mut subscriptions = self.subscriptions.lock().await;

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };
        let subscriptions = subscriptions
            .entry(identifier_entry.clone())
            .or_insert(Vec::new());

        if !subscriptions.is_empty() && identifier_entry.eq("userEvents") {
            return Err(Error::UserEvents);
        }

        if subscriptions.is_empty() {
            Self::subscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }

        let subscription_id = self.subscription_id;
        self.subscription_identifiers
            .insert(subscription_id, identifier.clone());
        subscriptions.push(SubscriptionData {
            sending_channel,
            subscription_id,
            id: identifier,
        });

        self.subscription_id += 1;
        Ok(subscription_id)
    }

    pub(crate) async fn remove_subscription(&mut self, subscription_id: u32) -> Result<()> {
        let identifier = self
            .subscription_identifiers
            .get(&subscription_id)
            .ok_or(Error::SubscriptionNotFound)?
            .clone();

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };

        self.subscription_identifiers.remove(&subscription_id);

        let mut subscriptions = self.subscriptions.lock().await;

        let subscriptions = subscriptions
            .get_mut(&identifier_entry)
            .ok_or(Error::SubscriptionNotFound)?;
        let index = subscriptions
            .iter()
            .position(|subscription_data| subscription_data.subscription_id == subscription_id)
            .ok_or(Error::SubscriptionNotFound)?;
        subscriptions.remove(index);

        if subscriptions.is_empty() {
            Self::unsubscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }
        Ok(())
    }
}

impl Drop for WsManager {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }
}
