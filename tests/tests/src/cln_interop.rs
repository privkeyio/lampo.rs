//! Interop between Lampo and Core Lightning on a BLAKE2b chain.
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use lampo_common::error;
use lampo_common::handler::Handler;
use lampo_common::json;
use lampo_common::model::{request, response};
use lampo_testing::LampoTesting;

use crate::init;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

struct Btc {
    port: String,
    user: String,
    pass: String,
}

impl Btc {
    fn cli(&self, args: &[&str]) -> String {
        let out = Command::new(env("INTEROP_BITCOIN_CLI"))
            .arg("-regtest")
            .arg("-rpcconnect=127.0.0.1")
            .arg(format!("-rpcport={}", self.port))
            .arg(format!("-rpcuser={}", self.user))
            .arg(format!("-rpcpassword={}", self.pass))
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "bitcoin-cli {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    }

    fn mine(&self, blocks: u64) {
        let addr = self.cli(&["-rpcwallet=default", "getnewaddress"]);
        self.cli(&["generatetoaddress", &blocks.to_string(), &addr]);
    }
}

struct Cln {
    dir: PathBuf,
    child: Child,
    port: u16,
    id: String,
}

impl Drop for Cln {
    fn drop(&mut self) {
        let _ = self.try_cli(&["stop"]);
        std::thread::sleep(Duration::from_secs(2));
        let _ = self.child.kill();
    }
}

impl Cln {
    fn try_cli(&self, args: &[&str]) -> Result<json::Value, String> {
        let out = Command::new(env("INTEROP_LIGHTNING_CLI"))
            .arg(format!("--lightning-dir={}", self.dir.display()))
            .arg("--network=regtest")
            .arg("--notifications=none")
            .args(args)
            .output()
            .map_err(|e| e.to_string())?;
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        if !out.status.success() {
            return Err(format!("{stdout} {}", String::from_utf8_lossy(&out.stderr)));
        }
        json::from_str(&stdout).map_err(|e| format!("{e}: {stdout}"))
    }

    fn cli(&self, args: &[&str]) -> json::Value {
        self.try_cli(args)
            .unwrap_or_else(|e| panic!("lightning-cli {args:?}: {e}"))
    }

    async fn start(btc: &Btc) -> Cln {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = PathBuf::from(env("INTEROP_DIR")).join(format!("cln-{port}"));
        std::fs::create_dir_all(&dir).unwrap();
        let child = Command::new(env("INTEROP_LIGHTNINGD"))
            .arg("--network=regtest")
            .arg(format!("--lightning-dir={}", dir.display()))
            .arg(format!("--addr=127.0.0.1:{port}"))
            .arg(format!("--bitcoin-cli={}", env("INTEROP_BITCOIN_CLI")))
            .arg("--bitcoin-rpcconnect=127.0.0.1")
            .arg(format!("--bitcoin-rpcport={}", btc.port))
            .arg(format!("--bitcoin-rpcuser={}", btc.user))
            .arg(format!("--bitcoin-rpcpassword={}", btc.pass))
            .arg(format!("--log-file={}/log", dir.display()))
            .arg("--log-level=debug")
            .arg("--developer")
            .arg("--dev-bitcoind-poll=1")
            .arg("--alias=CLN")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(dir.join("stderr")).unwrap())
            .spawn()
            .unwrap();
        let mut cln = Cln {
            dir,
            child,
            port,
            id: String::new(),
        };
        for _ in 0..120 {
            if let Ok(info) = cln.try_cli(&["getinfo"]) {
                cln.id = info["id"].as_str().unwrap().to_owned();
                log::info!(target: "interop", "cln getinfo {info}");
                return cln;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!(
            "lightningd did not start, see {}/log and {}/stderr",
            cln.dir.display(),
            cln.dir.display()
        );
    }

    async fn wait_synced(&self, btc: &Btc) {
        let height: u64 = btc.cli(&["getblockcount"]).parse().unwrap();
        for _ in 0..240 {
            let info = self.cli(&["getinfo"]);
            if info["blockheight"].as_u64() == Some(height) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("cln did not sync to {height}");
    }

    async fn wait_channel_state(&self, channel_id: &str, state: &str) {
        for _ in 0..240 {
            let chans = self.cli(&["listpeerchannels"]);
            if chans["channels"].as_array().unwrap().iter().any(|c| {
                c["channel_id"].as_str() == Some(channel_id) && c["state"].as_str() == Some(state)
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!(
            "channel {channel_id} never reached {state}: {}",
            self.cli(&["listpeerchannels"])
        );
    }

    // Mines until the close confirms, since the closing tx may reach the mempool after close returns.
    async fn wait_onchain(&self, btc: &Btc, channel_id: &str) {
        for _ in 0..120 {
            btc.mine(1);
            let chans = self.cli(&["listpeerchannels"]);
            if chans["channels"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["channel_id"].as_str() == Some(channel_id) && c["state"] == "ONCHAIN")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!(
            "channel {channel_id} never closed onchain: {}",
            self.cli(&["listpeerchannels"])
        );
    }
}

async fn lampo_channels(node: &LampoTesting) -> response::Channels {
    node.lampod()
        .call("channels", json::json!({}))
        .await
        .unwrap()
}

async fn wait_lampo_ready(node: &LampoTesting, channel_id: &str) {
    for _ in 0..240 {
        let chans = lampo_channels(node).await;
        if chans
            .channels
            .iter()
            .any(|c| c.channel_id == channel_id && c.ready)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("lampo channel {channel_id} never became ready");
}

async fn lampo_pay(node: &LampoTesting, invoice: String) -> response::PayResult {
    node.lampod()
        .call(
            "pay",
            request::Pay {
                invoice_str: invoice,
                amount: None,
                max_fee_msat: None,
                bolt12: None,
                timeout: Default::default(),
                timeout_secs: None,
            },
        )
        .await
        .unwrap()
}

/// Lampo against a Core Lightning node on a BLAKE2b regtest chain. Needs the
/// `INTEROP_*` binaries and directory, so CI runs it with `--ignored`.
#[tokio_test_shutdown_timeout::test(15)]
#[ignore = "needs Core Lightning, see .github/workflows/blake2b.yml"]
pub async fn cln_interop() -> error::Result<()> {
    init();
    let node = Arc::new(LampoTesting::tmp().await?);
    let (user, pass) = node.btc.cookie();
    let port = node.btc.rpc_url().rsplit(':').next().unwrap().to_owned();
    let btc = Btc { port, user, pass };
    let wallets = btc.cli(&["listwallets"]);
    log::info!(target: "interop", "bitcoind wallets {wallets}");
    if !wallets.contains("\"default\"") {
        btc.cli(&["createwallet", "default"]);
    }
    btc.mine(101);

    let cln = Cln::start(&btc).await;
    cln.wait_synced(&btc).await;
    let cln_port = cln.port as u64;

    // Lampo opens a channel to CLN.
    let _: response::Connect = node
        .lampod()
        .call(
            "connect",
            request::Connect {
                node_id: cln.id.clone(),
                addr: "127.0.0.1".to_owned(),
                port: cln_port,
            },
        )
        .await?;
    let opened: json::Value = node
        .lampod()
        .call(
            "fundchannel",
            request::OpenChannel {
                node_id: cln.id.clone(),
                amount: 1_000_000,
                public: true,
                port: None,
                addr: None,
                push_msat: None,
                sat_per_vbyte: None,
            },
        )
        .await?;
    log::info!(target: "interop", "lampo fundchannel {opened}");
    let mut chan1 = String::new();
    for _ in 0..60 {
        let chans = cln.cli(&["listpeerchannels"]);
        if let Some(c) = chans["channels"].as_array().unwrap().first() {
            chan1 = c["channel_id"].as_str().unwrap().to_owned();
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(!chan1.is_empty(), "cln never saw lampo's channel");
    btc.mine(10);
    cln.wait_channel_state(&chan1, "CHANNELD_NORMAL").await;
    wait_lampo_ready(&node, &chan1).await;
    println!("INTEROP: lampo -> cln channel {chan1} is ready on both sides");

    // Lampo pays a CLN bolt11 invoice.
    let inv = cln.cli(&["invoice", "100000000msat", "interop-1", "lampo pays cln"]);
    let pay = lampo_pay(&node, inv["bolt11"].as_str().unwrap().to_owned()).await;
    assert!(pay.payment_preimage.is_some(), "{pay:?}");
    let listed = cln.cli(&["listinvoices", "interop-1"]);
    assert_eq!(listed["invoices"][0]["status"], "paid", "{listed}");
    println!("INTEROP: lampo paid a cln bolt11 invoice");

    // Lampo pays a CLN offer.
    let offer = cln.cli(&["offer", "50000000msat", "cln offer"]);
    let pay = lampo_pay(&node, offer["bolt12"].as_str().unwrap().to_owned()).await;
    assert!(pay.payment_preimage.is_some(), "{pay:?}");
    let offer_id = offer["offer_id"].as_str().unwrap();
    let mut paid = false;
    for _ in 0..40 {
        let listed = cln.cli(&["listinvoices", "-k", &format!("offer_id={offer_id}")]);
        if listed["invoices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["status"] == "paid")
        {
            paid = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(paid, "cln offer invoice not marked paid");
    println!("INTEROP: lampo paid a cln bolt12 offer");

    // CLN pays a Lampo bolt11 invoice.
    let invoice: response::Invoice = node
        .lampod()
        .call(
            "invoice",
            request::GenerateInvoice {
                description: "cln pays lampo".to_owned(),
                amount_msat: Some(20_000_000),
                expiring_in: None,
            },
        )
        .await?;
    let paid = cln.cli(&["pay", &invoice.bolt11]);
    assert_eq!(paid["status"], "complete", "{paid}");
    println!("INTEROP: cln paid a lampo bolt11 invoice");

    // CLN pays a Lampo offer.
    let offer: response::Offer = node
        .lampod()
        .call(
            "offer",
            request::GenerateOffer {
                description: Some("lampo offer".to_owned()),
                amount_msat: Some(10_000_000),
                currency: None,
                currency_amount: None,
            },
        )
        .await?;
    let fetched = cln.cli(&["fetchinvoice", &offer.bolt12]);
    let paid = cln.cli(&["pay", fetched["invoice"].as_str().unwrap()]);
    assert_eq!(paid["status"], "complete", "{paid}");
    println!("INTEROP: cln paid a lampo bolt12 offer");

    // Splicing is not offered, so Lampo must refuse it cleanly.
    let splice = node
        .lampod()
        .call::<_, json::Value>(
            "splicein",
            request::SpliceIn {
                node_id: cln.id.clone(),
                channel_id: None,
                amount: 100_000,
                sat_per_vbyte: None,
            },
        )
        .await;
    let err = format!("{:?}", splice.expect_err("splice must be refused"));
    assert!(err.to_lowercase().contains("splic"), "{err}");
    println!("INTEROP: lampo refused the splice: {err}");
    cln.wait_channel_state(&chan1, "CHANNELD_NORMAL").await;

    // CLN opens a channel to Lampo from a non-coinbase output.
    let addr = cln.cli(&["newaddr", "p2tr"]);
    let addr = addr["p2tr"].as_str().unwrap().to_owned();
    btc.cli(&["-rpcwallet=default", "sendtoaddress", &addr, "0.02"]);
    btc.mine(1);
    cln.wait_synced(&btc).await;
    let mut confirmed = false;
    for _ in 0..60 {
        let funds = cln.cli(&["listfunds"]);
        if funds["outputs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["status"] == "confirmed")
        {
            confirmed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(confirmed, "cln never saw its funding output confirm");
    cln.cli(&[
        "connect",
        &format!("{}@127.0.0.1:{}", node.info.node_id, node.port),
    ]);
    let funded = cln.cli(&["fundchannel", &node.info.node_id, "500000"]);
    let chan2 = funded["channel_id"].as_str().unwrap().to_owned();
    btc.mine(10);
    cln.wait_channel_state(&chan2, "CHANNELD_NORMAL").await;
    wait_lampo_ready(&node, &chan2).await;
    println!("INTEROP: cln -> lampo channel {chan2} is ready on both sides");

    // Lampo closes its channel cooperatively, so CLN's channel is the only path left.
    let _: json::Value = node
        .lampod()
        .call(
            "close",
            request::CloseChannel {
                node_id: cln.id.clone(),
                channel_id: Some(chan1.clone()),
                force: false,
            },
        )
        .await?;
    cln.wait_onchain(&btc, &chan1).await;
    println!("INTEROP: lampo closed {chan1} cooperatively");

    let invoice: response::Invoice = node
        .lampod()
        .call(
            "invoice",
            request::GenerateInvoice {
                description: "over cln's channel".to_owned(),
                amount_msat: Some(40_000_000),
                expiring_in: None,
            },
        )
        .await?;
    let paid = cln.try_cli(&["pay", &invoice.bolt11]);
    if paid.is_err() {
        println!(
            "INTEROP: channels at pay failure {}",
            cln.cli(&["listpeerchannels"])
        );
    }
    let paid = paid.unwrap();
    assert_eq!(paid["status"], "complete", "{paid}");
    let chans = cln.cli(&["listpeerchannels"]);
    let chan2_out = chans["channels"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["channel_id"].as_str() == Some(chan2.as_str()))
        .and_then(|c| c["out_fulfilled_msat"].as_u64())
        .unwrap_or(0);
    assert!(chan2_out >= 40_000_000, "{chans}");
    println!("INTEROP: cln paid lampo 40000 sat over {chan2}");

    let closed = cln.cli(&["close", &chan2]);
    assert_eq!(closed["type"], "mutual", "{closed}");
    cln.wait_onchain(&btc, &chan2).await;
    println!("INTEROP: both channels closed cooperatively");
    let log = std::fs::read_to_string(cln.dir.join("log")).unwrap_or_default();
    let broken: Vec<&str> = log.lines().filter(|l| l.contains("**BROKEN**")).collect();
    assert!(broken.is_empty(), "cln BROKEN log lines: {broken:?}");
    Ok(())
}
