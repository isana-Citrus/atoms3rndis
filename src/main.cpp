/*
 * AtomS3 RNDIS Host – Wi-Fi ルーター
 *
 * AtomS3 が USB ホストとして動作し、接続した RNDIS Brainデバイスに
 * DHCP で IP を配布しつつ、WiFi 経由でインターネットに接続させる。
 *
 * LAN : USB RNDIS or USB NCM – 192.168.37.1  (DHCP サーバ)
 * WLAN: WiFi STA             – DHCP で動的 IP 取得
 * NAT : ip_napt_enable で LAN→WAN を NAT
 *
 * 物理接続:
 *   AtomS3 USB-C → (USB-C to Micro-USB B ケーブル) → Brain の Micro-USB 端子
 */

/*
 * ── 大まかなフロー ──────────────────────────────────────────────────────────
 *
 *  [setup()]
 *   1. WiFi (WAN側) に接続し、上流 DNS アドレスを取得する
 *   2. esp-netif で USB LAN インターフェース (192.168.37.1) を生成し
 *      DHCP サーバを起動。DHCP でクライアントに DNS として自分自身を通知する
 *   3. セマフォ・キュー・TX バッファを初期化する
 *   4. USB ホストライブラリを起動する
 *   5. 以下 3 タスクを起動する:
 *        usb_host_lib_task  … USB ホスト低レベルイベントを処理するデーモン
 *        rndis_client_task  … USB デバイスの接続/切断イベントを処理する
 *        rndis_setup_task   … RNDIS/NCM ハンドシェイクと受信開始を行う
 *
 *  [デバイス接続時 (client_event_cb → rndis_setup_task)]
 *   1. usb_host_device_open でデバイスを開く
 *   2. parse_config_desc でディスクリプタを解析し、エンドポイントを特定する
 *   3. クラスコードから RNDIS か CDC-NCM かリンクモードを判定する
 *   4. 通信用・データ用インターフェースを Claim し、SET_INTERFACE を発行する
 *   5. RNDIS_INITIALIZE / NCM_GET_NTB_PARAMETERS などの初期化メッセージを交換する
 *   6. RNDIS の場合はパケットフィルタを設定する
 *   7. Bulk IN (受信) と Interrupt IN (状態通知) の受信を開始する
 *   8. esp-netif を "接続済み" 状態に遷移させ、ip_napt で NAT を有効化する
 *   9. DNS リレータスクを初回のみ起動する
 *
 *  [データ受信フロー (rx_callback)]
 *   RNDIS モード  : RNDIS パケットヘッダ (44 バイト) を除去 → deliver_rx_frame
 *   CDC-NCM モード: NTB16 ブロックをパースして各イーサネットフレームを取り出す
 *                   → deliver_rx_frame
 *   deliver_rx_frame → esp_netif_receive → lwIP スタック → DHCP/DNS/NAT 処理
 *
 *  [データ送信フロー (usb_driver_transmit)]
 *   lwIP → esp_netif 送信コールバック → usb_driver_transmit
 *   RNDIS モード  : RNDIS パケットヘッダを先頭に付けて Bulk OUT 転送
 *   CDC-NCM モード: NTB16 ヘッダ・NDP16 を構築してデータグラムを格納し Bulk OUT 転送
 *
 *  [DNS リレー (dns_relay_task)]
 *   UDP ポート 53 で listen → クライアントの DNS クエリを Wi-Fi 側の上流 DNS へ転送
 *   → 上流から返答を受け取りクライアントへ返す
 *
 *  [loop()]
 *   HTTP クライアントが接続してきたら handle_http_client でステータスとログを
 *   HTML ページとして返す（2 秒ごとに自動リロード）
 */

#include <M5Unified.h>
#include <WiFi.h>
#include <WiFiServer.h>
#include "secret.h"

// ESP-IDF USB ホストライブラリ
#include "usb/usb_host.h"

// ESP-IDF ネットワークスタック
#include "esp_netif.h"
#include "lwip/lwip_napt.h"
#include "lwip/ip4_addr.h"
#include "lwip/dns.h"
#include "lwip/sockets.h"
#include "lwip/inet.h"

// FreeRTOS
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

// ─── RNDIS メッセージ型 ───────────────────────────────────────────────────────
// ホスト→デバイス方向のコントロールメッセージ種別
#define RNDIS_PACKET_MSG        0x00000001u  // イーサネットフレームを運ぶデータパケット
#define RNDIS_INITIALIZE_MSG    0x00000002u  // 初期化要求（ホストからデバイスへ）
#define RNDIS_QUERY_MSG         0x00000004u  // OID 情報取得要求
#define RNDIS_SET_MSG           0x00000005u  // OID 情報設定要求
// デバイス→ホスト方向の応答メッセージ種別 (上位ビット=1)
#define RNDIS_INITIALIZE_CMPLT  0x80000002u  // 初期化完了応答
#define RNDIS_QUERY_CMPLT       0x80000004u  // OID 取得完了応答
#define RNDIS_SET_CMPLT         0x80000005u  // OID 設定完了応答
#define RNDIS_STATUS_SUCCESS    0x00000000u  // 処理成功を示すステータス値

// OID (Object Identifier) – NDIS デバイスの属性・設定を表す識別子
#define OID_GEN_SUPPORTED_LIST          0x00010101u  // サポートする OID 一覧
#define OID_GEN_HARDWARE_STATUS         0x00010102u  // ハードウェア状態
#define OID_GEN_MEDIA_SUPPORTED         0x00010103u  // サポートするメディア種別
#define OID_GEN_MEDIA_IN_USE            0x00010104u  // 現在使用中のメディア種別
#define OID_GEN_MAXIMUM_FRAME_SIZE      0x00010106u  // 最大フレームサイズ
#define OID_GEN_MAXIMUM_TOTAL_SIZE      0x00010111u  // 最大転送サイズ
#define OID_802_3_PERMANENT_ADDRESS     0x01010101u  // デバイスの MAC アドレス取得
#define OID_GEN_CURRENT_PACKET_FILTER   0x0001010Eu  // パケットフィルタ設定
#define NDIS_PACKET_FILTER_ALL          0x0000000Fu  // すべてのパケットを受信するフィルタ値

// ─── RNDIS メッセージ構造体 ─────────────────────────────────────────────────
// USB コントロール転送で送受信するバイナリメッセージのレイアウトを定義する
// #pragma pack(push, 1) でパディングを除去し、仕様通りのオフセットを保証する

#pragma pack(push, 1)

// RNDIS 初期化要求メッセージ (ホスト → デバイス)
typedef struct {
    uint32_t MessageType, MessageLength, RequestId;   // 共通ヘッダ: メッセージ種別・長さ・要求ID
    uint32_t MajorVersion, MinorVersion, MaxTransferSize; // RNDIS バージョンと最大転送サイズ
} rndis_init_msg_t;

// RNDIS 初期化完了応答 (デバイス → ホスト)
typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Status; // 共通ヘッダ + 結果ステータス
    uint32_t MajorVersion, MinorVersion, DeviceFlags, Medium; // デバイス属性
    uint32_t MaxPacketsPerTransfer, MaxTransferSize, PacketAlignmentFactor; // 転送パラメータ
    uint32_t AFListOffset, AFListSize;  // 追加フィルタリスト (通常 0)
} rndis_init_cmplt_t;

// OID 情報取得要求 (ホスト → デバイス)
typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Oid; // 取得する OID を指定
    uint32_t InformationBufferLength, InformationBufferOffset, Reserved;
} rndis_query_msg_t;

// OID 情報取得完了応答 (デバイス → ホスト)
typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Status;
    uint32_t InformationBufferLength, InformationBufferOffset; // データのオフセットと長さ
} rndis_query_cmplt_t;

// OID 情報設定要求 (ホスト → デバイス)
typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Oid;
    uint32_t InformationBufferLength, InformationBufferOffset, Reserved;
} rndis_set_msg_t;

// OID 情報設定完了応答 (デバイス → ホスト)
typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Status;
} rndis_set_cmplt_t;

// RNDIS データパケットヘッダ – イーサネットフレームをラップする
typedef struct {
    uint32_t MessageType, MessageLength; // 0x00000001 固定 / ヘッダ+ペイロード合計長
    uint32_t DataOffset, DataLength;     // ペイロード先頭オフセット(RequestId基準) と長さ
    uint32_t OOBDataOffset, OOBDataLength, NumOOBDataElements; // Out-of-band データ (通常 0)
    uint32_t PerPacketInfoOffset, PerPacketInfoLength; // パーパケット情報 (通常 0)
    uint32_t VcHandle, Reserved;
} rndis_packet_msg_t;

// CDC-NCM NTB (Network Transfer Block) パラメータ – GET_NTB_PARAMETERS で取得
typedef struct {
    uint16_t wLength;                   // この構造体のサイズ
    uint16_t bmNtbFormatsSupported;     // ビット0=NTB16対応, ビット1=NTB32対応
    uint32_t dwNtbInMaxSize;            // デバイス → ホスト方向の最大 NTB サイズ
    uint16_t wNdpInDivisor;             // ペイロードアライメント除数 (IN)
    uint16_t wNdpInPayloadRemainder;    // ペイロードアライメント余り (IN)
    uint16_t wNdpInAlignment;           // NDP 先頭のアライメント (IN)
    uint16_t reserved0;
    uint32_t dwNtbOutMaxSize;           // ホスト → デバイス方向の最大 NTB サイズ
    uint16_t wNdpOutDivisor;            // ペイロードアライメント除数 (OUT)
    uint16_t wNdpOutPayloadRemainder;   // ペイロードアライメント余り (OUT)
    uint16_t wNdpOutAlignment;          // NDP 先頭のアライメント (OUT)
    uint16_t reserved1;
} cdc_ncm_ntb_parameters_t;

// CDC-NCM NTB16 ヘッダ – NTB ブロック先頭に配置される固定ヘッダ
typedef struct {
    uint32_t dwSignature;   // 0x484D434E ("NCMH") で正当性を確認
    uint16_t wHeaderLength; // このヘッダのバイト数
    uint16_t wSequence;     // シーケンス番号 (デバッグ用)
    uint16_t wBlockLength;  // NTB 全体のバイト数
    uint16_t wNdpIndex;     // 最初の NDP16 へのオフセット
} cdc_ncm_nth16_t;

// CDC-NCM NDP16 ヘッダ – データグラムポインタテーブルのヘッダ
typedef struct {
    uint32_t dwSignature;    // 0x304D434E ("NCM0") で正当性を確認
    uint16_t wLength;        // NDP ヘッダ + DPE 配列の合計バイト数
    uint16_t wNextNdpIndex;  // 次の NDP16 へのオフセット (0 = 終端)
} cdc_ncm_ndp16_t;

// CDC-NCM DPE16 – 各データグラム (イーサネットフレーム) の位置と長さ
typedef struct {
    uint16_t wDatagramIndex;  // NTB 先頭からのフレーム開始オフセット
    uint16_t wDatagramLength; // フレームのバイト数 (0 = 終端エントリ)
} cdc_ncm_dpe16_t;

#pragma pack(pop)

// DataOffset=36 → ヘッダ先頭から 44 バイト後にデータ (8 + 36)
#define RNDIS_PACKET_HEADER_SIZE  44u
#define RNDIS_DATA_OFFSET         36u
#define BULK_BUF_SIZE             2048u
#define CDC_NCM_GET_NTB_PARAMETERS 0x80u
#define CDC_NCM_SET_NTB_FORMAT     0x84u
#define CDC_NCM_SET_NTB_INPUT_SIZE 0x86u
#define CDC_NCM_NTH16_SIGNATURE    0x484D434Eu
#define CDC_NCM_NDP16_SIGNATURE    0x304D434Eu

// ─── グローバル状態 ───────────────────────────────────────────────────────────

static WiFiServer server(80);          // HTTP 管理画面サーバ (ポート 80)
static IPAddress s_local_ip;           // WiFi 側で取得したローカル IP
static bool s_wifi_connected = false;  // WiFi 接続状態フラグ
static bool s_http_rndis_only = true;  // HTTP アクセスを RNDIS サブネットのみに制限するか
static IPAddress s_upstream_dns_ip;    // WiFi から取得した上流 DNS サーバ IP
static volatile uint32_t s_dns_query_count = 0;    // DNS リレー: クエリ受信回数
static volatile uint32_t s_dns_reply_count = 0;    // DNS リレー: 応答返送回数
static volatile uint32_t s_dns_timeout_count = 0;  // DNS リレー: 上流タイムアウト回数
static bool s_dns_task_started = false;             // DNS リレータスクが起動済みかフラグ
static usb_transfer_status_t s_last_ctrl_status = USB_TRANSFER_STATUS_COMPLETED; // 直前のコントロール転送結果
static uint8_t s_ctrl_out_bm_request_type = 0x21;  // コントロール OUT の bmRequestType (動的に変更)
static uint8_t s_ctrl_in_bm_request_type  = 0xA1;  // コントロール IN の bmRequestType (動的に変更)
static uint16_t s_ctrl_w_index = 0;                 // コントロール転送の wIndex (インターフェース番号)

// ログバッファ
#define LOG_SIZE 8192
static char s_log_buffer[LOG_SIZE] = "";
static int s_log_pos = 0;

static void log_append(const char *fmt, ...) {
    if (s_log_pos >= LOG_SIZE - 200) return;
    va_list args;
    va_start(args, fmt);
    s_log_pos += vsnprintf(s_log_buffer + s_log_pos, LOG_SIZE - s_log_pos, fmt, args);
    va_end(args);
    s_log_pos += snprintf(s_log_buffer + s_log_pos, LOG_SIZE - s_log_pos, "\n");
}

static usb_host_client_handle_t s_client_hdl = NULL; // USB ホストクライアントハンドル
static usb_device_handle_t      s_dev_hdl    = NULL; // 接続中デバイスのハンドル
static uint8_t  s_comm_itf_num = 0xFF;  // コントロール用インターフェース番号 (0xFF=未検出)
static uint8_t  s_data_itf_num = 0xFF;  // データ用インターフェース番号 (0xFF=未検出)
static uint8_t  s_comm_itf_alt = 0;     // コントロールインターフェースの Alternate Setting
static uint8_t  s_data_itf_alt = 0;     // データインターフェースの Alternate Setting
static uint8_t  s_ep_out       = 0;     // Bulk OUT エンドポイントアドレス
static uint8_t  s_ep_in        = 0;     // Bulk IN エンドポイントアドレス
static uint16_t s_ep_out_mps   = 0;     // Bulk OUT の最大パケットサイズ
static uint16_t s_ep_in_mps    = 0;     // Bulk IN の最大パケットサイズ
static uint32_t s_req_id       = 1;     // RNDIS メッセージの要求ID (送るたびにインクリメント)
static uint8_t  s_comm_class   = 0;     // コントロールインターフェースのクラスコード
static uint8_t  s_comm_subclass= 0;     // コントロールインターフェースのサブクラスコード
static uint8_t  s_comm_proto   = 0;     // コントロールインターフェースのプロトコルコード
static uint8_t  s_data_class   = 0;     // データインターフェースのクラスコード
static uint8_t  s_data_subclass= 0;     // データインターフェースのサブクラスコード
static uint8_t  s_data_proto   = 0;     // データインターフェースのプロトコルコード
static cdc_ncm_ntb_parameters_t s_ncm_params = {}; // CDC-NCM 用の NTB パラメータ
static uint16_t s_ncm_tx_sequence = 0;  // CDC-NCM 送信 NTB のシーケンス番号
static uint32_t s_rx_buf_size = BULK_BUF_SIZE; // Bulk IN 受信バッファサイズ

// USB リンクモード – デバイスのディスクリプタ解析後に決定される
typedef enum {
    USB_LINK_MODE_RNDIS,    // Windows RNDIS プロトコル (Brain のデフォルト)
    USB_LINK_MODE_CDC_NCM,  // CDC NCM プロトコル (brainux 等の Linux 系カーネル)
} usb_link_mode_t;
static usb_link_mode_t s_link_mode = USB_LINK_MODE_RNDIS; // 接続時に自動判定

static esp_netif_t *s_usb_netif = nullptr;   // USB LAN 側の esp-netif インスタンス
static esp_netif_ip_info_t s_usb_ip;         // USB LAN 側の固定 IP 情報 (192.168.37.1)

// lwIP タスクコンテキストで NAPT を有効化するコールバック
// esp_netif_tcpip_exec 経由で呼ぶことで lwIP 内部から安全にNAPTを設定できる
static esp_err_t enable_napt_cb(void *) {
    ip_napt_enable(s_usb_ip.ip.addr, 1); // USB LAN インターフェースで NAT を有効化
    return ESP_OK;
}

// RNDIS/NCM 接続状態マシン
typedef enum {
    RNDIS_STATE_IDLE,        // デバイス未接続
    RNDIS_STATE_CONNECTED,   // デバイスを開いてインターフェースを Claim した状態
    RNDIS_STATE_INITIALIZED, // RNDIS/NCM 初期化メッセージ交換完了
    RNDIS_STATE_READY,       // Bulk IN 受信中・データ転送可能な状態
    RNDIS_STATE_ERROR,       // 初期化失敗
} rndis_state_t;
static volatile rndis_state_t s_rndis_state = RNDIS_STATE_IDLE;

// コントロール転送完了通知用セマフォ (コントロール転送はコールバックで完了を通知)
static SemaphoreHandle_t s_ctrl_sem = NULL;
// TX 完了通知用セマフォ (1パケットずつ直列送信のため完了まで次を待つ)
static SemaphoreHandle_t s_tx_sem   = NULL;
// Bulk IN/OUT 転送バッファ
static usb_transfer_t *s_rx_xfer = NULL; // Bulk IN 受信用の転送オブジェクト
static usb_transfer_t *s_tx_xfer = NULL; // Bulk OUT 送信用の転送オブジェクト
// Interrupt IN 転送バッファ（RNDIS 状態通知）
static usb_transfer_t *s_intr_xfer = NULL;
static uint8_t s_ep_intr = 0;  // Interrupt IN エンドポイントアドレス
// デバイス接続通知キュー（アドレスを event callback → setup task に渡す）
static QueueHandle_t s_dev_queue = NULL;

// ─── DNS リレータスク ────────────────────────────────────────────────────────
// RNDIS クライアントから UDP/53 に届いた DNS クエリを Wi-Fi 側の上流 DNS へ転送し、
// 返答を元のクライアントへ返すシンプルなリレー。
// NAT だけでも動く場合があるが、RNDIS デバイスが AtomS3 を DNS と認識している場合に必要。
static void dns_relay_task(void *arg) {
    (void)arg;

    const int kDnsPort = 53;
    uint8_t *query_buf = (uint8_t *)malloc(1232); // RFC 5625 推奨の DNS メッセージ最大サイズ
    uint8_t *reply_buf = (uint8_t *)malloc(1232);
    if (!query_buf || !reply_buf) {
        log_append("DNS relay: buffer alloc failed");
        free(query_buf);
        free(reply_buf);
        vTaskDelete(nullptr);
        return;
    }

    int listen_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP); // クライアント受信用 UDP ソケット
    if (listen_sock < 0) {
        log_append("DNS relay: socket create failed");
        free(query_buf);
        free(reply_buf);
        vTaskDelete(nullptr);
        return;
    }

    sockaddr_in listen_addr = {};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(kDnsPort);
    //listen_addr.sin_addr.s_addr = htonl(INADDR_ANY); // 全インターフェースで待ち受け
    listen_addr.sin_addr.s_addr = s_usb_ip.ip.addr; // USB LAN インターフェースで待ち受け
    if (bind(listen_sock, (sockaddr *)&listen_addr, sizeof(listen_addr)) < 0) {
        log_append("DNS relay: bind UDP/53 failed");
        close(listen_sock);
        free(query_buf);
        free(reply_buf);
        vTaskDelete(nullptr);
        return;
    }

    timeval rx_to = {};
    rx_to.tv_sec = 2;
    rx_to.tv_usec = 0;
    setsockopt(listen_sock, SOL_SOCKET, SO_RCVTIMEO, &rx_to, sizeof(rx_to));

    log_append("DNS relay: listening UDP/53");

    while (true) {
        sockaddr_in client_addr = {};
        socklen_t client_len = sizeof(client_addr);
        int query_len = recvfrom(listen_sock, query_buf, 1232, 0,
                                 (sockaddr *)&client_addr, &client_len);
        if (query_len <= 0) {
            // 受信エラー/未到着時に高優先度スピンしないよう必ずCPUを明け渡す
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }
        s_dns_query_count++;

        int upstream_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (upstream_sock < 0) {
            log_append("DNS relay: upstream socket create failed");
            continue;
        }

        timeval upstream_to = {};
        upstream_to.tv_sec = 2;
        upstream_to.tv_usec = 0;
        setsockopt(upstream_sock, SOL_SOCKET, SO_RCVTIMEO, &upstream_to, sizeof(upstream_to));

        char dns_ip_str[16];
        snprintf(dns_ip_str, sizeof(dns_ip_str), "%u.%u.%u.%u",
                 s_upstream_dns_ip[0], s_upstream_dns_ip[1], s_upstream_dns_ip[2], s_upstream_dns_ip[3]);

        sockaddr_in upstream_addr = {};
        upstream_addr.sin_family = AF_INET;
        upstream_addr.sin_port = htons(kDnsPort);
        if (!inet_aton(dns_ip_str, &upstream_addr.sin_addr)) {
            log_append("DNS relay: invalid upstream DNS IP");
            close(upstream_sock);
            continue;
        }

        int sent = sendto(upstream_sock, query_buf, query_len, 0,
                          (sockaddr *)&upstream_addr, sizeof(upstream_addr));
        if (sent != query_len) {
            log_append("DNS relay: send upstream failed");
            close(upstream_sock);
            continue;
        }

        int reply_len = recvfrom(upstream_sock, reply_buf, 1232, 0, nullptr, nullptr);
        close(upstream_sock);
        if (reply_len <= 0) {
            s_dns_timeout_count++;
            if ((s_dns_timeout_count % 16) == 1) {
                log_append("DNS relay: upstream timeout count=%u", (unsigned)s_dns_timeout_count);
            }
            continue;
        }

        int sent_back = sendto(listen_sock, reply_buf, reply_len, 0, (sockaddr *)&client_addr, client_len);
        if (sent_back == reply_len) {
            s_dns_reply_count++;
            if ((s_dns_reply_count % 16) == 1) {
                log_append("DNS relay: ok q=%u r=%u t=%u",   // 問い合わせ/応答/タイムアウト統計
                           (unsigned)s_dns_query_count,
                           (unsigned)s_dns_reply_count,
                           (unsigned)s_dns_timeout_count);
            }
        } else {
            log_append("DNS relay: send back failed len=%d sent=%d", reply_len, sent_back);
        }
    }
}

// ─── コントロール転送ヘルパー ─────────────────────────────────────────────────

// コントロール転送完了コールバック – USB ホストデーモンタスクから呼ばれる
static void ctrl_xfer_cb(usb_transfer_t *xfer) {
    // ISR ではないので xSemaphoreGive を直接呼んでよい
    xSemaphoreGive(s_ctrl_sem); // 待機中のタスクに転送完了を通知
}

// value を alignment の倍数に切り上げる (CDC-NCM NDP アライメント計算用)
static uint16_t align_up_u16(uint16_t value, uint16_t alignment) {
    if (alignment <= 1) return value;
    return (uint16_t)(((value + alignment - 1) / alignment) * alignment);
}

// CDC-NCM ペイロードを (value % divisor == remainder) の条件でアライメントする
// CDC-NCM 仕様 Table 6.3 の NDP ペイロード配置ルールに従う
static uint16_t align_ncm_payload_u16(uint16_t value, uint16_t divisor, uint16_t remainder) {
    if (divisor == 0) return value;
    uint16_t mod = (uint16_t)(value % divisor);
    if (mod == remainder) return value;
    if (mod < remainder) return (uint16_t)(value + (remainder - mod));
    return (uint16_t)(value + (divisor - mod + remainder));
}

// USB 標準リクエスト SET_INTERFACE – CDC Data インターフェースの Alternate Setting を切り替える
// CDC-NCM では Alternate Setting 1 でバルクエンドポイントが有効になるため必要
static esp_err_t std_set_interface(uint8_t itf_num, uint8_t alt_setting) {
    usb_transfer_t *xfer = NULL;
    esp_err_t ret = usb_host_transfer_alloc(sizeof(usb_setup_packet_t), 0, &xfer);
    if (ret != ESP_OK) {
        log_append("SET_INTERFACE: alloc failed (%d)", ret);
        return ret;
    }

    xfer->device_handle    = s_dev_hdl;
    xfer->bEndpointAddress = 0;
    xfer->callback         = ctrl_xfer_cb;
    xfer->context          = nullptr;
    xfer->timeout_ms       = 2000;
    xfer->num_bytes        = sizeof(usb_setup_packet_t);

    usb_setup_packet_t *setup = (usb_setup_packet_t *)xfer->data_buffer;
    setup->bmRequestType = 0x01; // Host→Device, Standard, Interface
    setup->bRequest      = 0x0B; // SET_INTERFACE
    setup->wValue        = alt_setting;
    setup->wIndex        = itf_num;
    setup->wLength       = 0;

    xSemaphoreTake(s_ctrl_sem, 0);
    ret = usb_host_transfer_submit_control(s_client_hdl, xfer);
    if (ret != ESP_OK) {
        log_append("SET_INTERFACE: submit failed ret=%d if=%u alt=%u", ret, itf_num, alt_setting);
        usb_host_transfer_free(xfer);
        return ret;
    }

    bool ok = (xSemaphoreTake(s_ctrl_sem, pdMS_TO_TICKS(2000)) == pdTRUE);
    if (!ok) {
        log_append("SET_INTERFACE: timeout if=%u alt=%u", itf_num, alt_setting);
        ret = ESP_ERR_TIMEOUT;
    } else if (xfer->status != USB_TRANSFER_STATUS_COMPLETED) {
        log_append("SET_INTERFACE: status=%d if=%u alt=%u", xfer->status, itf_num, alt_setting);
        ret = ESP_FAIL;
    } else {
        log_append("SET_INTERFACE: OK if=%u alt=%u", itf_num, alt_setting);
        ret = ESP_OK;
    }

    usb_host_transfer_free(xfer);
    return ret;
}

// USB コントロール転送 (OUT方向) の低レベル実装 – bmRequestType と wIndex を直接指定
static esp_err_t ctrl_out_raw(uint8_t bRequest, uint8_t bmRequestType, uint16_t wIndex,
                              const void *data, uint16_t len) {
    usb_transfer_t *xfer = NULL;
    esp_err_t ret = usb_host_transfer_alloc(sizeof(usb_setup_packet_t) + len, 0, &xfer);
    if (ret != ESP_OK) {
        log_append("ctrl_out: alloc failed (%d)", ret);
        return ret;
    }

    xfer->device_handle    = s_dev_hdl;
    xfer->bEndpointAddress = 0;
    xfer->callback         = ctrl_xfer_cb;
    xfer->context          = nullptr;
    xfer->timeout_ms       = 2000;
    xfer->num_bytes        = sizeof(usb_setup_packet_t) + len;

    usb_setup_packet_t *s = (usb_setup_packet_t *)xfer->data_buffer;
    s->bmRequestType = bmRequestType;
    s->bRequest      = bRequest;
    s->wValue        = 0;
    s->wIndex        = wIndex;
    s->wLength       = len;
    if (len > 0) {
        memcpy(xfer->data_buffer + sizeof(usb_setup_packet_t), data, len);
    }

    xSemaphoreTake(s_ctrl_sem, 0);
    ret = usb_host_transfer_submit_control(s_client_hdl, xfer);
    if (ret != ESP_OK) {
        log_append("ctrl_out: submit failed (%d)", ret);
        usb_host_transfer_free(xfer);
        return ret;
    }
    bool ok = (xSemaphoreTake(s_ctrl_sem, pdMS_TO_TICKS(2000)) == pdTRUE);
    if (!ok) {
        log_append("ctrl_out: timeout");
        s_last_ctrl_status = USB_TRANSFER_STATUS_TIMED_OUT;
        ret = ESP_FAIL;
    } else if (xfer->status != USB_TRANSFER_STATUS_COMPLETED) {
        s_last_ctrl_status = xfer->status;
        log_append("ctrl_out: status=%d req=%u bm=%02X wIndex=%u",
                   xfer->status, bRequest, bmRequestType, wIndex);
        ret = ESP_FAIL;
    } else {
        s_last_ctrl_status = USB_TRANSFER_STATUS_COMPLETED;
    }
    usb_host_transfer_free(xfer);
    return ret;
}

static esp_err_t ctrl_out_try(uint8_t bRequest, uint8_t bmRequestType, uint16_t wIndex,
                              const void *data, uint16_t len, const char *label) {
    log_append("ctrl_out: try %s req=%u bm=%02X wIndex=%u", label, bRequest, bmRequestType, wIndex);
    esp_err_t ret = ctrl_out_raw(bRequest, bmRequestType, wIndex, data, len);
    if (ret == ESP_OK) {
        s_ctrl_out_bm_request_type = bmRequestType;
        s_ctrl_in_bm_request_type = (uint8_t)(bmRequestType | 0x80);
        s_ctrl_w_index = wIndex;
        log_append("ctrl_out: selected %s", label);
    }
    return ret;
}

// RNDIS/NCM コントロール OUT の上位ラッパー – STALL 時に複数の宛先を順番に試行する
// デバイスによって wIndex やレシピエントが異なるため、複数パターンをフォールバック送信する
static esp_err_t ctrl_out(uint8_t bRequest, const void *data, uint16_t len) {
    esp_err_t ret = ctrl_out_try(bRequest, 0x21, s_comm_itf_num, data, len, "comm-interface"); // 通常はここで成功
    if (ret == ESP_OK) return ESP_OK;
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL &&
        s_data_itf_num != 0xFF && s_data_itf_num != s_comm_itf_num) {
        ret = ctrl_out_try(bRequest, 0x21, s_data_itf_num, data, len, "data-interface"); // データインターフェースで再試行
        if (ret == ESP_OK || s_last_ctrl_status != USB_TRANSFER_STATUS_STALL) return ret;
    }
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL && s_comm_itf_num != 0) {
        ret = ctrl_out_try(bRequest, 0x21, 0, data, len, "interface-zero"); // インターフェース 0 で再試行
        if (ret == ESP_OK || s_last_ctrl_status != USB_TRANSFER_STATUS_STALL) return ret;
    }
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL) {
        ret = ctrl_out_try(bRequest, 0x20, s_comm_itf_num, data, len, "device-recipient"); // デバイスレシピエントで再試行
        if (ret == ESP_OK || s_last_ctrl_status != USB_TRANSFER_STATUS_STALL) return ret;
    }
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL && s_comm_itf_num != 0) {
        ret = ctrl_out_try(bRequest, 0x20, 0, data, len, "device-recipient-zero"); // 最終フォールバック
    }
    return ret;
}

// USB コントロール転送 (IN方向) の低レベル実装 – デバイスからデータを受信する
static esp_err_t ctrl_in_raw(uint8_t bRequest, uint8_t bmRequestType, uint16_t wIndex, void *buf,
                             uint16_t buf_len, uint16_t *actual_len) {
    usb_transfer_t *xfer = NULL;
    esp_err_t ret = usb_host_transfer_alloc(sizeof(usb_setup_packet_t) + buf_len, 0, &xfer);
    if (ret != ESP_OK) return ret;

    xfer->device_handle    = s_dev_hdl;
    xfer->bEndpointAddress = 0;
    xfer->callback         = ctrl_xfer_cb;
    xfer->context          = nullptr;
    xfer->timeout_ms       = 2000;
    xfer->num_bytes        = sizeof(usb_setup_packet_t) + buf_len;

    usb_setup_packet_t *s = (usb_setup_packet_t *)xfer->data_buffer;
    s->bmRequestType = bmRequestType;
    s->bRequest      = bRequest;
    s->wValue        = 0;
    s->wIndex        = wIndex;
    s->wLength       = buf_len;

    *actual_len = 0;
    xSemaphoreTake(s_ctrl_sem, 0);
    ret = usb_host_transfer_submit_control(s_client_hdl, xfer);
    if (ret != ESP_OK) {
        log_append("ctrl_in submit failed: ret=%d", ret);
        usb_host_transfer_free(xfer);
        return ret;
    }
    bool ok = (xSemaphoreTake(s_ctrl_sem, pdMS_TO_TICKS(2000)) == pdTRUE);
    if (!ok) {
        log_append("ctrl_in semaphore timeout");
        s_last_ctrl_status = USB_TRANSFER_STATUS_TIMED_OUT;
        usb_host_transfer_free(xfer);
        return ESP_ERR_TIMEOUT;
    }
    if (xfer->status != USB_TRANSFER_STATUS_COMPLETED) {
        s_last_ctrl_status = xfer->status;
        log_append("ctrl_in: status=%d req=%u bm=%02X wIndex=%u",
                   xfer->status, bRequest, bmRequestType, wIndex);
        usb_host_transfer_free(xfer);
        return ESP_FAIL;
    }
    s_last_ctrl_status = USB_TRANSFER_STATUS_COMPLETED;
    int resp = xfer->actual_num_bytes - (int)sizeof(usb_setup_packet_t);
    if (resp > 0 && resp <= buf_len) {
        memcpy(buf, xfer->data_buffer + sizeof(usb_setup_packet_t), resp);
        *actual_len = (uint16_t)resp;
    }
    usb_host_transfer_free(xfer);
    return ESP_OK;
}

static esp_err_t ctrl_in_try(uint8_t bRequest, uint8_t bmRequestType, uint16_t wIndex, void *buf,
                             uint16_t buf_len, uint16_t *actual_len, const char *label) {
    log_append("ctrl_in: try %s req=%u bm=%02X wIndex=%u", label, bRequest, bmRequestType, wIndex);
    return ctrl_in_raw(bRequest, bmRequestType, wIndex, buf, buf_len, actual_len);
}

static esp_err_t ctrl_in(uint8_t bRequest, void *buf, uint16_t buf_len,
                         uint16_t *actual_len) {
    esp_err_t ret = ctrl_in_try(bRequest, s_ctrl_in_bm_request_type, s_ctrl_w_index,
                                buf, buf_len, actual_len, "selected");
    if (ret == ESP_OK) return ESP_OK;
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL &&
        s_data_itf_num != 0xFF && s_data_itf_num != s_comm_itf_num) {
        ret = ctrl_in_try(bRequest, 0xA1, s_data_itf_num, buf, buf_len, actual_len, "data-interface");
        if (ret == ESP_OK || s_last_ctrl_status != USB_TRANSFER_STATUS_STALL) return ret;
    }
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL && s_comm_itf_num != 0) {
        ret = ctrl_in_try(bRequest, 0xA1, 0, buf, buf_len, actual_len, "interface-zero");
        if (ret == ESP_OK || s_last_ctrl_status != USB_TRANSFER_STATUS_STALL) return ret;
    }
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL) {
        ret = ctrl_in_try(bRequest, 0xA0, s_comm_itf_num, buf, buf_len, actual_len, "device-recipient");
        if (ret == ESP_OK || s_last_ctrl_status != USB_TRANSFER_STATUS_STALL) return ret;
    }
    if (s_last_ctrl_status == USB_TRANSFER_STATUS_STALL && s_comm_itf_num != 0) {
        ret = ctrl_in_try(bRequest, 0xA0, 0, buf, buf_len, actual_len, "device-recipient-zero");
    }
    return ret;
}

// ─── ディスクリプタ解析 ──────────────────────────────────────────────────────
//
// USB Configuration Descriptor を先頭から順に走査してインターフェースとエンドポイントを特定する。
// クラスコードを優先しつつ、不明な場合はエンドポイント種別で推測する:
//   Interrupt IN あり → RNDIS/CDC コントロールインターフェース候補
//   Bulk IN + Bulk OUT → RNDIS/NCM データインターフェース候補
// 複数候補がある場合はスコアが高い方を採用する (RNDIS > CDC Comm > Interrupt IN のみ)

static bool parse_config_desc(void) {
    const usb_config_desc_t *cfg = NULL;
    esp_err_t err = usb_host_get_active_config_descriptor(s_dev_hdl, &cfg);
    if (err != ESP_OK || !cfg) {
        M5.Display.printf("get_desc:%d\n", err);
        log_append("get_desc: ret=%d", err);
        return false;
    }

    M5.Display.printf("nItf:%d len:%d\n", cfg->bNumInterfaces, cfg->wTotalLength);
    log_append("config_desc: nItf=%d len=%d", cfg->bNumInterfaces, cfg->wTotalLength);

    const uint8_t *p   = (const uint8_t *)cfg;
    const uint8_t *end = p + cfg->wTotalLength;

    uint8_t  cur_itf     = 0xFF;
    uint8_t  cur_alt     = 0;
    uint8_t  cur_class   = 0;
    uint8_t  cur_subclass= 0;
    uint8_t  cur_proto   = 0;
    bool     cur_intr_in = false; // Interrupt IN あり
    uint8_t  tmp_ep_intr = 0;     // Interrupt IN エンドポイント
    bool     cur_bulk_in = false;
    bool     cur_bulk_out= false;
    uint8_t  tmp_ep_in   = 0;
    uint8_t  tmp_ep_out  = 0;
    int      best_comm_score = -1;
    int      best_data_score = -1;

    auto commit_itf = [&]() {
        if (cur_itf == 0xFF) return;
        M5.Display.printf(" Itf%d alt%d cl:%02X/%02X/%02X %s%s%s\n",
                          cur_itf, cur_alt, cur_class, cur_subclass, cur_proto,
                          cur_intr_in  ? "I" : "-",
                          cur_bulk_in  ? "i" : "-",
                          cur_bulk_out ? "o" : "-");
        log_append("itf=%d alt=%d class=%02X/%02X/%02X flags=%s%s%s",
               cur_itf, cur_alt, cur_class, cur_subclass, cur_proto,
               cur_intr_in  ? "I" : "-",
               cur_bulk_in  ? "i" : "-",
               cur_bulk_out ? "o" : "-");
        const bool is_rndis_comm = (cur_class == 0xE0 && cur_subclass == 0x01 && cur_proto == 0x03);
        const bool is_cdc_comm = (cur_class == 0x02);
        const bool is_cdc_data = (cur_class == 0x0A);
        int comm_score = -1;
        if (is_rndis_comm) comm_score = 3;
        else if (is_cdc_comm) comm_score = 2;
        else if (cur_intr_in) comm_score = 1;
        if (comm_score > best_comm_score) {
            s_comm_itf_num = cur_itf;
            s_comm_itf_alt = cur_alt;
            s_ep_intr = tmp_ep_intr;
            s_comm_class = cur_class;
            s_comm_subclass = cur_subclass;
            s_comm_proto = cur_proto;
            best_comm_score = comm_score;
        }
        int data_score = -1;
        if (cur_bulk_in && cur_bulk_out && is_cdc_data) data_score = 2;
        else if (cur_bulk_in && cur_bulk_out) data_score = 1;
        if (data_score > best_data_score) {
            s_data_itf_num = cur_itf;
            s_data_itf_alt = cur_alt;
            s_ep_in  = tmp_ep_in;
            s_ep_out = tmp_ep_out;
            s_data_class = cur_class;
            s_data_subclass = cur_subclass;
            s_data_proto = cur_proto;
            best_data_score = data_score;
        }
    };

    while (p < end) {
        const usb_standard_desc_t *d = (const usb_standard_desc_t *)p;
        if (d->bLength < 2) break;

        if (d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            commit_itf();
            const usb_intf_desc_t *itf = (const usb_intf_desc_t *)p;
            cur_itf      = itf->bInterfaceNumber;
            cur_alt      = itf->bAlternateSetting;
            cur_class    = itf->bInterfaceClass;
            cur_subclass = itf->bInterfaceSubClass;
            cur_proto    = itf->bInterfaceProtocol;
            cur_intr_in  = false;
            tmp_ep_intr  = 0;
            cur_bulk_in  = false;
            cur_bulk_out = false;
            tmp_ep_in    = 0;
            tmp_ep_out   = 0;
        } else if (d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT && cur_itf != 0xFF) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)p;
            uint8_t ep_type = ep->bmAttributes & 0x03;
            bool is_in = (ep->bEndpointAddress & 0x80) != 0;
            const char *ep_type_str = "???";
            if (ep_type == 0)  ep_type_str = "CTL";
            else if (ep_type == 1) ep_type_str = "ISO";
            else if (ep_type == 2) ep_type_str = "BLK";
            else if (ep_type == 3) ep_type_str = "INT";
            M5.Display.printf("  EP:%02X %s %s\n", ep->bEndpointAddress, ep_type_str, is_in ? "IN" : "OUT");
            log_append("ep=%02X type=%s dir=%s", ep->bEndpointAddress, ep_type_str, is_in ? "IN" : "OUT");
            if (ep_type == USB_TRANSFER_TYPE_INTR && is_in) {
                cur_intr_in = true;
                tmp_ep_intr = ep->bEndpointAddress;
            } else if (ep_type == USB_TRANSFER_TYPE_BULK) {
                uint16_t mps = (ep->wMaxPacketSize & 0x07FF);
                if (is_in)  {
                    cur_bulk_in  = true;
                    tmp_ep_in    = ep->bEndpointAddress;
                    s_ep_in_mps  = mps;
                } else {
                    cur_bulk_out = true;
                    tmp_ep_out   = ep->bEndpointAddress;
                    s_ep_out_mps = mps;
                }
            }
        }
        p += d->bLength;
    }
    commit_itf(); // 最後のインターフェース

    // コントロールインターフェースが見つからない場合は最初の非データインターフェースを使う
    if (s_comm_itf_num == 0xFF && s_data_itf_num != 0xFF) {
        s_comm_itf_num = (s_data_itf_num == 0) ? 1 : 0;
        s_comm_itf_alt = 0;
    }

    M5.Display.printf("comm:%d alt:%d data:%d alt:%d\n",
                      s_comm_itf_num, s_comm_itf_alt, s_data_itf_num, s_data_itf_alt);
    M5.Display.printf("tmp_in:%02X tmp_out:%02X\n", tmp_ep_in, tmp_ep_out);
    M5.Display.printf("s_in:%02X s_out:%02X\n", s_ep_in, s_ep_out);
    M5.Display.printf("IN:%02X OUT:%02X\n", s_ep_in, s_ep_out);
    log_append("selected: comm=%d alt=%d data=%d alt=%d",
               s_comm_itf_num, s_comm_itf_alt, s_data_itf_num, s_data_itf_alt);
    log_append("selected-class: comm=%02X/%02X/%02X data=%02X/%02X/%02X",
               s_comm_class, s_comm_subclass, s_comm_proto,
               s_data_class, s_data_subclass, s_data_proto);
    log_append("endpoints: tmp_in=%02X tmp_out=%02X s_in=%02X s_out=%02X",
               tmp_ep_in, tmp_ep_out, s_ep_in, s_ep_out);
    log_append("EP MPS - IN:%d OUT:%d", s_ep_in_mps, s_ep_out_mps);

    return (s_data_itf_num != 0xFF && s_ep_in != 0 && s_ep_out != 0);
}

// CDC-NCM 初期化シーケンス
// GET_NTB_PARAMETERS でデバイスの NTB 最大サイズ・アライメント情報を取得し、
// SET_NTB_FORMAT / SET_NTB_INPUT_SIZE でホスト側のパラメータをデバイスに通知する
static bool cdc_ncm_initialize(void) {
    s_ctrl_out_bm_request_type = 0x21; // Class, Interface, Host→Device
    s_ctrl_in_bm_request_type = 0xA1;  // Class, Interface, Device→Host
    s_ctrl_w_index = s_comm_itf_num;

    memset(&s_ncm_params, 0, sizeof(s_ncm_params));
    uint16_t len = 0;
    if (ctrl_in(CDC_NCM_GET_NTB_PARAMETERS, &s_ncm_params, sizeof(s_ncm_params), &len) != ESP_OK) {
        log_append("NCM GET_NTB_PARAMETERS failed");
        return false;
    }
    if (len < sizeof(s_ncm_params)) {
        log_append("NCM GET_NTB_PARAMETERS short response len=%u", len);
        return false;
    }

    log_append("NCM params: fmt=%04X inMax=%u outMax=%u inDiv=%u inRem=%u inAlign=%u outDiv=%u outRem=%u outAlign=%u",
               s_ncm_params.bmNtbFormatsSupported,
               (unsigned)s_ncm_params.dwNtbInMaxSize,
               (unsigned)s_ncm_params.dwNtbOutMaxSize,
               s_ncm_params.wNdpInDivisor,
               s_ncm_params.wNdpInPayloadRemainder,
               s_ncm_params.wNdpInAlignment,
               s_ncm_params.wNdpOutDivisor,
               s_ncm_params.wNdpOutPayloadRemainder,
               s_ncm_params.wNdpOutAlignment);

    if ((s_ncm_params.bmNtbFormatsSupported & 0x0001u) == 0) {
        log_append("NCM unsupported: NTB16 not supported");
        return false;
    }

    if (ctrl_out(CDC_NCM_SET_NTB_FORMAT, nullptr, 0) != ESP_OK) {
        log_append("NCM SET_NTB_FORMAT failed");
        return false;
    }

    uint32_t ntb_input_size = s_ncm_params.dwNtbInMaxSize;
    if (ntb_input_size == 0) ntb_input_size = BULK_BUF_SIZE;
    if (ntb_input_size < BULK_BUF_SIZE) ntb_input_size = BULK_BUF_SIZE;
    if (ntb_input_size > 16384u) ntb_input_size = 16384u;
    if (ctrl_out(CDC_NCM_SET_NTB_INPUT_SIZE, &ntb_input_size, sizeof(ntb_input_size)) != ESP_OK) {
        log_append("NCM SET_NTB_INPUT_SIZE failed");
        return false;
    }

    s_rx_buf_size = ntb_input_size;

    s_ncm_tx_sequence = 0;
    log_append("NCM INIT: OK inputSize=%u rxBuf=%u", (unsigned)ntb_input_size, (unsigned)s_rx_buf_size);
    return true;
}

// ─── RNDIS ハンドシェイク ─────────────────────────────────────────────────────

// RNDIS 初期化シーケンス Step 1: INITIALIZE_MSG を送り INITIALIZE_CMPLT を受け取る
// 成功するとデバイスが RNDIS プロトコルを認識した状態になる
static bool rndis_initialize(void) {
    rndis_init_msg_t msg = {};
    msg.MessageType     = RNDIS_INITIALIZE_MSG;
    msg.MessageLength   = sizeof(msg);
    msg.RequestId       = s_req_id++;
    msg.MajorVersion    = 1;
    msg.MaxTransferSize = BULK_BUF_SIZE;
    if (ctrl_out(0x00, &msg, sizeof(msg)) != ESP_OK) {
        log_append("RNDIS INIT: ctrl_out failed");
        return false;
    }
    vTaskDelay(pdMS_TO_TICKS(100));

    rndis_init_cmplt_t cmplt = {};
    uint16_t len = 0;
    if (ctrl_in(0x01, &cmplt, sizeof(cmplt), &len) != ESP_OK) {
        log_append("RNDIS INIT: ctrl_in failed");
        return false;
    }
    bool ok = (cmplt.MessageType == RNDIS_INITIALIZE_CMPLT && cmplt.Status == RNDIS_STATUS_SUCCESS);
    log_append("RNDIS INIT: %s", ok ? "OK" : "FAIL");
    return ok;
}

// RNDIS OID_802_3_PERMANENT_ADDRESS クエリ – デバイスの MAC アドレスを取得する
static bool rndis_query_mac(uint8_t mac_out[6]) {
    rndis_query_msg_t msg = {};
    msg.MessageType = RNDIS_QUERY_MSG;
    msg.MessageLength = sizeof(msg);
    msg.RequestId = s_req_id++;
    msg.Oid = OID_802_3_PERMANENT_ADDRESS;
    if (ctrl_out(0x00, &msg, sizeof(msg)) != ESP_OK) {
        log_append("RNDIS QUERY_MAC: ctrl_out failed");
        return false;
    }
    vTaskDelay(pdMS_TO_TICKS(100));

    uint8_t buf[sizeof(rndis_query_cmplt_t) + 6] = {};
    uint16_t len = 0;
    if (ctrl_in(0x01, buf, sizeof(buf), &len) != ESP_OK) {
        log_append("RNDIS QUERY_MAC: ctrl_in failed");
        return false;
    }
    const rndis_query_cmplt_t *c = (const rndis_query_cmplt_t *)buf;
    if (c->MessageType != RNDIS_QUERY_CMPLT || c->Status != RNDIS_STATUS_SUCCESS) {
        log_append("RNDIS QUERY_MAC: bad response (type=%08X status=%08X)", c->MessageType, c->Status);
        return false;
    }
    if (c->InformationBufferLength < 6) {
        log_append("RNDIS QUERY_MAC: buffer too short (%d)", c->InformationBufferLength);
        return false;
    }
    // InformationBufferOffset は RequestId フィールド先頭からのオフセット
    const uint8_t *mac = (const uint8_t *)&c->RequestId + c->InformationBufferOffset;
    memcpy(mac_out, mac, 6);
    log_append("RNDIS QUERY_MAC: OK - %02X:%02X:%02X:%02X:%02X:%02X", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    return true;
}

// RNDIS OID_GEN_CURRENT_PACKET_FILTER 設定 – すべてのパケットを受信するフィルタを有効化する
// これを設定しないとデバイスがパケットを送ってこない
static bool rndis_set_filter(void) {
    uint8_t buf[sizeof(rndis_set_msg_t) + 4] = {};
    rndis_set_msg_t *msg = (rndis_set_msg_t *)buf;
    msg->MessageType              = RNDIS_SET_MSG;
    msg->MessageLength            = sizeof(buf);
    msg->RequestId                = s_req_id++;
    msg->Oid                      = OID_GEN_CURRENT_PACKET_FILTER;
    msg->InformationBufferLength  = 4;
    msg->InformationBufferOffset  = sizeof(rndis_set_msg_t) - 8; // from RequestId
    uint32_t f = NDIS_PACKET_FILTER_ALL;
    memcpy(buf + sizeof(rndis_set_msg_t), &f, 4);
    if (ctrl_out(0x00, buf, sizeof(buf)) != ESP_OK) {
        log_append("RNDIS SET_FILTER: ctrl_out failed");
        return false;
    }
    vTaskDelay(pdMS_TO_TICKS(100));

    rndis_set_cmplt_t cmplt = {};
    uint16_t len = 0;
    if (ctrl_in(0x01, &cmplt, sizeof(cmplt), &len) != ESP_OK) {
        log_append("RNDIS SET_FILTER: ctrl_in failed");
        return false;
    }
    bool ok = (cmplt.MessageType == RNDIS_SET_CMPLT && cmplt.Status == RNDIS_STATUS_SUCCESS);
    log_append("RNDIS SET_FILTER: %s", ok ? "OK" : "FAIL");
    return ok;
}

// 汎用OID QUERY（複数OIDの初期化クエリに使用）
static bool rndis_query_oid(uint32_t oid, void *buf_out, uint16_t buf_len) {
    rndis_query_msg_t msg = {};
    msg.MessageType = RNDIS_QUERY_MSG;
    msg.MessageLength = sizeof(msg);
    msg.RequestId = s_req_id++;
    msg.Oid = oid;
    if (ctrl_out(0x00, &msg, sizeof(msg)) != ESP_OK) {
        log_append("OID QUERY 0x%08X: ctrl_out failed", oid);
        return false;
    }
    vTaskDelay(pdMS_TO_TICKS(50));

    uint8_t resp_buf[1024] = {};
    uint16_t resp_len = 0;
    if (ctrl_in(0x01, resp_buf, sizeof(resp_buf), &resp_len) != ESP_OK) {
        log_append("OID QUERY 0x%08X: ctrl_in failed", oid);
        return false;
    }
    const rndis_query_cmplt_t *c = (const rndis_query_cmplt_t *)resp_buf;
    if (c->MessageType != RNDIS_QUERY_CMPLT || c->Status != RNDIS_STATUS_SUCCESS) {
        log_append("OID QUERY 0x%08X: status=%08X", oid, c->Status);
        return false;
    }
    if (c->InformationBufferLength > buf_len) {
        log_append("OID QUERY 0x%08X: data too large (%d)", oid, c->InformationBufferLength);
        return false;
    }
    if (buf_out && c->InformationBufferLength > 0) {
        const uint8_t *data = (const uint8_t *)&c->RequestId + c->InformationBufferOffset;
        memcpy(buf_out, data, c->InformationBufferLength);
    }
    log_append("OID QUERY 0x%08X: OK (%d bytes)", oid, c->InformationBufferLength);
    return true;
}

// ─── Bulk IN/OUT コールバック ─────────────────────────────────────────────────

// デバッグ用の受信統計カウンタ
static uint32_t s_rx_count = 0;       // Bulk IN 転送完了回数
static uint32_t s_rx_bytes = 0;       // 受信合計バイト数
static uint32_t s_rx_frame_count = 0; // deliver_rx_frame に渡したフレーム数
static uint32_t s_rx_ipv4_count = 0;  // 受信した IPv4 フレーム数
static uint32_t s_rx_ipv6_count = 0;  // 受信した IPv6 フレーム数
static uint32_t s_rx_arp_count = 0;   // 受信した ARP フレーム数

// イーサネットフレームから IPv4/UDP の送受信ポート番号を取得する (ログ用)
static bool parse_ipv4_udp_ports(const uint8_t *frame, size_t len, uint16_t *src_port, uint16_t *dst_port) {
    if (!frame || len < 14 + 20 + 8) return false;
    uint16_t ether_type = ((uint16_t)frame[12] << 8) | frame[13];
    if (ether_type != 0x0800) return false;

    const uint8_t *ip = frame + 14;
    uint8_t version = ip[0] >> 4;
    uint8_t ihl = (ip[0] & 0x0F) * 4;
    if (version != 4 || ihl < 20) return false;
    if (len < 14 + ihl + 8) return false;
    if (ip[9] != 17) return false;

    const uint8_t *udp = ip + ihl;
    uint16_t sp = ((uint16_t)udp[0] << 8) | udp[1];
    uint16_t dp = ((uint16_t)udp[2] << 8) | udp[3];

    if (src_port) *src_port = sp;
    if (dst_port) *dst_port = dp;
    return true;
}

// イーサネットフレームから ICMPv6 タイプを取得する (RS/RA/NS/NA のログ用)
static bool parse_ipv6_icmp_type(const uint8_t *frame, size_t len, uint8_t *icmpv6_type) {
    if (!frame || len < 14 + 40 + 1) return false;
    uint16_t ether_type = ((uint16_t)frame[12] << 8) | frame[13];
    if (ether_type != 0x86DD) return false;

    const uint8_t *ip6 = frame + 14;
    uint8_t version = ip6[0] >> 4;
    if (version != 6) return false;

    uint8_t next_header = ip6[6];
    if (next_header != 58) return false;
    if (icmpv6_type) *icmpv6_type = ip6[40];
    return true;
}

// RNDIS/NCM から取り出したイーサネットフレームを lwIP スタックへ注入する
// esp_netif_receive に渡すバッファは heap 確保済みで、lwIP が free コールバックで解放する
static void deliver_rx_frame(const uint8_t *frame, size_t len) {
    if (!frame || len == 0 || !s_usb_netif) return;

    s_rx_frame_count++;
    uint16_t ether_type = 0;
    if (len >= 14) {
        ether_type = ((uint16_t)frame[12] << 8) | frame[13];
        if (ether_type == 0x0800) s_rx_ipv4_count++;
        else if (ether_type == 0x86DD) s_rx_ipv6_count++;
        else if (ether_type == 0x0806) s_rx_arp_count++;
    }

    if ((s_rx_frame_count % 32) == 1) {
        log_append("RX frame[%u]: len=%d eth=0x%04X", (unsigned)s_rx_frame_count, (int)len, ether_type);
        log_append("RX summary: ipv4=%u ipv6=%u arp=%u", (unsigned)s_rx_ipv4_count,
                   (unsigned)s_rx_ipv6_count, (unsigned)s_rx_arp_count);
    }

    void *buf = malloc(len);
    if (!buf) {
        log_append("RX: alloc failed len=%d", (int)len);
        return;
    }

    memcpy(buf, frame, len);
    uint16_t src_port = 0, dst_port = 0;
    if (parse_ipv4_udp_ports((const uint8_t *)buf, len, &src_port, &dst_port)) {
        if ((src_port == 67 || src_port == 68) && (dst_port == 67 || dst_port == 68)) {
            log_append("RX: DHCP frame len=%d %d->%d", (int)len, src_port, dst_port);
        }
        if (src_port == 53 || dst_port == 53) {
            log_append("RX: DNS frame len=%d %d->%d", (int)len, src_port, dst_port);
        }
    } else if ((s_rx_frame_count % 32) == 1 && len >= 14) {
        log_append("RX: non-UDP frame eth=0x%04X len=%d", ether_type, (int)len);
    }

    if (ether_type == 0x86DD) {
        uint8_t icmpv6_type = 0;
        if (parse_ipv6_icmp_type((const uint8_t *)buf, len, &icmpv6_type)) {
            if ((s_rx_frame_count % 32) == 1 || icmpv6_type == 133 || icmpv6_type == 134 ||
                icmpv6_type == 135 || icmpv6_type == 136) {
                log_append("RX: ICMPv6 type=%u len=%d", icmpv6_type, (int)len);
            }
        }
    }

    if (s_rx_frame_count == 8 && s_rx_ipv4_count == 0) {
        log_append("RX hint: IPv4 frame not seen yet (peer may not start DHCP client on usb)");
    }

    esp_err_t nret = esp_netif_receive(s_usb_netif, buf, len, buf);
    if (nret != ESP_OK) {
        log_append("RX: esp_netif_receive failed ret=%d", nret);
        free(buf);
    }
}

// Bulk IN 転送完了コールバック – RNDIS または NCM フレームを解析して deliver_rx_frame へ渡す
// 処理後は次の Bulk IN 転送を再投入してループを維持する
static void rx_callback(usb_transfer_t *xfer) {
    if (xfer->status == USB_TRANSFER_STATUS_COMPLETED) {
        s_rx_bytes += xfer->actual_num_bytes;
        s_rx_count++;
        if (xfer->actual_num_bytes > 0 && (s_rx_count % 512 == 1))
            log_append("RX[%d]: bytes=%d total=%d", s_rx_count, xfer->actual_num_bytes, s_rx_bytes);

        if (s_link_mode == USB_LINK_MODE_CDC_NCM &&
            xfer->actual_num_bytes >= (int)sizeof(cdc_ncm_nth16_t)) {
            const uint8_t *base = xfer->data_buffer;
            const cdc_ncm_nth16_t *nth = (const cdc_ncm_nth16_t *)base;
            if (nth->dwSignature == CDC_NCM_NTH16_SIGNATURE &&
                nth->wBlockLength <= (uint16_t)xfer->actual_num_bytes &&
                nth->wNdpIndex + sizeof(cdc_ncm_ndp16_t) <= nth->wBlockLength) {
                log_append("NCM RX: nth seq=%u block=%u ndp=%u", nth->wSequence, nth->wBlockLength, nth->wNdpIndex);
                uint16_t ndp_index = nth->wNdpIndex;
                int ndp_count = 0;
                int delivered = 0;
                while (ndp_index != 0 && ndp_count < 4 &&
                       ndp_index + sizeof(cdc_ncm_ndp16_t) <= nth->wBlockLength) {
                    const cdc_ncm_ndp16_t *ndp = (const cdc_ncm_ndp16_t *)(base + ndp_index);
                    if (ndp->dwSignature != CDC_NCM_NDP16_SIGNATURE ||
                        ndp->wLength < (sizeof(cdc_ncm_ndp16_t) + sizeof(cdc_ncm_dpe16_t) * 2) ||
                        ndp_index + ndp->wLength > nth->wBlockLength) {
                        log_append("NCM RX: invalid NDP sig=%08X len=%u", ndp->dwSignature, ndp->wLength);
                        break;
                    }

                    const cdc_ncm_dpe16_t *dpe = (const cdc_ncm_dpe16_t *)(base + ndp_index + sizeof(cdc_ncm_ndp16_t));
                    int dpe_count = (ndp->wLength - sizeof(cdc_ncm_ndp16_t)) / sizeof(cdc_ncm_dpe16_t);
                    for (int idx = 0; idx < dpe_count; ++idx) {
                        uint16_t frame_index = dpe[idx].wDatagramIndex;
                        uint16_t frame_len = dpe[idx].wDatagramLength;
                        if (frame_index == 0 || frame_len == 0) break;
                        if ((uint32_t)frame_index + frame_len > nth->wBlockLength) {
                            log_append("NCM RX: invalid frame index=%u len=%u", frame_index, frame_len);
                            break;
                        }
                        deliver_rx_frame(base + frame_index, frame_len);
                        delivered++;
                    }

                    if (ndp->wNextNdpIndex == 0 || ndp->wNextNdpIndex == ndp_index) break;
                    ndp_index = ndp->wNextNdpIndex;
                    ndp_count++;
                }
                if (delivered == 0) {
                    log_append("NCM RX: no datagrams in NTB");
                }
            } else {
                log_append("NCM RX: invalid NTH sig=%08X len=%d", nth->dwSignature, xfer->actual_num_bytes);
            }
        } else if (xfer->actual_num_bytes > (int)RNDIS_PACKET_HEADER_SIZE) {
            const rndis_packet_msg_t *hdr = (const rndis_packet_msg_t *)xfer->data_buffer;
            if (hdr->MessageType == RNDIS_PACKET_MSG) {
                uint32_t off = 8 + hdr->DataOffset; // &DataOffset からのオフセット
                uint32_t dlen = hdr->DataLength;
                if (off + dlen <= (uint32_t)xfer->actual_num_bytes) {
                    deliver_rx_frame(xfer->data_buffer + off, dlen);
                }
            }
        }
    } else {
        log_append("RX error: status=%d bytes=%d", xfer->status, xfer->actual_num_bytes);
    }
    if (s_rndis_state == RNDIS_STATE_READY) {
        esp_err_t ret = usb_host_transfer_submit(xfer); // エラー時も次の受信を再投入
        if (ret != ESP_OK) {
            log_append("RX resubmit failed: ret=%d", ret);
        }
    }
}

// Bulk OUT 転送完了コールバック – 送信完了を s_tx_sem で通知して次の送信を許可する
static void tx_callback(usb_transfer_t *xfer) {
    xSemaphoreGive(s_tx_sem); // usb_driver_transmit の xSemaphoreTake と対になる
}

static void intr_callback(usb_transfer_t *xfer) {
    // Interrupt IN データは通常無視（状態通知の到着を検出するだけ）
    if (s_rndis_state == RNDIS_STATE_READY)
        usb_host_transfer_submit(xfer); // 次の通知を待機
}

static void start_intr(void) {
    if (s_ep_intr == 0) return; // Interrupt IN がない
    if (s_intr_xfer == NULL) {
        esp_err_t ret = usb_host_transfer_alloc(64, 0, &s_intr_xfer);
        if (ret != ESP_OK || s_intr_xfer == NULL) {
            log_append("INTR alloc failed!");
            return;
        }
        s_intr_xfer->callback = intr_callback;
        s_intr_xfer->context  = nullptr;
        s_intr_xfer->timeout_ms = 0;
    }
    s_intr_xfer->device_handle    = s_dev_hdl;
    s_intr_xfer->bEndpointAddress = s_ep_intr;
    s_intr_xfer->num_bytes        = 64;
    esp_err_t ret = usb_host_transfer_submit(s_intr_xfer);
    log_append("INTR submit: ret=%d ep=0x%02X", ret, s_ep_intr);
}

// Bulk IN 受信を開始する – 必要なら転送バッファを再確保してから投入する
static void start_rx(void) {
    if (s_rx_xfer == NULL || s_rx_xfer->num_bytes < (int)s_rx_buf_size) {
        if (s_rx_xfer != NULL) {
            usb_host_transfer_free(s_rx_xfer);
            s_rx_xfer = NULL;
        }
        esp_err_t ret = usb_host_transfer_alloc(s_rx_buf_size, 0, &s_rx_xfer);
        log_append("RX alloc: ret=%d ptr=%p size=%u", ret, s_rx_xfer, (unsigned)s_rx_buf_size);
        if (ret != ESP_OK || s_rx_xfer == NULL) {
            log_append("RX alloc failed!");
            return;
        }
        s_rx_xfer->callback = rx_callback;
        s_rx_xfer->context  = nullptr;
        s_rx_xfer->timeout_ms = 0;
    }

    if (s_dev_hdl == NULL) {
        log_append("RX: device handle is NULL!");
        return;
    }
    if (s_ep_in == 0) {
        log_append("RX: endpoint is 0!");
        return;
    }

    s_rx_xfer->device_handle    = s_dev_hdl;
    s_rx_xfer->bEndpointAddress = s_ep_in;
    s_rx_xfer->num_bytes        = s_rx_buf_size;

    log_append("RX submit: dev=%p ep=0x%02X size=%u (state=%d)",
               s_dev_hdl, s_ep_in, (unsigned)s_rx_buf_size, s_rndis_state);
    esp_err_t ret = usb_host_transfer_submit(s_rx_xfer);
    log_append("RX submit result: ret=%d", ret);
    if (ret != ESP_OK) {
        log_append("RX CRITICAL: submit failed, may need device reset");
    }
}

// 切断時/初期化失敗時の後始末を一元化して再接続を安定させる
static void cleanup_rndis_device(bool from_disconnect) {
    // 切断時にコールバック再投入を止める
    s_rndis_state = RNDIS_STATE_IDLE;

    // 送信セマフォが取りっぱなしだと再接続後に TX が永久 busy になるため、必ず解放可能状態へ戻す
    if (s_tx_sem) {
        xSemaphoreTake(s_tx_sem, 0);
        xSemaphoreGive(s_tx_sem);
    }

    if (s_usb_netif) {
        esp_netif_action_disconnected(s_usb_netif, nullptr, 0, nullptr);
        esp_netif_action_stop(s_usb_netif, nullptr, 0, nullptr);
        log_append("esp_netif: action_disconnected + action_stop");
    }

    // まだハンドルが有効ならインターフェース解放とクローズを試行
    if (s_dev_hdl) {
        if (s_ep_in) {
            usb_host_endpoint_halt(s_dev_hdl, s_ep_in);
            usb_host_endpoint_flush(s_dev_hdl, s_ep_in);
        }
        if (s_ep_out) {
            usb_host_endpoint_halt(s_dev_hdl, s_ep_out);
            usb_host_endpoint_flush(s_dev_hdl, s_ep_out);
        }
        if (s_ep_intr) {
            usb_host_endpoint_halt(s_dev_hdl, s_ep_intr);
            usb_host_endpoint_flush(s_dev_hdl, s_ep_intr);
        }

        if (s_comm_itf_num != 0xFF) {
            esp_err_t r = usb_host_interface_release(s_client_hdl, s_dev_hdl, s_comm_itf_num);
            log_append("Release COMM(if=%d): ret=%d", s_comm_itf_num, r);
        }
        if (s_data_itf_num != 0xFF && s_data_itf_num != s_comm_itf_num) {
            esp_err_t r = usb_host_interface_release(s_client_hdl, s_dev_hdl, s_data_itf_num);
            log_append("Release DATA(if=%d): ret=%d", s_data_itf_num, r);
        }
        esp_err_t c = usb_host_device_close(s_client_hdl, s_dev_hdl);
        log_append("Device close: ret=%d", c);
        s_dev_hdl = NULL;
    }

    s_comm_itf_num = 0xFF;
    s_data_itf_num = 0xFF;
    s_comm_itf_alt = 0;
    s_data_itf_alt = 0;
    s_ep_intr = 0;
    s_ep_in = 0;
    s_ep_out = 0;
    s_ep_in_mps = 0;
    s_ep_out_mps = 0;
    s_comm_class = 0;
    s_comm_subclass = 0;
    s_comm_proto = 0;
    s_data_class = 0;
    s_data_subclass = 0;
    s_data_proto = 0;
    memset(&s_ncm_params, 0, sizeof(s_ncm_params));
    s_ncm_tx_sequence = 0;
    s_rx_buf_size = BULK_BUF_SIZE;
    s_link_mode = USB_LINK_MODE_RNDIS;

    // 転送オブジェクト自体は保持して再接続時に再利用する（in-flight free 競合回避）
    if (s_rx_xfer) {
        s_rx_xfer->device_handle = NULL;
        s_rx_xfer->bEndpointAddress = 0;
    }
    if (s_intr_xfer) {
        s_intr_xfer->device_handle = NULL;
        s_intr_xfer->bEndpointAddress = 0;
    }

    if (from_disconnect) {
        M5.Display.println("Dev gone");
        log_append("Dev gone");
    }
}

// ─── RNDIS セットアップタスク（event callback とは別タスクで実行） ────────────
// client_event_cb でのブロッキング処理を避けるため、デバイスアドレスをキューで受け取り
// このタスク内でデバイスオープン〜RNDIS/NCM 初期化〜受信開始を順番に実行する
static void rndis_setup_task(void *arg) {
    uint8_t dev_addr;
    for (;;) {
        if (xQueueReceive(s_dev_queue, &dev_addr, portMAX_DELAY) != pdTRUE) continue;

        // デバイスオープン
        if (usb_host_device_open(s_client_hdl, dev_addr, &s_dev_hdl) != ESP_OK) {
            M5.Display.println("Open fail");
            log_append("Open fail");
            cleanup_rndis_device(false);
            continue;
        }

        // ディスクリプタ解析（インターフェース番号・エンドポイント検索）
        s_comm_itf_num = 0xFF; s_data_itf_num = 0xFF;
        s_comm_itf_alt = 0; s_data_itf_alt = 0;
        s_ep_intr = 0;
        s_ep_in_mps = 0; s_ep_out_mps = 0;
        s_ep_in = 0; s_ep_out = 0;
        if (!parse_config_desc()) {
            M5.Display.println("Desc fail");
            log_append("Desc fail");
            cleanup_rndis_device(false);
            continue;
        }

        const bool is_cdc_ncm = (s_comm_class == 0x02 && s_comm_subclass == 0x0D &&
                                 s_data_class == 0x0A);
        s_link_mode = is_cdc_ncm ? USB_LINK_MODE_CDC_NCM : USB_LINK_MODE_RNDIS;
        log_append("Link mode: %s", is_cdc_ncm ? "CDC-NCM" : "RNDIS");

        // インターフェース Claim
        esp_err_t ret_comm = usb_host_interface_claim(s_client_hdl, s_dev_hdl, s_comm_itf_num, s_comm_itf_alt);
        esp_err_t ret_data = usb_host_interface_claim(s_client_hdl, s_dev_hdl, s_data_itf_num, s_data_itf_alt);
        log_append("Claim COMM(if=%d alt=%d): ret=%d", s_comm_itf_num, s_comm_itf_alt, ret_comm);
        log_append("Claim DATA(if=%d alt=%d): ret=%d", s_data_itf_num, s_data_itf_alt, ret_data);
        if (ret_comm != ESP_OK || ret_data != ESP_OK) {
            M5.Display.setTextColor(RED);
            M5.Display.println("Itf claim fail");
            M5.Display.setTextColor(WHITE);
            log_append("Itf claim fail");
            s_rndis_state = RNDIS_STATE_ERROR;
            cleanup_rndis_device(false);
            continue;
        }

        s_rndis_state = RNDIS_STATE_CONNECTED;
        M5.Display.println("Itf claimed");
        log_append("Itf claimed");

        if (s_data_itf_alt != 0) {
            esp_err_t set_alt_ret = std_set_interface(s_data_itf_num, s_data_itf_alt);
            if (set_alt_ret != ESP_OK) {
                M5.Display.setTextColor(RED);
                M5.Display.println("SET_INTERFACE fail");
                M5.Display.setTextColor(WHITE);
                log_append("SET_INTERFACE failed for data if=%u alt=%u ret=%d",
                           s_data_itf_num, s_data_itf_alt, set_alt_ret);
                s_rndis_state = RNDIS_STATE_ERROR;
                cleanup_rndis_device(false);
                continue;
            }
        }
        s_ctrl_out_bm_request_type = 0x21;
        s_ctrl_in_bm_request_type = 0xA1;
        s_ctrl_w_index = s_comm_itf_num;

        bool init_ok = false;
        if (s_link_mode == USB_LINK_MODE_CDC_NCM) {
            init_ok = cdc_ncm_initialize();
        } else {
            init_ok = rndis_initialize();
        }
        if (!init_ok) {
            M5.Display.setTextColor(RED);
            M5.Display.println(s_link_mode == USB_LINK_MODE_CDC_NCM ? "NCM INIT fail" : "RNDIS INIT fail");
            M5.Display.setTextColor(WHITE);
            log_append("%s INIT fail", s_link_mode == USB_LINK_MODE_CDC_NCM ? "NCM" : "RNDIS");
            s_rndis_state = RNDIS_STATE_ERROR;
            cleanup_rndis_device(false);
            continue;
        }
        s_rndis_state = RNDIS_STATE_INITIALIZED;
        M5.Display.println("INIT OK");
        log_append("INIT OK");

        // MAC 取得（Windows CE RNDISはQUERY_MACをサポートしていない可能性があるのでスキップ）
        uint8_t client_mac[6] = {0x02, 0x50, 0xF2, 0x00, 0x00, 0x01}; // Default RNDIS MAC
        log_append("RNDIS MAC: using default %02X:%02X:%02X:%02X:%02X:%02X",
                   client_mac[0], client_mac[1], client_mac[2], client_mac[3], client_mac[4], client_mac[5]);

        if (s_link_mode == USB_LINK_MODE_RNDIS) {
            if (!rndis_set_filter()) {
                M5.Display.setTextColor(RED);
                M5.Display.println("Filter fail");
                M5.Display.setTextColor(WHITE);
                log_append("RNDIS SET_FILTER failed");
                s_rndis_state = RNDIS_STATE_ERROR;
                cleanup_rndis_device(false);
                continue;
            }
        }

        // デバイスが完全に初期化されるまで大幅に待機（Windows CE RNDIS ドライバ初期化時間）
        log_append("Setting up TX/RX/INTR transfers");
        vTaskDelay(pdMS_TO_TICKS(2000));

        // TX バッファ設定
        s_tx_xfer->device_handle    = s_dev_hdl;
        s_tx_xfer->bEndpointAddress = s_ep_out;
        log_append("TX buffer configured: ep=0x%02X", s_ep_out);

        // Interrupt IN 開始（デバイス状態通知受信）
        log_append("Starting INTR...");
        start_intr();
        vTaskDelay(pdMS_TO_TICKS(200));

        // Bulk IN 受信開始（十分な遅延後に投入）
        log_append("Starting RX...");
        s_rndis_state = RNDIS_STATE_READY;
        if (s_usb_netif) {
            esp_netif_action_start(s_usb_netif, nullptr, 0, nullptr);
            esp_netif_action_connected(s_usb_netif, nullptr, 0, nullptr);
            esp_netif_tcpip_exec(enable_napt_cb, nullptr); // lwIPタスクで NAPT 有効化
            log_append("esp_netif: action_start + action_connected, NAPT enabled");
        }
        if (!s_dns_task_started) {
            if (xTaskCreate(dns_relay_task, "dns_relay", 4096, nullptr,
                            tskIDLE_PRIORITY + 1, nullptr) == pdPASS) {
                s_dns_task_started = true;
                log_append("DNS relay: task started");
            } else {
                log_append("DNS relay: task create failed");
            }
        }
        start_rx();
        log_append("RX started, waiting for data...");
        M5.Display.fillScreen(BLACK);
        M5.Display.setCursor(0, 0);

        M5.Display.setTextColor(GREEN);
        M5.Display.println("RNDIS Ready!");
        M5.Display.setTextColor(WHITE);
        M5.Display.print("WLAN: ");
        M5.Display.println(WiFi.localIP());
        // RNDIS側のIP表示
        if (s_usb_netif) {
            esp_netif_ip_info_t ip_info;
            if (esp_netif_get_ip_info(s_usb_netif, &ip_info) == ESP_OK) {
                M5.Display.print("USB: ");
                M5.Display.println(ip4addr_ntoa((const ip4_addr_t *)&ip_info.ip));
            }
        }
    }
}

// ─── USB クライアントイベントコールバック ─────────────────────────────────────
// rndis_client_task (usb_host_client_handle_events) から呼ばれるイベントハンドラ
// ブロッキング処理はここで行わず、キューに渡して rndis_setup_task に委譲する
static void client_event_cb(const usb_host_client_event_msg_t *msg, void *arg) {
    if (msg->event == USB_HOST_CLIENT_EVENT_NEW_DEV) {
        M5.Display.println("Dev connected");
        log_append("Dev connected");
        log_append("USB Device connected");
        // ブロッキング処理は rndis_setup_task に委譲する
        uint8_t addr = msg->new_dev.address;
        xQueueReset(s_dev_queue);
        if (xQueueSend(s_dev_queue, &addr, 0) != pdTRUE) {
            log_append("WARN: dev queue send failed");
        }
    } else if (msg->event == USB_HOST_CLIENT_EVENT_DEV_GONE) {
        log_append("USB Device disconnected");
        cleanup_rndis_device(true);
        ESP.restart(); // 再接続はいろいろ安定しないのでソフトリセット
    }
}

// ─── USB ホスト / クライアントタスク ─────────────────────────────────────────

// USB ホストライブラリの低レベルイベントループ – 最高優先度で動かしてハードウェアイベントを処理する
static void usb_host_lib_task(void *arg) {
    for (;;) {
        uint32_t flags;
        usb_host_lib_handle_events(portMAX_DELAY, &flags); // USB ホストライブラリのイベント処理
        if (flags & USB_HOST_LIB_EVENT_FLAGS_NO_CLIENTS)
            usb_host_device_free_all(); // クライアントが全員いなくなったらデバイスを解放
    }
}

// USB クライアントのイベントループ – client_event_cb を呼び出すポンプ
static void rndis_client_task(void *arg) {
    const usb_host_client_config_t cfg = {
        .is_synchronous = false,
        .max_num_event_msg = 8,
        .async = {
            .client_event_callback = client_event_cb,
            .callback_arg          = nullptr,
        },
    };
    usb_host_client_register(&cfg, &s_client_hdl);
    for (;;) {
        usb_host_client_handle_events(s_client_hdl, portMAX_DELAY);
    }
}

// ─── ESP-netif USB ドライバ ────────────────────────────────────────────────────
// esp-netif と USB Bulk OUT 転送を繋ぐドライバ層
// lwIP が送信したいパケットは esp_netif_transmit 経由でここに来る

// esp_netif が受信バッファを解放する際に呼ぶコールバック (deliver_rx_frame で malloc したメモリ)
static void usb_driver_free_rx_buf(void *h, void *buf) { free(buf); }

// lwIP から呼ばれる送信関数 – RNDIS または NCM ヘッダを付与して Bulk OUT 転送を投入する
static esp_err_t usb_driver_transmit(void *h, void *buf, size_t len) {
    if (s_rndis_state != RNDIS_STATE_READY || !s_tx_xfer || !s_dev_hdl) {
        log_append("TX drop: state=%d tx=%p dev=%p", s_rndis_state, s_tx_xfer, s_dev_hdl);
        return ESP_ERR_INVALID_STATE;
    }

    if (xSemaphoreTake(s_tx_sem, pdMS_TO_TICKS(50)) != pdTRUE) {
        log_append("TX busy: semaphore timeout");
        return ESP_ERR_TIMEOUT;
    }

    uint16_t src_port = 0, dst_port = 0;
    if (parse_ipv4_udp_ports((const uint8_t *)buf, len, &src_port, &dst_port)) {
        if ((src_port == 67 || src_port == 68) && (dst_port == 67 || dst_port == 68)) {
            log_append("TX: DHCP frame len=%d %d->%d", (int)len, src_port, dst_port);
        }
        if (src_port == 53 || dst_port == 53) {
            log_append("TX: DNS frame len=%d %d->%d", (int)len, src_port, dst_port);
        }
    }

    if (s_link_mode == USB_LINK_MODE_CDC_NCM) {
        uint16_t ndp_alignment = s_ncm_params.wNdpOutAlignment;
        uint16_t ndp_index = align_up_u16((uint16_t)sizeof(cdc_ncm_nth16_t), ndp_alignment);
        uint16_t ndp_length = (uint16_t)(sizeof(cdc_ncm_ndp16_t) + sizeof(cdc_ncm_dpe16_t) * 2);
        uint16_t payload_index = align_ncm_payload_u16((uint16_t)(ndp_index + ndp_length),
                                                       s_ncm_params.wNdpOutDivisor,
                                                       s_ncm_params.wNdpOutPayloadRemainder);
        uint32_t block_length = (uint32_t)payload_index + (uint32_t)len;
        uint32_t ntb_out_max = s_ncm_params.dwNtbOutMaxSize;
        if (ntb_out_max == 0 || ntb_out_max > BULK_BUF_SIZE) ntb_out_max = BULK_BUF_SIZE;
        if (block_length > ntb_out_max) {
            log_append("TX drop: NCM oversize len=%d block=%u max=%u", (int)len,
                       (unsigned)block_length, (unsigned)ntb_out_max);
            xSemaphoreGive(s_tx_sem);
            return ESP_ERR_INVALID_ARG;
        }

        memset(s_tx_xfer->data_buffer, 0, block_length);
        cdc_ncm_nth16_t *nth = (cdc_ncm_nth16_t *)s_tx_xfer->data_buffer;
        nth->dwSignature = CDC_NCM_NTH16_SIGNATURE;
        nth->wHeaderLength = sizeof(cdc_ncm_nth16_t);
        nth->wSequence = s_ncm_tx_sequence++;
        nth->wBlockLength = (uint16_t)block_length;
        nth->wNdpIndex = ndp_index;

        cdc_ncm_ndp16_t *ndp = (cdc_ncm_ndp16_t *)(s_tx_xfer->data_buffer + ndp_index);
        ndp->dwSignature = CDC_NCM_NDP16_SIGNATURE;
        ndp->wLength = ndp_length;
        ndp->wNextNdpIndex = 0;

        cdc_ncm_dpe16_t *dpe = (cdc_ncm_dpe16_t *)(s_tx_xfer->data_buffer + ndp_index + sizeof(cdc_ncm_ndp16_t));
        dpe[0].wDatagramIndex = payload_index;
        dpe[0].wDatagramLength = (uint16_t)len;
        dpe[1].wDatagramIndex = 0;
        dpe[1].wDatagramLength = 0;

        memcpy(s_tx_xfer->data_buffer + payload_index, buf, len);
        s_tx_xfer->num_bytes = (int)block_length;
    } else {
        if (len > BULK_BUF_SIZE - RNDIS_PACKET_HEADER_SIZE) {
            log_append("TX drop: oversize len=%d", (int)len);
            xSemaphoreGive(s_tx_sem);
            return ESP_ERR_INVALID_ARG;
        }

        rndis_packet_msg_t *hdr = (rndis_packet_msg_t *)s_tx_xfer->data_buffer;
        hdr->MessageType         = RNDIS_PACKET_MSG;
        hdr->MessageLength       = (uint32_t)(RNDIS_PACKET_HEADER_SIZE + len);
        hdr->DataOffset          = RNDIS_DATA_OFFSET;
        hdr->DataLength          = (uint32_t)len;
        hdr->OOBDataOffset       = 0; hdr->OOBDataLength    = 0;
        hdr->NumOOBDataElements  = 0; hdr->PerPacketInfoOffset = 0;
        hdr->PerPacketInfoLength = 0; hdr->VcHandle          = 0; hdr->Reserved = 0;
        memcpy(s_tx_xfer->data_buffer + RNDIS_PACKET_HEADER_SIZE, buf, len);
        s_tx_xfer->num_bytes = (int)(RNDIS_PACKET_HEADER_SIZE + len);
    }

    esp_err_t tret = usb_host_transfer_submit(s_tx_xfer);
    if (tret != ESP_OK) {
        log_append("TX submit failed: ret=%d ep=0x%02X bytes=%d", tret, s_ep_out, s_tx_xfer->num_bytes);
        xSemaphoreGive(s_tx_sem);
        return tret;
    }
    return ESP_OK;
}

struct UsbDriver { esp_netif_driver_base_t base; }; // esp_netif ドライバ構造体 (base が先頭必須)

// esp_netif_attach 後に呼ばれるコールバック – ドライバ関数ポインタを netif に登録する
static esp_err_t usb_driver_post_attach(esp_netif_t *netif, void *args) {
    UsbDriver *drv = (UsbDriver *)args;
    drv->base.netif = netif;
    const esp_netif_driver_ifconfig_t ifcfg = {
        .handle                = drv,
        .transmit              = usb_driver_transmit,
        .transmit_wrap         = nullptr,
        .driver_free_rx_buffer = usb_driver_free_rx_buf,
    };
    return esp_netif_set_driver_config(netif, &ifcfg);
}

// ─── USB 側 esp-netif ─────────────────────────────────────────────────────────
// WiFi側とIPアドレス体系が被るとうまく通信できない気がするので
// 必要であれば編集が必要です
static esp_netif_t *create_usb_netif(void) {
    IP4_ADDR(&s_usb_ip.ip,      192, 168, 37, 1);// USB 側の固定IPアドレス
    IP4_ADDR(&s_usb_ip.gw,      192, 168, 37, 1);// ゲートウェイは自分自身なので基本上と同じで問題ないはず
    IP4_ADDR(&s_usb_ip.netmask, 255, 255, 255, 0);// サブネットマスクも適切に設定。基本変えなくてよい

    esp_netif_inherent_config_t base = ESP_NETIF_INHERENT_DEFAULT_ETH();
    base.flags      = (esp_netif_flags_t)(ESP_NETIF_DHCP_SERVER | ESP_NETIF_FLAG_AUTOUP);
    base.ip_info    = &s_usb_ip;
    base.if_key     = "USB0";
    base.if_desc    = "USB RNDIS Host";
    base.route_prio = 10;

    const esp_netif_config_t cfg = {
        .base   = &base,
        .driver = nullptr,
        .stack  = ESP_NETIF_NETSTACK_DEFAULT_ETH,
    };
    return esp_netif_new(&cfg);
}

// ─── HTTP サーバー ────────────────────────────────────────────────────────────
// ポート 80 で動作する管理 Web UI。デバイス状態とログを HTML で表示する。
// s_http_rndis_only が true の場合は RNDIS サブネット (192.168.37.x) からのみアクセス可。

// アクセス元 IP が USB LAN サブネットに属するか確認する
static bool is_allowed_http_client(const IPAddress &remote_ip) {
    if (!s_http_rndis_only) return true;

    IPAddress netif_ip((uint32_t)s_usb_ip.ip.addr);
    IPAddress netmask((uint32_t)s_usb_ip.netmask.addr);

    uint32_t remote = (uint32_t)remote_ip;
    uint32_t base = (uint32_t)netif_ip;
    uint32_t mask = (uint32_t)netmask;
    return (remote & mask) == (base & mask);
}

static void send_http_forbidden(WiFiClient &client) {
    const char *body = "<!DOCTYPE html><html><head><meta charset='UTF-8'><title>403 Forbidden</title></head><body><h1>403 Forbidden</h1><p>RNDIS側からのみアクセス可能です。</p></body></html>";
    client.println("HTTP/1.1 403 Forbidden");
    client.println("Content-Type: text/html; charset=UTF-8");
    client.println(String("Content-Length: ") + strlen(body));
    client.println("Connection: close");
    client.println();
    client.print(body);
}

// HTTP リクエストを処理してステータス/ログを HTML で返す
// ページは 2 秒ごとに自動リフレッシュされる (meta refresh)
static void handle_http_client(WiFiClient client) {
    IPAddress remote_ip = client.remoteIP();
    if (!is_allowed_http_client(remote_ip)) {
        log_append("HTTP reject: remote=%s", remote_ip.toString().c_str());
        send_http_forbidden(client);
        client.stop();
        return;
    }

    String request = "";
    while (client.connected() && client.available()) {
        char c = client.read();
        request += c;
        if (request.endsWith("\r\n\r\n")) break;
    }

    // HTML レスポンス作成（heap 断片化防止のため事前確保）
    String html;
    html.reserve(LOG_SIZE + 2048);
    html = "<!DOCTYPE html><html><head><meta charset='UTF-8'><title>AtomS3 RNDIS Router</title>";
    html += "<meta http-equiv='refresh' content='2'></head><body>";
    html += "<h1>AtomS3 RNDIS Router</h1>";
    html += "<h2>Status</h2>";
    html += "<p>Local IP: " + s_local_ip.toString() + "</p>";
    html += "<p>WiFi: " + String(s_wifi_connected ? "Connected" : "Disconnected") + "</p>";
    html += "<p>USB Device: " + String(s_dev_hdl ? "Connected" : "Disconnected") + "</p>";
    if (s_dev_hdl) {
        html += "<p>COMM IF: " + String(s_comm_itf_num) + "</p>";
        html += "<p>DATA IF: " + String(s_data_itf_num) + "</p>";
        html += "<p>Endpoints - IN: 0x" + String(s_ep_in, HEX) + " OUT: 0x" + String(s_ep_out, HEX) + "</p>";
    }
    html += "<h2>Log</h2>";
    html += "<button type='button' onclick='copyLog()' style='margin-bottom:8px; padding:6px 10px;'>ログをクリップボードにコピー</button>";
    html += "<span id='copyStatus' style='margin-left:8px; font-size:12px; color:#444;'></span>";
    html += "<textarea id='logBox' style='width:100%; height:400px; font-family:monospace; font-size:12px;' readonly>";
    html += s_log_buffer;
    html += "</textarea>";
    html += "<script>";
    html += "function copyLog(){";
    html += "var box=document.getElementById('logBox');";
    html += "var status=document.getElementById('copyStatus');";
    html += "if(!box||!status)return;";
    html += "var text=box.value;";
    html += "if(navigator.clipboard&&window.isSecureContext){";
    html += "navigator.clipboard.writeText(text).then(function(){status.textContent='コピーしました';},function(){fallbackCopy(box,status);});";
    html += "}else{";
    html += "fallbackCopy(box,status);";
    html += "}";
    html += "}";
    html += "function fallbackCopy(box,status){";
    html += "box.focus();box.select();box.setSelectionRange(0,box.value.length);";
    html += "try{document.execCommand('copy');status.textContent='コピーしました';}";
    html += "catch(e){status.textContent='コピー失敗: 手動コピーしてください';}";
    html += "}";
    html += "</script>";
    html += "</body></html>";

    // HTTP ヘッダ送信
    client.println("HTTP/1.1 200 OK");
    client.println("Content-Type: text/html; charset=UTF-8");
    client.println("Content-Length: " + String(html.length()));
    client.println("Connection: close");
    client.println();
    client.println(html);
    client.stop();
}

// ─── setup / loop ─────────────────────────────────────────────────────────────

void setup() {
    auto m5cfg = M5.config();
    M5.begin(m5cfg);                    // M5Unified 初期化 (ディスプレイ・ボタン等)
    M5.Display.setRotation(1);          // 横向き表示
    M5.Display.setTextSize(1);
    M5.Display.setTextColor(WHITE);

    // 1. WiFi 接続 (WAN)
    M5.Display.println("WiFi...");
    WiFi.begin(WIFI_SSID, WIFI_PASS);
    while (WiFi.status() != WL_CONNECTED) { delay(500); M5.Display.print("."); }
    M5.Display.println("\nOK");
    s_local_ip = WiFi.localIP();
    s_wifi_connected = true;
    IPAddress s_dns_ip = WiFi.dnsIP(); // Wi-Fi プライマリ DNS を取得
    s_upstream_dns_ip = s_dns_ip;

    // HTTP サーバー開始
    server.begin();
    M5.Display.println("HTTP server started");
    log_append("HTTP server: mode=%s", s_http_rndis_only ? "RNDIS only" : "all interfaces");

    // 2. esp-netif (DHCP サーバ, 192.168.37.1)
    s_usb_netif = create_usb_netif();
    M5.Display.printf("HTTP: http://%d.%d.%d.%d/\n",
                      ip4_addr1(&s_usb_ip.ip), ip4_addr2(&s_usb_ip.ip),
                      ip4_addr3(&s_usb_ip.ip), ip4_addr4(&s_usb_ip.ip));
    static UsbDriver drv;
    drv.base.post_attach = usb_driver_post_attach;
    drv.base.netif       = nullptr;
    esp_netif_attach(s_usb_netif, &drv);

    // DHCP サーバーを明示的に開始
    M5.Display.println("Starting DHCP server...");
    log_append("esp_netif: creating USB interface");

    esp_netif_dhcps_stop(s_usb_netif); // 一度停止
    log_append("esp_netif: stopped DHCP server");

    esp_err_t ret = esp_netif_set_ip_info(s_usb_netif, &s_usb_ip);
    log_append("esp_netif: set_ip_info ret=%d (IP=%d.%d.%d.%d)", ret,
               ip4_addr1(&s_usb_ip.ip), ip4_addr2(&s_usb_ip.ip),
               ip4_addr3(&s_usb_ip.ip), ip4_addr4(&s_usb_ip.ip));

    // DHCP で DNS サーバーに AtomS3 自身を通知する（DNSリレー前提）  // RNDIS側は常に本機へ問い合わせ
    log_append("DHCP DNS: %d.%d.%d.%d (AtomS3)",
               ip4_addr1(&s_usb_ip.ip), ip4_addr2(&s_usb_ip.ip), ip4_addr3(&s_usb_ip.ip), ip4_addr4(&s_usb_ip.ip));
    esp_netif_dns_info_t dns_info = {};
    dns_info.ip.u_addr.ip4 = s_usb_ip.ip;
    dns_info.ip.type = ESP_IPADDR_TYPE_V4;
    esp_netif_set_dns_info(s_usb_netif, ESP_NETIF_DNS_MAIN, &dns_info);

    ret = esp_netif_dhcps_start(s_usb_netif);
    log_append("esp_netif: dhcps_start ret=%d", ret);

    if (ret == ESP_OK) {
        M5.Display.println("DHCP server started");
        log_append("DHCP server: ready at %d.%d.%d.%d",
               ip4_addr1(&s_usb_ip.ip), ip4_addr2(&s_usb_ip.ip),
               ip4_addr3(&s_usb_ip.ip), ip4_addr4(&s_usb_ip.ip));

        // ─ DNS リレーサーバー設定 ─
        // RNDIS側からのDNS問い合わせをWi-Fi側に転送するよう lwIP を設定
        ip_addr_t upstream_dns;
        IP_ADDR4(&upstream_dns, s_dns_ip[0], s_dns_ip[1], s_dns_ip[2], s_dns_ip[3]);
        dns_setserver(0, &upstream_dns);
        log_append("DNS relay: upstream DNS set to %d.%d.%d.%d",
                   s_dns_ip[0], s_dns_ip[1], s_dns_ip[2], s_dns_ip[3]);
        log_append("DNS relay: task deferred until RNDIS ready");
    
    } else {
        M5.Display.println("DHCP server START FAILED");
        log_append("ERROR: DHCP server failed to start!");
    }

    // 3. セマフォ・キュー初期化
    s_ctrl_sem = xSemaphoreCreateBinary();         // コントロール転送完了待ち用
    s_tx_sem   = xSemaphoreCreateBinary();         // TX 完了待ち用
    xSemaphoreGive(s_tx_sem);                      // 初期状態は「送信可能」
    s_dev_queue = xQueueCreate(2, sizeof(uint8_t)); // デバイスアドレス通知キュー (深さ2)

    // 4. TX 転送バッファ（デバイス接続前に確保しておく）
    usb_host_transfer_alloc(BULK_BUF_SIZE, 0, &s_tx_xfer); // 送信バッファを事前確保
    s_tx_xfer->callback   = tx_callback;
    s_tx_xfer->context    = nullptr;
    s_tx_xfer->timeout_ms = 2000;

    // 5. USB ホストライブラリ起動
    const usb_host_config_t host_cfg = {
        .skip_phy_setup      = false,         // USB PHY を初期化する
        .root_port_unpowered = false,          // ルートポートに電源を供給する
        .intr_flags          = ESP_INTR_FLAG_LEVEL1, // 割り込み優先度レベル 1
    };
    usb_host_install(&host_cfg);

    // 6. タスク起動 (優先度は USB 低レベル > クライアント > セットアップ の順)
    xTaskCreate(usb_host_lib_task,  "usb_lib",    4096, nullptr,
                configMAX_PRIORITIES - 1, nullptr); // USB ホストデーモン (最高優先度)
    xTaskCreate(rndis_client_task,  "usb_cli",    4096, nullptr,
                configMAX_PRIORITIES - 2, nullptr); // USB クライアント (接続/切断イベント)
    xTaskCreate(rndis_setup_task,   "rndis_stup", 8192, nullptr,
                configMAX_PRIORITIES - 3, nullptr); // RNDIS ハンドシェイク (スタック大きめ)

    M5.Display.fillScreen(BLACK);
    M5.Display.setCursor(0, 0);
    M5.Display.setTextColor(YELLOW);
    M5.Display.println("Waiting USB...");
    M5.Display.setTextColor(WHITE);
    M5.Display.print("WLAN: ");
    M5.Display.println(WiFi.localIP());
}

void loop() {
    M5.update(); // ボタン状態を更新 (現在は使用していないが将来の拡張用)

    // HTTP クライアントが接続していれば処理する (ブロッキングなし)
    WiFiClient client = server.available();
    if (client) {
        handle_http_client(client); // ステータス・ログを HTML で返す
    }

    delay(10); // CPU 独占を防ぐための短いウェイト
}
