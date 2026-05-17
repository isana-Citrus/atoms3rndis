/*
 * AtomS3 RNDIS Host – Wi-Fi ルーター
 *
 * AtomS3 が USB ホストとして動作し、接続した RNDIS デバイス（brainnet 等）に
 * DHCP で IP を配布しつつ、WiFi 経由でインターネットに接続させる。
 *
 * LAN : USB RNDIS  – 192.168.4.1  (DHCP サーバ)
 * WAN : WiFi STA   – DHCP で動的 IP 取得
 * NAT : ip_napt_enable で LAN→WAN を NAT
 *
 * 物理接続:
 *   AtomS3 USB-C → (USB-C to Micro-USB B ケーブル) → brainnet の Micro-USB 端子
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
#define RNDIS_PACKET_MSG        0x00000001u
#define RNDIS_INITIALIZE_MSG    0x00000002u
#define RNDIS_QUERY_MSG         0x00000004u
#define RNDIS_SET_MSG           0x00000005u
#define RNDIS_INITIALIZE_CMPLT  0x80000002u
#define RNDIS_QUERY_CMPLT       0x80000004u
#define RNDIS_SET_CMPLT         0x80000005u
#define RNDIS_STATUS_SUCCESS    0x00000000u

// OID
#define OID_GEN_SUPPORTED_LIST          0x00010101u
#define OID_GEN_HARDWARE_STATUS         0x00010102u
#define OID_GEN_MEDIA_SUPPORTED         0x00010103u
#define OID_GEN_MEDIA_IN_USE            0x00010104u
#define OID_GEN_MAXIMUM_FRAME_SIZE      0x00010106u
#define OID_GEN_MAXIMUM_TOTAL_SIZE      0x00010111u
#define OID_802_3_PERMANENT_ADDRESS     0x01010101u
#define OID_GEN_CURRENT_PACKET_FILTER   0x0001010Eu
#define NDIS_PACKET_FILTER_ALL          0x0000000Fu

// ─── RNDIS メッセージ構造体 ─────────────────────────────────────────────────

#pragma pack(push, 1)

typedef struct {
    uint32_t MessageType, MessageLength, RequestId;
    uint32_t MajorVersion, MinorVersion, MaxTransferSize;
} rndis_init_msg_t;

typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Status;
    uint32_t MajorVersion, MinorVersion, DeviceFlags, Medium;
    uint32_t MaxPacketsPerTransfer, MaxTransferSize, PacketAlignmentFactor;
    uint32_t AFListOffset, AFListSize;
} rndis_init_cmplt_t;

typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Oid;
    uint32_t InformationBufferLength, InformationBufferOffset, Reserved;
} rndis_query_msg_t;

typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Status;
    uint32_t InformationBufferLength, InformationBufferOffset;
} rndis_query_cmplt_t;

typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Oid;
    uint32_t InformationBufferLength, InformationBufferOffset, Reserved;
} rndis_set_msg_t;

typedef struct {
    uint32_t MessageType, MessageLength, RequestId, Status;
} rndis_set_cmplt_t;

typedef struct {
    uint32_t MessageType, MessageLength;
    uint32_t DataOffset, DataLength;
    uint32_t OOBDataOffset, OOBDataLength, NumOOBDataElements;
    uint32_t PerPacketInfoOffset, PerPacketInfoLength;
    uint32_t VcHandle, Reserved;
} rndis_packet_msg_t;

#pragma pack(pop)

// DataOffset=36 → ヘッダ先頭から 44 バイト後にデータ (8 + 36)
#define RNDIS_PACKET_HEADER_SIZE  44u
#define RNDIS_DATA_OFFSET         36u
#define BULK_BUF_SIZE             2048u

// ─── グローバル状態 ───────────────────────────────────────────────────────────

static WiFiServer server(80);
static IPAddress s_local_ip;
static bool s_wifi_connected = false;
static bool s_http_rndis_only = true;
static IPAddress s_upstream_dns_ip;
static volatile uint32_t s_dns_query_count = 0;
static volatile uint32_t s_dns_reply_count = 0;
static volatile uint32_t s_dns_timeout_count = 0;
static bool s_dns_task_started = false;

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

static usb_host_client_handle_t s_client_hdl = NULL;
static usb_device_handle_t      s_dev_hdl    = NULL;
static uint8_t  s_comm_itf_num = 0xFF;  // CDC Comm interface
static uint8_t  s_data_itf_num = 0xFF;  // CDC Data interface
static uint8_t  s_comm_itf_alt = 0;     // CDC Comm alternate setting
static uint8_t  s_data_itf_alt = 0;     // CDC Data alternate setting
static uint8_t  s_ep_out       = 0;
static uint8_t  s_ep_in        = 0;
static uint16_t s_ep_out_mps   = 0;
static uint16_t s_ep_in_mps    = 0;
static uint32_t s_req_id       = 1;

static esp_netif_t *s_usb_netif = nullptr;
static esp_netif_ip_info_t s_usb_ip;

// lwIP タスクコンテキストで NAPT を有効化するコールバック
static esp_err_t enable_napt_cb(void *) {
    ip_napt_enable(s_usb_ip.ip.addr, 1);
    return ESP_OK;
}

typedef enum {
    RNDIS_STATE_IDLE,
    RNDIS_STATE_CONNECTED,
    RNDIS_STATE_INITIALIZED,
    RNDIS_STATE_READY,
    RNDIS_STATE_ERROR,
} rndis_state_t;
static volatile rndis_state_t s_rndis_state = RNDIS_STATE_IDLE;

// コントロール転送完了通知用セマフォ
static SemaphoreHandle_t s_ctrl_sem = NULL;
// TX 完了通知用セマフォ
static SemaphoreHandle_t s_tx_sem   = NULL;
// Bulk IN/OUT 転送バッファ
static usb_transfer_t *s_rx_xfer = NULL;
static usb_transfer_t *s_tx_xfer = NULL;
// Interrupt IN 転送バッファ（RNDIS 状態通知）
static usb_transfer_t *s_intr_xfer = NULL;
static uint8_t s_ep_intr = 0;  // Interrupt IN エンドポイント
// デバイス接続通知キュー（アドレスを event callback → setup task に渡す）
static QueueHandle_t s_dev_queue = NULL;

static void dns_relay_task(void *arg) {
    (void)arg;

    const int kDnsPort = 53;
    uint8_t *query_buf = (uint8_t *)malloc(1232);
    uint8_t *reply_buf = (uint8_t *)malloc(1232);
    if (!query_buf || !reply_buf) {
        log_append("DNS relay: buffer alloc failed");
        free(query_buf);
        free(reply_buf);
        vTaskDelete(nullptr);
        return;
    }

    int listen_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
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
    listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);
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

static void ctrl_xfer_cb(usb_transfer_t *xfer) {
    // USB host daemon task から呼ばれる（ISR ではない）
    xSemaphoreGive(s_ctrl_sem);
}

static esp_err_t ctrl_out(uint8_t bRequest, const void *data, uint16_t len) {
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
    s->bmRequestType = 0x21; // Host→Device, Class, Interface
    s->bRequest      = bRequest;
    s->wValue        = 0;
    s->wIndex        = s_comm_itf_num;
    s->wLength       = len;
    memcpy(xfer->data_buffer + sizeof(usb_setup_packet_t), data, len);

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
        ret = ESP_FAIL;
    } else if (xfer->status != USB_TRANSFER_STATUS_COMPLETED) {
        log_append("ctrl_out: status=%d", xfer->status);
        ret = ESP_FAIL;
    }
    usb_host_transfer_free(xfer);
    return ret;
}

static esp_err_t ctrl_in(uint8_t bRequest, void *buf, uint16_t buf_len,
                          uint16_t *actual_len) {
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
    s->bmRequestType = 0xA1; // Device→Host, Class, Interface
    s->bRequest      = bRequest;
    s->wValue        = 0;
    s->wIndex        = s_comm_itf_num;
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
        usb_host_transfer_free(xfer);
        return ESP_ERR_TIMEOUT;
    }
    if (xfer->status != USB_TRANSFER_STATUS_COMPLETED) {
        log_append("ctrl_in status=%d", xfer->status);
        usb_host_transfer_free(xfer);
        return ESP_FAIL;
    }
    int resp = xfer->actual_num_bytes - (int)sizeof(usb_setup_packet_t);
    if (resp > 0 && resp <= buf_len) {
        memcpy(buf, xfer->data_buffer + sizeof(usb_setup_packet_t), resp);
        *actual_len = (uint16_t)resp;
    }
    usb_host_transfer_free(xfer);
    return ESP_OK;
}

// ─── ディスクリプタ解析 ──────────────────────────────────────────────────────
//
// クラスコードに依存せず、エンドポイントの種類でインターフェースを識別する:
//   Interrupt IN あり → RNDIS コントロールインターフェース
//   Bulk IN + Bulk OUT → RNDIS データインターフェース

static bool parse_config_desc(void) {
    const usb_config_desc_t *cfg = NULL;
    esp_err_t err = usb_host_get_active_config_descriptor(s_dev_hdl, &cfg);
    if (err != ESP_OK || !cfg) {
        M5.Display.printf("get_desc:%d\n", err);
        return false;
    }

    M5.Display.printf("nItf:%d len:%d\n", cfg->bNumInterfaces, cfg->wTotalLength);

    const uint8_t *p   = (const uint8_t *)cfg;
    const uint8_t *end = p + cfg->wTotalLength;

    uint8_t  cur_itf     = 0xFF;
    uint8_t  cur_alt     = 0;
    uint8_t  cur_class   = 0;
    bool     cur_intr_in = false; // Interrupt IN あり
    uint8_t  tmp_ep_intr = 0;     // Interrupt IN エンドポイント
    bool     cur_bulk_in = false;
    bool     cur_bulk_out= false;
    uint8_t  tmp_ep_in   = 0;
    uint8_t  tmp_ep_out  = 0;

    auto commit_itf = [&]() {
        if (cur_itf == 0xFF) return;
        M5.Display.printf(" Itf%d alt%d cl:%02X %s%s%s\n",
                          cur_itf, cur_alt, cur_class,
                          cur_intr_in  ? "I" : "-",
                          cur_bulk_in  ? "i" : "-",
                          cur_bulk_out ? "o" : "-");
        // Interrupt IN だけある = コントロールインターフェース
        if (cur_intr_in && s_comm_itf_num == 0xFF) {
            s_comm_itf_num = cur_itf;
            s_comm_itf_alt = cur_alt;
            s_ep_intr = tmp_ep_intr;
        }
        // Bulk IN + Bulk OUT = データインターフェース
        if (cur_bulk_in && cur_bulk_out && s_data_itf_num == 0xFF) {
            s_data_itf_num = cur_itf;
            s_data_itf_alt = cur_alt;
            s_ep_in  = tmp_ep_in;
            s_ep_out = tmp_ep_out;
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
    log_append("EP MPS - IN:%d OUT:%d", s_ep_in_mps, s_ep_out_mps);

    return (s_data_itf_num != 0xFF && s_ep_in != 0 && s_ep_out != 0);
}

// ─── RNDIS ハンドシェイク ─────────────────────────────────────────────────────

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

static uint32_t s_rx_count = 0;
static uint32_t s_rx_bytes = 0;

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

static void rx_callback(usb_transfer_t *xfer) {
    if (xfer->status == USB_TRANSFER_STATUS_COMPLETED) {
        s_rx_bytes += xfer->actual_num_bytes;
        s_rx_count++;
        if (xfer->actual_num_bytes > 0 && (s_rx_count % 512 == 1))
            log_append("RX[%d]: bytes=%d total=%d", s_rx_count, xfer->actual_num_bytes, s_rx_bytes);

        if (xfer->actual_num_bytes > (int)RNDIS_PACKET_HEADER_SIZE) {
            const rndis_packet_msg_t *hdr = (const rndis_packet_msg_t *)xfer->data_buffer;
            if (hdr->MessageType == RNDIS_PACKET_MSG) {
                uint32_t off = 8 + hdr->DataOffset; // &DataOffset からのオフセット
                uint32_t dlen = hdr->DataLength;
                if (off + dlen <= (uint32_t)xfer->actual_num_bytes && s_usb_netif) {
                    void *buf = malloc(dlen);
                    if (buf) {
                        memcpy(buf, xfer->data_buffer + off, dlen);
                        uint16_t src_port = 0, dst_port = 0;
                        if (parse_ipv4_udp_ports((const uint8_t *)buf, dlen, &src_port, &dst_port)) {
                            if ((src_port == 67 || src_port == 68) && (dst_port == 67 || dst_port == 68)) {
                            log_append("RX: DHCP frame len=%d %d->%d", dlen, src_port, dst_port);
                            }
                            if (src_port == 53 || dst_port == 53) {
                                log_append("RX: DNS frame len=%d %d->%d", dlen, src_port, dst_port);
                            }
                        }
                        esp_err_t nret = esp_netif_receive(s_usb_netif, buf, dlen, buf);
                        if (nret != ESP_OK) {
                            log_append("RX: esp_netif_receive failed ret=%d", nret);
                            free(buf);
                        }
                    }
                }
            }
        }
    } else {
        log_append("RX error: status=%d bytes=%d", xfer->status, xfer->actual_num_bytes);
    }
    if (s_rndis_state == RNDIS_STATE_READY)
        usb_host_transfer_submit(xfer); // エラー時も次の受信を再投入
}

static void tx_callback(usb_transfer_t *xfer) {
    xSemaphoreGive(s_tx_sem);
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

static void start_rx(void) {
    if (s_rx_xfer == NULL) {
        esp_err_t ret = usb_host_transfer_alloc(BULK_BUF_SIZE, 0, &s_rx_xfer);
        log_append("RX alloc: ret=%d ptr=%p", ret, s_rx_xfer);
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
    s_rx_xfer->num_bytes        = BULK_BUF_SIZE;

    log_append("RX submit: dev=%p ep=0x%02X (state=%d)", s_dev_hdl, s_ep_in, s_rndis_state);
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
    }
}

// ─── RNDIS セットアップタスク（event callback とは別タスクで実行） ────────────

static void rndis_setup_task(void *arg) {
    uint8_t dev_addr;
    for (;;) {
        if (xQueueReceive(s_dev_queue, &dev_addr, portMAX_DELAY) != pdTRUE) continue;

        // デバイスオープン
        if (usb_host_device_open(s_client_hdl, dev_addr, &s_dev_hdl) != ESP_OK) {
            M5.Display.println("Open fail");
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
            cleanup_rndis_device(false);
            continue;
        }

        // インターフェース Claim
        esp_err_t ret_comm = usb_host_interface_claim(s_client_hdl, s_dev_hdl, s_comm_itf_num, s_comm_itf_alt);
        esp_err_t ret_data = usb_host_interface_claim(s_client_hdl, s_dev_hdl, s_data_itf_num, s_data_itf_alt);
        log_append("Claim COMM(if=%d alt=%d): ret=%d", s_comm_itf_num, s_comm_itf_alt, ret_comm);
        log_append("Claim DATA(if=%d alt=%d): ret=%d", s_data_itf_num, s_data_itf_alt, ret_data);
        if (ret_comm != ESP_OK || ret_data != ESP_OK) {
            M5.Display.setTextColor(RED);
            M5.Display.println("Itf claim fail");
            M5.Display.setTextColor(WHITE);
            s_rndis_state = RNDIS_STATE_ERROR;
            cleanup_rndis_device(false);
            continue;
        }

        s_rndis_state = RNDIS_STATE_CONNECTED;
        M5.Display.println("Itf claimed");

        // RNDIS INITIALIZE
        if (!rndis_initialize()) {
            M5.Display.setTextColor(RED);
            M5.Display.println("RNDIS INIT fail");
            M5.Display.setTextColor(WHITE);
            s_rndis_state = RNDIS_STATE_ERROR;
            cleanup_rndis_device(false);
            continue;
        }
        s_rndis_state = RNDIS_STATE_INITIALIZED;
        M5.Display.println("INIT OK");

        // MAC 取得（Windows CE RNDISはQUERY_MACをサポートしていない可能性があるのでスキップ）
        uint8_t client_mac[6] = {0x02, 0x50, 0xF2, 0x00, 0x00, 0x01}; // Default RNDIS MAC
        log_append("RNDIS MAC: using default %02X:%02X:%02X:%02X:%02X:%02X",
                   client_mac[0], client_mac[1], client_mac[2], client_mac[3], client_mac[4], client_mac[5]);

        // パケットフィルタ設定
        if (!rndis_set_filter()) {
            M5.Display.setTextColor(RED);
            M5.Display.println("Filter fail");
            M5.Display.setTextColor(WHITE);
            log_append("RNDIS SET_FILTER failed");
            s_rndis_state = RNDIS_STATE_ERROR;
            cleanup_rndis_device(false);
            continue;
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

static void client_event_cb(const usb_host_client_event_msg_t *msg, void *arg) {
    if (msg->event == USB_HOST_CLIENT_EVENT_NEW_DEV) {
        M5.Display.println("Dev connected");
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

static void usb_host_lib_task(void *arg) {
    for (;;) {
        uint32_t flags;
        usb_host_lib_handle_events(portMAX_DELAY, &flags);
        if (flags & USB_HOST_LIB_EVENT_FLAGS_NO_CLIENTS)
            usb_host_device_free_all();
    }
}

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

static void usb_driver_free_rx_buf(void *h, void *buf) { free(buf); }

static esp_err_t usb_driver_transmit(void *h, void *buf, size_t len) {
    if (s_rndis_state != RNDIS_STATE_READY || !s_tx_xfer || !s_dev_hdl) {
        log_append("TX drop: state=%d tx=%p dev=%p", s_rndis_state, s_tx_xfer, s_dev_hdl);
        return ESP_ERR_INVALID_STATE;
    }
    if (len > BULK_BUF_SIZE - RNDIS_PACKET_HEADER_SIZE) {
        log_append("TX drop: oversize len=%d", (int)len);
        return ESP_ERR_INVALID_ARG;
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
    esp_err_t tret = usb_host_transfer_submit(s_tx_xfer);
    if (tret != ESP_OK) {
        log_append("TX submit failed: ret=%d ep=0x%02X bytes=%d", tret, s_ep_out, s_tx_xfer->num_bytes);
        xSemaphoreGive(s_tx_sem);
        return tret;
    }
    return ESP_OK;
}

struct UsbDriver { esp_netif_driver_base_t base; };

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
    IP4_ADDR(&s_usb_ip.ip,      192, 168, 37, 1);
    IP4_ADDR(&s_usb_ip.gw,      192, 168, 37, 1);
    IP4_ADDR(&s_usb_ip.netmask, 255, 255, 255, 0);

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
    M5.begin(m5cfg);
    M5.Display.setRotation(1);
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

    // 2. esp-netif (DHCP サーバ, 192.168.4.1)
    s_usb_netif = create_usb_netif();
    M5.Display.printf("HTTP: http://192.168.4.1/\n");
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
    log_append("esp_netif: set_ip_info ret=%d (IP=192.168.4.1)", ret);

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
        log_append("DHCP server: ready at 192.168.4.1");

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
    s_ctrl_sem = xSemaphoreCreateBinary();
    s_tx_sem   = xSemaphoreCreateBinary();
    xSemaphoreGive(s_tx_sem);
    s_dev_queue = xQueueCreate(2, sizeof(uint8_t));

    // 5. TX 転送バッファ（デバイス接続前に確保しておく）
    usb_host_transfer_alloc(BULK_BUF_SIZE, 0, &s_tx_xfer);
    s_tx_xfer->callback   = tx_callback;
    s_tx_xfer->context    = nullptr;
    s_tx_xfer->timeout_ms = 2000;

    // 6. USB ホストライブラリ起動
    const usb_host_config_t host_cfg = {
        .skip_phy_setup      = false,
        .root_port_unpowered = false,
        .intr_flags          = ESP_INTR_FLAG_LEVEL1,
    };
    usb_host_install(&host_cfg);

    // 7. タスク起動
    // USB ホストデーモン (最高優先度, USB ハードウェアイベント処理)
    xTaskCreate(usb_host_lib_task,  "usb_lib",   4096, nullptr,
                configMAX_PRIORITIES - 1, nullptr);
    // USB クライアント (デバイス接続/切断イベント)
    xTaskCreate(rndis_client_task,  "usb_cli",   4096, nullptr,
                configMAX_PRIORITIES - 2, nullptr);
    // RNDIS ハンドシェイク (ブロッキング処理を安全に実行)
    xTaskCreate(rndis_setup_task,   "rndis_stup", 8192, nullptr,
                configMAX_PRIORITIES - 3, nullptr);

    M5.Display.fillScreen(BLACK);
    M5.Display.setCursor(0, 0);
    M5.Display.setTextColor(YELLOW);
    M5.Display.println("Waiting USB...");
    M5.Display.setTextColor(WHITE);
    M5.Display.print("WLAN: ");
    M5.Display.println(WiFi.localIP());
}

void loop() {
    M5.update();

    // HTTP クライアント処理
    WiFiClient client = server.available();
    if (client) {
        handle_http_client(client);
    }

    delay(10);
}
