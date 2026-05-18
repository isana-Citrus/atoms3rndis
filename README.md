# Wi-FiをRNDIS経由でbrainにつなげるやつ
AtomS3(ESP32S3)を使いbrainをwi-fiにつなげるためのAtomS3側コードです  
PCの代わりにAtomS3にインターネット共有をやらせようというもくろみです  
コパイロット君にほぼ言われるがままのコードなのでその点はご了承おば  
PW-SH7/PW-SH2それぞれのWinCE/Brainuxで動作確認してますので大体は動くと思います  

## 用意するもの
- [M5Stack Atom S3](https://docs.m5stack.com/ja/core/AtomS3) - S3Lite,S3Rや他のESP32S3系でも動くかも(要コード改編)
- USB Type-A to C ケーブル (Atom S3にプログラム書き込み用なのでパソコンとがつなげられればなんでもよい)
- VSCode + PlatformIO拡張機能 が機能してAtomS3に書き込みができるPC

- brainとAtomS3をつなぐためのケーブルとか どっちかのパターンを用意してください
  1. AtomS3底面に直で電源供給する場合
     - [USB Type-C to MicroUSBケーブル](https://www.biccamera.com/bc/item/8329484/) OTGケーブルじゃなくてよい
     - Atom S3に5Vを供給するための何か (S3底面に5VとGNDがあるのでそこに5Vをかけられる何かしらの機械)
  1. 電源別供給できるOTGケーブル的な奴と普通のUSBAtoCを使う場合(これでもいけた)
     - [【ノーブランド品】OTGケーブル micro USB-USB A メス Galaxy/NOTE/スマホ用 OTGケーブル USB機器給電端子付](https://www.amazon.co.jp/dp/B00S5PRILQ)とか類似品

## 注意点
- Atom S3の底面から5V入れるとそのままUSB端子に5V出てきます、PC接続前に必ずAtomS3底面電源供給は**切断**する事
- 2.4 GHz Wi-Fi (IEEE 802.11b/g/n)にのみ接続できます(ESP32S3側のHW制約)
- このプログラムを書き込むとAtomS3のUSB端子は強制OTGモードになるため、プログラムを書き込みなおしたいときはAtomS3をダウンロードモードで起動する必要があります
- brain側は事前にRNDISを使えるようにセットアップしておく必要があります

## build&デプロイ環境
- VSCode + PlatformIO拡張機能 が必要です。左記をセットアップ後ダウンロードしたフォルダを開くとPlatformIOが`platformio.ini`を認識して自動的にbuildデプロイ環境を整えてくれるはずです(時間かかります)
- フォルダをVSCodeで開いてPlatformIO拡張機能側の準備が終わると、AtomS3をPCと接続すれば書き込めると思います

## 動作確認
1. [WinCEの場合](https://brain.fandom.com/ja/wiki/Web%E3%83%96%E3%83%A9%E3%82%A6%E3%82%B6#%E3%82%A4%E3%83%B3%E3%82%BF%E3%83%BC%E3%83%8D%E3%83%83%E3%83%88%E6%8E%A5%E7%B6%9A) or [Brainuxの場合](https://wiki.brainux.org/tips/usb-ethernet-gadget/#usb-%E3%82%B3%E3%83%B3%E3%83%88%E3%83%AD%E3%83%BC%E3%83%A9%E3%81%AE%E5%8B%95%E4%BD%9C%E3%83%A2%E3%83%BC%E3%83%89%E3%82%92%E5%A4%89%E6%9B%B4%E3%81%99%E3%82%8B)をやったBrainを用意
1. include/secret_sample.h を inculde/secret.h にコピーリネームして、接続したいWi-Fiの情報を入力
1. build&デプロイする(platformio的にはuploadボタン)
1. PCとAtomS3の接続を切って、AtomS3底面に5Vを供給する or OTGケーブル経由で電源供給する
1. 画面にWLAN IP(wi-fi側のIP)が表示されたらWiFi接続完了です
1. brainと繋ぎましょう

## その他
- Wi-Fi側とRNDIS側のIPアドレス体系が被ると通信できないので`IP4_ADDR(&s_usb_ip.ip,      192, 168, 37, 1);`周りの値をうまい具合に変えるといけるはず。(デフォルトは192.168.37.1/24)
- AtomS3でデバッグ用にhttpサーバーが動いています。デフォルトは一応RNDIS側のみアクセス。`static bool s_http_rndis_only = true;`を変えるとWLANからもアクセスできます。
- AtomS3はUSB切断を検知すると自動リセットするようになってます(現状USB側の再接続できない)
- コードの利用は自己責任で…

## 改変するなら・・・
- せっかくhttpサーバー動いてるからbrain側からPOSTして接続先SSIDとか変えられるようにできそう
- NAT周りをいじったらWLANからBrain側への通信もできるようになりそう