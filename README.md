# Hajimi - 轻量级 C++ 内网穿透 / 反向代理

QUIC 异步 wrapper 可通过 `xmake f -m debug --quic=y` 开启，默认关闭。
接口与使用示例见 [src/quic/README.md](src/quic/README.md)。

基于 C++23 协程与 ilias 异步运行时实现的反向代理工具，支持内网穿透、WebUI 可视化管理、动态端口转发与 DuckDNS 动态域名。

## 🌟 特性
- **单连接多路复用**：客户端与服务端之间仅一条 TCP 长连接，控制消息与所有隧道数据通过 `streamId` 复用。
- **隧道级流控**：每条隧道独立发送窗口（初始 128KB）+ `WindowUpdate`，慢连接不会拖垮其他隧道。
- **零拷贝友好的缓冲区**：仿 `sk_buff` 的分级内存池 `SkBuffer`，预留 headroom 直接拼帧头。
- **WebUI 控制台**：查看在线客户端、延迟、实时上下行速率，动态新增 / 删除转发规则；页面编译进可执行文件。
- **DuckDNS 自动更新**：定期更新域名记录，IPv4 由 DuckDNS 自动识别，探测到公网 IPv6 时一并更新。
- **IPv6 / 双栈**：转发端口默认监听 `[::]`；客户端连接支持域名、IPv4、`[ipv6]:port`，并采用 Happy Eyeballs 方式多地址连接。
- **健壮性**：客户端断线自动重连（退避 2s → 15s，每次重连重新解析 DNS）

---

## 🔨 构建

依赖 [xmake](https://xmake.io) 与支持 C++23 的编译器，第三方库（ilias、nlohmann_json、argparse）由 xmake 自动拉取。

```bash
xmake f -m release
xmake
# 产物: build/<plat>/<arch>/release/hajimi
```

---

## 🚀 使用方法

### 1. 启动服务端 (Server)

```bash
# WebUI 默认监听 127.0.0.1 的随机端口，实际端口见启动日志 "[WebUi] listen on ..."
hajimi --listen "0.0.0.0:8000"

# 指定 WebUI 地址
hajimi --listen "0.0.0.0:8000" --webui "127.0.0.1:9000"
```

**启用 DuckDNS：**
```bash
hajimi --listen "[::]:8000" --duckdns "myhome:a7c4d0ad-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# 或分开指定，并修改更新间隔（秒，默认 300）
hajimi --listen "[::]:8000" --duckdns-domain "myhome" --duckdns-token "a7c4d0ad-..." --duckdns-interval 600
```

### 2. 启动内网客户端 (Client)

```bash
hajimi --connect "myhome.duckdns.org:8000" --name "mypc"
hajimi --connect "1.2.3.4:8000" --name "mypc"
hajimi --connect "[2001:db8::1]:8000" --name "mypc"
```
> `--name` 默认为 `hajimi-client`，同名客户端只能同时在线一个。

启用 QUIC 后，客户端默认验证服务端的证书链和域名；Windows 会额外使用系统根证书库。公共 CA 证书可直接连接，服务端的 `--cert-file` 应提供包含中间证书的完整链（如 Let's Encrypt 的 `fullchain.pem`）。

```bash
# Force QUIC to verify the connection without a TCP fallback
hajimi --connect "myhome.duckdns.org:8000" --transport quic

# Trust a private CA or a self-signed certificate
hajimi --connect "myhome.duckdns.org:8000" --transport quic --ca-file "ca.pem"
```

`--insecure` 可显式关闭证书验证

### 3. 配置反向代理转发

打开 WebUI，填写：
1. **服务端监听端口**：服务端对外开放的端口，如 `666`
2. **目标客户端**：已上线的客户端，如 `mypc`
3. **客户端内网地址**：默认 `127.0.0.1`
4. **客户端目标端口**：客户端本地服务端口，如 `1145`

之后访问 `server_ip:666` 的 TCP 流量会被转发到 `mypc` 的 `127.0.0.1:1145`。

也可以直接调用 HTTP API：

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/api/status` | 服务端状态、客户端、规则、DuckDNS 信息 |
| `POST` | `/api/rules` | 新增规则 `{"listen_port":666,"client_name":"mypc","target_host":"127.0.0.1","target_port":1145}` |
| `DELETE` | `/api/rules` | 删除规则 `{"listen_port":666}` |

> ⚠️ WebUI 无鉴权，请勿暴露到公网。

---

## 📁 项目结构

| 文件 | 说明 |
|---|---|
| `src/main.cpp` | 命令行入口 |
| `src/protocol.hpp` | 线协议：`u16 长度 + u8 类型 + payload` |
| `src/skbuf.hpp` | Socket 缓冲区与内存池 |
| `src/server.cpp` | 服务端：会话管理、转发规则、隧道 |
| `src/client.cpp` | 客户端：注册、重连、本地转发 |
| `src/web.cpp` / `static/index.html` | WebUI 与 HTTP API |
| `src/duckdns.cpp` | DuckDNS 更新 |
| `src/utils.hpp` | ScopeExit、Happy Eyeballs 连接 |
