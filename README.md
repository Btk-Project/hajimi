# Hajimi - 轻量级 Python Asyncio 网络反向代理

基于 Python 标准库 `asyncio` 实现的零外部依赖反向代理系统，支持内网穿透、WebUI 可视化管理与动态端口转发。

## 🌟 特性
- **零第三方依赖 (Zero External Dependencies)**：纯 Python 3.8+ 标准库编写，无需 `pip install` 任何包。
- **DuckDNS 动态域名自动同步 (DDNS Support)**：
  - 服务端支持绑定 DuckDNS，定期自动更新公网 IP。
  - **智能协议匹配**：绑定 IPv4 时仅更新 IPv4 (A 记录)；绑定 IPv6 时自动检测并仅更新 IPv6 (AAAA 记录)；双栈时两者均更新。
  - Web 控制台实时显示 DuckDNS 同步状态与 IP。
- **域名连接与解析**：客户端原生支持直接连接域名（如 `xxx.duckdns.org:8000`），内置 DNS 解析与自动重连。
- **独立/复用 WebUI 自由配置**：
  - 默认单端口多路复用：一个端口同时搞定控制流、桥接通道与 WebUI。
  - 支持 `--webui "ip:port"` 指定独立管理端口，方便分离公网暴露端口与内网管理面板。
- **原生 IPv6 / 双栈支持 (Full IPv6 & Dual-Stack Support)**：
  - 支持 `[::]:8000`、`::1`、`fe80::...` 等 IPv6 格式。
  - 代理转发端口默认绑定双栈（`host=None`），同时允许 IPv4 与 IPv6 访客连接。
  - 支持将流量转发到客户端本地的 IPv6 服务（如 `::1:1145`）。
- **WebUI 可视化控制台**：浏览器直接访问 Web 端口即可查看在线客户端列表、动态新增/停止反向代理端口规则。
- **高并发与健壮性**：客户端内置自动断线重连与心跳保活机制，数据传输采用双向异步流管道中继。

---

## 🚀 使用方法

### 1. 启动服务端 (Server)

**模式 A：单端口复用**
```bash
python main.py --listen "0.0.0.0:8000"
# WebUI 直接访问: http://127.0.0.1:8000
```

**模式 B：启用 DuckDNS 动态域名自动更新**
```bash
# 绑定 IPv4 时更新 IPv4:
python main.py --listen "0.0.0.0:8000" --duckdns "myhome:a7c4d0ad-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# 绑定 IPv6 时更新 IPv6:
python main.py --listen "[::]:8000" --duckdns-domain "myhome" --duckdns-token "a7c4d0ad-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

**模式 C：独立 WebUI 端口**
```bash
python main.py --listen "0.0.0.0:8000" --webui "0.0.0.0:9000"
# WebUI 访问: http://127.0.0.1:9000
```

### 2. 启动内网客户端 (Client)
客户端支持连接到 IP 或 DuckDNS 域名：
```bash
# 连接到 DuckDNS 域名:
python main.py --connect "myhome.duckdns.org:8000" --name "mypc"

# 连接到 IP (IPv4 或 IPv6):
python main.py --connect "1.2.3.4:8000" --name "mypc"
python main.py --connect "[2001:db8::1]:8000" --name "mypc"
```
> 注：支持 `domain:port`、`domain::port` 与 `[ipv6]:port` 等多种写法，客户端名称支持引号包裹。

### 3. 配置反向代理转发
在服务端 Web 控制台界面：
1. **服务端监听端口 (Listen Port)**：输入希望在服务端对外开放的端口，如 `666`。
2. **目标客户端 (Target Client)**：下拉选择已上线的客户端，如 `mypc`。
3. **客户端内网地址**：默认 `127.0.0.1`。
4. **客户端目标端口 (Target Port)**：输入客户端本地服务端口，如 `1145`。
5. 点击 **“启动代理”**。

此时，访问服务端 `server_ip:666` 的所有 TCP 流量将被自动、透明地穿透转发到客户端 `mypc` 的本地 `1145` 端口上！

---

## 🧪 运行测试
执行自动化集成测试脚本：
```bash
python test_proxy.py
```