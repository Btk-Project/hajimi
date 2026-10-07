# Hajimi

基于 C++23 和 ilias 协程运行时的 TCP 反向代理，支持内网穿透、逻辑隧道复用、窗口流控、WebUI 管理和 DuckDNS 更新。

## 使用

启动服务端，显式指定 WebUI 地址：

```powershell
xmake run hajimi --listen "0.0.0.0:8000" --webui "127.0.0.1:9000"
```

浏览器访问 `http://127.0.0.1:9000`。省略 `--webui` 时，WebUI 默认监听 `127.0.0.1` 的随机端口，实际端口见启动日志；控制连接与 WebUI 使用独立监听器。

在内网机器启动客户端：

```powershell
xmake run hajimi --connect "your-server:8000" --name "mypc"
```

服务端 WebUI 中选择 `mypc`，填写服务端转发端口、客户端目标地址和目标端口，即可建立规则。例如服务端端口 `6666` 对应客户端 `127.0.0.1:1145`。

客户端支持域名、IPv4 和带方括号的 IPv6 地址，连接断开后自动重连。转发规则监听器采用 IPv6 双栈配置，允许 IPv4 和 IPv6 访问。

启用 DuckDNS：

```powershell
xmake run hajimi --listen "[::]:8000" --webui "127.0.0.1:9000" --duckdns "myhome:your-token"
```

也可以使用 `--duckdns-domain`、`--duckdns-token` 和 `--duckdns-interval` 单独配置
