#include "client_controller.hpp"

#include <QClipboard>
#include <QGuiApplication>
#include <QHostInfo>
#include <QNetworkInterface>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QSettings>
#include <QUuid>

ClientController::ClientController(QObject *parent) : QObject(parent) {
    const auto saved = QJsonDocument::fromJson(QSettings().value("connections").toByteArray()).array();
    for (const auto &value : saved) {
        auto *connection = new ClientConnection(value.toObject().toVariantMap(), this);
        mConnections.append(connection);
        connect(connection, &ClientConnection::configChanged, this, &ClientController::saveConnections);
    }
}

ClientConnection *ClientController::addConnection(const QString &mode) {
    if (mode != "reverse" && mode != "forward") return nullptr;
    const auto name = mode == "reverse" ? "hajimi-client" : "forward-" + QUuid::createUuid().toString(QUuid::Id128).left(8);
    auto *connection = new ClientConnection({
        {"mode", mode}, {"server", defaultServerAddress()}, {"name", name}, {"transport", "auto"},
        {"local", "127.0.0.1:25565"}, {"target", "arch:25565"}, {"caFile", ""}, {"insecure", false}
    }, this);
    mConnections.append(connection);
    connect(connection, &ClientConnection::configChanged, this, &ClientController::saveConnections);
    saveConnections();
    emit connectionsChanged();
    return connection;
}

void ClientController::removeConnection(QObject *object) {
    auto *connection = qobject_cast<ClientConnection*>(object);
    if (!connection || connection->running() || !mConnections.removeOne(connection)) return;
    emit connectionsChanged();
    connection->deleteLater();
    saveConnections();
}

void ClientController::saveConnections() {
    QVariantList configs;
    for (auto *object : mConnections) {
        configs.append(static_cast<ClientConnection*>(object)->config());
    }
    QSettings().setValue("connections", QJsonDocument::fromVariant(configs).toJson(QJsonDocument::Compact));
}

void ClientController::setError(const QString &message) {
    mError = message;
    emit errorChanged();
}

QString ClientController::defaultServerAddress() const {
    if (mWebuiUrl.host().isEmpty()) return {};
    QUrl server;
    server.setHost(mWebuiUrl.host());
    server.setPort(1145);
    return server.authority();
}

bool ClientController::loadWebUi(const QString &address) {
    setError({});
    QUrl url = QUrl::fromUserInput(address.trimmed());
    if (!url.isValid() || url.host().isEmpty() ||
        (url.scheme() != "http" && url.scheme() != "https")) {
        setError("请输入有效的 HTTP 或 HTTPS WebUI 地址");
        return false;
    }
    if (url.port() == -1) {
        url.setPort(8848);
    }
    mWebuiUrl = url;
    for (auto *object : mConnections) {
        auto *connection = static_cast<ClientConnection*>(object);
        if (!connection->running() && connection->config().value("server").toString().trimmed().isEmpty()) {
            connection->setOption("server", defaultServerAddress());
        }
    }
    emit webuiUrlChanged();
    return true;
}

void ClientController::copyToClipboard(const QString &text) {
    QGuiApplication::clipboard()->setText(text);
}

void ClientController::checkNetwork() {
    if (mCheckingNetwork) {
        return;
    }
    bool hasIpv6 = false;
    bool hasTailscale = false;
    for (const auto &iface : QNetworkInterface::allInterfaces()) {
        if (!iface.flags().testFlag(QNetworkInterface::IsUp) ||
            !iface.flags().testFlag(QNetworkInterface::IsRunning)) {
            continue;
        }
        const auto name = iface.name() + " " + iface.humanReadableName();
        hasTailscale |= name.contains("tailscale", Qt::CaseInsensitive);
        for (const auto &entry : iface.addressEntries()) {
            // A global address is only a hint, not proof of IPv6 reachability.
            hasIpv6 |= entry.ip().isInSubnet(QHostAddress("2000::"), 3);
        }
    }
    mIpv6Hint = hasIpv6 ? "检测到本机全局 IPv6 地址，仍需尝试连接。"
                       : "未检测到本机全局 IPv6 地址，此路径可能不可用。";
    mTailscaleHint = hasTailscale ? "检测到 Tailscale 网卡，正在解析 pi.tail12b2a2.ts.net…"
                               : "未检测到启用的 Tailscale 网卡，可复制地址手动尝试。";
    mCheckingNetwork = hasTailscale;
    emit networkHintsChanged();
    if (!hasTailscale) {
        return;
    }
    QHostInfo::lookupHost("pi.tail12b2a2.ts.net", this, [this](const QHostInfo &info) {
        QStringList addresses;
        for (const auto &address : info.addresses()) {
            addresses.append(address.toString());
        }
        mTailscaleHint = info.error() == QHostInfo::NoError && !addresses.isEmpty()
            ? "DNS 解析结果：" + addresses.join("、") + "；仍需尝试连接。"
            : "Tailscale DNS 暂未解析成功，可手动填写 IP 或其他地址。";
        mCheckingNetwork = false;
        emit networkHintsChanged();
    });
}
