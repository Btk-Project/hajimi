#include "client_controller.hpp"

#include <QClipboard>
#include <QDir>
#include <QFileInfo>
#include <QGuiApplication>
#include <QHostAddress>
#include <QHostInfo>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QSettings>
#include <QRegularExpression>
#include <QStandardPaths>
#include <QUuid>
#include <functional>

namespace {
const QString ipv6Host = "busystudent-pi.duckdns.org";
const QString tailscaleHost = "pi.tail12b2a2.ts.net";
using ResolveCallback = std::function<void(QString, QString)>;

bool isTailscaleHost(QString host) {
    if (host.endsWith('.')) host.chop(1);
    return host.endsWith(".ts.net", Qt::CaseInsensitive);
}

QString findTailscale() {
#ifdef Q_OS_WIN
    const QString executable = "tailscale.exe";
    QStringList locations;
    for (const auto &variable : {"ProgramW6432", "ProgramFiles", "ProgramFiles(x86)"}) {
        const auto directory = qEnvironmentVariable(variable);
        if (!directory.isEmpty()) locations << QDir(directory).filePath("Tailscale/tailscale.exe");
    }
    locations << "C:/Program Files/Tailscale/tailscale.exe";
#elif defined(Q_OS_MACOS)
    const QString executable = "tailscale";
    const QStringList locations{"/Applications/Tailscale.app/Contents/MacOS/Tailscale",
                                "/usr/local/bin/tailscale", "/opt/homebrew/bin/tailscale"};
#else
    const QString executable = "tailscale";
    const QStringList locations{"/usr/bin/tailscale", "/usr/local/bin/tailscale", "/bin/tailscale"};
#endif
    const auto program = QStandardPaths::findExecutable(executable);
    if (!program.isEmpty()) return program;
    for (const auto &location : locations) {
        const QFileInfo file(location);
        if (file.isFile() && file.isExecutable()) return location;
    }
    return {};
}

void resolveAddress(QObject *owner, const QString &host, ResolveCallback done) {
    if (!isTailscaleHost(host)) {
        QHostInfo::lookupHost(host, owner, [done](const QHostInfo &info) {
            if (info.error() == QHostInfo::NoError) {
                for (const auto &address : info.addresses()) {
                    if (address.protocol() == QAbstractSocket::IPv6Protocol) {
                        done(address.toString(), {});
                        return;
                    }
                }
            }
            done({}, "系统 DNS 未解析出 IPv6 地址，此路径不可用。");
        });
        return;
    }
    const auto program = findTailscale();
    if (program.isEmpty()) {
        done({}, "默认安装位置和 PATH 中未找到 Tailscale，此路径不可用。");
        return;
    }
    auto *process = new QProcess(owner);
    auto *timer = new QTimer(process);
    timer->setSingleShot(true);
    QObject::connect(timer, &QTimer::timeout, process, [process] { process->kill(); });
    const auto finish = [process, timer, done](QString address, QString error) {
        timer->stop();
        process->deleteLater();
        done(std::move(address), std::move(error));
    };
    QObject::connect(process, &QProcess::errorOccurred, owner, [finish](QProcess::ProcessError error) {
        if (error == QProcess::FailedToStart) finish({}, "无法启动 Tailscale，此路径不可用。");
    });
    QObject::connect(process, &QProcess::finished, owner, [process, finish](int code, QProcess::ExitStatus status) {
        // Only accept a successful pong, never an address mentioned in an error or timeout.
        static const QRegularExpression pong(R"((?:^|\n)pong from [^\r\n]* \(([^,()\s]+)(?:, [0-9]+)?\) via )");
        static const QRegularExpression self(R"((?:^|\n)(\S+) is local Tailscale IP(?:\r?\n|$))");
        const auto output = QString::fromUtf8(process->readAllStandardOutput());
        auto match = pong.match(output);
        if (!match.hasMatch()) match = self.match(output);
        const QHostAddress address(match.hasMatch() ? match.captured(1) : "");
        if (status == QProcess::NormalExit && code == 0 && match.hasMatch() && !address.isNull()) {
            finish(address.toString(), {});
        } else {
            finish({}, "tailscale ping 未获得有效回复，没有可用的 Tailscale 连接。");
        }
    });
    // Accept DERP replies too, and bound both the ping and the subprocess lifetime.
    process->start(program, {"ping", "--c=1", "--timeout=5s", "--until-direct=false", host});
    timer->start(8000);
}
} // namespace

ClientController::ClientController(QObject *parent) : QObject(parent) {
    const auto saved = QJsonDocument::fromJson(QSettings().value("connections").toByteArray()).array();
    for (const auto &value : saved) {
        auto *connection = new ClientConnection(value.toObject().toVariantMap(), this);
        mConnections.append(connection);
        connect(connection, &ClientConnection::configChanged, this, &ClientController::saveConnections);
    }
}

ClientController::~ClientController() {
    // Stop subprocesses while the controller state captured by callbacks is still alive.
    for (auto *process : findChildren<QProcess*>(QString(), Qt::FindDirectChildrenOnly)) {
        process->disconnect(this);
        if (process->state() != QProcess::NotRunning) {
            process->kill();
            process->waitForFinished(1000);
        }
        delete process;
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
    const auto request = ++mWebuiRequest;
    mResolvingWebui = false;
    emit resolvingWebuiChanged();
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
    if (isTailscaleHost(url.host()) || url.host().compare(ipv6Host, Qt::CaseInsensitive) == 0) {
        mResolvingWebui = true;
        emit resolvingWebuiChanged();
        resolveAddress(this, url.host(), [this, request, url](const QString &ip, const QString &error) mutable {
            if (request != mWebuiRequest) return;
            mResolvingWebui = false;
            emit resolvingWebuiChanged();
            if (ip.isEmpty()) {
                setError(error);
                return;
            }
            url.setHost(ip);
            setWebuiUrl(url);
        });
    } else {
        setWebuiUrl(url);
    }
    return true;
}

void ClientController::setWebuiUrl(const QUrl &url) {
    if (mWebuiUrl == url) {
        emit webuiUrlChanged();
        emit webuiReloadRequested();
        return;
    }
    mWebuiUrl = url;
    for (auto *object : mConnections) {
        auto *connection = static_cast<ClientConnection*>(object);
        if (!connection->running() && connection->config().value("server").toString().trimmed().isEmpty()) {
            connection->setOption("server", defaultServerAddress());
        }
    }
    emit webuiUrlChanged();
}

void ClientController::copyToClipboard(const QString &text) {
    QGuiApplication::clipboard()->setText(text);
}

void ClientController::checkNetwork() {
    resolveIpv6();
    resolveTailscale();
}

void ClientController::resolveIpv6() {
    if (mResolvingIpv6) return;
    mResolvingIpv6 = true;
    mIpv6Address.clear();
    mIpv6Hint = "正在通过系统 DNS 解析 IPv6 地址…";
    emit networkHintsChanged();
    resolveAddress(this, ipv6Host, [this](const QString &ip, const QString &error) {
        mIpv6Address = ip;
        mIpv6Hint = ip.isEmpty() ? error : "IPv6：" + ip + "；仍需尝试连接。";
        mResolvingIpv6 = false;
        emit networkHintsChanged();
    });
}

void ClientController::resolveTailscale() {
    if (mResolvingTailscale) return;
    mResolvingTailscale = true;
    mTailscaleAddress.clear();
    mTailscaleHint = "正在通过 tailscale ping 解析地址…";
    emit networkHintsChanged();
    resolveAddress(this, tailscaleHost, [this](const QString &ip, const QString &error) {
        mTailscaleAddress = ip;
        mTailscaleHint = ip.isEmpty() ? error : "Tailscale：" + ip + "（ping 成功）";
        mResolvingTailscale = false;
        emit networkHintsChanged();
    });
}
