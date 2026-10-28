#include "connection.hpp"

#include <QCoreApplication>
#include <QDir>
#include <QFileInfo>
#include <QStandardPaths>
#include <QUrl>
#include <QRegularExpression>

ClientConnection::ClientConnection(QVariantMap config, QObject *parent)
    : QObject(parent), mConfig(std::move(config)) {
    mProcess.setProcessChannelMode(QProcess::MergedChannels);
    mStopTimer.setSingleShot(true);
    mStopTimer.setInterval(2000);
    connect(&mStopTimer, &QTimer::timeout, &mProcess, &QProcess::kill);
    connect(&mProcess, &QProcess::stateChanged, this, &ClientConnection::stateChanged);
    connect(&mProcess, &QProcess::readyReadStandardOutput, this, [this] {
        appendLog(mDecoder(mProcess.readAllStandardOutput()));
    });
    connect(&mProcess, &QProcess::errorOccurred, this, [this](QProcess::ProcessError error) {
        if (error == QProcess::FailedToStart) {
            setError("启动失败：" + mProcess.errorString());
        }
    });
    connect(&mProcess, &QProcess::finished, this, [this](int code, QProcess::ExitStatus status) {
        mStopTimer.stop();
        appendLog(mDecoder(mProcess.readAllStandardOutput()));
        appendLog(QString("\n进程已退出（退出码 %1）\n").arg(code));
        if (!mStopping && (status == QProcess::CrashExit || code != 0)) {
            setError(QString("连接进程异常退出（退出码 %1），请查看日志").arg(code));
        }
        mStopping = false;
    });
}

ClientConnection::~ClientConnection() {
    if (running()) {
        mStopping = true;
        mProcess.terminate();
        if (!mProcess.waitForFinished(2000)) {
            mProcess.kill();
            mProcess.waitForFinished();
        }
    }
}

void ClientConnection::setConfig(const QVariantMap &config) {
    if (!running() && mConfig != config) {
        mConfig = config;
        emit configChanged();
    }
}

void ClientConnection::setOption(const QString &key, const QVariant &value) {
    auto config = mConfig;
    config[key] = value;
    setConfig(config);
}

QString ClientConnection::status() const {
    switch (mProcess.state()) {
    case QProcess::Starting: return "正在启动";
    case QProcess::Running: return "进程运行中，连接状态见日志";
    default: return "已停止";
    }
}

QStringList ClientConnection::arguments() const {
    auto server = mConfig.value("server").toString().trimmed();
    QUrl endpoint("tcp://" + server);
    if (endpoint.isValid() && !endpoint.host().isEmpty() && endpoint.port() == -1) {
        server += ":1145";
    }
    QStringList args{"--connect", server, "--name", mConfig.value("name").toString().trimmed(),
                     "--transport", mConfig.value("transport", "auto").toString()};
    if (mConfig.value("mode") == "forward") {
        args << "--forward" << mConfig.value("local").toString().trimmed() + "@" + mConfig.value("target").toString().trimmed();
    }
    const auto ca = mConfig.value("caFile").toString().trimmed();
    if (!ca.isEmpty()) args << "--ca-file" << ca;
    if (mConfig.value("insecure").toBool()) args << "--insecure";
    return args;
}

QString ClientConnection::command() const {
    QStringList escaped;
    for (auto arg : arguments()) {
        if (arg.contains(QRegularExpression("[^a-zA-Z0-9_./:@=,-]"))) {
            arg.replace("'", "'\\''");
            arg = "'" + arg + "'";
        }
        escaped << arg;
    }
    return "hajimi " + escaped.join(' ');
}

void ClientConnection::start() {
    if (running()) return;
    setError({});
    const auto args = arguments();
    const QUrl endpoint("tcp://" + args[1]);
    if (endpoint.host().isEmpty() || endpoint.port() < 1 || endpoint.port() > 65535 ||
        mConfig.value("name").toString().trimmed().isEmpty()) {
        setError("请填写服务端地址和本连接的客户端名称（服务端默认端口 1145）");
        return;
    }
    if (mConfig.value("mode") == "forward" &&
        (mConfig.value("local").toString().trimmed().isEmpty() || mConfig.value("target").toString().trimmed().isEmpty())) {
        setError("请填写本地监听地址与目标客户端:端口");
        return;
    }
#ifdef Q_OS_WIN
    const QString executable = "hajimi.exe";
#else
    const QString executable = "hajimi";
#endif
    auto program = QDir(QCoreApplication::applicationDirPath()).filePath(executable);
    if (!QFileInfo(program).isExecutable()) program = QStandardPaths::findExecutable(executable);
    if (program.isEmpty()) {
        setError("找不到 hajimi，请放在 GUI 同目录或加入 PATH");
        return;
    }
    mStopping = false;
    mDecoder.resetState();
    appendLog("\n" + command() + "\n");
    // Arguments are passed directly; the displayed command is never executed by a shell.
    mProcess.start(program, args);
}

void ClientConnection::stop() {
    if (running() && !mStopping) {
        mStopping = true;
        mStopTimer.start();
        mProcess.terminate();
    }
}

void ClientConnection::clearLog() {
    mLog.clear();
    emit logChanged();
}

void ClientConnection::appendLog(const QString &text) {
    if (text.isEmpty()) return;
    mLog = (mLog + text).right(128 * 1024);
    emit logChanged();
}

void ClientConnection::setError(const QString &text) {
    mError = text;
    emit errorChanged();
}
