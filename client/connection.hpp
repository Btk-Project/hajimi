#pragma once

#include <QObject>
#include <QProcess>
#include <QStringDecoder>
#include <QTimer>
#include <QVariantMap>

class ClientConnection : public QObject {
    Q_OBJECT
    Q_PROPERTY(QVariantMap config READ config WRITE setConfig NOTIFY configChanged)
    Q_PROPERTY(bool running READ running NOTIFY stateChanged)
    Q_PROPERTY(QString status READ status NOTIFY stateChanged)
    Q_PROPERTY(QString log READ log NOTIFY logChanged)
    Q_PROPERTY(QString error READ error NOTIFY errorChanged)
    Q_PROPERTY(QString command READ command NOTIFY configChanged)

public:
    explicit ClientConnection(QVariantMap config, QObject *parent = nullptr);
    ~ClientConnection() override;

    QVariantMap config() const { return mConfig; }
    void setConfig(const QVariantMap &config);
    bool running() const { return mProcess.state() != QProcess::NotRunning; }
    QString status() const;
    QString log() const { return mLog; }
    QString error() const { return mError; }
    QString command() const;

    Q_INVOKABLE void setOption(const QString &key, const QVariant &value);
    Q_INVOKABLE void start();
    Q_INVOKABLE void stop();
    Q_INVOKABLE void clearLog();

Q_SIGNALS:
    void configChanged();
    void stateChanged();
    void logChanged();
    void errorChanged();

private:
    QStringList arguments() const;
    void appendLog(const QString &text);
    void setError(const QString &message);

    QVariantMap mConfig;
    QProcess mProcess;
    QTimer mStopTimer;
    QStringDecoder mDecoder{QStringDecoder::Utf8};
    QString mLog;
    QString mError;
    bool mStopping = false;
};
