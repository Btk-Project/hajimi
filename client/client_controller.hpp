#pragma once

#include <QObject>
#include <QUrl>
#include "connection.hpp"

class ClientController : public QObject {
    Q_OBJECT
    Q_PROPERTY(QList<QObject*> connections READ connections NOTIFY connectionsChanged)
    Q_PROPERTY(QString error READ error NOTIFY errorChanged)
    Q_PROPERTY(QUrl webuiUrl READ webuiUrl NOTIFY webuiUrlChanged)
    Q_PROPERTY(QString ipv6Hint READ ipv6Hint NOTIFY networkHintsChanged)
    Q_PROPERTY(QString tailscaleHint READ tailscaleHint NOTIFY networkHintsChanged)
    Q_PROPERTY(bool checkingNetwork READ checkingNetwork NOTIFY networkHintsChanged)

public:
    explicit ClientController(QObject *parent = nullptr);
    QList<QObject*> connections() const { return mConnections; }
    QString error() const { return mError; }
    QUrl webuiUrl() const { return mWebuiUrl; }
    QString ipv6Hint() const { return mIpv6Hint; }
    QString tailscaleHint() const { return mTailscaleHint; }
    bool checkingNetwork() const { return mCheckingNetwork; }

    Q_INVOKABLE ClientConnection *addConnection(const QString &mode);
    Q_INVOKABLE void removeConnection(QObject *connection);
    Q_INVOKABLE bool loadWebUi(const QString &address);
    Q_INVOKABLE void checkNetwork();
    Q_INVOKABLE void copyToClipboard(const QString &text);

Q_SIGNALS:
    void connectionsChanged();
    void errorChanged();
    void webuiUrlChanged();
    void networkHintsChanged();

private:
    QString defaultServerAddress() const;
    void saveConnections();
    void setError(const QString &message);
    QList<QObject*> mConnections;
    QString mError;
    QUrl mWebuiUrl;
    QString mIpv6Hint;
    QString mTailscaleHint;
    bool mCheckingNetwork = false;
};
