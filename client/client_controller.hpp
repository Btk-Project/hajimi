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
    Q_PROPERTY(QString ipv6Address READ ipv6Address NOTIFY networkHintsChanged)
    Q_PROPERTY(QString tailscaleAddress READ tailscaleAddress NOTIFY networkHintsChanged)
    Q_PROPERTY(bool resolvingIpv6 READ resolvingIpv6 NOTIFY networkHintsChanged)
    Q_PROPERTY(bool resolvingTailscale READ resolvingTailscale NOTIFY networkHintsChanged)
    Q_PROPERTY(bool checkingNetwork READ checkingNetwork NOTIFY networkHintsChanged)
    Q_PROPERTY(bool resolvingWebui READ resolvingWebui NOTIFY resolvingWebuiChanged)

public:
    explicit ClientController(QObject *parent = nullptr);
    ~ClientController() override;
    QList<QObject*> connections() const { return mConnections; }
    QString error() const { return mError; }
    QUrl webuiUrl() const { return mWebuiUrl; }
    QString ipv6Hint() const { return mIpv6Hint; }
    QString tailscaleHint() const { return mTailscaleHint; }
    QString ipv6Address() const { return mIpv6Address; }
    QString tailscaleAddress() const { return mTailscaleAddress; }
    bool resolvingIpv6() const { return mResolvingIpv6; }
    bool resolvingTailscale() const { return mResolvingTailscale; }
    bool checkingNetwork() const { return mResolvingIpv6 || mResolvingTailscale; }
    bool resolvingWebui() const { return mResolvingWebui; }

    Q_INVOKABLE ClientConnection *addConnection(const QString &mode);
    Q_INVOKABLE void removeConnection(QObject *connection);
    Q_INVOKABLE bool loadWebUi(const QString &address);
    Q_INVOKABLE void checkNetwork();
    Q_INVOKABLE void resolveIpv6();
    Q_INVOKABLE void resolveTailscale();
    Q_INVOKABLE void copyToClipboard(const QString &text);

Q_SIGNALS:
    void connectionsChanged();
    void errorChanged();
    void webuiUrlChanged();
    void webuiReloadRequested();
    void resolvingWebuiChanged();
    void networkHintsChanged();

private:
    QString defaultServerAddress() const;
    void setWebuiUrl(const QUrl &url);
    void saveConnections();
    void setError(const QString &message);
    QList<QObject*> mConnections;
    QString mError;
    QUrl mWebuiUrl;
    QString mIpv6Hint;
    QString mTailscaleHint;
    QString mIpv6Address;
    QString mTailscaleAddress;
    bool mResolvingIpv6 = false;
    bool mResolvingTailscale = false;
    bool mResolvingWebui = false;
    int mWebuiRequest = 0;
};
