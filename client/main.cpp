#include <QGuiApplication>
#include <QQmlApplicationEngine>
#include <QQuickWindow>
#include <QNetworkProxy>

#include "client_controller.hpp"

int main(int argc, char *argv[]) {
    // Initialize shared graphics contexts for the QML WebEngine plugin.
    QCoreApplication::setAttribute(Qt::AA_ShareOpenGLContexts);
    QQuickWindow::setGraphicsApi(QSGRendererInterface::OpenGL);
    QGuiApplication app(argc, argv);
    app.setApplicationName("hajimi-gui");
    app.setOrganizationName("Hajimi");
    // Use the same direct network path as the CLI, including Tailscale addresses.
    QNetworkProxy::setApplicationProxy(QNetworkProxy::NoProxy);

    qmlRegisterUncreatableType<ClientController>("Hajimi", 1, 0, "ClientController",
                                               "Created by the application");
    qmlRegisterUncreatableType<ClientConnection>("Hajimi", 1, 0, "ClientConnection",
                                               "Created by the controller");
    // Keep the controller alive until all QML objects have been destroyed.
    ClientController controller;
    QQmlApplicationEngine engine;
    engine.setInitialProperties({{"appController", QVariant::fromValue(&controller)}});
    engine.load(QUrl(QStringLiteral("qrc:/qml/main.qml")));
    if (engine.rootObjects().isEmpty()) {
        return 1;
    }
    return app.exec();
}
