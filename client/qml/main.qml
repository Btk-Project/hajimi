import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import Hajimi 1.0

ApplicationWindow {
    id: root
    required property ClientController appController
    width: 1120
    height: 760
    minimumWidth: 760
    minimumHeight: 540
    visible: true
    title: "Hajimi"
    Component.onCompleted: root.appController.checkNetwork()

    header: ToolBar {
        RowLayout {
            anchors.fill: parent
            TabBar {
                id: tabs
                objectName: "pageTabs"
                Layout.fillWidth: true
                TabButton { text: "WebUI" }
                TabButton { text: "连接" }
            }
            ToolButton {
                id: shortcuts
                objectName: "addressShortcuts"
                text: "⋯"
                ToolTip.visible: hovered
                ToolTip.text: "常用地址与网络检查"
                Accessible.name: "常用地址"
                onClicked: addressMenu.popup()
                MouseArea {
                    anchors.fill: parent
                    acceptedButtons: Qt.RightButton
                    onClicked: addressMenu.popup()
                }
                Menu {
                    id: addressMenu
                    Menu {
                        title: "IPv6 / DuckDNS"
                        MenuItem {
                            text: "填入 WebUI 地址（8848）"
                            onTriggered: { tabs.currentIndex = 0; webAddress.text = "http://busystudent-pi.duckdns.org:8848/" }
                        }
                        MenuItem {
                            text: "复制 WebUI 地址"
                            onTriggered: root.appController.copyToClipboard("http://busystudent-pi.duckdns.org:8848/")
                        }
                        MenuItem {
                            text: "复制客户端地址（1145）"
                            onTriggered: root.appController.copyToClipboard("busystudent-pi.duckdns.org:1145")
                        }
                        MenuSeparator {}
                        MenuItem { text: root.appController.ipv6Hint; enabled: false }
                    }
                    Menu {
                        title: "Tailscale / pi.tail12b2a2.ts.net"
                        MenuItem {
                            text: "填入 WebUI 地址（8848）"
                            onTriggered: { tabs.currentIndex = 0; webAddress.text = "http://pi.tail12b2a2.ts.net:8848/" }
                        }
                        MenuItem {
                            text: "复制 WebUI 地址"
                            onTriggered: root.appController.copyToClipboard("http://pi.tail12b2a2.ts.net:8848/")
                        }
                        MenuItem {
                            text: "复制客户端地址（1145）"
                            onTriggered: root.appController.copyToClipboard("pi.tail12b2a2.ts.net:1145")
                        }
                        MenuSeparator {}
                        MenuItem { text: root.appController.tailscaleHint; enabled: false }
                    }
                    MenuSeparator {}
                    MenuItem { text: "两条路径均可能不可用，也可手动填写地址"; enabled: false }
                    MenuItem {
                        text: root.appController.checkingNetwork ? "检查中…" : "检查本机网络"
                        enabled: !root.appController.checkingNetwork
                        onTriggered: root.appController.checkNetwork()
                    }
                }
            }
        }
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 16
        spacing: 12

        Label {
            Layout.fillWidth: true
            visible: text.length > 0
            text: root.appController.error
            color: "#d43838"
            wrapMode: Text.Wrap
        }

        StackLayout {
            currentIndex: tabs.currentIndex
            Layout.fillWidth: true
            Layout.fillHeight: true

            ColumnLayout {
                spacing: 10
                RowLayout {
                    Label { text: "WebUI 地址" }
                    TextField {
                        id: webAddress
                        objectName: "webAddress"
                        Layout.fillWidth: true
                        placeholderText: "手动填写服务器地址，默认端口 8848"
                        onAccepted: openWeb.clicked()
                    }
                    Button {
                        id: openWeb
                        objectName: "openWeb"
                        text: "加载"
                        onClicked: {
                            const previous = root.appController.webuiUrl.toString()
                            if (!root.appController.loadWebUi(webAddress.text))
                                return
                            webAddress.text = root.appController.webuiUrl.toString()
                            // Reload only the same URL; a changed URL navigates through its binding.
                            if (previous === root.appController.webuiUrl.toString() && webLoader.item)
                                webLoader.item.reload()
                        }
                    }
                    Button {
                        text: "刷新"
                        enabled: root.appController.webuiUrl.toString().length > 0 && webLoader.status === Loader.Ready
                        onClicked: webLoader.item.reload()
                    }
                }
                Label {
                    Layout.fillWidth: true
                    text: "直接加载服务端 WebUI，在页面中查看客户端和管理转发规则。"
                    wrapMode: Text.Wrap
                }
                Loader {
                    id: webLoader
                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    active: root.appController.webuiUrl.toString().length > 0
                    source: "WebUiPage.qml"
                    onLoaded: item.address = Qt.binding(function() { return root.appController.webuiUrl })
                }
                Label {
                    Layout.fillWidth: true
                    visible: webLoader.status === Loader.Error
                    text: "无法创建内嵌浏览器，请为当前 Qt 安装 QtWebEngine QML 模块。"
                    wrapMode: Text.Wrap
                    color: "#d43838"
                }
                Label {
                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    visible: !webLoader.active
                    text: "输入服务端的 WebUI 地址后点击加载。"
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                }
            }

            ColumnLayout {
                spacing: 10
                RowLayout {
                    Button {
                        objectName: "addReverse"
                        text: "新增反向代理"
                        onClicked: root.appController.addConnection("reverse")
                    }
                    Button {
                        objectName: "addForward"
                        text: "新增正向代理"
                        onClicked: root.appController.addConnection("forward")
                    }
                    Item { Layout.fillWidth: true }
                    Label { text: "每张卡片独立运行，配置自动保存" }
                }
                ScrollView {
                    id: connectionScroll
                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    contentWidth: availableWidth
                    ColumnLayout {
                        width: connectionScroll.availableWidth
                        spacing: 12
                        Label {
                            Layout.fillWidth: true
                            visible: root.appController.connections.length === 0
                            text: "新增一张连接卡片：反向代理将本机注册到服务端；正向代理将本地端口连接到指定客户端的端口。"
                            wrapMode: Text.Wrap
                        }
                        Repeater {
                            model: root.appController.connections
                            delegate: ConnectionCard {
                                required property var modelData
                                connection: modelData
                                appController: root.appController
                                Layout.fillWidth: true
                            }
                        }
                    }
                }
            }
        }
    }
}
