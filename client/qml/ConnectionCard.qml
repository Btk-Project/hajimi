import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import Hajimi 1.0

Frame {
    id: card
    objectName: "connectionCard"
    required property ClientConnection connection
    required property ClientController appController
    readonly property bool forwarding: connection.config.mode === "forward"

    function saveFields() {
        connection.config = {
            mode: connection.config.mode,
            server: server.text,
            name: clientName.text,
            transport: transport.currentText,
            local: localAddress.text,
            target: target.text,
            caFile: caFile.text,
            insecure: insecure.checked
        }
    }

    ColumnLayout {
        anchors.fill: parent
        spacing: 8
        RowLayout {
            Label {
                text: card.forwarding ? "正向代理" : "反向代理"
                font.pixelSize: 18
                font.bold: true
            }
            Label { text: card.connection.status }
            Item { Layout.fillWidth: true }
            Button {
                objectName: "startConnection"
                text: "启动"
                enabled: !card.connection.running
                onClicked: { card.saveFields(); card.connection.start() }
            }
            Button {
                objectName: "stopConnection"
                text: "停止"
                enabled: card.connection.running
                onClicked: card.connection.stop()
            }
            Button {
                text: "复制命令"
                onClicked: { card.saveFields(); card.appController.copyToClipboard(card.connection.command) }
            }
            Button {
                objectName: "copyConnectionInfo"
                text: "复制信息"
                onClicked: { card.saveFields(); copyMenu.popup() }
                Menu {
                    id: copyMenu
                    MenuItem {
                        objectName: "copyServer"
                        text: "服务器地址"
                        onTriggered: card.appController.copyToClipboard(server.text)
                    }
                    MenuItem {
                        objectName: "copyConnectionName"
                        text: "连接名称"
                        onTriggered: card.appController.copyToClipboard(clientName.text)
                    }
                    MenuItem {
                        text: "传输方式"
                        onTriggered: card.appController.copyToClipboard(transport.currentText)
                    }
                    MenuItem {
                        text: "本地监听地址"
                        visible: card.forwarding
                        height: visible ? implicitHeight : 0
                        onTriggered: card.appController.copyToClipboard(localAddress.text)
                    }
                    MenuItem {
                        text: "目标客户端:端口"
                        visible: card.forwarding
                        height: visible ? implicitHeight : 0
                        onTriggered: card.appController.copyToClipboard(target.text)
                    }
                    MenuSeparator {}
                    MenuItem {
                        text: "整张卡片的信息"
                        onTriggered: card.appController.copyToClipboard(
                            "服务器地址：" + server.text + "\n连接名称：" + clientName.text +
                            "\n传输方式：" + transport.currentText +
                            (card.forwarding ? "\n本地监听地址：" + localAddress.text + "\n目标客户端:端口：" + target.text : ""))
                    }
                }
            }
            Button {
                text: "删除"
                enabled: !card.connection.running
                onClicked: card.appController.removeConnection(card.connection)
            }
        }
        Label {
            Layout.fillWidth: true
            text: card.forwarding ? "将本地监听端口转发到服务端上指定客户端的端口。"
                                  : "将本机注册到服务端，供 WebUI 规则或其他客户端访问。"
            wrapMode: Text.Wrap
        }
        RowLayout {
            Layout.fillWidth: true
            Label { text: "服务端地址" }
            TextField {
                id: server
                objectName: "connectionServer"
                Layout.fillWidth: true
                Layout.preferredWidth: 360
                Layout.minimumWidth: 120
                text: card.connection.config.server
                readOnly: card.connection.running
                selectByMouse: true
                placeholderText: "域名或 IP，默认端口 1145"
                onEditingFinished: card.saveFields()
            }
            Label { text: "连接名称" }
            TextField {
                id: clientName
                objectName: "connectionName"
                Layout.fillWidth: true
                Layout.preferredWidth: 240
                Layout.minimumWidth: 100
                text: card.connection.config.name
                readOnly: card.connection.running
                selectByMouse: true
                placeholderText: "同一服务端上的连接名称须唯一"
                onEditingFinished: card.saveFields()
            }
        }
        RowLayout {
            Layout.fillWidth: true
            visible: card.forwarding
            Label { text: "本地监听" }
            TextField {
                id: localAddress
                objectName: "connectionLocal"
                Layout.fillWidth: true
                Layout.preferredWidth: 300
                Layout.minimumWidth: 120
                text: card.connection.config.local
                readOnly: card.connection.running
                selectByMouse: true
                placeholderText: "127.0.0.1:25565"
                onEditingFinished: card.saveFields()
            }
            Label { text: "→"; font.pixelSize: 22 }
            Label { text: "目标客户端:端口" }
            TextField {
                id: target
                objectName: "connectionTarget"
                Layout.fillWidth: true
                Layout.preferredWidth: 300
                Layout.minimumWidth: 100
                text: card.connection.config.target
                readOnly: card.connection.running
                selectByMouse: true
                placeholderText: "arch:25565"
                onEditingFinished: card.saveFields()
            }
        }
        RowLayout {
            Label { text: "传输方式" }
            ComboBox {
                id: transport
                objectName: "connectionTransport"
                model: ["auto", "tcp", "quic"]
                enabled: !card.connection.running
                currentIndex: Math.max(0, model.indexOf(card.connection.config.transport))
                onActivated: card.saveFields()
            }
            CheckBox {
                id: certificateOptions
                text: "高级选项"
                checked: card.connection.config.insecure || (card.connection.config.caFile || "").length > 0
            }
            CheckBox { id: showLog; text: "显示日志" }
            Item { Layout.fillWidth: true }
            Button {
                objectName: "copyConnectionLog"
                text: "复制日志"
                visible: showLog.checked
                enabled: card.connection.log.length > 0
                onClicked: card.appController.copyToClipboard(card.connection.log)
            }
            Button {
                text: "清空日志"
                visible: showLog.checked
                onClicked: card.connection.clearLog()
            }
        }
        RowLayout {
            visible: certificateOptions.checked
            Label { text: "CA 文件（--ca-file）" }
            TextField {
                id: caFile
                objectName: "connectionCaFile"
                Layout.fillWidth: true
                text: card.connection.config.caFile
                readOnly: card.connection.running
                selectByMouse: true
                placeholderText: "可选：信任的 CA 或自签名证书文件"
                onEditingFinished: card.saveFields()
            }
            CheckBox {
                id: insecure
                objectName: "connectionInsecure"
                text: "--insecure（跳过证书验证）"
                checked: card.connection.config.insecure
                enabled: !card.connection.running
                onToggled: card.saveFields()
            }
        }
        Label {
            Layout.fillWidth: true
            visible: text.length > 0
            text: card.connection.error
            color: "#d43838"
            wrapMode: Text.Wrap
        }
        ScrollView {
            Layout.fillWidth: true
            Layout.preferredHeight: 160
            visible: showLog.checked
            TextArea {
                id: logText
                objectName: "connectionLog"
                text: card.connection.log
                readOnly: true
                selectByMouse: true
                textFormat: TextEdit.PlainText
                wrapMode: TextEdit.Wrap
                font.family: "monospace"
                onTextChanged: cursorPosition = length
                MouseArea {
                    anchors.fill: parent
                    acceptedButtons: Qt.RightButton
                    onClicked: logMenu.popup()
                }
                Menu {
                    id: logMenu
                    MenuItem {
                        text: "复制选中内容"
                        enabled: logText.selectedText.length > 0
                        onTriggered: card.appController.copyToClipboard(logText.selectedText)
                    }
                    MenuItem {
                        text: "复制全部日志"
                        enabled: logText.length > 0
                        onTriggered: card.appController.copyToClipboard(card.connection.log)
                    }
                    MenuItem {
                        text: "全选"
                        enabled: logText.length > 0
                        onTriggered: logText.selectAll()
                    }
                }
            }
        }
    }
}
