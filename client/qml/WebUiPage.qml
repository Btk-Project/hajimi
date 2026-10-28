import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtWebEngine

ColumnLayout {
    id: page
    property url address
    function reload() { browser.reload() }

    spacing: 0
    ProgressBar {
        Layout.fillWidth: true
        visible: browser.loading
        value: browser.loadProgress / 100
    }
    Label {
        id: loadError
        objectName: "webLoadError"
        Layout.fillWidth: true
        visible: text.length > 0
        color: "#d43838"
        wrapMode: Text.Wrap
    }
    WebEngineView {
        id: browser
        objectName: "webView"
        Layout.fillWidth: true
        Layout.fillHeight: true
        url: page.address
        settings.errorPageEnabled: false
        onLoadingChanged: function(request) {
            if (request.status === WebEngineView.LoadFailedStatus)
                loadError.text = "WebUI 加载失败（" + request.url + "）：" + request.errorString
            else if (request.status === WebEngineView.LoadStartedStatus || request.status === WebEngineView.LoadSucceededStatus)
                loadError.text = ""
        }
    }
}
