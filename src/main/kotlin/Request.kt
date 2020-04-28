class Request {
    var topic: String = ""
    var key: String = ""
    var timestamp: Long = 0L
    var deleteRequest: Boolean = false
    var deleteEntireTableWhenInDeleteMode: Boolean = false
}
