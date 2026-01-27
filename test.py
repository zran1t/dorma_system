import json
import websocket

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

def on_open(ws):
    print("WS connected")

    sub_msg = {
        "id": "1512",
        "op": "subscribe",
        "args": [
            {
                "channel": "instruments",
                "instType": "SPOT"
            }
        ]
    }

    ws.send(json.dumps(sub_msg))
    print("subscribe sent")

def on_message(ws, message):
    data = json.loads(message)
    print(data)

def on_error(ws, error):
    print("ERROR:", error)

def on_close(ws, close_status_code, close_msg):
    print("WS closed", close_status_code, close_msg)

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()
