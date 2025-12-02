from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

LINE_CHANNEL_ACCESS_TOKEN = "HUd3CZsp2l+dO5jLo4mQoBHgqK64H8to90HSxV19RYZcWhy3d5NEtyawGusBKSIIBZZEyR9+UId/s1N1zRxlhnE8sNmjsj4GPE03LA9v2qvoS3PelKfJQHxT4q7HXOSGhnWSdXsAH7z5XX/t221xxAdB04t89/1O/w1cDnyilFU="
LINE_CHANNEL_SECRET = "c61654e734e126a9a70b651210bf3120"

app = Flask(__name__)

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)


@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers["X-Line-Signature"]
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return "OK"


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    print(">>> REAL USER_ID:", user_id)

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"registered: {user_id}")
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
