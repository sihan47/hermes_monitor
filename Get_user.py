from linebot.v3.messaging import MessagingApi, ApiClient, Configuration

CHANNEL_ACCESS_TOKEN = "HUd3CZsp2l+dO5jLo4mQoBHgqK64H8to90HSxV19RYZcWhy3d5NEtyawGusBKSIIBZZEyR9+UId/s1N1zRxlhnE8sNmjsj4GPE03LA9v2qvoS3PelKfJQHxT4q7HXOSGhnWSdXsAH7z5XX/t221xxAdB04t89/1O/w1cDnyilFU="

config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)

with ApiClient(config) as api_client:
    api = MessagingApi(api_client)

    user_ids = []
    next_token = None

    while True:
        # 不用任何 model，直接傳參數
        res = api.get_followers(start=next_token)

        user_ids.extend(res.user_ids)

        if not res.next:
            break

        next_token = res.next

    print("All Users:")
    for uid in user_ids:
        print(uid)

