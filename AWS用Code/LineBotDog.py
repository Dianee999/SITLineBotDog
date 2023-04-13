from linebot import (LineBotApi, WebhookHandler)
from linebot.exceptions import (LineBotApiError, InvalidSignatureError)
from linebot.models import (MessageEvent, TextMessage, TextSendMessage, SourceUser, SourceGroup, SourceRoom, TemplateSendMessage, ConfirmTemplate, MessageAction, ButtonsTemplate, 
    ImageCarouselTemplate, ImageCarouselColumn, URIAction, PostbackAction, DatetimePickerAction, CameraAction, CameraRollAction, LocationAction, CarouselTemplate, CarouselColumn, 
    PostbackEvent, StickerMessage, StickerSendMessage, LocationMessage, LocationSendMessage, ImageMessage, VideoMessage, AudioMessage, FileMessage, UnfollowEvent, FollowEvent, 
    JoinEvent, LeaveEvent, BeaconEvent, MemberJoinedEvent, MemberLeftEvent, FlexSendMessage, BubbleContainer, ImageComponent, BoxComponent, TextComponent, SpacerComponent, 
    IconComponent, ButtonComponent, SeparatorComponent, QuickReply, QuickReplyButton, ImageSendMessage)
import requests, traceback, logging, boto3, json, sys, os
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from datetime import datetime

# === 將這個 Lambda 設定的環境變數 (environment variable) 輸出作為參考 ] ===
logger = logging.getLogger()
logger.setLevel(logging.INFO) 
channel_secret = os.getenv('LINE_CHANNEL_SECRET', None)
channel_access_token = os.getenv('LINE_CHANNEL_ACCESS_TOKEN', None)
if not channel_secret or not channel_access_token:
    logger.error('需要在 Lambda 的環境變數 (Environment variables) 裡新增 LINE_CHANNEL_SECRET 和 LINE_CHANNEL_ACCESS_TOKEN 作為環境變數。')
    sys.exit(1)
line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)    
logger.info(os.environ)  

# ===[ 定義你的函式 ] ===
def get_userOperations(userId):
    return None

# ===[ 指定需要呼叫的AWS資源 ] === 
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
client = boto3.client('lambda')

# === [ 定義回覆使用者輸入的文字訊息 - 依據使用者狀態，回傳組成 LINE 的 Template 元素 ] ===
def compose_textReplyMessage(userId, userOperations, messageText):
    if messageText == '新手教學':
        return TextSendMessage(text = '汪汪，讓本犬告訴你該如何使用!!\n\n\
1. 「今日通知」， 透過今天的收盤情況，告訴你明天一早適合購買的投信是哪些，主人再根據自己需求去購買。\n\n\
2. 「查詢紀錄」，可以讓主人查詢最近購買投信的紀錄，可以指定想找尋的日期喔!~\n\n\
3. 「其他功能」，這個功能可以幫助主人寄信給客服和找到基本教學喔\n\n\
汪汪，若有其他問題，可以留訊息給客服喔~~')
    if messageText == '聯絡客服':
        return TextSendMessage(text = '汪汪!!請留下您想留給客服人員的訊息!!')
    if '其他功能' in messageText:
        quick_reply_list = QuickReply(items=[
        QuickReplyButton(action=MessageAction(label="新手教學", text="新手教學")),
        QuickReplyButton(action=MessageAction(label="聯絡客服", text="聯絡客服"))])
        return TextSendMessage(text='汪汪! 請告訴本犬想知道的訊息！', quick_reply=quick_reply_list)
    elif '查詢紀錄' in messageText:
        return TextSendMessage(text = '汪汪! 抱歉，此項功能目前維護中。')
    elif '今日通知' in messageText:
        today = datetime.now().strftime('%Y-%m-%d')  #西元年(yyyymmdd)
        table_name = f'{str(today)}-message'
        try:
            # 使用 Table() 方法獲取 DynamoDB 資料表
            table = dynamodb.Table(table_name)
            # 取得資料表中的所有項目
            response = table.scan()
            # 擷取所有項目
            items = response['Items']
            for item in items:
                if item['action'] == 'buy':
                        message = TextSendMessage(text = f"{today}\n推薦明天購買的證券ID是{item['stock']}\n")
                elif item['action'] == 'sell':
                        message = TextSendMessage(text =  f"{today}\n推薦明天賣出的證券ID是{item['stock']}\n")
                line_bot_api.push_message(userId, message)
            return TextSendMessage(text = '汪汪! 以上是今天給您的資訊')
        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            return TextSendMessage(text = '汪汪! 抱歉，此項功能目前維護中。')
    else:
        response = client.invoke(
            FunctionName='ConnectStaff', # 要调用的 Lambda 函数的名称
            InvocationType='Event', # 指定调用类型为事件
            Payload=json.dumps({ # 指定调用参数
                'userId': userId,
                'messageText': messageText
            })
        )
        return TextSendMessage(text='汪汪!已收到您的訊息了!')

# === [ 定義回覆使用者與程式使用者界面互動時回傳結果後的訊息 - 依據使用者狀態，回傳組成 LINE 的 Template 元素 ] ===
def compose_postbackReplyMessage(userId, userOperations, messageData):
    return TextSendMessage(text='好的！已收到您的動作 %s！' % messageData)

# === [ 主程式 - 這裡是主要的 lambda_handler 程式進入點區段，相當於 main() ]==================================================================================== 
def lambda_handler(event, context):
    
    # ==== [ 處理文字 TextMessage 訊息程式區段 ] ===
    @handler.add(MessageEvent, message=TextMessage)    
    def handle_text_message(event):
        userId = event.source.user_id
        messageText = event.message.text
        userOperations = get_userOperations(userId)
        logger.info('收到 MessageEvent 事件 | 使用者 %s 輸入了 [%s] 內容' % (userId, messageText))
        line_bot_api.reply_message(event.reply_token, compose_textReplyMessage(userId, userOperations, messageText))

    # ==== [ 處理使用者按下相關按鈕回應後的後續動作 PostbackEvent 程式區段 ] ===
    @handler.add(PostbackEvent)   
    def handle_postback(event):
        userId = event.source.user_id 
        messageData = json.loads(event.postback.data)
        userOperations = get_userOperations(userId)        
        logger.info('收到 PostbackEvent 事件 | 使用者 %s' % userId)        
        line_bot_api.reply_message(event.reply_token, compose_postbackReplyMessage(userId, userOperations, messageData))

    # ==== [ 處理追縱 FollowEvent 的程式區段 ] === 
    @handler.add(FollowEvent)  
    def handle_follow(event):
        userId = event.source.user_id
        # 當使用者加入進來便會更新users資料表，這樣可以方便一次推播訊息
        table = dynamodb.Table('users')
        table.put_item(
            Item={
                'userId': userId
            }
        )
        logger.info('收到 FollowEvent 事件 | 使用者 %s' % userId)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='歡迎您的加入 !'))      

# === [ 這裡才是 lambda_handler 主程式 ]===================================================================================== 
    try:
        signature = event['headers']['x-line-signature']  # === 取得 event (事件) x-line-signature 標頭值 (header value)
        body = event['body']  # === 取得事件本文內容(body)
        # eventheadershost = event['headers']['host']        
        handler.handle(body, signature)  # === 處理 webhook 事件本文內容(body)
    
    # === [ 發生錯誤的簽章內容(InvalidSignatureError)的程式區段 ] ===
    except InvalidSignatureError:
        return {
            'statusCode': 400,
            'body': json.dumps('InvalidSignature') }        
    
    # === [ 發生錯誤的LineBotApi內容(LineBotApiError)的程式區段 ] ===
    except LineBotApiError as e:
        logger.error('呼叫 LINE Messaging API 時發生意外錯誤: %s' % e.message)
        for m in e.error.details:
            logger.error('-- %s: %s' % (m.property, m.message))
        return {
            'statusCode': 400,
            'body': json.dumps(traceback.format_exc()) }
    
    # === [ 沒有錯誤(回應200 OK)的程式區段 ] ===
    return {
        'statusCode': 200,
        'body': json.dumps('OK') }