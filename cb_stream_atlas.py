import websocket, json
import dateutil.parser
from dateutil.parser import parse
import parstream

import websocket, json
import dateutil.parser

#Adding global variables that keep track of all candle sticksand update when new data arrives
minutes_processed = {}
minute_candlesticks = []
current_tick = None
previous_tick = None

#Sending Coinbase Pro a Json payload subscribe type and list of channel to subscribe with pair product IDs
def on_open(ws):
  print("Connection Opened")

  #Python dictionary as payload message to send
  subscribe_message = {
    "type": "subscribe",
    "channels": [
        {
            "name": "ticker",
            "product_ids": [
                "BTC-USD"
              ]
          }
      ]
    }

  #Converting python dictionary to Json payload
  ws.send(json.dumps(subscribe_message))



def on_message(ws, message):
  global current_tick, previous_tick
  previous_tick = current_tick
  current_tick = json.loads(message)
  #print('MA PRINT',current_tick)
  #print("=== Received Tick ===")
  print("{} @ {}".format(current_tick['time'], current_tick['price']))

  #Keeping track of when minutes change and store the closing price 
  tick_datetime_object = dateutil.parser.parse(current_tick['time'])
  #Formatting datetime
  tick_dt = tick_datetime_object.strftime("%m/%d/%Y %H:%M")
  #Printing just the minute number
  #print(tick_datetime_object.minute)
  #print(tick_dt)

  # Detecting if there is a new minute 
  if not tick_dt in minutes_processed:
    #print("starting new candlestick")
    minutes_processed[tick_dt] = True
    #print(minutes_processed)
    # Initializing first candlestick by adding first candlestick to dictionary to keep track of whether the candlestick is unique and adding cancdlestick to candlestick list []
    
    if len(minute_candlesticks) > 0:
      minute_candlesticks[-1]['close'] = previous_tick['price']
    
    minute_candlesticks.append({
      "minute": tick_dt,
      "open": current_tick['price'],
      "high": current_tick['price'],
      "low": current_tick['price']
    })
    print(minute_candlesticks)

  if len(minute_candlesticks) > 0:
    current_candlestick = minute_candlestick[-1]
    if current_tick['price'] > current_candlestick['high']:
      current_candlestick['high'] = current_tick['price']
    if current_tick['price'] < current_candlestick['low']:
      current_candlestick['low'] = current_tick['price']
  
    print("=== Candlesticks ===")
    for candlestick in minute_candlesticks:
      print(candlestick)    



def on_close(ws):
    print("closed connection")

#Coinbase Pro websocket feed
if __name__ == "__main__":
    print("ATLAS HF CB PRO Starting...")
    socket = 'wss://ws-feed.pro.coinbase.com'
    ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
    #Output will be timestamped data
    #stream_object = parse_stream()
    #ws.run_forever()
    while(True):
      parstream(ws.run_forever())

