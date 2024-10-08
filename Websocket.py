import time
from datetime import datetime, timedelta
import pymysql
from dhanhq import marketfeed, dhanhq
from config import client_id, access_token, security_id, quantity, profit_threshold,loss_threshold,ltp_threshold
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import sys


from utility_methods import place_order  # Import utility functions

# Add your Dhan Client ID and Access Token
dhan = dhanhq(client_id, access_token)

# Structure for subscribing is (exchange_segment, "security_id", subscription_type)
instruments = [(marketfeed.NSE_FNO, security_id, marketfeed.Ticker)]

# Get the current timestamp with milliseconds in a human-readable format
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# Configure logging
log_file = 'WebSocket.log'
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024,  # Use RotatingFileHandler directly
                                   backupCount=2)  # 5 MB per log file, keep last 2 backups
file_handler.setFormatter(log_formatter)
logging.basicConfig(level=logging.INFO, handlers=[file_handler])

# Initialize MySQL connection
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             database='omkartrader')

cursor = connection.cursor()

def wait_for_next_interval(interval_minutes=1):
    now = datetime.now()
    # Calculate the number of seconds remaining until the next interval
    next_interval = (now.minute // interval_minutes + 1) * interval_minutes
    next_start_time = now.replace(minute=next_interval % 60, second=0, microsecond=0)

    # Handle the hour overflow when the minute goes past 59
    if next_interval >= 60 and now.hour == 23:
        next_start_time = next_start_time.replace(hour=0)

    wait_seconds = (next_start_time - now).total_seconds()
    print(f"Waiting for {wait_seconds} seconds to start at the next 3-minute interval ({next_start_time.time()}).")
    logging.info(f"Waiting for {wait_seconds} seconds to start at the next 3-minute interval ({next_start_time.time()}).")
    time.sleep(wait_seconds)

# Function to collect all prices over a 30-second interval
def collect_prices_for_interval(data):
    prices = []
    start_time = datetime.now()
    logging.info(f"Price Collection started at :  {start_time} .")
    print(f"Price Collection started at :  {start_time} .")
    end_time = start_time + timedelta(seconds=60)

    while datetime.now() < end_time:
        response1 = data.get_data()
        current_ltp1 = float(response1['LTP'])
        logging.info(f"Collected - {current_ltp1} ")
        prices.append(current_ltp1)
        time.sleep(0.5)  # Adjust based on frequency of price changes

    logging.info(f"Collected {prices} prices over the interval.")
    logging.info(f"Length of Collected {len(prices)} prices over the interval.")
    v= datetime.now()
    logging.info(f"Price Collection ended at :  {v} .")
    return prices

def check_price_differenceandaandletype(prices):
    tradeAllowSignal1 = False  # Initialize with a default value
    candleType1 = None  # Initialize with None as default
    per80ofCandlePrice1 = 0.0  # Initialize variables to 0.0 or another default value
    per70ofCandlePrice1 = 0.0
    per30ofCandlePrice1 = 0.0
    first_price1 = None
    last_price1 = None

    if len(prices) >= 2:  # Ensure there are at least two price points
        first_price1 = prices[0]
        last_price1 = prices[-1]
        pricediffrence = last_price1 - first_price1

        price_change_percentage = (last_price1 - first_price1) / first_price1 * 100

        if pricediffrence > 0:
            per80ofCandlePrice1 = (pricediffrence * 0.8) + first_price1
            per70ofCandlePrice1 = (pricediffrence * 0.7) + first_price1
            per30ofCandlePrice1 = (pricediffrence * 0.3) + first_price1

        logging.info(f"First Price: {first_price1}, Last Price: {last_price1}")
        logging.info(f"Price Change Percentage: {price_change_percentage}%")

        if price_change_percentage < 0:
            tradeAllowSignal1 = False
            logging.info(f"Price change is {price_change_percentage} less than 0 percent, not allowing trade.")
        elif 0 <= price_change_percentage <= 5:
            tradeAllowSignal1 = False
            logging.info(f"Price change is {price_change_percentage} between 0 and 5, not allowing trade.")
        elif 5 <= price_change_percentage < 15:
            tradeAllowSignal1 = True
            candleType1 = "NORMALBULLISH"
            logging.info(
                f"Price change is {price_change_percentage} between 5 and 15 percent, allowing trade. Normal bullish 3 min candle.")
        elif 15 <= price_change_percentage < 25:
            tradeAllowSignal1 = True
            candleType1 = "BIGBAR"
            logging.info(
                f"Price change is {price_change_percentage} between 15 and 25 percent, allowing trade. BIG BAR 3 min candle.")
        elif 25 <= price_change_percentage <= 40:
            tradeAllowSignal1 = True
            candleType1 = "SUPERBIGBAR"
            logging.info(
                f"Price change is {price_change_percentage} between 25 and 40 percent, allowing trade. SUPER BIG BAR 3 min candle.")
        elif price_change_percentage > 40:
            tradeAllowSignal1 = False
            logging.info(f"Price change is {price_change_percentage} greater than 40, not favorable for trade.")
    else:
        logging.warning("Not enough price data to calculate the change.")
        tradeAllowSignal1 = False

    print(tradeAllowSignal1, candleType1, first_price1, last_price1, per30ofCandlePrice1, per70ofCandlePrice1, per80ofCandlePrice1)
    logging.info(f"tradeAllowSignal1 = {tradeAllowSignal1},candleType1 ={candleType1}, first_price1 ={first_price1},"
                 f"last_price1 ={last_price1}, per30ofCandlePrice1 ={per30ofCandlePrice1}, per70ofCandlePrice1 ={per70ofCandlePrice1},"
                 f"per80ofCandlePrice1 = {per80ofCandlePrice1} ")
    return tradeAllowSignal1, candleType1, first_price1, last_price1, per30ofCandlePrice1, per70ofCandlePrice1, per80ofCandlePrice1

def store_trade_results(ltp_change, profit_or_loss_percent, buy_price=None, sell_price=None):
    sql = "INSERT INTO trade_results2 (ltp_change, profit_loss_percent, buy_price, sell_price) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (ltp_change, profit_or_loss_percent, buy_price, sell_price))
    connection.commit()

try:
    wait_for_next_interval()
    data = marketfeed.DhanFeed(client_id, access_token, instruments)

    # Variable to track the initial LTP and trade prices
    initial_ltp = None
    previous_ltp = None  # Variable to store the previous LTP
    trade_executed = False
    buy_price = None  # Track the buy price
    sell_price = None  # Track the sell price
    tradeAllowSignal1 = None
    candleType1 = None
    first_price1 = None
    last_price1 = None
    per30ofCandlePrice1 =0.0
    per70ofCandlePrice1 =0.0
    per80ofCandlePrice1 =0.0

    while True:
        data.run_forever()

        if trade_executed is False:
            prices = collect_prices_for_interval(data)
            tradeAllowSignal1, candleType1, first_price1, last_price1, per30ofCandlePrice1, per70ofCandlePrice1, per80ofCandlePrice1 = check_price_differenceandaandletype(prices)

        response = data.get_data()
        # Check if the response contains LTP data
        # Check if the response contains LTP data
        if 'LTP' in response:
            # Convert LTP to float, as it's coming as a string
            response = data.get_data()
            current_ltp = float(response['LTP'])
            print(f"Current LTP: {current_ltp}")

            # Set initial LTP when the script starts
            if initial_ltp is None:
                initial_ltp = current_ltp
                previous_ltp = current_ltp  # Set previous LTP to the initial LTP
                print(f"Initial LTP set to: {initial_ltp}")

            # Calculate percentage change in LTP
            ltp_change = (current_ltp - initial_ltp) / initial_ltp
            print(f"Previous LTP: {previous_ltp}, LTP Change: {ltp_change:.4f}")
            initial_ltp = current_ltp
            # Update previous LTP before potential trade
            previous_ltp = current_ltp

            # Check for 2% LTP change to trigger a trade
            if not trade_executed and tradeAllowSignal1:
                if candleType1 == "NORMALBULLISH":
                    logging.info(
                        f"_____________________________________________ LTP change :{ltp_change}____currentLTP :{current_ltp}")
                    logging.info(f"Trade Started at {current_time} with market order at LTP: {current_ltp}")
                    response = place_order('buy', dhan.BUY, security_id, quantity, current_ltp)
                    print(f"buy Order response {response} ")
                    logging.info("Buy order response: %s", response)
                    # Extract the first part of the tuple (the dictionary)
                    response_dict = response[0]
                    # Now, get the orderId from the 'data' key
                    order_id = response_dict.get('data', {}).get('orderId')
                    print("orderId is :" + order_id)
                    logging.info("Order ID: %s", order_id)
                    time.sleep(1)
                    order_status_response1 = dhan.get_order_by_id(order_id)
                    print(f"buy Order response getOrderById API {order_status_response1} ")
                    logging.info("Buy Order getOrderById API response: %s", order_status_response1)

                    quantity = order_status_response1['data'].get(
                        'quantity')  # or 'filled_qty' if you want filled quantity
                    price = order_status_response1['data'].get('price')
                    logging.info("Quantity: %s, Price: %s", quantity, price)
                    logging.info("Buy price set at: %s", price)
                    if price is not None:
                        buy_price = price  # Record the buy price
                        trade_executed = True
                    else:
                        buy_price = current_ltp

                    print(f"Buy price set at: {buy_price}")
                    # Set flag to indicate trade is executed
                    # Store dummy trade result with buy price
                    # store_trade_results(ltp_change, 0, buy_price=buy_price)
                    initial_ltp = current_ltp  # Reset initial LTP for profit/loss tracking

                elif candleType1 == "BIGBAR":
                    logging.info(
                        f"_____________________________________________ LTP change :{ltp_change}____currentLTP :{current_ltp}")
                    logging.info(f"Trade Started at {current_time} with market order at LTP: {current_ltp}")
                    if current_ltp < per80ofCandlePrice1:
                        response = place_order('buy', dhan.BUY, security_id, quantity, current_ltp)
                        print(f"buy Order response {response} ")
                        logging.info("Buy order response: %s", response)
                        # Extract the first part of the tuple (the dictionary)
                        response_dict = response[0]
                        # Now, get the orderId from the 'data' key
                        order_id = response_dict.get('data', {}).get('orderId')
                        print("orderId is :" + order_id)
                        logging.info("Order ID: %s", order_id)
                        time.sleep(1)
                        order_status_response1 = dhan.get_order_by_id(order_id)
                        print(f"buy Order response getOrderById API {order_status_response1} ")
                        logging.info("Buy Order getOrderById API response: %s", order_status_response1)

                        quantity = order_status_response1['data'].get(
                            'quantity')  # or 'filled_qty' if you want filled quantity
                        price = order_status_response1['data'].get('price')
                        logging.info("Quantity: %s, Price: %s", quantity, price)
                        logging.info("Buy price set at: %s", price)
                        if price is not None:
                            buy_price = price  # Record the buy price
                            trade_executed = True
                        else:
                            buy_price = current_ltp

                        print(f"Buy price set at: {buy_price}")
                        # Set flag to indicate trade is executed
                        # Store dummy trade result with buy price
                        # store_trade_results(ltp_change, 0, buy_price=buy_price)
                        initial_ltp = current_ltp  # Reset initial LTP for profit/loss tracking

                elif candleType1 == "SUPERBIGBAR":
                    logging.info(
                        f"_____________________________________________ LTP change :{ltp_change}____currentLTP :{current_ltp}")
                    logging.info(f"Trade Started at {current_time} with market order at LTP: {current_ltp}")
                    if current_ltp < per70ofCandlePrice1:
                        response = place_order('buy', dhan.BUY, security_id, quantity, current_ltp)
                        print(f"buy Order response {response} ")
                        logging.info("Buy order response: %s", response)
                        # Extract the first part of the tuple (the dictionary)
                        response_dict = response[0]
                        # Now, get the orderId from the 'data' key
                        order_id = response_dict.get('data', {}).get('orderId')
                        print("orderId is :" + order_id)
                        logging.info("Order ID: %s", order_id)
                        time.sleep(1)
                        order_status_response1 = dhan.get_order_by_id(order_id)
                        print(f"buy Order response getOrderById API {order_status_response1} ")
                        logging.info("Buy Order getOrderById API response: %s", order_status_response1)

                        quantity = order_status_response1['data'].get(
                            'quantity')  # or 'filled_qty' if you want filled quantity
                        price = order_status_response1['data'].get('price')
                        logging.info("Quantity: %s, Price: %s", quantity, price)
                        logging.info("Buy price set at: %s", price)
                        if price is not None:
                            buy_price = price  # Record the buy price
                            trade_executed = True
                        else:
                            buy_price = current_ltp

                        print(f"Buy price set at: {buy_price}")
                        # Set flag to indicate trade is executed
                        # Store dummy trade result with buy price
                        # store_trade_results(ltp_change, 0, buy_price=buy_price)
                        initial_ltp = current_ltp  # Reset initial LTP for profit/loss tracking

            # If trade is executed, track profit/loss
            if trade_executed:
                isAveraged = False
                profit_or_loss_percent_change = None
                pnl = current_ltp - buy_price
                running = pnl * quantity
                if buy_price is not None and buy_price > 0:
                    profit_or_loss_percent_change = (current_ltp - buy_price) / buy_price
                    print(
                        f"In trade and profit/loss % running is: {profit_or_loss_percent_change} running profit/loss :{running}")
                    logging.info("In trade, profit/loss %%: %s, Running PnL: %s", profit_or_loss_percent_change,
                                 running)
                else:
                    print("Problem in trade execution check manually.. ")
                    logging.error("Problem in trade execution, check manually.")

                if profit_or_loss_percent_change >= profit_threshold:
                    print(f"Booking profit at LTP: {current_ltp}, Profit: {pnl}")
                    print(f"Sell price set at: {sell_price}")
                    response = place_order('sell', dhan.SELL, security_id, quantity, current_ltp)
                    logging.info("Sell order response: %s", response)
                    # Extract the first part of the tuple (the dictionary)
                    response_dict = response[0]
                    # Now, get the orderId from the 'data' key
                    order_id = response_dict.get('data', {}).get('orderId')
                    print("orderId is :" + order_id)
                    logging.info("Sell Order ID: %s", order_id)
                    time.sleep(1)
                    order_status_response1 = dhan.get_order_by_id(order_id)
                    print(f"Sell Order response getOrderById API : {order_status_response1} ")

                    quantity = order_status_response1['data'].get(
                        'quantity')  # or 'filled_qty' if you want filled quantity
                    sell_price = order_status_response1['data'].get('price')

                    logging.info("Actual sell price: %s", sell_price)

                    print(f"sell Order response {response} ")
                    # store_trade_results(ltp_change, profit_or_loss_percent_change * 100, buy_price=buy_price, sell_price=sell_price)
                    profit = (sell_price - buy_price) * quantity
                    logging.info(
                        f"Trade Ended at {current_time} with market order at sell price : {sell_price} profit: {profit}")
                    print("Terminating program gracefully.")
                    sys.exit(0)  # Exit successfully


                elif profit_or_loss_percent_change <= 0:
                    # put average buy trade at first_price that is bottom of candle
                    if isAveraged is False and first_price1 > current_ltp:
                        logging.info(
                            f"_____________________________________________ LTP change :{ltp_change}____currentLTP :{current_ltp}")
                        logging.info(
                            f"Averaging Trade Started at {current_time} with market order at LTP: {current_ltp}")

                        response = place_order('buy', dhan.BUY, security_id, quantity, current_ltp)
                        print(f"Average buy Order response {response} ")
                        logging.info("Average Buy order response: %s", response)
                        # Extract the first part of the tuple (the dictionary)
                        response_dict = response[0]
                        # Now, get the orderId from the 'data' key
                        order_id = response_dict.get('data', {}).get('orderId')
                        print("orderId is :" + order_id)
                        logging.info("Order ID: %s", order_id)
                        time.sleep(1)
                        order_status_response1 = dhan.get_order_by_id(order_id)
                        print(f"Average buy Order response getOrderById API {order_status_response1} ")
                        logging.info("Average Buy Order getOrderById API response: %s", order_status_response1)

                        quantity = order_status_response1['data'].get(
                            'quantity')  # or 'filled_qty' if you want filled quantity
                        price = order_status_response1['data'].get('price')
                        logging.info("Quantity: %s, Price: %s", quantity, price)
                        logging.info("Average Buy price set at: %s", price)
                        # ____________________________________________________#
                        if price is not None:
                            buy_price = (buy_price + price) / 2  # Buy price average in case bottom of candle reached
                        # ____________________________________________________#

                        # store_trade_results(ltp_change, 0, buy_price=buy_price)
                        initial_ltp = current_ltp  # Reset initial LTP for profit/loss tracking
                        isAveraged = True  # set flag as true to not average again

                    elif isAveraged is True and profit_or_loss_percent_change >= loss_threshold:
                        print(f"Booking loss at LTP: {current_ltp}, Loss: {profit_or_loss_percent_change * 100}%")
                        sell_price = current_ltp  # Record the sell price
                        print(f"Sell price set at: {sell_price}")
                        response = place_order('sell', dhan.SELL, security_id, quantity, current_ltp)
                        logging.info("Sell order response: %s", response)
                        # Extract the first part of the tuple (the dictionary)
                        response_dict = response[0]
                        # Now, get the orderId from the 'data' key
                        order_id = response_dict.get('data', {}).get('orderId')
                        print("orderId is :" + order_id)
                        logging.info("Sell Order ID: %s", order_id)
                        time.sleep(1)
                        order_status_response1 = dhan.get_order_by_id(order_id)
                        print(f"Sell Order response getOrderById API : {order_status_response1} ")
                        quantity = order_status_response1['data'].get(
                            'quantity')  # or 'filled_qty' if you want filled quantity
                        sell_price = order_status_response1['data'].get('price')

                        logging.info("Actual sell price: %s", sell_price)
                        print(f"sell Order response {response} ")
                        # store_trade_results(ltp_change, profit_or_loss_percent_change * 100, buy_price=buy_price, sell_price=sell_price)
                        loss = (sell_price - buy_price) * quantity
                        logging.info(
                            f"Trade Ended at {current_time} with market order at sell price : {sell_price} loss is :{loss} ")
                        print("Terminating program gracefully.")
                        sys.exit(0)  # Exit successfully

        time.sleep(1)  # Wait for 0.2 seconds before checking LTP again

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close MySQL connection
    cursor.close()
    connection.close()

    # Close WebSocket connection
    data.disconnect()
