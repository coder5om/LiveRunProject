import time
from datetime import datetime, timedelta
import pymysql
from dhanhq import marketfeed, dhanhq
from config import client_id, access_token, optionPercentChangeIn30Sec
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime


from utility_methods import place_order  # Import utility functions

# Add your Dhan Client ID and Access Token

# 41487 PE
dhan = dhanhq(client_id, access_token)
security_id = "41306"
fund = 50000
quantity = 50

# Structure for subscribing is (exchange_segment, "security_id", subscription_type)
instruments = [(marketfeed.NSE_FNO, security_id, marketfeed.Ticker)]

# Get the current timestamp with milliseconds in a human-readable format
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# Set capital and thresholds
capital = 10000  # Initial capital
profit_threshold = 0.3  # 5% profit
loss_threshold = -0.15 # 10% loss
ltp_threshold = 0.02  # 1% change to trigger a trade
tradeAllowSignal = None

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

def wait_until_next_interval():
    now = datetime.now()
    next_interval = now + timedelta(seconds=30 - now.second % 30)
    next_interval = next_interval.replace(microsecond=0)
    wait_time = (next_interval - now).total_seconds()
    logging.info(f"Waiting {wait_time:.2f} seconds until the next interval.")
    time.sleep(wait_time)  # Sleep until the next interval

# Function to collect all prices over a 30-second interval
def collect_prices_for_interval(data):
    prices = []
    start_time = datetime.now()
    logging.info(f"Price Collection started at :  {start_time} .")
    end_time = start_time + timedelta(seconds=180)

    while datetime.now() < end_time:
        response = data.get_data()
        current_ltp = float(response['LTP'])
        prices.append(current_ltp)
        time.sleep(0.5)  # Adjust based on frequency of price changes

    logging.info(f"Collected {prices} prices over the interval.")
    logging.info(f"Length of Collected {len(prices)} prices over the interval.")
    v= datetime.now()
    logging.info(f"Price Collection ended at :  {v} .")
    return prices

def check_price_difference(prices):

    if len(prices) >= 2:  # Ensure there are at least two price points
        first_price = prices[0]
        last_price = prices[-1]
        price_change_percentage = (last_price - first_price) / first_price * 100

        logging.info(f"First Price: {first_price}, Last Price: {last_price}")
        logging.info(f"Price Change Percentage: {price_change_percentage}%")

        if price_change_percentage >= optionPercentChangeIn30Sec :
            tradeAllowSignal = True
            logging.info(f"Price change is {optionPercentChangeIn30Sec}  or more, allowing trade.")
        else:
            tradeAllowSignal = False
            logging.info(f"Price change is less than {optionPercentChangeIn30Sec} , not allowing trade.")

    else:
        logging.warning("Not enough price data to calculate the change.")
        tradeAllowSignal = False

    return tradeAllowSignal

def store_trade_results(ltp_change, profit_or_loss_percent, buy_price=None, sell_price=None):
    sql = "INSERT INTO trade_results2 (ltp_change, profit_loss_percent, buy_price, sell_price) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (ltp_change, profit_or_loss_percent, buy_price, sell_price))
    connection.commit()

try:
    data = marketfeed.DhanFeed(client_id, access_token, instruments)

    # Variable to track the initial LTP and trade prices
    initial_ltp = None
    previous_ltp = None  # Variable to store the previous LTP
    trade_executed = False
    buy_price = None  # Track the buy price
    sell_price = None  # Track the sell price

    while True:
        data.run_forever()
        response = data.get_data()
        print("Response received:", response)
        if trade_executed is False:
            wait_until_next_interval()
        if trade_executed is False:
            prices = collect_prices_for_interval(data)
            tradeAllowSignal = check_price_difference(prices)

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

            print(f"initial LTP: {initial_ltp}")

            # Calculate percentage change in LTP
            ltp_change = (current_ltp - initial_ltp) / initial_ltp
            print(f"Previous LTP: {previous_ltp}, LTP Change: {ltp_change:.4f}")

            initial_ltp = current_ltp

            # Update previous LTP before potential trade
            previous_ltp = current_ltp

            # Check for 2% LTP change to trigger a trade
            if not trade_executed and tradeAllowSignal:
                print(f"Placing Market order at LTP: {current_ltp}")
                logging.info(f"_______________________________________________LTP change :{ltp_change}____currentLTP :{current_ltp}")
                logging.info(f"Trade Started at {current_time} with market order at LTP: {current_ltp}")
                response = place_order('buy', dhan.BUY, security_id, quantity, current_ltp)
                print(f"buy Order response {response} ")
                logging.info("Buy order response: %s", response)
                # Extract the first part of the tuple (the dictionary)
                response_dict = response[0]
                # Now, get the orderId from the 'data' key
                order_id = response_dict.get('data', {}).get('orderId')
                print("orderId is :" +order_id)
                logging.info("Order ID: %s", order_id)
                time.sleep(1)
                order_status_response1 = dhan.get_order_by_id(order_id)
                print(f"buy Order response getOrderById API {order_status_response1} ")
                logging.info("Buy Order getOrderById API response: %s", order_status_response1)

                quantity = order_status_response1['data'].get('quantity')  # or 'filled_qty' if you want filled quantity
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
                store_trade_results(ltp_change, 0, buy_price=buy_price)
                initial_ltp = current_ltp  # Reset initial LTP for profit/loss tracking

            # If trade is executed, track profit/loss
            if trade_executed:
                profit_or_loss_percent_change = None
                pnl = current_ltp - buy_price
                running =pnl * quantity
                if buy_price is not None and buy_price>0:
                    profit_or_loss_percent_change = (current_ltp - buy_price) / buy_price
                    print(f"In trade and profit/loss % running is: {profit_or_loss_percent_change} running profit/loss :{running}")
                    logging.info("In trade, profit/loss %%: %s, Running PnL: %s", profit_or_loss_percent_change, running)
                else:
                    print("Problem in trade execution check manually.. ")
                    logging.error("Problem in trade execution, check manually.")
                # Check for profit booking condition (5% increase)
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
                    store_trade_results(ltp_change, profit_or_loss_percent_change * 100, buy_price=buy_price, sell_price=sell_price)
                    profit = (sell_price-buy_price)*quantity
                    logging.info(f"Trade Ended at {current_time} with market order at sell price : {sell_price} profit: {profit}")
                    time.sleep(300)
                    trade_executed = False  # Reset trade status after booking profit
                    initial_ltp = current_ltp  # Reset LTP for next trade

                # Check for stop-loss condition (10% decrease)
                elif profit_or_loss_percent_change <= loss_threshold:
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
                    quantity = order_status_response1['data'].get('quantity')  # or 'filled_qty' if you want filled quantity
                    sell_price = order_status_response1['data'].get('price')

                    logging.info("Actual sell price: %s", sell_price)
                    print(f"sell Order response {response} ")
                    store_trade_results(ltp_change, profit_or_loss_percent_change * 100, buy_price=buy_price, sell_price=sell_price)
                    loss = (sell_price-buy_price)*quantity
                    logging.info(f"Trade Ended at {current_time} with market order at sell price : {sell_price} loss is :{loss} ")
                    time.sleep(300)
                    trade_executed = False  # Reset trade status after booking loss
                    initial_ltp = current_ltp  # Reset LTP for next trade

        time.sleep(1)  # Wait for 0.2 seconds before checking LTP again

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close MySQL connection
    cursor.close()
    connection.close()

    # Close WebSocket connection
    data.disconnect()
