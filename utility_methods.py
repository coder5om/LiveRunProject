from dhanhq import dhanhq
from config import client_id, access_token
import time
import logging
from logging.handlers import RotatingFileHandler

dhan = dhanhq(client_id, access_token)

# Configure logging
log_file = 'utility_methods.log'
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024,  # Use RotatingFileHandler directly
                                   backupCount=2)  # 5 MB per log file, keep last 2 backups
file_handler.setFormatter(log_formatter)
logging.basicConfig(level=logging.INFO, handlers=[file_handler])

orderStatus = 'TRADED'

def place_order(order_type, transaction_type, security_id, quantity, current_ltp):
    """Place an order and return the response along with time taken to place it."""
    try:
        start_time = time.time()  # Record the start time

        # Place the order
        response = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=current_ltp# Price for buy limit order
        )
        end_time = time.time()  # Record the end time
        time_taken = end_time - start_time  # Calculate the difference in seconds
        logging.info(f"Order placed in {time_taken:.2f} seconds")  # Print the time taken

        return response, time_taken  # Return response and time taken to place the order

    except Exception as e:
        print(f"Error placing order: {e}")
        logging.info(f"Error placing order: {e}")
        return None, None
