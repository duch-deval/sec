import logging
import sys
from cal_finder.data_collection import run_data_collection
from cal_finder.text_extraction import run_text_extraction

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    run_data_collection()
    run_text_extraction()
