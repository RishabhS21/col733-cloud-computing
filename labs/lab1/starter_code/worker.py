import logging
import sys
import time
from typing import Any
from typing import Dict
from base import Worker
from config import config
from mrds import MyRedis
import csv
import pandas as pd

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']

    # Write the code for the worker thread here.
    while True:
      # Read
      task = rds.rds.xreadgroup(Worker.GROUP, self.name, {config["IN"]: '>'}, count=1)
            
      if not task:
          break
      
      task_id, task_data = task[0][1][0]
      fname = task_data.get(config["FNAME"].encode())

      word_count = self._count_words(fname.decode()) 

      # Add word count to the Redis sorted set
      with rds.rds.pipeline() as pipe:
          for word, count in word_count.items():
              pipe.zincrby(config["COUNT"], count, word)
          # pipe.execute()
          pipe.xack(config["IN"], Worker.GROUP, task_id).execute()[-1]
      # Acknowledging the task completion
      # rds.rds.xack(config["IN"], Worker.GROUP, task_id)
      logging.info(f"{self.name} finished processing file: {fname}")
      if self.crash:
        # DO NOT MODIFY THIS!!!
        logging.critical(f"CRASHING!")
        sys.exit()

      if self.slow:
        # DO NOT MODIFY THIS!!!
        logging.critical(f"Sleeping!")
        time.sleep(1)

    logging.info("Exiting")

  def _count_words(self, file_name: str) -> Dict[str, int]:
    """Helper function to count words only in the 'text' column of a CSV file."""
    word_count = {}
    try:
        df = pd.read_csv(file_name, lineterminator='\n')
        df["text"] = df["text"].astype(str)
        for text in df.loc[:,"text"]:
          if text == '\n':
            continue

          for word in text.split(" "):
            if word not in word_count:
                word_count[word] = 0
            word_count[word] = word_count[word] + 1
           
    except Exception as e:
        # Unable to find the requested file
        logging.error(f"Error reading {file_name}: {e}")
    return word_count