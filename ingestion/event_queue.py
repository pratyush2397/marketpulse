import heapq
import logging
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

logger= logging.getLogger(__name__)

@dataclass(order=True)
class StockEvent:
    priority: float
    ticker: str = field(compare=False)
    data: pd.DataFrame = field(compare=False)

    def __post_init__(self):
        self.priority = -abs(self.priority)  # Use negative for max-heap behavior

class EventQueue:

    def __init__(self):
        self._heap = []
        self._count=0
        logger.info("Initialized EventQueue")


    def push(self,ticker: str,df:pd.DataFrame, volume: float):
        event = StockEvent(priority=-volume, ticker=ticker, data=df)
        heapq.heappush(self._heap,(event.priority,self._count,event))
        self._count+=1
        logger.info(f"Pushed event for {ticker} with volume {volume} to the queue")

    def pop(self) -> Optional[StockEvent]:

        if self.is_empty():
            logger.warning("Attempted to pop from an empty queue")
            return None
        _,_, event = heapq.heappop(self._heap)
        logger.info(f"Popped event for {event.ticker} with priority {-event.priority} from the queue")
        return event
    
    def peek(self) -> Optional[StockEvent]:
        """See next event without removing it."""
        if self.is_empty():
            return None
        _, _, event = self._heap[0]
        return event

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def size(self) -> int:
        return len(self._heap)

    def __str__(self):
        return f"EventQueue | size: {self.size()}"


if __name__ == "__main__":
    queue = EventQueue()

    # Push stocks with different volumes
    import pandas as pd
    dummy_df = pd.DataFrame()  # placeholder data

    queue.push("SOME_STOCK", dummy_df, volume=10_000)
    queue.push("INFY.NS",    dummy_df, volume=1_000_000)
    queue.push("RELIANCE.NS",dummy_df, volume=5_000_000)

    print(f"Queue size: {queue.size()}")
    print(f"Next up: {queue.peek().ticker}")

    # Pop in priority order — should be RELIANCE first
    while not queue.is_empty():
        event = queue.pop()
        print(f"Processing: {event.ticker}")