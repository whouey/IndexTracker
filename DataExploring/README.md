# Data Exploring

The main subject for now is `TAIEX`, which is compiled by `TWSE`, please check [`this website`](https://www.taiwanindex.com.tw/index/index/t00).

The index measures the performance of whole stock market of taiwan.

And according to the website above, the derivative includes ETF, futures and options, which also are the subject of this project.

But first of all, the trading days should be specified.

## Trading days

Every saturday and sunday in Taiwan is usually off, as well as the holidays announced.  

However, when the holiday is happen to tuesday or thursday, the monday before or the friday after the day will be off as well to get a longer vacation, and the workday would be made up in the other saturday by announcement.

The list of holidays can be checked [`here`](https://www.twse.com.tw/en/holidaySchedule/holidaySchedule) .

Check [`this notebook`](./TWSE/TradingDays.ipynb) for detail.

## TAIEX

A stock index is a value to evaluate a market or a subset of a market.

`TAIEX` is compiled to express the performance of the whole stock market of Taiwan.

The information about `TAIEX` is mostly exposed by TWSE official website, and provided in several formats.

The index value is updated every 5 seconds, and can be traced back for years.

Information exposed [`here`](https://www.twse.com.tw/en/page/trading/exchange/MI_5MINS_INDEX.html) . Moreover, CSV format is also available.

Check [`this notebook`](./TWSE/TAIEX.ipynb) for detail.

The data formatted in a CSV file and 3240 records per day.

## ETF 

An exchange-traded fund (ETF) in short is a fund that holds a basket of it's target, and sold the shares on the stock market.

The `006204.tw` is a index-based ETF, hold securities by ratio, to mimic the trend of the value of `TAIEX`.

The detailed stock data is not provided by official websites.  
Fortunately, `FinMind`, a third party project collects all the historical data of Taiwan stock market, Come to the rescue.  

Check [`this notebook`](./FinMind/006204.ipynb) for detail.

The data is obtained through Web API and comes in Json format, but the record is usually less than 100 per day.

## Futures

The definition of Futures won't be covered.

The stock index futures based on `TAIEX` in Taiwan has 2 contracts:
+ [`TX`](https://www.taifex.com.tw/enl/eng2/tX) 
+ [`MTX`](https://www.taifex.com.tw/enl/eng2/mTX) 

Data of both contracts are provided by official website of `TAIFEX`, but only the last 30 trading days.
As a result, keeping a backup is imperative.

Check [`this notebook`](./TAIFEX/Futures.ipynb) for detail.

The data is served and archived as zipped CSV files, and the total amount of records are roughly 500000 per day.

## Options

The definition of Options won't be covered as well.

The only Options contract based on `TAIEX` is [`TXO`](https://www.taifex.com.tw/enl/eng2/tXO) .

The data is also provided by official website of `TAIFEX`, and only last 30 trading days as well.

Check [`this notebook`](./TAIFEX/Options.ipynb) for detail.

Similar to Futures, the data is served and archived as zipped CSV files, and the total amount of records are roughly 500000 per day.

## Summery

The regular trading session in Taiwan is 9:00~13:30 in local time, but the Futures and Options have after-hours trading session.

Overall, data will be released by 18:00 local time every trading day.

Raw data produced everyday is normally over a million, and can be aggregated into under 100 thousands.