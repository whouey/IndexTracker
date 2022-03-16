## index
| Column Name | Description |
| --- | --- |
| id | unique identity as primary key |
| name | the name of the index |
| market | the market the index belongs to |  

## index_history
| Column Name | Description |
| --- | --- |
| index_id | the id of belonging index |
| scale | aggregation period, in minutes |
| datetime | when is the period started |
| price_open | first price of the period |
| price_high | highest price of the period |
| price_low | lowest price of the period |
| price_close | last price of the period |
| price_mean | average price of the period |
| price_std | standard deviation of the prices of the period |

## derivative
| Column Name | Description |
| --- | --- |
| id | unique identity as primary key |
| index_id | the id of belonging index |
| name | the name of the derivative financial instrument |
| derivative_type | the type of the derivative financial instrument, can be `ETF`, `Futures` or `Options` |

## derivative_detail
| Column Name | Description |
| --- | --- |
| id | unique identity as primary key |
| derivative_id | the id of belonging derivative |
| expire | the expiration of the contract, for futures and options. |
| expire_code | corresponding label of the expiration, one of `W`, `M`, `M+1`, `M+2`, `Q+1`, `Q+2` and `Q+3` |
| strike_price | the strike price of the contract, for options. |
| option_type | the type of option of the contract, cane be `P` or `C`, stands for `Put Option` or `Call Option`. |

## derivative_detail_history
| Column Name | Description |
| --- | --- |
| derivative_detail_id | the id of belonging contract |
| scale | aggregation period, in minutes |
| datetime | when is the period started |
| price_open | first price of the period |
| price_high | highest price of the period |
| price_low | lowest price of the period |
| price_close | last price of the period |
| price_mean | average price of the period |
| price_std | standard deviation of the prices of the period |
| volume | the total volume of the period |