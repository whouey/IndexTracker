import io, zipfile, datetime
from typing import Generator, Dict, List, Tuple

def _get_weekly_settlement_name(date:datetime.date):
    week = 1 + (date.day - 1) // 7
    if week == 3:
        return f'{date.year:4}{date.month:02}'
    else:
        return f'{date.year:4}{date.month:02}W{week:1}'
        
def _get_monthly_settlement_date(date:datetime.date) -> datetime.date:
    return date.replace(day = 15 + (2 - date.weekday() + date.day - 1) % 7)

def _get_next_weekly_settlement_date(date:datetime.date) -> datetime.date:
    return date + datetime.timedelta(days= 1 + (1 - date.weekday()) % 7)

def _get_expiration_code_map(date:datetime.date) -> Generator[Tuple[str, str], None, None]:
    
    if date.weekday() == 2:
        yield ('W', _get_weekly_settlement_name(date))
    
    next_settlement = _get_next_weekly_settlement_date(date)
    yield ('W', _get_weekly_settlement_name(next_settlement))

    year = date.year
    month = date.month
    settlement_month = _get_monthly_settlement_date(date)
    if date > settlement_month:
        month += 1

    expiration_codes = ['M', 'M+1', 'M+2', 'Q+1', 'Q+2', 'Q+3']

    for i in range(3):
        m = month - 1 + i # map to 0~11 for calculating
        yield (expiration_codes[i], f'{(year + (m // 12)):4}{((m % 12) + 1):02}')
    for i in range(3):
        m = month - 1 + (i + 1) * 3 + ( - month % 3)
        yield (expiration_codes[i + 3], f'{(year + (m // 12)):4}{((m % 12) + 1):02}')

        
def extract_zipped_content(content:str) -> Generator[str, None, None]:
    f = io.BytesIO(content)
    with zipfile.ZipFile(f) as zf:
        for name in zf.namelist():
            yield zf.read(name)

def get_expiration_code_map(date:datetime.date) -> Dict[str, List[str]]:
    maps = list(_get_expiration_code_map(date))
    unique = set([x[1] for x in maps])
    return {x : [y[0] for y in maps if y[1] == x] for x in unique}

def roundup_to_minutes(dt:datetime.datetime, minutes:int = 1) -> datetime.datetime:
    return (dt + datetime.timedelta(seconds=-dt.timestamp() % (minutes*60))).astimezone(datetime.timezone.utc)

def get_paramed_url(url:str, params:Dict[str, str]) -> str:
    return url + '?{}'.format('&'.join([f'{k}={v}' for k, v in params.items()]))