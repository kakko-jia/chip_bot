from datetime import datetime
import pytz

print(f"系統當前時區：{datetime.now().astimezone().tzinfo}")
print(f"UTC 時間：{datetime.now(pytz.utc)}")