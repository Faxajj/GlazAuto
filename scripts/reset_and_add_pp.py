"""
Одноразовый скрипт: удалить все аккаунты и добавить один Personal Pay
с указанными device_id и idToken. Название: почта и CVU (если удалось получить).
"""
import sys
from pathlib import Path

# чтобы импортировать app
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import add_account, delete_account, list_accounts, update_account
from app.drivers.personalpay import get_balance

CREDENTIALS = {
    "device_id": "178DC6C7-C43C-4DCC-B3BC-DBFAAA0F8FD2",
    "auth_token": "eyJ0eXAiOiJKV1QiLCJraWQiOiJ3VTNpZklJYUxPVUFSZVJCL0ZHNmVNMVAxUU09IiwiYWxnIjoiUlMyNTYifQ.eyJhdF9oYXNoIjoiSVUyMkVFZFV1aDJMaG5XSHB5WlhDZyIsInN1YiI6IjVmNWY2YTAyLWVjZjQtNDY2Ni04OTQwLTI1ZDE3OTI3ZGE0ZiIsImF1ZGl0VHJhY2tpbmdJZCI6IjRmMTViNTZhLTJlZGMtNGI2Ni1iOGJhLTNmNGY0M2RjODQ2YS0xMTk4NDkxMDYiLCJzdWJuYW1lIjoiNWY1ZjZhMDItZWNmNC00NjY2LTg5NDAtMjVkMTc5MjdkYTRmIiwiaXNzIjoiaHR0cHM6Ly9pZHBzZXNpb24udGVsZWNvbS5jb20uYXI6NDQzL29wZW5hbS9vYXV0aDIvZmludGVjaC1hcHAiLCJ0b2tlbk5hbWUiOiJpZF90b2tlbiIsInNpZCI6IktEUlNIc1JKZVgrbHlDQXJOSXNURHB0L1lFem5CbE9YWENYYm5VNXdpQzA9IiwiYXVkIjoiU3JXeVVwVDdoZDJ3MkMwM0Q5VDZQMDhtejZRY3ZQWERxVE9ocEFvOUZyb1VoTHhMVXZTYU1XSDVUbWttQTI1OEZNTXBzb1lPdHJEIiwiY19oYXNoIjoieW5lQ3JGM0FOZ2trc1VmNEh3SERTdyIsImFjciI6Im1hZ2ljTGlua1ZhbGlkYXRpb24iLCJvcmcuZm9yZ2Vyb2NrLm9wZW5pZGNvbm5lY3Qub3BzIjoieGY5N0pfMEJjdXhRQ3dveVFldl94UUpZeVIwIiwic19oYXNoIjoiMVBtaUtBN1gxdkhmM21TLU1BN1JDdyIsImF6cCI6IlNyV3lVcFQ3aGQydzJDMDNEOVQ2UDA4bXo2UWN2UFhEcVRPaHBBbzlGcm9VaEx4TFV2U2FNV0g1VG1rbUEyNThGTU1wc29ZT3RyRCIsImF1dGhfdGltZSI6MTc3MjQ3MTA1MywibmFtZSI6IjVmNWY2YTAyLWVjZjQtNDY2Ni04OTQwLTI1ZDE3OTI3ZGE0ZiIsInJlYWxtIjoiL2ZpbnRlY2gtYXBwIiwicmVnaXN0cmF0aW9uIjoibWFnaWNsaW5rIiwiZXhwIjoxNzcyNTE0MjUzLCJ0b2tlblR5cGUiOiJKV1RUb2tlbiIsImlhdCI6MTc3MjQ3MTA1MywiZmFtaWx5X25hbWUiOiJ3aWthc2J2c3lAaW5ib3gubHYiLCJlbWFpbCI6Indpa2FzYnZzeUBpbmJveC5sdiJ9.FLL2NuWs7Z4mUb5I_z83TQzDOOzSV9ELnRi3S42JbyvDmyUcz1JmVtCuTVSAvrRqRnqNjjBVLx61R4iJdkmQo-2-9LRNnFzP2u-mviPOlADl67lVBcMdiVYvxttOHRPMqmYAK3RDnHCaEdvEJnX_x6RxLSwfYwGxl_U3PV76bfOP7pR9HYb6snrz8c9-2rZ83ntH68MZYl0hA4fueUrqAnHPz5quIZdhJvjlR_CP2nagaHNQZKx2KLV_IoTJyK-GEFiLiCNYMRz9LKn6kxuaXrtRiB0Bz-U4BOEJ953BJXJ_81BTcqBDah8F0Hgor3Ks5kEZ6alSEpfKEgkVMWYeVQ",
}
EMAIL = "wikashvsy@inbox.lv"


def main():
    accounts = list_accounts()
    for acc in accounts:
        delete_account(acc["id"])
        print(f"Удалён аккаунт: {acc['label']} ({acc['bank_type']})")

    new_id = add_account("personalpay", EMAIL, CREDENTIALS)
    print(f"Добавлен аккаунт id={new_id}, пока название: {EMAIL}")

    try:
        balance_info = get_balance(CREDENTIALS)
        cvu = (balance_info.get("cvu_number") or "").strip()
        if cvu:
            label = f"{cvu} — {EMAIL}"
            update_account(new_id, label=label)
            print(f"Получен CVU, название обновлено: {label}")
        else:
            print("CVU в ответе не найден, название оставлено: ", EMAIL)
    except Exception as e:
        print(f"Запрос баланса/CVU не выполнен (токен или сеть): {e}")
        print("Название оставлено: ", EMAIL)

    print("Готово. Открой дашборд и выбери аккаунт.")


if __name__ == "__main__":
    main()
