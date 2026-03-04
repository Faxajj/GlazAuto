#!/bin/bash
# Запускать из папки glazauto.pro (где лежат main.py, requirements.txt и т.д.)
# Восстанавливает структуру app/, app/static, app/templates, app/drivers

set -e
mkdir -p app/static app/templates app/drivers

mv main.py database.py __init__.py app/ 2>/dev/null || true
mv style.css app/static/ 2>/dev/null || true
mv base.html index.html add_account.html edit_account.html receipt.html app/templates/ 2>/dev/null || true
mv personalpay.py universalcoins.py app/drivers/ 2>/dev/null || true

# app/drivers/__init__.py — если его нет, создаём
if [ ! -f app/drivers/__init__.py ]; then
  cat > app/drivers/__init__.py << 'PYEOF'
"""
Драйверы банков: UniversalCoins, Personal Pay.
"""
from .universalcoins import get_balance as uc_balance, create_withdraw as uc_withdraw
from .personalpay import get_balance as pp_balance, get_accounts as pp_accounts, create_withdraw as pp_withdraw, beneficiary_discovery as pp_discover

BANK_TYPES = {
    "universalcoins": {"name": "UniversalCoins", "slug": "universalcoins"},
    "personalpay": {"name": "Personal Pay", "slug": "personalpay"},
}

def get_balance(bank_type: str, credentials: dict) -> dict:
    if bank_type == "universalcoins":
        return uc_balance(credentials)
    if bank_type == "personalpay":
        return pp_balance(credentials)
    raise ValueError(f"Unknown bank_type: {bank_type}")

def create_withdraw(bank_type: str, credentials: dict, **kwargs) -> dict:
    if bank_type == "universalcoins":
        return uc_withdraw(credentials, **kwargs)
    if bank_type == "personalpay":
        return pp_withdraw(
            credentials,
            destination=kwargs.get("destination") or kwargs.get("cvu_recipient", ""),
            amount=kwargs.get("amount", 0),
            comments=kwargs.get("comments", "Varios (VAR)"),
        )
    raise ValueError(f"Unknown bank_type: {bank_type}")

def get_accounts_display(bank_type: str, credentials: dict) -> dict:
    if bank_type == "personalpay":
        return pp_accounts(credentials)
    return {}

def discover_beneficiary(bank_type: str, credentials: dict, destination: str) -> dict:
    if bank_type == "personalpay":
        return pp_discover(credentials, destination)
    return {}
PYEOF
fi

echo "Done. Structure: app/, app/static, app/templates, app/drivers"
