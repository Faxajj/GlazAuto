"""
Драйверы банков: UniversalCoins, Personal Pay.
Каждый принимает credentials (dict) и выполняет login, get_balance, withdraw.
"""
from .universalcoins import get_balance as uc_balance, create_withdraw as uc_withdraw
from .personalpay import get_balance as pp_balance, get_accounts as pp_accounts, create_withdraw as pp_withdraw, beneficiary_discovery as pp_discover
from .astropay import get_balance as ap_balance, get_activities as ap_activities, create_withdraw as ap_withdraw

BANK_TYPES = {
    "universalcoins": {"name": "UniversalCoins", "slug": "universalcoins"},
    "personalpay":    {"name": "Personal Pay",   "slug": "personalpay"},
    "astropay":       {"name": "AstroPay",        "slug": "astropay"},
}


def get_balance(bank_type: str, credentials: dict) -> dict:
    if bank_type == "universalcoins":
        return uc_balance(credentials)
    if bank_type == "personalpay":
        return pp_balance(credentials)
    if bank_type == "astropay":
        return ap_balance(credentials)
    raise ValueError(f"Unknown bank_type: {bank_type}")


def create_withdraw(bank_type: str, credentials: dict, **kwargs) -> dict:
    if bank_type == "universalcoins":
        return uc_withdraw(credentials, **kwargs)
    if bank_type == "personalpay":
        # PP: destination, amount, comments
        return pp_withdraw(
            credentials,
            destination=kwargs.get("destination") or kwargs.get("cvu_recipient", ""),
            amount=kwargs.get("amount", 0),
            comments=kwargs.get("comments", "Varios (VAR)"),
        )
    if bank_type == "astropay":
        return ap_withdraw(
            credentials,
            destination=kwargs.get("destination") or kwargs.get("cvu_recipient", ""),
            amount=kwargs.get("amount", 0),
            comments=kwargs.get("comments", "Varios"),
        )
    raise ValueError(f"Unknown bank_type: {bank_type}")


def get_accounts_display(bank_type: str, credentials: dict) -> dict:
    """Для Personal Pay — сырой ответ financial-accounts; для UC не нужен."""
    if bank_type == "personalpay":
        return pp_accounts(credentials)
    return {}


def discover_beneficiary(bank_type: str, credentials: dict, destination: str) -> dict:
    if bank_type == "personalpay":
        return pp_discover(credentials, destination)
    return {}
