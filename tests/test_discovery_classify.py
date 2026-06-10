"""Tests de classify_url (discovery_v2) — calidad de docs INT (2026-06-10).

Prioridad Rafa: buenos docs = Annual Reports (cuentas anuales, incl. paraguas) +
cartas/entrevistas del gestor; rechazar ruido corporativo/ESG.
"""
import pytest

from agents.discovery_v2 import classify_url


def _dt(name):
    return classify_url("https://x.com/" + name, name).get("doc_type")


@pytest.mark.parametrize("name", [
    "docu-robeco-integrated-annual-report-2025.pdf",   # corporativo, NO el AR del fondo
    "Annual-Impact-Report-2024.pdf",
    "docu-202605-robeco-country-esg-report.pdf",
    "responsible-investment-policy.pdf",
    "robeco-monthly-market-monitor.pdf",
    "2021-global-retirement-index-full-report.pdf",
    "stewardship-report-2024.pdf",
])
def test_corporate_esg_rejected(name):
    assert _dt(name) == "skip", f"{name} debería rechazarse"


@pytest.mark.parametrize("name", [
    "Sifter-Fund-Annual-Report-300820.pdf",
    "Trojan-Funds-Ireland-Annual-Report-2024.pdf",
    "Annual_Financial_Report_2024_en.pdf",            # cuentas del paraguas = AR
    "Incometric-Fund-Annual-Accounts-2024.pdf",
    "comptes-annuels-2024.pdf",
])
def test_annual_report_recognised(name):
    assert _dt(name) == "annual_report", f"{name} debería ser annual_report"


@pytest.mark.parametrize("name", [
    "DNCA_semi_annual_report_2024.pdf",               # NO debe caer en annual_report
    "Halbjahresbericht_2024.pdf",
    "rapport-semestriel-2024.pdf",
])
def test_semi_annual_not_misclassified_as_annual(name):
    assert _dt(name) == "semi_annual_report", f"{name} debería ser semi_annual_report"


@pytest.mark.parametrize("name", [
    "SifterFund-Quarterly-report-Q1-2020.pdf",
    "Equam-informe-trimestral-2024.pdf",
    "entrevista-gestor-citywire.pdf",
    "fund-manager-interview-2025.pdf",
])
def test_letters_and_interviews_recognised(name):
    assert _dt(name) == "quarterly_letter", f"{name} debería ser quarterly_letter"


def test_real_factsheet_still_factsheet():
    assert _dt("sifter-fund-factsheet-december-2025.pdf") == "factsheet"
