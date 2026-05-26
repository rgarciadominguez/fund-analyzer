"""Tests for tools/fund_name_utils.py (G1 helper)."""
import pytest

from tools.fund_name_utils import (
    extract_fund_short,
    is_class_tail,
)


class TestExtractFundShort:
    """G1 fix: ensure fund_short doesn't break to generic "Action F" for INT funds."""

    def test_sextant_quality_focus_action_f(self):
        """The original bug: 'Action F' is the only token left, useless for queries."""
        assert extract_fund_short("SEXTANT QUALITY FOCUS - Action F") == "SEXTANT QUALITY FOCUS"

    def test_sextant_quality_focus_action_a(self):
        assert extract_fund_short("SEXTANT QUALITY FOCUS - Action A") == "SEXTANT QUALITY FOCUS"

    def test_es_fund_with_comma_fi(self):
        """ES funds with ', FI' suffix don't have ' - ' so stay unchanged."""
        assert extract_fund_short("Magallanes European Equity, FI") == "Magallanes European Equity, FI"

    def test_es_fund_cobas(self):
        assert extract_fund_short("Cobas Selección, FI") == "Cobas Selección, FI"

    def test_class_i_pattern(self):
        assert extract_fund_short("Avantage Fund - Class I") == "Avantage Fund"

    def test_class_in_middle_not_at_end(self):
        """If 'Class I' is not at the end, no trim."""
        assert extract_fund_short("Magallanes - Iberian Equity - Class I") == "Magallanes - Iberian Equity"

    def test_dnca_invest_alpha_bonds_no_class_tail(self):
        """If last segment isn't a class pattern, keep full name (SICAV umbrella case)."""
        # "I EUR" doesn't match class tail strictly, keep full
        assert extract_fund_short("DNCA INVEST - Alpha Bonds I EUR") == "DNCA INVEST - Alpha Bonds I EUR"

    def test_robecosam_i_eur(self):
        """'I EUR' matches the [A-Z0-9]{1,3} EUR pattern at end → strip."""
        assert extract_fund_short("RobecoSAM Sustainable Healthy Living - I EUR") == "RobecoSAM Sustainable Healthy Living"

    def test_no_separator(self):
        """No ' - ' separator → return as-is."""
        assert extract_fund_short("Trojan Fund O Acc") == "Trojan Fund O Acc"

    def test_empty_string(self):
        assert extract_fund_short("") == ""

    def test_none_safe(self):
        assert extract_fund_short(None) == ""  # type: ignore

    def test_b_eur_acc_class(self):
        assert extract_fund_short("Some Fund - B EUR Acc") == "Some Fund"

    def test_z_single_letter_class(self):
        assert extract_fund_short("Pure Equity - Z") == "Pure Equity"


class TestIsClassTail:
    """is_class_tail() helper validation."""

    def test_action_f(self):
        assert is_class_tail("Action F") is True

    def test_class_i(self):
        assert is_class_tail("Class I") is True

    def test_klasse_a(self):
        assert is_class_tail("Klasse A") is True

    def test_b_eur(self):
        assert is_class_tail("B EUR") is True

    def test_b_eur_acc(self):
        assert is_class_tail("B EUR Acc") is True

    def test_single_letter_z(self):
        assert is_class_tail("Z") is True

    def test_acc(self):
        assert is_class_tail("Acc") is True

    def test_not_class_tail_fund_name(self):
        assert is_class_tail("Sextant Quality Focus") is False

    def test_not_class_tail_empty(self):
        assert is_class_tail("") is False
        assert is_class_tail(None) is False  # type: ignore

    def test_not_class_tail_long_word(self):
        assert is_class_tail("Investissement Responsable") is False
